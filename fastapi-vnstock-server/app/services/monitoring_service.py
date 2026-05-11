from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from psycopg import connect
from psycopg.rows import dict_row

from app.core.config import settings
from app.services.alert_dispatcher_service import dispatch_alert_to_channels
from app.services.alert_message_format import format_alert_plain_text
from app.services.experience_service import ensure_experience_table, get_experience_claude_runtime_metrics
from app.services.signal_engine_service import ensure_signals_table, get_signal_scoring_claude_runtime_metrics
from app.services.short_term_automation_service import ensure_short_term_automation_runs_table
from app.services.trading_core_service import get_kill_switch, get_monitoring_summary, get_positions, log_risk_event
from app.services.vnstock_api_service import VNStockApiService
from vnstock.connector.dnse import Trade

_vnstock_api_service = VNStockApiService()


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _normalize_dnse_records(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if hasattr(data, "to_dict"):
        try:
            converted = data.to_dict(orient="records")
        except TypeError:
            converted = data.to_dict()
        if isinstance(converted, list):
            return [row for row in converted if isinstance(row, dict)]
        if isinstance(converted, dict):
            return [converted]
        return []
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _to_finite_number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed:  # NaN
        return None
    if parsed in {float("inf"), float("-inf")}:
        return None
    return parsed


def _pick_first_finite_number(record: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        parsed = _to_finite_number(record.get(key))
        if parsed is not None:
            return parsed
    return None


def _fetch_dnse_balance_row_for_sub_account(
    sub_account: str,
    dnse_access_token: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    sub = str(sub_account or "").strip()
    if not sub:
        return None, "missing_sub_account"
    try:
        token = str(dnse_access_token or "").strip()
        trade = Trade()
        if token:
            trade.token = token
        else:
            username = str(settings.dnse_username or "").strip()
            password = str(settings.dnse_password or "").strip()
            if not username or not password:
                return None, "missing_dnse_credentials"
            login_token = trade.login(username, password)
            if not login_token:
                return None, "dnse_login_failed"
        rows = _normalize_dnse_records(trade.account_balance(sub_account=sub))
        if not rows:
            return None, "dnse_balance_empty"
        return rows[0], None
    except Exception as exc:
        return None, f"dnse_balance_error:{exc}"


def ensure_monitoring_tables() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS bot_health_snapshots (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        account_mode VARCHAR(10) NOT NULL CHECK (account_mode IN ('REAL', 'DEMO')),
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        bot_status VARCHAR(32) NOT NULL,
        metrics JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_bot_health_snapshots_mode_captured
        ON bot_health_snapshots (account_mode, captured_at DESC);
    CREATE TABLE IF NOT EXISTS alert_logs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        account_mode VARCHAR(10),
        rule_id VARCHAR(96) NOT NULL,
        severity VARCHAR(16) NOT NULL,
        message TEXT NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_alert_logs_created ON alert_logs (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_alert_logs_mode_rule_created
        ON alert_logs (account_mode, rule_id, created_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def _order_stats(conn, account_mode: str, since: datetime) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'REJECTED')::int AS rejected_count,
                COUNT(*) FILTER (WHERE status = 'FILLED')::int AS filled_count,
                COUNT(*) FILTER (WHERE status IN ('NEW', 'SENT', 'ACK', 'PARTIAL'))::int AS open_count,
                COALESCE(SUM(quantity * price) FILTER (WHERE side = 'BUY' AND status = 'FILLED'), 0)::float8 AS buy_notional_filled,
                COALESCE(SUM(quantity * price) FILTER (WHERE side = 'SELL' AND status = 'FILLED'), 0)::float8 AS sell_notional_filled
            FROM orders_core
            WHERE account_mode = %(account_mode)s AND created_at >= %(since)s
            """,
            {"account_mode": account_mode, "since": since},
        )
        row = cur.fetchone() or {}
    return dict(row)


def _last_signal_age_seconds(conn) -> tuple[datetime | None, float | None]:
    ensure_signals_table()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT created_at FROM signals ORDER BY created_at DESC LIMIT 1")
        row = cur.fetchone()
    if not row or not row.get("created_at"):
        return None, None
    ts: datetime = row["created_at"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age = max(0.0, (_utc_now() - ts).total_seconds())
    return ts, age


def _peak_equity_proxy(conn, account_mode: str, window_days: int = 30) -> float:
    since = _utc_now() - timedelta(days=window_days)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX((metrics->>'equity_proxy')::double precision), 0)
            FROM bot_health_snapshots
            WHERE account_mode = %(account_mode)s AND captured_at >= %(since)s
            """,
            {"account_mode": account_mode, "since": since},
        )
        row = cur.fetchone()
    peak = float(row[0] or 0) if row else 0.0
    return peak


def compute_operational_metrics(account_mode: str) -> dict[str, Any]:
    ensure_monitoring_tables()
    mode = str(account_mode).upper()
    window_errors = timedelta(minutes=settings.monitoring_error_window_minutes)
    since_errors = _utc_now() - window_errors
    with connect(settings.database_url) as conn:
        stats = _order_stats(conn, mode, since_errors)
        last_sig_ts, sig_age = _last_signal_age_seconds(conn)
        positions = get_positions(mode)
        exposure_proxy = sum(float(p["total_qty"]) * float(p["avg_price"]) for p in positions)
        buy_n = float(stats.get("buy_notional_filled") or 0)
        sell_n = float(stats.get("sell_notional_filled") or 0)
        inventory_cost = sum(float(p["total_qty"]) * float(p["avg_price"]) for p in positions)
        equity_proxy = sell_n - buy_n + inventory_cost
        peak = max(_peak_equity_proxy(conn, mode), equity_proxy, 1e-9)
        drawdown_proxy_pct = max(0.0, (peak - equity_proxy) / peak * 100.0) if peak > 0 else 0.0

    return {
        "account_mode": mode,
        "equity_proxy": round(equity_proxy, 6),
        "exposure_proxy": round(exposure_proxy, 6),
        "peak_equity_proxy_window": round(peak, 6),
        "drawdown_proxy_pct": round(drawdown_proxy_pct, 4),
        "gross_buy_notional_filled_window": round(buy_n, 6),
        "gross_sell_notional_filled_window": round(sell_n, 6),
        "rejected_orders_window": int(stats.get("rejected_count") or 0),
        "filled_orders_window": int(stats.get("filled_count") or 0),
        "open_orders_estimate": int(stats.get("open_count") or 0),
        "open_position_symbols": len(positions),
        "last_signal_at": last_sig_ts.isoformat() if last_sig_ts else None,
        "signal_age_seconds": round(sig_age, 3) if sig_age is not None else None,
        "signal_missing": last_sig_ts is None,
    }


def _float_safe(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _last_daily_close_mark(symbol: str) -> tuple[float | None, str | None]:
    sym = str(symbol).strip().upper()
    if not sym:
        return None, "empty_symbol"
    try:
        raw = _vnstock_api_service.call_quote(
            "history",
            source="VCI",
            symbol=sym,
            method_kwargs={"interval": "1D", "count_back": 5},
        )
    except Exception as exc:
        return None, str(exc)
    if not isinstance(raw, list) or not raw:
        return None, "empty_quote_history"
    last_close: float | None = None
    for row in reversed(raw):
        if not isinstance(row, dict):
            continue
        close = _float_safe(row.get("close"))
        if close is not None and close > 0:
            last_close = close
            break
    if last_close is None:
        return None, "no_valid_close_in_history"
    return last_close, None


def _experience_closed_trade_kpis(conn, account_mode: str) -> dict[str, Any]:
    ensure_experience_table()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)::int AS closed_trades,
                COALESCE(SUM(pnl_value), 0)::float8 AS realized_pnl_sum_vnd,
                COUNT(*) FILTER (WHERE pnl_value > 0)::int AS wins,
                COUNT(*) FILTER (WHERE pnl_value < 0)::int AS losses,
                COUNT(*) FILTER (WHERE pnl_value = 0)::int AS breakevens
            FROM experience
            WHERE account_mode = %(account_mode)s
            """,
            {"account_mode": account_mode},
        )
        row = cur.fetchone() or {}
    closed = int(row.get("closed_trades") or 0)
    wins = int(row.get("wins") or 0)
    losses = int(row.get("losses") or 0)
    decided = wins + losses
    win_rate = (wins / decided * 100.0) if decided > 0 else None
    return {
        "experience_closed_trades": closed,
        "experience_win_rate_pct": round(win_rate, 4) if win_rate is not None else None,
        "experience_realized_pnl_sum_vnd": round(float(row.get("realized_pnl_sum_vnd") or 0.0), 6),
        "experience_win_loss_decided": decided,
        "experience_breakevens": int(row.get("breakevens") or 0),
    }


def _compute_open_position_mtm(_account_mode: str, positions: list[dict[str, Any]]) -> dict[str, Any]:
    max_symbols = int(settings.monitoring_dashboard_mtm_max_symbols)
    rows: list[tuple[str, int, float, float]] = []
    for p in positions:
        sym = str(p.get("symbol", "")).strip().upper()
        qty = int(p.get("total_qty") or 0)
        avg = float(p.get("avg_price") or 0.0)
        if sym and qty > 0 and avg > 0:
            rows.append((sym, qty, avg, float(qty) * avg))
    rows.sort(key=lambda r: r[3], reverse=True)
    valued_rows = rows[:max_symbols]
    truncated_symbols = [r[0] for r in rows[max_symbols:]]

    marks: dict[str, float] = {}
    mark_errors: dict[str, str] = {}
    for sym, _qty, _avg, _notional in valued_rows:
        price, err = _last_daily_close_mark(sym)
        if price is not None:
            marks[sym] = price
        else:
            mark_errors[sym] = err or "unknown_error"

    exposure_cost_basis_vnd = sum(r[3] for r in rows)
    exposure_market_vnd = 0.0
    unrealized_pnl_vnd = 0.0
    used_marks = 0
    used_cost_fallback = 0

    for sym, qty, avg, _notional in rows:
        mark = marks.get(sym)
        if mark is None:
            mark = avg
            used_cost_fallback += 1
        else:
            used_marks += 1
        exposure_market_vnd += float(qty) * float(mark)
        unrealized_pnl_vnd += float(qty) * (float(mark) - float(avg))

    if used_marks == 0:
        method = "COST_BASIS_ONLY"
        notes = (
            "Khong lay duoc gia dong cua gan nhat tu nguon thi truong; "
            "exposure va PnL chua thuc hien dang su dung gia von trung binh (khong phai MTM)."
        )
    elif used_cost_fallback > 0:
        method = "MARK_TO_MARKET_DAILY_CLOSE_PARTIAL_FALLBACK"
        notes = (
            "Mot phan ma dung gia dong cua phien gan nhat (VCI history); "
            "cac ma loi API hoac vuot gioi han symbol dung gia von trung binh lam fallback."
        )
    else:
        method = "MARK_TO_MARKET_DAILY_CLOSE"
        notes = "Gia danh dau: dong cua phien gan nhat (VCI history), count_back nho."

    return {
        "valuation_method": method,
        "valuation_notes": notes,
        "exposure_cost_basis_vnd": round(exposure_cost_basis_vnd, 6),
        "exposure_market_vnd": round(exposure_market_vnd, 6),
        "unrealized_pnl_vnd": round(unrealized_pnl_vnd, 6),
        "mark_price_field": "close_last_daily_bar",
        "symbols_with_mark": sorted(marks.keys()),
        "symbols_mark_failed": mark_errors,
        "symbols_truncated_from_mtm_fetch": truncated_symbols,
        "mtm_fetch_cap": max_symbols,
    }


def compute_dashboard_trading_kpis(account_mode: str) -> dict[str, Any]:
    mode = str(account_mode).upper()
    positions = get_positions(mode)
    mtm = _compute_open_position_mtm(mode, positions)
    exp = {}
    try:
        with connect(settings.database_url, row_factory=dict_row) as conn:
            exp = _experience_closed_trade_kpis(conn, mode)
    except Exception as exc:
        exp = {
            "experience_closed_trades": 0,
            "experience_win_rate_pct": None,
            "experience_realized_pnl_sum_vnd": 0.0,
            "experience_win_loss_decided": 0,
            "experience_breakevens": 0,
            "experience_query_error": str(exc),
        }

    win_rate_note = (
        "Ty le thang tu bang experience (cac lenh dong co ghi nhan); khong phai toan bo lenh FILLED."
        if not exp.get("experience_query_error")
        else "Khong doc duoc bang experience; win-rate/PnL realized tam thoi khong day du."
    )
    out = {
        **mtm,
        **exp,
        "win_rate_scope_notes": win_rate_note,
        "realized_pnl_scope_notes": (
            "experience_realized_pnl_sum_vnd la tong pnl_value tu experience, "
            "phan anh cac lan dong lenh da ghi nhan hoc tap — khong thay the so sach ke toan day du."
        ),
        "drawdown_scope_notes": (
            "drawdown_proxy_pct lay tu bot_health_snapshots + equity_proxy noi bo (cua so giao dich); "
            "khong phai drawdown NAV day du khi thieu so du tien mat he thong."
        ),
    }
    return out


def build_account_monitoring_dashboard(
    account_mode: str,
    sub_account: str | None = None,
    dnse_access_token: str | None = None,
) -> dict[str, Any]:
    mode = str(account_mode).upper()
    if mode not in {"REAL", "DEMO"}:
        raise ValueError("account_mode must be REAL or DEMO")
    base = get_monitoring_summary(mode)
    metrics = compute_operational_metrics(mode)
    kill = get_kill_switch(mode)
    kill_active = bool(kill.get("active"))
    bot_status = _classify_bot_status(metrics, kill_active)
    kpis: dict[str, Any] = {}
    try:
        kpis = compute_dashboard_trading_kpis(mode)
    except Exception as exc:
        kpis = {"kpis_error": str(exc)}
    if mode == "REAL" and str(sub_account or "").strip():
        balance_row, balance_error = _fetch_dnse_balance_row_for_sub_account(
            str(sub_account),
            dnse_access_token=dnse_access_token,
        )
        if balance_row is not None:
            cash = _pick_first_finite_number(
                balance_row,
                ["cashBalance", "cash_balance", "availableCash", "available_cash", "cash", "buyingPower", "buying_power"],
            )
            net_asset = _pick_first_finite_number(
                balance_row,
                ["netAssetValue", "net_asset_value", "totalAsset", "total_asset", "nav"],
            )
            unrealized = _pick_first_finite_number(
                balance_row,
                ["unrealizedPnl", "unrealized_pnl", "floatingPnl", "floating_pnl", "openPnl", "open_pnl"],
            )
            drawdown_pct = _pick_first_finite_number(
                balance_row,
                ["drawdownPct", "drawdown_pct", "maxDrawdownPct", "max_drawdown_pct"],
            )
            if cash is not None and net_asset is not None:
                kpis["exposure_market_vnd"] = round(max(0.0, net_asset - cash), 6)
            if net_asset is not None and unrealized is not None:
                kpis["exposure_cost_basis_vnd"] = round(max(0.0, net_asset - unrealized), 6)
            if unrealized is not None:
                kpis["unrealized_pnl_vnd"] = round(unrealized, 6)
            if drawdown_pct is not None:
                kpis["drawdown_proxy_pct"] = round(drawdown_pct, 6)
            kpis["valuation_method"] = "DNSE_SUB_ACCOUNT_BALANCE_RUNTIME"
            kpis["valuation_notes"] = (
                f"KPI REAL duoc tinh tu DNSE account-balance sub-account={str(sub_account).strip()} "
                "tren server-side; field thieu se fallback ve monitoring core."
            )
        elif balance_error:
            kpis["valuation_notes"] = (
                f"Fallback monitoring core do khong doc duoc DNSE balance cho sub-account={str(sub_account).strip()}: {balance_error}"
            )
    if "drawdown_proxy_pct" not in kpis:
        kpis["drawdown_proxy_pct"] = float(metrics.get("drawdown_proxy_pct") or 0.0)
    merged = dict(base)
    merged["operational_health"] = {"bot_status": bot_status, "kill_switch": kill, "metrics": metrics}
    merged["kpis"] = kpis
    merged["ai_runtime"] = {
        "claude_signal_scoring": get_signal_scoring_claude_runtime_metrics(),
        "claude_experience": get_experience_claude_runtime_metrics(),
    }
    return merged


def _classify_bot_status(metrics: dict[str, Any], kill_active: bool) -> str:
    if kill_active:
        return "HALTED"
    if int(metrics.get("rejected_orders_window") or 0) >= settings.monitoring_error_count_threshold:
        return "ERROR"
    if bool(metrics.get("signal_missing")):
        return "DEGRADED"
    age = metrics.get("signal_age_seconds")
    if age is not None and age > settings.monitoring_signal_stale_minutes * 60:
        return "DEGRADED"
    if float(metrics.get("drawdown_proxy_pct") or 0) >= settings.monitoring_drawdown_alert_pct * 0.75:
        return "DEGRADED"
    return "OK"


def insert_health_snapshot(account_mode: str, bot_status: str, metrics: dict[str, Any]) -> dict[str, Any]:
    ensure_monitoring_tables()
    mode = str(account_mode).upper()
    snap_id = uuid4()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_health_snapshots (id, account_mode, captured_at, bot_status, metrics)
                VALUES (%(id)s, %(mode)s, NOW(), %(bot_status)s, %(metrics)s::jsonb)
                RETURNING id::text AS id, account_mode, captured_at, bot_status, metrics
                """,
                {
                    "id": snap_id,
                    "mode": mode,
                    "bot_status": bot_status,
                    "metrics": json.dumps(metrics),
                },
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row or {})


def insert_alert_record(
    *,
    account_mode: str | None,
    rule_id: str,
    severity: str,
    message: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    ensure_monitoring_tables()
    aid = uuid4()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alert_logs (id, account_mode, rule_id, severity, message, payload, created_at)
                VALUES (%(id)s, %(account_mode)s, %(rule_id)s, %(severity)s, %(message)s, %(payload)s::jsonb, NOW())
                RETURNING id::text AS id, account_mode, rule_id, severity, message, payload, created_at
                """,
                {
                    "id": aid,
                    "account_mode": account_mode,
                    "rule_id": rule_id,
                    "severity": severity,
                    "message": message,
                    "payload": json.dumps(payload),
                },
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row or {})


def _recent_alert_count(conn, account_mode: str | None, rule_id: str, since: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int FROM alert_logs
            WHERE rule_id = %(rule_id)s AND created_at >= %(since)s
            AND (account_mode IS NOT DISTINCT FROM %(account_mode)s)
            """,
            {"rule_id": rule_id, "since": since, "account_mode": account_mode},
        )
        row = cur.fetchone()
    return int(row[0] if row else 0)


def _should_emit_alert(conn, account_mode: str | None, rule_id: str) -> bool:
    cooldown = timedelta(minutes=settings.monitoring_alert_cooldown_minutes)
    since = _utc_now() - cooldown
    return _recent_alert_count(conn, account_mode, rule_id, since) == 0


def evaluate_alert_rules(account_mode: str) -> list[dict[str, Any]]:
    ensure_monitoring_tables()
    mode = str(account_mode).upper()
    metrics = compute_operational_metrics(mode)
    kill = get_kill_switch(mode)
    kill_active = bool(kill.get("active"))
    bot_status = _classify_bot_status(metrics, kill_active)
    insert_health_snapshot(mode, bot_status, metrics)

    fired: list[dict[str, Any]] = []
    with connect(settings.database_url) as conn:
        def dispatch_and_capture(alert_row: dict[str, Any]) -> None:
            try:
                delivery = dispatch_alert_to_channels(str(alert_row.get("message") or ""))
                alert_row["delivery"] = delivery
                if any(
                    bool(channel.get("enabled")) and not bool(channel.get("delivered"))
                    for channel in [delivery.get("telegram", {}), delivery.get("slack", {})]
                ):
                    log_risk_event(
                        mode,
                        "ALERT_DELIVERY_ERROR",
                        payload={
                            "alert_id": alert_row.get("id"),
                            "rule_id": alert_row.get("rule_id"),
                            "delivery": delivery,
                        },
                    )
            except Exception as exc:
                alert_row["delivery"] = {"error": str(exc)}
                log_risk_event(
                    mode,
                    "ALERT_DELIVERY_ERROR",
                    payload={
                        "alert_id": alert_row.get("id"),
                        "rule_id": alert_row.get("rule_id"),
                        "error": str(exc),
                    },
                )

        # Drawdown
        dd = float(metrics.get("drawdown_proxy_pct") or 0)
        rule_dd = f"drawdown_breach:{mode}"
        if dd >= settings.monitoring_drawdown_alert_pct and _should_emit_alert(conn, mode, rule_dd):
            details = {
                "drawdown_proxy_pct": dd,
                "threshold_pct": settings.monitoring_drawdown_alert_pct,
                "equity_proxy": metrics.get("equity_proxy"),
            }
            msg = format_alert_plain_text(
                rule_id=rule_dd,
                severity="HIGH",
                account_mode=mode,
                title="Drawdown proxy crossed configured threshold",
                details=details,
            )
            row = insert_alert_record(
                account_mode=mode,
                rule_id=rule_dd,
                severity="HIGH",
                message=msg,
                payload=details,
            )
            dispatch_and_capture(row)
            fired.append(row)
            log_risk_event(mode, "DRAWDOWN_ALERT", payload={"metrics": metrics, "alert_id": row.get("id")})

        # Repeated rejections / execution errors
        rej = int(metrics.get("rejected_orders_window") or 0)
        rule_err = f"execution_errors:{mode}"
        if rej >= settings.monitoring_error_count_threshold and _should_emit_alert(conn, mode, rule_err):
            details = {
                "rejected_orders_window": rej,
                "window_minutes": settings.monitoring_error_window_minutes,
                "threshold": settings.monitoring_error_count_threshold,
            }
            sev = "CRITICAL" if rej >= settings.monitoring_error_count_threshold * 2 else "HIGH"
            msg = format_alert_plain_text(
                rule_id=rule_err,
                severity=sev,
                account_mode=mode,
                title="Repeated order rejections in monitoring window",
                details=details,
            )
            row = insert_alert_record(
                account_mode=mode,
                rule_id=rule_err,
                severity=sev,
                message=msg,
                payload=details,
            )
            dispatch_and_capture(row)
            fired.append(row)
            log_risk_event(mode, "EXECUTION_ERRORS_ALERT", payload={"metrics": metrics, "alert_id": row.get("id")})

        # Stale or missing signals
        stale_limit_sec = settings.monitoring_signal_stale_minutes * 60
        sig_age = metrics.get("signal_age_seconds")
        stale = bool(metrics.get("signal_missing")) or (
            sig_age is not None and float(sig_age) > stale_limit_sec
        )
        rule_stale = f"stale_signals:{mode}"
        if stale and _should_emit_alert(conn, mode, rule_stale):
            details = {
                "signal_age_seconds": sig_age,
                "signal_missing": metrics.get("signal_missing"),
                "stale_after_seconds": stale_limit_sec,
            }
            msg = format_alert_plain_text(
                rule_id=rule_stale,
                severity="MEDIUM",
                account_mode=mode,
                title="Market signals missing or older than threshold",
                details=details,
            )
            row = insert_alert_record(
                account_mode=mode,
                rule_id=rule_stale,
                severity="MEDIUM",
                message=msg,
                payload=details,
            )
            dispatch_and_capture(row)
            fired.append(row)

    return fired


def evaluate_alerts_for_modes(modes: list[str] | None) -> dict[str, Any]:
    target = [m.upper() for m in modes] if modes else ["REAL", "DEMO"]
    all_fired: list[dict[str, Any]] = []
    for m in target:
        if m not in {"REAL", "DEMO"}:
            continue
        all_fired.extend(evaluate_alert_rules(m))
    return {"evaluated_modes": [m for m in target if m in {"REAL", "DEMO"}], "alerts_raised": all_fired}


def get_monitoring_summary_all() -> dict[str, Any]:
    """Aggregate summary for both account modes (GET /monitoring/summary)."""
    out: dict[str, Any] = {"captured_at": _utc_now().isoformat(), "modes": {}}
    for mode in ("REAL", "DEMO"):
        metrics = compute_operational_metrics(mode)
        kill = get_kill_switch(mode)
        kill_active = bool(kill.get("active"))
        bot_status = _classify_bot_status(metrics, kill_active)
        out["modes"][mode] = {
            "bot_status": bot_status,
            "kill_switch": kill,
            "metrics": metrics,
        }
    out["ai_runtime"] = {
        "claude_signal_scoring": get_signal_scoring_claude_runtime_metrics(),
        "claude_experience": get_experience_claude_runtime_metrics(),
    }
    return out


def list_recent_alerts(*, limit: int = 50) -> list[dict[str, Any]]:
    ensure_monitoring_tables()
    safe = max(1, min(limit, 200))
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, account_mode, rule_id, severity, message, payload, created_at
                FROM alert_logs
                ORDER BY created_at DESC
                LIMIT %(limit)s
                """,
                {"limit": safe},
            )
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def list_recent_runtime_logs(*, account_mode: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    """
    Runtime operation logs for dashboard scrolling view.
    Source: short_term_automation_runs persisted rows (scheduler/manual cycles).
    """
    safe = max(1, min(int(limit), 500))
    ensure_short_term_automation_runs_table()
    mode = str(account_mode or "").strip().upper()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if mode in {"REAL", "DEMO"}:
                cur.execute(
                    """
                    SELECT
                        id::text AS id,
                        started_at,
                        run_status,
                        scanned,
                        buy_candidates,
                        risk_rejected,
                        executed,
                        execution_rejected,
                        errors,
                        detail
                    FROM short_term_automation_runs
                    WHERE detail->>'account_mode' = %(account_mode)s
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"account_mode": mode, "limit": safe},
                )
            else:
                cur.execute(
                    """
                    SELECT
                        id::text AS id,
                        started_at,
                        run_status,
                        scanned,
                        buy_candidates,
                        risk_rejected,
                        executed,
                        execution_rejected,
                        errors,
                        detail
                    FROM short_term_automation_runs
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": safe},
                )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        detail = dict(row.get("detail") or {})
        row_mode = str(detail.get("account_mode") or "").strip().upper() or None
        exchange_scope = str(detail.get("exchange_scope") or "-")
        run_status = str(row.get("run_status") or "-")
        scanned = int(row.get("scanned") or 0)
        buy_candidates = int(row.get("buy_candidates") or 0)
        risk_rejected = int(row.get("risk_rejected") or 0)
        executed = int(row.get("executed") or 0)
        execution_rejected = int(row.get("execution_rejected") or 0)
        errors = int(row.get("errors") or 0)
        skip_reason = str(detail.get("skip_reason") or "").strip()
        runtime_message = (
            f"{run_status} | scope={exchange_scope} | scan={scanned} | candidate={buy_candidates} | "
            f"risk_rej={risk_rejected} | exec={executed} | exec_rej={execution_rejected} | err={errors}"
        )
        if skip_reason:
            runtime_message = f"{runtime_message} | skip_reason={skip_reason}"
        out.append(
            {
                "id": str(row.get("id")),
                "account_mode": row_mode,
                "source": "short_term_automation_runs",
                "level": "INFO" if errors == 0 else "WARN",
                "message": runtime_message,
                "payload": detail,
                "created_at": row.get("started_at"),
            }
        )
    return out
