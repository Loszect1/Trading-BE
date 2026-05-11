from __future__ import annotations

from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from psycopg import connect
from psycopg.rows import dict_row

from app.core.config import settings
from app.services.price_unit_service import normalize_vn_price_to_vnd
from app.services.trading_core_service import ensure_trading_core_tables, evaluate_risk, roll_settlement


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def build_recommendation_from_signal(signal: dict[str, Any]) -> dict[str, Any]:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    setup = metadata.get("setup") if isinstance(metadata.get("setup"), dict) else {}
    levels = metadata.get("trade_levels") if isinstance(metadata.get("trade_levels"), dict) else {}
    return {
        "symbol": str(signal.get("symbol") or "").strip().upper(),
        "entry": levels.get("entry", signal.get("entry_price")),
        "take_profit": levels.get("take_profit", signal.get("take_profit_price")),
        "stop_loss": levels.get("stop_loss", signal.get("stoploss_price")),
        "confidence": signal.get("confidence"),
        "reason": signal.get("reason"),
        "setup_type": setup.get("setup_type"),
        "setup": setup,
        "freshness": metadata.get("freshness") if isinstance(metadata.get("freshness"), dict) else {},
        "relative_strength": metadata.get("relative_strength") if isinstance(metadata.get("relative_strength"), dict) else {},
        "sector_breadth": metadata.get("sector_breadth") if isinstance(metadata.get("sector_breadth"), dict) else {},
        "trade_levels": levels,
        "metadata": metadata,
        "signal_id": signal.get("id"),
        "source_strategy": signal.get("strategy_type") or "SHORT_TERM",
    }


def normalize_real_recommendation_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        entry = _to_float(row.get("entry"))
        take_profit = _to_float(row.get("take_profit"))
        stop_loss = _to_float(row.get("stop_loss"))
        confidence = _to_float(row.get("confidence"))
        if not symbol or entry is None or take_profit is None or stop_loss is None:
            continue
        entry = normalize_vn_price_to_vnd(entry)
        take_profit = normalize_vn_price_to_vnd(take_profit)
        stop_loss = normalize_vn_price_to_vnd(stop_loss)
        item: dict[str, Any] = {
            "symbol": symbol,
            "entry": entry,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "confidence": max(0.0, min(100.0, confidence if confidence is not None else 50.0)),
            "reason": str(row.get("reason") or "").strip(),
        }
        for key in (
            "exchange",
            "setup_type",
            "setup",
            "freshness",
            "relative_strength",
            "sector_breadth",
            "trade_levels",
            "metadata",
            "signal_id",
            "source_strategy",
            "risk_result",
            "risk_status",
            "risk_reason",
            "reward_risk",
            "suggested_quantity",
            "suggested_notional",
            "settlement_pressure",
            "account_preflight",
        ):
            if key in row:
                item[key] = row.get(key)
        out.append(item)
    return out


def get_real_t2_settlement_pressure() -> dict[str, Any]:
    """
    Snapshot pending REAL T+2 stock lots. In VN equities, BUY lots are paid for but
    cannot be sold until settlement, so pending notional is treated as locked
    short-term exposure rather than available inventory.
    """
    try:
        ensure_trading_core_tables()
        roll_settlement("REAL")
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        symbol,
                        COALESCE(SUM(qty - available_qty), 0)::int AS pending_qty,
                        COALESCE(SUM((qty - available_qty) * avg_price), 0)::float8 AS pending_notional,
                        MIN(settle_date) AS next_settle_date
                    FROM position_lots
                    WHERE account_mode = 'REAL'
                      AND qty > available_qty
                    GROUP BY symbol
                    ORDER BY symbol
                    """
                )
                rows = [dict(row) for row in cur.fetchall()]
    except Exception as exc:
        return {
            "available": False,
            "reason": "settlement_snapshot_unavailable",
            "error": str(exc)[:200],
            "total_pending_qty": 0,
            "total_pending_notional": 0.0,
            "next_settle_date": None,
            "by_symbol": {},
        }

    by_symbol: dict[str, dict[str, Any]] = {}
    total_qty = 0
    total_notional = 0.0
    next_dates: list[str] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        pending_qty = max(0, int(row.get("pending_qty") or 0))
        pending_notional = max(0.0, float(row.get("pending_notional") or 0.0))
        next_settle_date = row.get("next_settle_date")
        next_settle_date_str = str(next_settle_date) if next_settle_date is not None else None
        by_symbol[symbol] = {
            "pending_qty": pending_qty,
            "pending_notional": round(pending_notional, 4),
            "next_settle_date": next_settle_date_str,
        }
        total_qty += pending_qty
        total_notional += pending_notional
        if next_settle_date_str:
            next_dates.append(next_settle_date_str)

    return {
        "available": True,
        "market_rule": "VN_T_PLUS_2",
        "total_pending_qty": total_qty,
        "total_pending_notional": round(total_notional, 4),
        "next_settle_date": min(next_dates) if next_dates else None,
        "by_symbol": by_symbol,
    }


def _setup_type_for_t2(item: dict[str, Any]) -> str:
    setup_type = str(item.get("setup_type") or "").strip().upper()
    if setup_type:
        return setup_type
    setup = item.get("setup") if isinstance(item.get("setup"), dict) else {}
    return str(setup.get("setup_type") or "").strip().upper()


def get_real_account_preflight_context(cash_base: float) -> dict[str, Any]:
    try:
        ensure_trading_core_tables()
        tz = ZoneInfo(settings.short_term_scan_timezone)
        today = datetime.now(tz=tz).date()
        day_start = datetime.combine(today, time.min, tzinfo=tz)
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE side = 'BUY' AND status IN ('NEW', 'SENT', 'ACK', 'PARTIAL'))::int AS open_buy_orders,
                        COUNT(*) FILTER (WHERE status IN ('NEW', 'SENT', 'ACK', 'PARTIAL'))::int AS open_orders,
                        COUNT(*) FILTER (WHERE side = 'BUY' AND created_at >= %(day_start)s)::int AS buy_orders_today,
                        COUNT(*) FILTER (WHERE created_at >= %(day_start)s)::int AS orders_today
                    FROM orders_core
                    WHERE account_mode = 'REAL'
                    """,
                    {"day_start": day_start},
                )
                order_stats = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    SELECT symbol, side, status, quantity, price
                    FROM orders_core
                    WHERE account_mode = 'REAL'
                      AND status IN ('NEW', 'SENT', 'ACK', 'PARTIAL')
                    ORDER BY created_at DESC
                    LIMIT 200
                    """
                )
                open_orders = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT
                        pl.symbol,
                        COALESCE(SUM(pl.qty), 0)::int AS total_qty,
                        COALESCE(SUM(pl.qty * pl.avg_price), 0)::float8 AS position_notional,
                        ms.info
                    FROM position_lots pl
                    LEFT JOIN market_symbols ms ON ms.symbol = pl.symbol
                    WHERE pl.account_mode = 'REAL'
                    GROUP BY pl.symbol, ms.info
                    HAVING SUM(pl.qty) > 0
                    """
                )
                positions = [dict(row) for row in cur.fetchall()]
    except Exception as exc:
        return {
            "available": False,
            "source": "db_unavailable",
            "reason": "account_preflight_context_unavailable",
            "error": str(exc)[:200],
            "daily_new_orders": 0,
            "max_daily_new_orders": int(settings.strategy_max_daily_new_orders),
            "open_buy_symbols": [],
            "symbol_exposure": {},
            "sector_exposure": {},
        }

    open_buy_symbols = sorted(
        {
            str(row.get("symbol") or "").strip().upper()
            for row in open_orders
            if str(row.get("side") or "").upper() == "BUY" and str(row.get("symbol") or "").strip()
        }
    )
    symbol_exposure: dict[str, float] = {}
    sector_exposure: dict[str, float] = {}
    for row in positions:
        symbol = str(row.get("symbol") or "").strip().upper()
        notional = max(0.0, float(row.get("position_notional") or 0.0))
        if symbol:
            symbol_exposure[symbol] = round(notional, 4)
        info = row.get("info") if isinstance(row.get("info"), dict) else {}
        industry = ""
        for key in ("industry", "industry_name", "industryName", "icb_name", "icbName", "sector"):
            value = info.get(key) if isinstance(info, dict) else None
            if isinstance(value, str) and value.strip():
                industry = value.strip()
                break
        if industry:
            sector_exposure[industry] = round(sector_exposure.get(industry, 0.0) + notional, 4)
    return {
        "available": True,
        "source": "orders_core_position_lots",
        "cash_base": round(float(cash_base), 4),
        "daily_new_orders": int(order_stats.get("buy_orders_today") or 0),
        "orders_today": int(order_stats.get("orders_today") or 0),
        "open_orders": int(order_stats.get("open_orders") or 0),
        "open_buy_orders": int(order_stats.get("open_buy_orders") or 0),
        "max_daily_new_orders": int(settings.strategy_max_daily_new_orders),
        "open_buy_symbols": open_buy_symbols,
        "symbol_exposure": symbol_exposure,
        "sector_exposure": sector_exposure,
        "max_symbol_exposure_pct": float(settings.strategy_max_symbol_exposure_pct),
        "max_sector_exposure_pct": float(settings.strategy_max_sector_exposure_pct),
    }


def _item_industry(item: dict[str, Any]) -> str:
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    listing = metadata.get("listing_context") if isinstance(metadata.get("listing_context"), dict) else {}
    industry = listing.get("industry")
    return str(industry or "").strip()


def get_real_cash_snapshot() -> dict[str, Any]:
    sub_account = (settings.dnse_sub_account or settings.dnse_default_sub_account or "").strip()
    if not sub_account:
        return {"available": False, "source": "config_cash", "reason": "missing_dnse_sub_account", "cash": None}
    try:
        from app.services.monitoring_service import _fetch_dnse_balance_row_for_sub_account, _pick_first_finite_number

        row, error = _fetch_dnse_balance_row_for_sub_account(
            sub_account,
            dnse_access_token=settings.dnse_access_token or None,
        )
        if row is None:
            return {"available": False, "source": "config_cash", "reason": error or "dnse_balance_unavailable", "cash": None}
        cash = _pick_first_finite_number(
            row,
            ["availableCash", "available_cash", "buyingPower", "buying_power", "cashBalance", "cash_balance", "cash"],
        )
        if cash is None or float(cash) <= 0:
            return {"available": False, "source": "config_cash", "reason": "dnse_cash_field_missing", "cash": None}
        return {
            "available": True,
            "source": "dnse_cash",
            "sub_account": sub_account,
            "cash": float(cash),
        }
    except Exception as exc:
        return {"available": False, "source": "config_cash", "reason": f"dnse_cash_error:{str(exc)[:160]}", "cash": None}


def preflight_real_recommendations(
    rows: list[dict[str, Any]],
    *,
    available_cash_vnd: float | None = None,
    settlement_pressure: dict[str, Any] | None = None,
    account_context: dict[str, Any] | None = None,
    cash_source: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalized = normalize_real_recommendation_rows(rows)
    cash_snapshot = get_real_cash_snapshot() if available_cash_vnd is None else {"available": True, "source": "request_cash", "cash": available_cash_vnd}
    cash_base = (
        float(available_cash_vnd)
        if available_cash_vnd is not None and float(available_cash_vnd) > 0
        else float(cash_snapshot.get("cash"))
        if cash_snapshot.get("cash") is not None and float(cash_snapshot.get("cash") or 0.0) > 0
        else float(settings.strategy_total_cash_vnd)
    )
    resolved_cash_source = str(cash_source or cash_snapshot.get("source") or ("request_cash" if available_cash_vnd is not None else "config_cash"))
    nav = max(1_000_000.0, cash_base * float(settings.strategy_alloc_short_term_pct))
    pressure = settlement_pressure if isinstance(settlement_pressure, dict) else get_real_t2_settlement_pressure()
    acct = account_context if isinstance(account_context, dict) else get_real_account_preflight_context(cash_base)
    daily_new_orders = int(acct.get("daily_new_orders") or 0)
    max_daily_new_orders = int(acct.get("max_daily_new_orders") or settings.strategy_max_daily_new_orders)
    open_buy_symbols = {str(sym).strip().upper() for sym in (acct.get("open_buy_symbols") or []) if str(sym).strip()}
    symbol_exposure = acct.get("symbol_exposure") if isinstance(acct.get("symbol_exposure"), dict) else {}
    sector_exposure = acct.get("sector_exposure") if isinstance(acct.get("sector_exposure"), dict) else {}
    total_pending_notional = max(0.0, float(pressure.get("total_pending_notional") or 0.0))
    pending_ratio = total_pending_notional / cash_base if cash_base > 0 else 0.0
    max_pending_ratio = float(settings.strategy_t2_max_pending_notional_pct)
    short_term_budget = max(0.0, cash_base * float(settings.strategy_alloc_short_term_pct))
    remaining_t2_budget = max(0.0, short_term_budget - total_pending_notional)
    if total_pending_notional > 0 and pending_ratio >= float(settings.strategy_t2_pending_pressure_haircut_start_pct):
        remaining_t2_budget *= max(0.0, 1.0 - float(settings.strategy_t2_pending_pressure_haircut_pct))
    by_symbol = pressure.get("by_symbol") if isinstance(pressure.get("by_symbol"), dict) else {}
    buyable: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for item in normalized:
        symbol_pressure = by_symbol.get(item["symbol"]) if isinstance(by_symbol.get(item["symbol"]), dict) else {}
        item_pressure = {
            "market_rule": "VN_T_PLUS_2",
            "available": bool(pressure.get("available", False)),
            "total_pending_qty": int(pressure.get("total_pending_qty") or 0),
            "total_pending_notional": round(total_pending_notional, 4),
            "pending_notional_ratio": round(pending_ratio, 6),
            "max_pending_notional_ratio": round(max_pending_ratio, 6),
            "short_term_budget": round(short_term_budget, 4),
            "remaining_t2_budget": round(remaining_t2_budget, 4),
            "next_settle_date": pressure.get("next_settle_date"),
            "symbol_pending_qty": int(symbol_pressure.get("pending_qty") or 0),
            "symbol_pending_notional": float(symbol_pressure.get("pending_notional") or 0.0),
            "symbol_next_settle_date": symbol_pressure.get("next_settle_date"),
        }
        freshness = item.get("freshness") if isinstance(item.get("freshness"), dict) else {}
        if freshness and not bool(freshness.get("is_fresh", True)):
            enriched = {
                **item,
                "risk_status": "REJECTED",
                "risk_reason": "stale_data",
                "risk_result": {"pass": False, "reason": "stale_data"},
                "settlement_pressure": item_pressure,
                "account_preflight": acct,
                "suggested_quantity": 0,
                "suggested_notional": 0.0,
            }
            rejected.append(enriched)
            continue
        risk_result = evaluate_risk(
            {
                "account_mode": "REAL",
                "symbol": item["symbol"],
                "nav": nav,
                "risk_per_trade": float(settings.strategy_risk_per_trade),
                "entry_price": float(item["entry"]),
                "stoploss_price": float(item["stop_loss"]),
                "take_profit_price": float(item["take_profit"]),
                "side": "BUY",
                "min_reward_risk": 2.0 if _setup_type_for_t2(item) == "NEWS_SPIKE" else 1.5,
                "daily_new_orders": daily_new_orders,
                "max_daily_new_orders": max_daily_new_orders,
            }
        )
        suggested_lot_size = int(risk_result.get("suggested_lot_size") or 0)
        effective_cash_cap = min(cash_base, remaining_t2_budget) if short_term_budget > 0 else cash_base
        cash_cap = int(effective_cash_cap // float(item["entry"])) if float(item["entry"]) > 0 else 0
        quantity = (max(0, min(suggested_lot_size, cash_cap)) // 100) * 100
        status = "BUYABLE" if bool(risk_result.get("pass")) and quantity >= 100 else "REJECTED"
        reason = str(risk_result.get("reason") or "ok")
        if status == "REJECTED" and reason == "ok":
            reason = "insufficient_size_or_cash_after_risk"
        proposed_notional = quantity * float(item["entry"])
        industry = _item_industry(item)
        symbol_current_exposure = float(symbol_exposure.get(item["symbol"]) or 0.0)
        sector_current_exposure = float(sector_exposure.get(industry) or 0.0) if industry else 0.0
        max_symbol_exposure = cash_base * float(settings.strategy_max_symbol_exposure_pct)
        max_sector_exposure = cash_base * float(settings.strategy_max_sector_exposure_pct)
        if bool(risk_result.get("pass")) and item["symbol"] in open_buy_symbols:
            status = "REJECTED"
            reason = "pending_order_conflict"
            quantity = 0
        elif bool(risk_result.get("pass")) and pending_ratio >= max_pending_ratio:
            status = "REJECTED"
            reason = "settlement_pressure_high"
            quantity = 0
        elif (
            status == "BUYABLE"
            and int(symbol_pressure.get("pending_qty") or 0) > 0
            and not bool(settings.strategy_t2_same_symbol_scale_in_allowed)
        ):
            status = "REJECTED"
            setup_type = _setup_type_for_t2(item)
            reason = "same_symbol_pending_t2"
            if setup_type in {"NEWS_SPIKE", "EXTENDED_CHASE"}:
                reason = "same_symbol_pending_t2_chase"
            quantity = 0
        elif bool(risk_result.get("pass")) and quantity < 100 and total_pending_notional > 0:
            reason = "capital_locked_t2"
        elif bool(risk_result.get("pass")) and symbol_current_exposure + proposed_notional > max_symbol_exposure:
            status = "REJECTED"
            reason = "symbol_exposure_limit"
            quantity = 0
        elif bool(risk_result.get("pass")) and industry and sector_current_exposure + proposed_notional > max_sector_exposure:
            status = "REJECTED"
            reason = "sector_exposure_limit"
            quantity = 0
        reward_risk = risk_result.get("reward_risk")
        enriched = {
            **item,
            "risk_result": risk_result,
            "risk_status": status,
            "risk_reason": reason,
            "reward_risk": reward_risk,
            "settlement_pressure": item_pressure,
            "account_preflight": {
                **acct,
                "cash_source": resolved_cash_source,
                "cash_snapshot": cash_snapshot,
                "symbol_current_exposure": round(symbol_current_exposure, 4),
                "sector_current_exposure": round(sector_current_exposure, 4),
                "industry": industry,
                "max_symbol_exposure": round(max_symbol_exposure, 4),
                "max_sector_exposure": round(max_sector_exposure, 4),
            },
            "suggested_quantity": quantity,
            "suggested_notional": round(quantity * float(item["entry"]), 4),
        }
        if status == "BUYABLE":
            buyable.append(enriched)
        else:
            rejected.append(enriched)
    return buyable, rejected
