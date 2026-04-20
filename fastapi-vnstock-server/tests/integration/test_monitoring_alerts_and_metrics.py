"""Postgres-backed monitoring tests: operational metrics from orders_core; alert rules + cooldown (no outbound network)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from psycopg import connect
from psycopg.errors import CheckViolation

from app.services.experience_service import ensure_experience_table
from app.services.monitoring_service import (
    build_account_monitoring_dashboard,
    compute_dashboard_trading_kpis,
    compute_operational_metrics,
    ensure_monitoring_tables,
    evaluate_alert_rules,
    evaluate_alerts_for_modes,
    get_monitoring_summary_all,
    insert_alert_record,
    insert_health_snapshot,
    list_recent_alerts,
)
from app.services.signal_engine_service import ensure_signals_table
from app.services.trading_core_service import ensure_trading_core_tables, list_risk_events


def _purge_unit_monitoring_rows(trading_db: str) -> None:
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM alert_logs WHERE rule_id LIKE 'unit_msvc_%'")
        conn.commit()


def _dispatch_stub(_message: str) -> dict:
    return {
        "telegram": {"enabled": False, "delivered": False},
        "slack": {"enabled": False, "delivered": False},
    }


def _base_metrics(mode: str) -> dict:
    m = mode.upper()
    return {
        "account_mode": m,
        "equity_proxy": 0.0,
        "exposure_proxy": 0.0,
        "peak_equity_proxy_window": 100.0,
        "drawdown_proxy_pct": 0.0,
        "gross_buy_notional_filled_window": 0.0,
        "gross_sell_notional_filled_window": 0.0,
        "rejected_orders_window": 0,
        "filled_orders_window": 0,
        "open_orders_estimate": 0,
        "open_position_symbols": 0,
        "last_signal_at": None,
        "signal_age_seconds": None,
        "signal_missing": False,
    }


@pytest.mark.postgres
def test_compute_operational_metrics_counts_rejected_orders(trading_db: str) -> None:
    ensure_trading_core_tables()
    now = datetime.now(tz=timezone.utc)
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            for i in range(4):
                cur.execute(
                    """
                    INSERT INTO orders_core (
                        id, account_mode, symbol, side, quantity, price, status, reason, created_at, updated_at
                    ) VALUES (
                        %(id)s, 'DEMO', 'TSTMET', 'BUY', 1, 1000.0, 'REJECTED', 'test', %(ts)s, %(ts)s
                    )
                    """,
                    {"id": uuid4(), "ts": now},
                )
        conn.commit()

    metrics = compute_operational_metrics("demo")
    assert metrics["rejected_orders_window"] == 4


@pytest.mark.postgres
def test_evaluate_alert_rules_drawdown_cooldown_second_call_silent(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.monitoring_service as msvc

    monkeypatch.setattr(msvc, "dispatch_alert_to_channels", _dispatch_stub)

    def metrics(mode: str) -> dict:
        out = _base_metrics(mode)
        out["drawdown_proxy_pct"] = 50.0
        return out

    monkeypatch.setattr(msvc, "compute_operational_metrics", metrics)

    first = evaluate_alert_rules("DEMO")
    dd_ids = [r["rule_id"] for r in first if r.get("rule_id") == "drawdown_breach:DEMO"]
    assert len(dd_ids) == 1

    second = evaluate_alert_rules("DEMO")
    dd_again = [r for r in second if r.get("rule_id") == "drawdown_breach:DEMO"]
    assert dd_again == []


@pytest.mark.postgres
def test_evaluate_alert_rules_execution_errors_critical_and_stale_signals(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.monitoring_service as msvc

    monkeypatch.setattr(msvc, "dispatch_alert_to_channels", _dispatch_stub)

    def metrics(mode: str) -> dict:
        out = _base_metrics(mode)
        out["drawdown_proxy_pct"] = 0.0
        out["rejected_orders_window"] = 11
        out["signal_missing"] = True
        out["signal_age_seconds"] = None
        return out

    monkeypatch.setattr(msvc, "compute_operational_metrics", metrics)

    fired = evaluate_alert_rules("DEMO")
    by_rule = {r["rule_id"]: r for r in fired}
    assert "execution_errors:DEMO" in by_rule
    assert by_rule["execution_errors:DEMO"]["severity"] == "CRITICAL"
    assert "stale_signals:DEMO" in by_rule


@pytest.mark.postgres
def test_compute_dashboard_trading_kpis_experience_and_mtm_stub(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.monitoring_service as msvc

    from app.services.trading_core_service import place_order

    place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTKPI",
            "side": "BUY",
            "quantity": 10,
            "price": 90.0,
            "idempotency_key": "idem-tstkpi-mtm-kpi",
            "auto_process": True,
        }
    )

    ensure_experience_table()
    monkeypatch.setattr(msvc, "_last_daily_close_mark", lambda _sym: (100.0, None))

    ts = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            for pnl, tid in [(100_000.0, "tr-kpi-1"), (50_000.0, "tr-kpi-2"), (-20_000.0, "tr-kpi-3")]:
                cur.execute(
                    """
                    INSERT INTO experience (
                        id, trade_id, account_mode, symbol, strategy_type,
                        entry_time, exit_time, pnl_value, pnl_percent, market_context,
                        root_cause, mistake_tags, improvement_action, confidence_after_review, reviewed_by
                    ) VALUES (
                        %(id)s, %(tid)s, 'DEMO', 'TSTKPI', 'SHORT_TERM',
                        %(ts)s, %(ts)s, %(pnl)s, 1.0, '{}'::jsonb,
                        'test', ARRAY[]::text[], 'test', 70.0, 'BOT'
                    )
                    """,
                    {"id": uuid4(), "tid": tid, "ts": ts, "pnl": pnl},
                )
        conn.commit()

    out = compute_dashboard_trading_kpis("DEMO")
    assert out["experience_closed_trades"] == 3
    assert out["experience_win_loss_decided"] == 3
    assert out["experience_win_rate_pct"] == pytest.approx(200.0 / 3.0, rel=1e-3)
    assert out["experience_realized_pnl_sum_vnd"] == pytest.approx(130_000.0, rel=1e-3)
    assert out["valuation_method"] == "MARK_TO_MARKET_DAILY_CLOSE"
    assert out["unrealized_pnl_vnd"] == pytest.approx(100.0, rel=1e-3)


@pytest.mark.postgres
def test_get_monitoring_summary_all_returns_both_modes(trading_db: str) -> None:
    ensure_trading_core_tables()
    out = get_monitoring_summary_all()
    assert "captured_at" in out
    assert set(out["modes"].keys()) == {"REAL", "DEMO"}
    for mode in ("REAL", "DEMO"):
        block = out["modes"][mode]
        assert block["bot_status"] in {"OK", "HALTED", "DEGRADED", "ERROR"}
        assert "kill_switch" in block
        assert block["metrics"]["account_mode"] == mode


@pytest.mark.postgres
def test_list_recent_alerts_returns_inserted_rows(trading_db: str) -> None:
    ensure_trading_core_tables()
    _purge_unit_monitoring_rows(trading_db)
    for i in range(3):
        insert_alert_record(
            account_mode="DEMO",
            rule_id=f"unit_msvc_list_{i}",
            severity="LOW",
            message=f"msg-{i}",
            payload={"i": i},
        )
    rows = list_recent_alerts(limit=200)
    ours = [r for r in rows if str(r.get("rule_id", "")).startswith("unit_msvc_list_")]
    assert len(ours) == 3


@pytest.mark.postgres
def test_evaluate_alerts_for_modes_filters_invalid_and_runs_both(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.monitoring_service as msvc

    monkeypatch.setattr(
        msvc,
        "evaluate_alert_rules",
        lambda mode: [{"rule_id": f"stub:{mode}", "severity": "LOW"}],
    )
    out = evaluate_alerts_for_modes(["demo", "bogus", "real"])
    assert out["evaluated_modes"] == ["DEMO", "REAL"]
    assert len(out["alerts_raised"]) == 2
    modes_fired = {a["rule_id"].split(":")[1] for a in out["alerts_raised"]}
    assert modes_fired == {"DEMO", "REAL"}


@pytest.mark.postgres
def test_evaluate_alerts_for_modes_defaults_when_none(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.monitoring_service as msvc

    monkeypatch.setattr(msvc, "evaluate_alert_rules", lambda mode: [{"rule_id": f"x:{mode}"}])
    out = evaluate_alerts_for_modes(None)
    assert out["evaluated_modes"] == ["REAL", "DEMO"]
    assert len(out["alerts_raised"]) == 2


@pytest.mark.postgres
def test_build_account_monitoring_dashboard_merges_blocks(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.monitoring_service as msvc

    monkeypatch.setattr(msvc, "_last_daily_close_mark", lambda _sym: (None, "stub"))
    ensure_trading_core_tables()
    dash = build_account_monitoring_dashboard("DEMO")
    assert dash["account_mode"] == "DEMO"
    assert "operational_health" in dash
    assert dash["operational_health"]["metrics"]["account_mode"] == "DEMO"
    assert "kpis" in dash
    assert "valuation_method" in dash["kpis"]


@pytest.mark.postgres
def test_evaluate_alert_rules_logs_delivery_channel_failure(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.core.config as cfg
    import app.services.monitoring_service as msvc

    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM alert_logs WHERE rule_id = 'drawdown_breach:DEMO'")
        conn.commit()

    monkeypatch.setattr(cfg.settings, "monitoring_drawdown_alert_pct", 5.0)

    def metrics(mode: str) -> dict:
        out = _base_metrics(mode)
        out["drawdown_proxy_pct"] = 99.0
        return out

    def flaky_dispatch(_message: str) -> dict:
        return {
            "telegram": {"enabled": True, "delivered": False},
            "slack": {"enabled": False, "delivered": False},
        }

    monkeypatch.setattr(msvc, "compute_operational_metrics", metrics)
    monkeypatch.setattr(msvc, "dispatch_alert_to_channels", flaky_dispatch)

    before = len(list_risk_events("DEMO", limit=200, event_type="ALERT_DELIVERY_ERROR"))
    fired = evaluate_alert_rules("DEMO")
    assert any(r.get("rule_id") == "drawdown_breach:DEMO" for r in fired)
    after = list_risk_events("DEMO", limit=200, event_type="ALERT_DELIVERY_ERROR")
    assert len(after) >= before + 1
    assert any(e.get("payload", {}).get("rule_id") == "drawdown_breach:DEMO" for e in after)


@pytest.mark.postgres
def test_evaluate_alert_rules_logs_delivery_dispatch_exception(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.core.config as cfg
    import app.services.monitoring_service as msvc

    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM alert_logs WHERE rule_id = 'drawdown_breach:DEMO'")
        conn.commit()

    monkeypatch.setattr(cfg.settings, "monitoring_drawdown_alert_pct", 5.0)

    def metrics(mode: str) -> dict:
        out = _base_metrics(mode)
        out["drawdown_proxy_pct"] = 50.0
        return out

    def boom(_message: str) -> dict:
        raise RuntimeError("dispatch_boom")

    monkeypatch.setattr(msvc, "compute_operational_metrics", metrics)
    monkeypatch.setattr(msvc, "dispatch_alert_to_channels", boom)

    before = len(list_risk_events("DEMO", limit=200, event_type="ALERT_DELIVERY_ERROR"))
    evaluate_alert_rules("DEMO")
    after = list_risk_events("DEMO", limit=200, event_type="ALERT_DELIVERY_ERROR")
    assert len(after) >= before + 1
    assert any("dispatch_boom" in str(e.get("payload", {}).get("error", "")) for e in after)


@pytest.mark.postgres
def test_ensure_monitoring_tables_idempotent(trading_db: str) -> None:
    ensure_monitoring_tables()
    ensure_monitoring_tables()
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('bot_health_snapshots', 'alert_logs')
                """
            )
            n = cur.fetchone()[0]
    assert n == 2


@pytest.mark.postgres
def test_insert_health_snapshot_invalid_account_mode_raises(trading_db: str) -> None:
    ensure_trading_core_tables()
    ensure_monitoring_tables()
    with pytest.raises(CheckViolation):
        insert_health_snapshot("NOT_A_MODE", "OK", {"probe": True})


@pytest.mark.postgres
def test_insert_health_snapshot_roundtrip(trading_db: str) -> None:
    ensure_trading_core_tables()
    row = insert_health_snapshot("DEMO", "OK", {"equity_proxy": 12.5})
    assert row.get("account_mode") == "DEMO"
    assert row.get("bot_status") == "OK"
    assert row.get("id")
    metrics = row.get("metrics")
    assert metrics is not None
    assert float(metrics.get("equity_proxy")) == pytest.approx(12.5)


@pytest.mark.postgres
def test_compute_operational_metrics_stale_signal_from_db(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.core.config as cfg
    from app.services.monitoring_service import _classify_bot_status

    ensure_trading_core_tables()
    ensure_signals_table()
    monkeypatch.setattr(cfg.settings, "monitoring_signal_stale_minutes", 1)

    old_ts = datetime.now(tz=timezone.utc) - timedelta(hours=4)
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM signals")
            cur.execute(
                """
                INSERT INTO signals (
                    id, strategy_type, symbol, action, confidence, reason, created_at
                ) VALUES (
                    %(id)s, 'SHORT_TERM', 'TSTSTALE', 'HOLD', 50.0, 'stale metrics probe', %(ts)s
                )
                """,
                {"id": uuid4(), "ts": old_ts},
            )
        conn.commit()

    metrics = compute_operational_metrics("DEMO")
    assert metrics["signal_missing"] is False
    assert metrics["signal_age_seconds"] is not None
    assert metrics["signal_age_seconds"] >= 3_600
    assert _classify_bot_status(metrics, kill_active=False) == "DEGRADED"


@pytest.mark.postgres
def test_evaluate_alert_rules_stale_signal_fires_from_db(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.core.config as cfg
    import app.services.monitoring_service as msvc

    ensure_trading_core_tables()
    ensure_signals_table()
    monkeypatch.setattr(cfg.settings, "monitoring_signal_stale_minutes", 1)
    monkeypatch.setattr(cfg.settings, "monitoring_drawdown_alert_pct", 99.0)
    monkeypatch.setattr(msvc, "dispatch_alert_to_channels", _dispatch_stub)

    old_ts = datetime.now(tz=timezone.utc) - timedelta(hours=5)
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM signals")
            cur.execute(
                """
                INSERT INTO signals (
                    id, strategy_type, symbol, action, confidence, reason, created_at
                ) VALUES (
                    %(id)s, 'SHORT_TERM', 'TSTSTALE', 'HOLD', 50.0, 'stale alert probe', %(ts)s
                )
                """,
                {"id": uuid4(), "ts": old_ts},
            )
            cur.execute("DELETE FROM alert_logs WHERE rule_id = 'stale_signals:DEMO'")
        conn.commit()

    fired = evaluate_alert_rules("DEMO")
    assert any(r.get("rule_id") == "stale_signals:DEMO" for r in fired)


@pytest.mark.postgres
def test_build_account_dashboard_kpis_error_path(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.monitoring_service as msvc

    def boom_kpis(_mode: str):
        raise RuntimeError("kpi_assembly_failed")

    monkeypatch.setattr(msvc, "compute_dashboard_trading_kpis", boom_kpis)
    ensure_trading_core_tables()
    dash = build_account_monitoring_dashboard("DEMO")
    assert dash["kpis"].get("kpis_error") == "kpi_assembly_failed"
    assert "drawdown_proxy_pct" in dash["kpis"]


def _insert_fresh_signal(trading_db: str, symbol: str = "TSTFRESH") -> None:
    ensure_signals_table()
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM signals WHERE symbol = %(sym)s", {"sym": symbol})
            cur.execute(
                """
                INSERT INTO signals (
                    id, strategy_type, symbol, action, confidence, reason, created_at
                ) VALUES (
                    %(id)s, 'SHORT_TERM', %(sym)s, 'HOLD', 50.0, 'evaluate_alert_rules full-path probe', NOW()
                )
                """,
                {"id": uuid4(), "sym": symbol},
            )
        conn.commit()


def _purge_probe_health_snapshots(trading_db: str) -> None:
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bot_health_snapshots WHERE account_mode = 'DEMO' AND metrics->>'unit_probe' = 'drawdown_path'"
            )
        conn.commit()


@pytest.mark.postgres
def test_evaluate_alert_rules_drawdown_full_path_real_metrics(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real ``compute_operational_metrics`` + DB peak snapshot; only outbound dispatch stubbed."""
    import app.core.config as cfg
    import app.services.monitoring_service as msvc

    ensure_trading_core_tables()
    ensure_monitoring_tables()
    _insert_fresh_signal(trading_db)
    _purge_probe_health_snapshots(trading_db)

    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM alert_logs WHERE rule_id IN ('drawdown_breach:DEMO','execution_errors:DEMO','stale_signals:DEMO')"
            )
        conn.commit()

    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_health_snapshots (id, account_mode, captured_at, bot_status, metrics)
                VALUES (%(id)s, 'DEMO', NOW(), 'OK', %(metrics)s::jsonb)
                """,
                {
                    "id": uuid4(),
                    "metrics": json.dumps({"equity_proxy": 1000.0, "unit_probe": "drawdown_path"}),
                },
            )
        conn.commit()

    monkeypatch.setattr(cfg.settings, "monitoring_drawdown_alert_pct", 15.0)
    monkeypatch.setattr(cfg.settings, "monitoring_error_count_threshold", 500)
    monkeypatch.setattr(msvc, "dispatch_alert_to_channels", _dispatch_stub)

    m = compute_operational_metrics("DEMO")
    assert m["drawdown_proxy_pct"] >= 99.0

    before_dd = len(list_risk_events("DEMO", limit=100, event_type="DRAWDOWN_ALERT"))
    fired = evaluate_alert_rules("DEMO")
    assert any(r.get("rule_id") == "drawdown_breach:DEMO" for r in fired)
    after_dd = list_risk_events("DEMO", limit=100, event_type="DRAWDOWN_ALERT")
    assert len(after_dd) >= before_dd + 1

    _purge_probe_health_snapshots(trading_db)


@pytest.mark.postgres
def test_evaluate_alert_rules_execution_errors_high_and_critical_real_metrics(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rejected order counts from ``orders_core`` only; thresholds via settings; dispatch stubbed."""
    import app.core.config as cfg
    import app.services.monitoring_service as msvc

    ensure_trading_core_tables()
    ensure_monitoring_tables()
    _insert_fresh_signal(trading_db, symbol="TSTEXMET")

    monkeypatch.setattr(cfg.settings, "monitoring_drawdown_alert_pct", 99.0)
    monkeypatch.setattr(cfg.settings, "monitoring_error_count_threshold", 5)
    monkeypatch.setattr(cfg.settings, "monitoring_error_window_minutes", 60)
    monkeypatch.setattr(msvc, "dispatch_alert_to_channels", _dispatch_stub)

    now = datetime.now(tz=timezone.utc)
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM alert_logs WHERE rule_id IN ('drawdown_breach:DEMO','execution_errors:DEMO','stale_signals:DEMO')"
            )
        conn.commit()

    def _run_case(rejected_count: int, expect_severity: str) -> None:
        with connect(trading_db) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM alert_logs WHERE rule_id IN ('drawdown_breach:DEMO','execution_errors:DEMO','stale_signals:DEMO')"
                )
                cur.execute(
                    "DELETE FROM orders_core WHERE symbol = %(sym)s AND account_mode = 'DEMO'",
                    {"sym": "TSTEXSEV"},
                )
                for i in range(rejected_count):
                    cur.execute(
                        """
                        INSERT INTO orders_core (
                            id, account_mode, symbol, side, quantity, price, status, reason, created_at, updated_at
                        ) VALUES (
                            %(id)s, 'DEMO', 'TSTEXSEV', 'BUY', 1, 1000.0, 'REJECTED', 'full-path probe', %(ts)s, %(ts)s
                        )
                        """,
                        {"id": uuid4(), "ts": now},
                    )
            conn.commit()

        before_exec = len(list_risk_events("DEMO", limit=200, event_type="EXECUTION_ERRORS_ALERT"))
        fired = evaluate_alert_rules("DEMO")
        hit = next((r for r in fired if r.get("rule_id") == "execution_errors:DEMO"), None)
        assert hit is not None
        assert hit["severity"] == expect_severity
        after_exec = list_risk_events("DEMO", limit=200, event_type="EXECUTION_ERRORS_ALERT")
        assert len(after_exec) >= before_exec + 1

    _run_case(5, "HIGH")
    _run_case(10, "CRITICAL")
