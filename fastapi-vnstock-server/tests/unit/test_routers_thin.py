"""
Thin FastAPI router tests: TestClient + monkeypatched service layer.

No Postgres, no external network. Exercises HTTP success paths and HTTPException mapping.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.routers.automation import router as automation_router
from app.routers.experience import router as experience_router
from app.routers.health import router as health_router
from app.routers.monitoring import router as monitoring_router
from app.routers.scanner import router as scanner_router
from app.routers.signal_engine import router as signal_engine_router
from app.routers.trading_core import router as trading_core_router


def _sched_status_dict(enabled: bool = True, account_mode: str = "DEMO") -> dict:
    return {
        "account_mode": account_mode,
        "enabled": enabled,
        "running": False,
        "poll_seconds": 60,
        "interval_minutes": 15,
        "timezone": "Asia/Ho_Chi_Minh",
    }


def _trading_client() -> TestClient:
    app = FastAPI()
    app.include_router(trading_core_router)
    return TestClient(app)


def _monitoring_client() -> TestClient:
    app = FastAPI()
    app.include_router(monitoring_router)
    return TestClient(app)


def _automation_client() -> TestClient:
    app = FastAPI()
    app.include_router(automation_router)
    return TestClient(app)


def _scanner_client() -> TestClient:
    app = FastAPI()
    app.include_router(scanner_router)
    return TestClient(app)


def _signal_engine_client() -> TestClient:
    app = FastAPI()
    app.include_router(signal_engine_router)
    return TestClient(app)


def _experience_client() -> TestClient:
    app = FastAPI()
    app.include_router(experience_router)
    return TestClient(app)


def _health_client() -> TestClient:
    app = FastAPI()
    app.include_router(health_router)
    return TestClient(app)


class TestMonitoringRouterThin:
    def test_summary_legacy_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.monitoring.get_monitoring_summary_all",
            lambda: {"modes": {"DEMO": {"ok": True}}},
        )
        monkeypatch.setattr("app.routers.monitoring.list_recent_alerts", lambda limit=30: [])
        client = _monitoring_client()
        r = client.get("/monitoring/summary")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert "recent_alerts" in body["data"]

    def test_summary_dashboard_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.monitoring.build_account_monitoring_dashboard",
            lambda mode: {"account_mode": mode, "kpis": {}},
        )
        monkeypatch.setattr("app.routers.monitoring.list_recent_alerts", lambda limit=30: [{"id": "1"}])
        client = _monitoring_client()
        r = client.get("/monitoring/summary", params={"account_mode": "DEMO"})
        assert r.status_code == 200
        assert r.json()["data"]["account_mode"] == "DEMO"

    def test_summary_value_error_400(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.monitoring.build_account_monitoring_dashboard",
            lambda mode: (_ for _ in ()).throw(ValueError("invalid account mode")),
        )
        client = _monitoring_client()
        r = client.get("/monitoring/summary", params={"account_mode": "REAL"})
        assert r.status_code == 400
        assert "invalid account mode" in r.json()["detail"]

    def test_summary_unexpected_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.monitoring.get_monitoring_summary_all",
            lambda: (_ for _ in ()).throw(RuntimeError("db down")),
        )
        client = _monitoring_client()
        r = client.get("/monitoring/summary")
        assert r.status_code == 500
        assert "db down" in r.json()["detail"]

    def test_evaluate_alerts_success_and_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.monitoring.evaluate_alerts_for_modes",
            lambda modes: {"evaluated": modes or ["REAL", "DEMO"]},
        )
        client = _monitoring_client()
        r = client.post("/monitoring/evaluate-alerts", json={})
        assert r.status_code == 200
        assert r.json()["data"]["evaluated"] == ["REAL", "DEMO"]

        monkeypatch.setattr(
            "app.routers.monitoring.evaluate_alerts_for_modes",
            lambda modes: (_ for _ in ()).throw(RuntimeError("alert boom")),
        )
        r2 = client.post("/monitoring/evaluate-alerts", json={"account_modes": ["DEMO"]})
        assert r2.status_code == 500
        assert "alert boom" in r2.json()["detail"]

    def test_kill_switch_success_and_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.monitoring.set_kill_switch",
            lambda mode, active, reason=None: {"account_mode": mode, "active": active, "reason": reason},
        )
        client = _monitoring_client()
        r = client.post(
            "/monitoring/kill-switch",
            json={"account_mode": "DEMO", "active": True, "reason": "test"},
        )
        assert r.status_code == 200
        assert r.json()["data"]["active"] is True

        monkeypatch.setattr(
            "app.routers.monitoring.set_kill_switch",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("kill fail")),
        )
        r2 = client.post("/monitoring/kill-switch", json={"account_mode": "DEMO", "active": False})
        assert r2.status_code == 500


class TestAutomationRouterThin:
    def test_run_cycle_success_http_exception_passthrough_and_500(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        payload = {
            "success": True,
            "run_status": "COMPLETED",
            "scanned": 2,
            "buy_candidates": 0,
            "risk_rejected": 0,
            "executed": 0,
            "execution_rejected": 0,
            "errors": 0,
            "detail": {},
        }
        monkeypatch.setattr(
            "app.routers.automation.run_short_term_production_cycle",
            lambda **kwargs: payload,
        )
        client = _automation_client()
        r = client.post("/automation/short-term/run-cycle", json={})
        assert r.status_code == 200
        assert r.json()["run_status"] == "COMPLETED"

        def _raise_http(**kwargs):
            raise HTTPException(status_code=429, detail="rate limited")

        monkeypatch.setattr("app.routers.automation.run_short_term_production_cycle", _raise_http)
        r_http = client.post("/automation/short-term/run-cycle", json={})
        assert r_http.status_code == 429
        assert r_http.json()["detail"] == "rate limited"

        monkeypatch.setattr(
            "app.routers.automation.run_short_term_production_cycle",
            lambda **kwargs: (_ for _ in ()).throw(ValueError("broken cycle")),
        )
        r500 = client.post("/automation/short-term/run-cycle", json={})
        assert r500.status_code == 500
        assert "broken cycle" in r500.json()["detail"]

    def test_last_run_none_success_and_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("app.routers.automation.get_last_short_term_automation_run", lambda: None)
        client = _automation_client()
        r = client.get("/automation/short-term/last-run")
        assert r.status_code == 200
        assert r.json()["data"] is None

        row = {
            "id": "00000000-0000-0000-0000-000000000001",
            "started_at": datetime(2026, 4, 1, 9, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 4, 1, 9, 1, tzinfo=timezone.utc),
            "run_status": "COMPLETED",
            "scanned": 1,
            "buy_candidates": 0,
            "risk_rejected": 0,
            "executed": 0,
            "execution_rejected": 0,
            "errors": 0,
            "detail": {},
        }
        monkeypatch.setattr(
            "app.routers.automation.get_last_short_term_automation_run",
            lambda: row,
        )
        r2 = client.get("/automation/short-term/last-run")
        assert r2.status_code == 200
        assert r2.json()["data"]["run_status"] == "COMPLETED"

        monkeypatch.setattr(
            "app.routers.automation.get_last_short_term_automation_run",
            lambda: (_ for _ in ()).throw(RuntimeError("db read")),
        )
        r3 = client.get("/automation/short-term/last-run")
        assert r3.status_code == 500

    def test_scheduler_status_and_toggle(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.automation.get_automation_scheduler_status",
            lambda account_mode="DEMO": _sched_status_dict(True, account_mode),
        )
        toggle = AsyncMock(return_value=_sched_status_dict(False))
        monkeypatch.setattr("app.routers.automation.set_automation_scheduler_enabled", toggle)
        client = _automation_client()
        r = client.get("/automation/scheduler/status?account_mode=REAL")
        assert r.status_code == 200
        assert r.json()["enabled"] is True
        assert r.json()["account_mode"] == "REAL"

        r2 = client.post("/automation/scheduler/toggle", json={"account_mode": "REAL", "enabled": False})
        assert r2.status_code == 200
        assert r2.json()["enabled"] is False
        toggle.assert_awaited_once()

        monkeypatch.setattr(
            "app.routers.automation.get_automation_scheduler_status",
            lambda account_mode="DEMO": (_ for _ in ()).throw(RuntimeError("status fail")),
        )
        r3 = client.get("/automation/scheduler/status")
        assert r3.status_code == 500

        monkeypatch.setattr(
            "app.routers.automation.get_automation_scheduler_status",
            lambda account_mode="DEMO": _sched_status_dict(True, account_mode),
        )
        toggle2 = AsyncMock(side_effect=RuntimeError("toggle fail"))
        monkeypatch.setattr("app.routers.automation.set_automation_scheduler_enabled", toggle2)
        r4 = client.post("/automation/scheduler/toggle", json={"account_mode": "DEMO", "enabled": True})
        assert r4.status_code == 500


class TestTradingCoreRouterThin:
    def test_risk_events_and_evaluate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("app.routers.trading_core.list_risk_events", lambda *a, **k: [{"id": "e1"}])
        monkeypatch.setattr(
            "app.routers.trading_core.evaluate_risk",
            lambda payload: {"pass": True, "reason": "ok"},
        )
        client = _trading_client()
        r = client.get("/risk/events")
        assert r.status_code == 200
        assert r.json()["data"][0]["id"] == "e1"

        monkeypatch.setattr(
            "app.routers.trading_core.list_risk_events",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("events fail")),
        )
        assert client.get("/risk/events").status_code == 500

        monkeypatch.setattr("app.routers.trading_core.list_risk_events", lambda *a, **k: [])
        r_ok = client.post(
            "/risk/evaluate",
            json={
                "symbol": "VNM",
                "nav": 1e9,
                "entry_price": 10,
                "stoploss_price": 9,
            },
        )
        assert r_ok.status_code == 200
        assert r_ok.json()["data"]["pass"] is True

        monkeypatch.setattr(
            "app.routers.trading_core.evaluate_risk",
            lambda payload: (_ for _ in ()).throw(RuntimeError("risk fail")),
        )
        risk_body = {
            "symbol": "VNM",
            "nav": 1e9,
            "entry_price": 10,
            "stoploss_price": 9,
        }
        assert client.post("/risk/evaluate", json=risk_body).status_code == 500

    def test_check_settlement_execution_place_variants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.trading_core.check_settlement",
            lambda mode, sym, qty: {"pass": True, "available": qty},
        )
        client = _trading_client()
        r = client.post(
            "/risk/check-settlement",
            json={"account_mode": "DEMO", "symbol": "VNM", "quantity": 10},
        )
        assert r.status_code == 200
        assert r.json()["data"]["pass"] is True

        monkeypatch.setattr(
            "app.routers.trading_core.check_settlement",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("settlement boom")),
        )
        assert (
            client.post(
                "/risk/check-settlement",
                json={"account_mode": "DEMO", "symbol": "VNM", "quantity": 1},
            ).status_code
            == 500
        )

        def _risk_fail(payload):
            return {"pass": False, "reason": "max_daily"}

        monkeypatch.setattr("app.routers.trading_core.check_settlement", lambda *a, **k: {"pass": True})
        monkeypatch.setattr("app.routers.trading_core.evaluate_risk", _risk_fail)
        buy_reject = client.post(
            "/execution/place",
            json={
                "account_mode": "DEMO",
                "symbol": "VNM",
                "side": "BUY",
                "quantity": 100,
                "price": 10.0,
            },
        )
        assert buy_reject.status_code == 200
        assert buy_reject.json()["success"] is False
        assert "risk_reject" in buy_reject.json()["data"]["reason"]

        monkeypatch.setattr(
            "app.routers.trading_core.evaluate_risk",
            lambda payload: {"pass": True, "reason": "ok"},
        )
        monkeypatch.setattr(
            "app.routers.trading_core.place_order",
            lambda payload: {"id": "ord1", "status": "NEW"},
        )
        buy_ok = client.post(
            "/execution/place",
            json={
                "account_mode": "DEMO",
                "symbol": "VNM",
                "side": "BUY",
                "quantity": 100,
                "price": 10.0,
            },
        )
        assert buy_ok.status_code == 200
        assert buy_ok.json()["data"]["id"] == "ord1"

        monkeypatch.setattr(
            "app.routers.trading_core.place_order",
            lambda payload: (_ for _ in ()).throw(RuntimeError("place fail")),
        )
        place_500 = client.post(
            "/execution/place",
            json={
                "account_mode": "DEMO",
                "symbol": "VNM",
                "side": "BUY",
                "quantity": 100,
                "price": 10.0,
            },
        )
        assert place_500.status_code == 500

        monkeypatch.setattr(
            "app.routers.trading_core.place_order",
            lambda payload: {"id": "ord2", "status": "NEW"},
        )
        monkeypatch.setattr(
            "app.routers.trading_core.check_settlement",
            lambda mode, sym, qty: {"pass": False, "available": 0},
        )
        sell_guard = client.post(
            "/execution/place",
            json={
                "account_mode": "DEMO",
                "symbol": "VNM",
                "side": "SELL",
                "quantity": 100,
                "price": 10.0,
            },
        )
        assert sell_guard.status_code == 200
        assert sell_guard.json()["success"] is False
        assert sell_guard.json()["data"]["reason"] == "settlement_guard_failed"

        monkeypatch.setattr(
            "app.routers.trading_core.check_settlement",
            lambda mode, sym, qty: {"pass": True},
        )
        monkeypatch.setattr(
            "app.routers.trading_core.place_order",
            lambda payload: {"reason": "kill_switch_active", "status": "REJECTED"},
        )
        sell_ks = client.post(
            "/execution/place",
            json={
                "account_mode": "DEMO",
                "symbol": "VNM",
                "side": "SELL",
                "quantity": 10,
                "price": 10.0,
            },
        )
        assert sell_ks.status_code == 200
        assert sell_ks.json()["success"] is False

    def test_process_reconcile_cancel_orders(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.trading_core.process_order",
            lambda oid: {"processed": True, "order_id": oid},
        )
        monkeypatch.setattr(
            "app.routers.trading_core.reconcile_order",
            lambda oid: {"processed": False, "order_id": oid},
        )
        monkeypatch.setattr(
            "app.routers.trading_core.cancel_order",
            lambda oid, reason: {"cancelled": True, "order_id": oid},
        )
        monkeypatch.setattr(
            "app.routers.trading_core.list_orders",
            lambda mode, limit: [{"id": "o1"}],
        )
        client = _trading_client()
        assert client.post("/execution/process/abc-123").status_code == 200
        assert client.post("/execution/reconcile/abc-123").status_code == 200
        r_cancel = client.post(
            "/execution/cancel",
            json={"order_id": "abc-12345", "reason": "test_cancel"},
        )
        assert r_cancel.status_code == 200
        assert r_cancel.json()["data"]["cancelled"] is True
        assert client.get("/orders").status_code == 200

        monkeypatch.setattr(
            "app.routers.trading_core.list_orders",
            lambda mode, limit: (_ for _ in ()).throw(RuntimeError("orders fail")),
        )
        assert client.get("/orders").status_code == 500

        monkeypatch.setattr("app.routers.trading_core.list_orders", lambda mode, limit: [])
        monkeypatch.setattr(
            "app.routers.trading_core.cancel_order",
            lambda oid, reason: (_ for _ in ()).throw(RuntimeError("cancel fail")),
        )
        assert (
            client.post(
                "/execution/cancel",
                json={"order_id": "abc-12345", "reason": "test_cancel"},
            ).status_code
            == 500
        )

    def test_positions_settlement_portfolio_events(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.trading_core.get_positions",
            lambda mode: [
                {
                    "symbol": "VNM",
                    "total_qty": 100,
                    "available_qty": 50,
                    "pending_settlement_qty": 50,
                    "avg_price": 10.0,
                }
            ],
        )
        monkeypatch.setattr(
            "app.routers.trading_core.get_settlement_rows",
            lambda mode, sym: [
                {
                    "symbol": sym or "VNM",
                    "buy_trade_date": date(2026, 3, 1),
                    "settle_date": date(2026, 3, 5),
                    "qty": 100,
                    "available_qty": 50,
                    "pending_settlement_qty": 50,
                    "avg_price": 10.0,
                }
            ],
        )
        monkeypatch.setattr(
            "app.routers.trading_core.get_portfolio_summary",
            lambda mode: {
                "account_mode": mode,
                "total_symbols": 1,
                "total_qty": 100,
                "total_available_qty": 50,
                "total_pending_settlement_qty": 50,
            },
        )
        monkeypatch.setattr(
            "app.routers.trading_core.get_order_events",
            lambda oid: [{"order_id": oid, "event": "NEW"}],
        )
        client = _trading_client()
        assert client.get("/positions").status_code == 200
        assert client.get("/positions/settlement", params={"symbol": "vnm"}).status_code == 200
        assert client.get("/portfolio/summary").status_code == 200
        assert client.get("/orders/o1/events").status_code == 200

        monkeypatch.setattr(
            "app.routers.trading_core.get_positions",
            lambda mode: (_ for _ in ()).throw(RuntimeError("pos fail")),
        )
        assert client.get("/positions").status_code == 500

    def test_trading_core_remaining_http_500_branches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.trading_core.process_order",
            lambda oid: (_ for _ in ()).throw(RuntimeError("proc fail")),
        )
        monkeypatch.setattr(
            "app.routers.trading_core.reconcile_order",
            lambda oid: (_ for _ in ()).throw(RuntimeError("rec fail")),
        )
        monkeypatch.setattr(
            "app.routers.trading_core.get_order_events",
            lambda oid: (_ for _ in ()).throw(RuntimeError("ev fail")),
        )
        monkeypatch.setattr(
            "app.routers.trading_core.get_settlement_rows",
            lambda mode, sym: (_ for _ in ()).throw(RuntimeError("settle fail")),
        )
        monkeypatch.setattr(
            "app.routers.trading_core.get_portfolio_summary",
            lambda mode: (_ for _ in ()).throw(RuntimeError("port fail")),
        )
        client = _trading_client()
        assert client.post("/execution/process/x").status_code == 500
        assert client.post("/execution/reconcile/x").status_code == 500
        assert client.get("/orders/x/events").status_code == 500
        assert client.get("/positions/settlement").status_code == 500
        assert client.get("/portfolio/summary").status_code == 500


class _BadSessionWindow:
    """Used to trigger scanner router's invalid schedule config (500) branch."""

    name = "bad"

    def slot_times(self, interval_minutes: int):
        raise ValueError("grid misaligned")


class TestScannerRouterThin:
    def test_short_term_schedule_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ref = datetime(2026, 4, 14, 2, 0, tzinfo=timezone.utc)
        upcoming = [
            datetime(2026, 4, 14, 9, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")),
        ]
        day_slots = [
            datetime(2026, 4, 14, 9, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")),
        ]
        monkeypatch.setattr("app.routers.scanner.get_vn_market_holiday_dates", lambda: frozenset())
        monkeypatch.setattr(
            "app.routers.scanner.next_run_datetimes",
            lambda **kwargs: upcoming,
        )
        monkeypatch.setattr(
            "app.routers.scanner.schedule_day_preview",
            lambda *a, **k: (True, day_slots),
        )
        client = _scanner_client()
        r = client.get("/scanner/short-term/schedule", params={"after": ref.isoformat(), "limit": 8})
        assert r.status_code == 200
        body = r.json()
        assert body["timezone"] == "Asia/Ho_Chi_Minh"
        assert len(body["upcoming_runs"]) == 1
        assert body["day_preview_is_trading_weekday"] is True
        assert len(body["sessions"]) == 2

        # Naive `after` is interpreted as local wall time in market TZ (router branch ref.tzinfo is None).
        r_naive = client.get(
            "/scanner/short-term/schedule",
            params={"after": "2026-04-14T09:00:00", "limit": 4},
        )
        assert r_naive.status_code == 200
        assert r_naive.json()["day_preview_date"] == "2026-04-14"

    def test_short_term_schedule_next_run_value_error_422(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "app.routers.scanner.next_run_datetimes",
            lambda **kwargs: (_ for _ in ()).throw(ValueError("bad limit")),
        )
        client = _scanner_client()
        r = client.get("/scanner/short-term/schedule")
        assert r.status_code == 422
        assert "bad limit" in r.json()["detail"]

    def test_short_term_schedule_next_run_runtime_error_500(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "app.routers.scanner.next_run_datetimes",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("calendar exhausted")),
        )
        client = _scanner_client()
        r = client.get("/scanner/short-term/schedule")
        assert r.status_code == 500
        assert "calendar exhausted" in r.json()["detail"]

    def test_short_term_schedule_preview_value_error_422(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "app.routers.scanner.next_run_datetimes",
            lambda **kwargs: [datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc)],
        )
        monkeypatch.setattr(
            "app.routers.scanner.schedule_day_preview",
            lambda *a, **k: (_ for _ in ()).throw(ValueError("unknown day")),
        )
        client = _scanner_client()
        r = client.get("/scanner/short-term/schedule")
        assert r.status_code == 422
        assert "unknown day" in r.json()["detail"]

    def test_short_term_schedule_session_slot_value_error_500(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "app.routers.scanner.next_run_datetimes",
            lambda **kwargs: [datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc)],
        )
        monkeypatch.setattr(
            "app.routers.scanner.schedule_day_preview",
            lambda *a, **k: (False, []),
        )
        monkeypatch.setattr(
            "app.routers.scanner.default_session_windows",
            lambda: (_BadSessionWindow(), _BadSessionWindow()),
        )
        client = _scanner_client()
        r = client.get("/scanner/short-term/schedule")
        assert r.status_code == 500
        assert "Invalid schedule config" in r.json()["detail"]


class TestSignalEngineRouterThin:
    def test_scanner_runs_and_signals_generate_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("app.routers.signal_engine.run_short_term", lambda n: [{"id": "st1"}])
        monkeypatch.setattr("app.routers.signal_engine.run_long_term", lambda n: [{"id": "lt1"}])
        monkeypatch.setattr("app.routers.signal_engine.run_technical", lambda n: [{"id": "tc1"}])
        monkeypatch.setattr(
            "app.routers.signal_engine.generate_all_signals",
            lambda: {"generated": 2},
        )
        client = _signal_engine_client()
        r1 = client.post("/scanner/short-term/run")
        assert r1.status_code == 200
        assert r1.json()["success"] is True
        assert r1.json()["count"] == 1

        r2 = client.post("/scanner/long-term/run")
        assert r2.status_code == 200
        assert r2.json()["count"] == 1

        r3 = client.post("/scanner/technical/run")
        assert r3.status_code == 200
        assert r3.json()["count"] == 1

        r4 = client.post("/signals/generate")
        assert r4.status_code == 200
        assert r4.json()["data"]["generated"] == 2

    def test_scanner_runs_and_signals_errors_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.routers.signal_engine.run_short_term",
            lambda n: (_ for _ in ()).throw(RuntimeError("st boom")),
        )
        client = _signal_engine_client()
        assert "Short-term scanner error" in client.post("/scanner/short-term/run").json()["detail"]

        monkeypatch.setattr(
            "app.routers.signal_engine.run_long_term",
            lambda n: (_ for _ in ()).throw(RuntimeError("lt boom")),
        )
        assert "Long-term scanner error" in client.post("/scanner/long-term/run").json()["detail"]

        monkeypatch.setattr(
            "app.routers.signal_engine.run_technical",
            lambda n: (_ for _ in ()).throw(RuntimeError("tc boom")),
        )
        assert "Technical scanner error" in client.post("/scanner/technical/run").json()["detail"]

        monkeypatch.setattr(
            "app.routers.signal_engine.generate_all_signals",
            lambda: (_ for _ in ()).throw(RuntimeError("gen boom")),
        )
        assert "Generate signals error" in client.post("/signals/generate").json()["detail"]

    def test_signals_list_success_and_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict[str, object] = {}

        def _capture_list(**kwargs):
            seen.update(kwargs)
            return [{"symbol": "VNM"}]

        monkeypatch.setattr("app.routers.signal_engine.list_signals", _capture_list)
        client = _signal_engine_client()
        r = client.get("/signals", params={"symbol": "vnm", "strategy_type": "SHORT_TERM", "limit": 10})
        assert r.status_code == 200
        assert r.json()["count"] == 1
        assert seen["symbol"] == "VNM"

        monkeypatch.setattr(
            "app.routers.signal_engine.list_signals",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("list boom")),
        )
        r2 = client.get("/signals")
        assert r2.status_code == 500
        assert "List signals error" in r2.json()["detail"]


class TestExperienceRouterThin:
    def test_get_experience_success_and_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("app.routers.experience.ensure_experience_table", lambda: None)
        monkeypatch.setattr(
            "app.routers.experience.list_experience",
            lambda **kwargs: [{"id": "e1"}],
        )
        client = _experience_client()
        r = client.get("/experience", params={"account_mode": "DEMO", "limit": 5})
        assert r.status_code == 200
        assert r.json()["count"] == 1

        monkeypatch.setattr(
            "app.routers.experience.list_experience",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("db list")),
        )
        r2 = client.get("/experience")
        assert r2.status_code == 500
        assert "Experience list error" in r2.json()["detail"]

    def test_get_experience_http_exception_passthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("app.routers.experience.ensure_experience_table", lambda: None)

        def _raise_http(**kwargs):
            raise HTTPException(status_code=403, detail="forbidden")

        monkeypatch.setattr("app.routers.experience.list_experience", _raise_http)
        client = _experience_client()
        r = client.get("/experience")
        assert r.status_code == 403
        assert r.json()["detail"] == "forbidden"

    def test_analyze_success_500_and_http_passthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("app.routers.experience.ensure_experience_table", lambda: None)
        monkeypatch.setattr(
            "app.routers.experience.create_experience_from_trade",
            lambda payload: {"id": "new1", "trade_id": payload["trade_id"]},
        )
        client = _experience_client()
        body = {
            "trade_id": "t-1",
            "account_mode": "DEMO",
            "symbol": "VNM",
            "strategy_type": "SHORT_TERM",
            "pnl_value": -50.0,
            "pnl_percent": -0.5,
        }
        r = client.post("/experience/analyze", json=body)
        assert r.status_code == 200
        assert r.json()["data"]["trade_id"] == "t-1"

        monkeypatch.setattr(
            "app.routers.experience.create_experience_from_trade",
            lambda payload: (_ for _ in ()).throw(RuntimeError("insert fail")),
        )
        r2 = client.post("/experience/analyze", json=body)
        assert r2.status_code == 500
        assert "Experience analyze error" in r2.json()["detail"]

        def _http_exc(payload):
            raise HTTPException(status_code=409, detail="dup")

        monkeypatch.setattr("app.routers.experience.create_experience_from_trade", _http_exc)
        r3 = client.post("/experience/analyze", json=body)
        assert r3.status_code == 409


class TestHealthRouterThin:
    def test_health_ok(self) -> None:
        client = _health_client()
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}