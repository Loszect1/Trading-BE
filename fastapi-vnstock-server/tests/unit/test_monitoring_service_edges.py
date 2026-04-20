"""Deterministic unit tests for monitoring_service DB helpers and summary/list paths (no real Postgres)."""

from __future__ import annotations

from typing import Any

import pytest

import app.services.monitoring_service as msvc


class _FakeCursor:
    def __init__(self, stub: "_ConnectStub"):
        self._stub = stub

    def execute(self, query: str, params: Any = None) -> None:
        self._stub.executions.append(("execute", query, params))
        if isinstance(params, dict) and "limit" in params:
            self._stub.last_limit = int(params["limit"])
        if self._stub.raise_on_execute:
            raise self._stub.raise_on_execute

    def fetchall(self) -> list[Any]:
        return list(self._stub.fetchall_rows)

    def fetchone(self) -> Any:
        return self._stub.fetchone_row

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, stub: "_ConnectStub"):
        self._stub = stub

    def cursor(self, row_factory: Any = None) -> _FakeCursor:
        self._stub.cursor_row_factories.append(row_factory)
        return _FakeCursor(self._stub)

    def commit(self) -> None:
        self._stub.commits += 1

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _ConnectStub:
    def __init__(self) -> None:
        self.executions: list[tuple[str, str, Any]] = []
        self.commits = 0
        self.fetchall_rows: list[Any] = []
        self.fetchone_row: Any = None
        self.raise_on_execute: BaseException | None = None
        self.cursor_row_factories: list[Any] = []
        self.last_limit: int | None = None

    def __call__(self, *_args: Any, **_kwargs: Any) -> _FakeConn:
        return _FakeConn(self)


def test_ensure_monitoring_tables_issues_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _ConnectStub()

    monkeypatch.setattr(msvc, "connect", stub)

    msvc.ensure_monitoring_tables()

    assert stub.commits == 1
    assert len(stub.executions) == 1
    _, q, _ = stub.executions[0]
    assert "CREATE TABLE IF NOT EXISTS bot_health_snapshots" in q
    assert "CREATE TABLE IF NOT EXISTS alert_logs" in q


def test_insert_health_snapshot_propagates_execute_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _ConnectStub()
    stub.raise_on_execute = RuntimeError("db_write_failed")

    monkeypatch.setattr(msvc, "ensure_monitoring_tables", lambda: None)
    monkeypatch.setattr(msvc, "connect", stub)

    with pytest.raises(RuntimeError, match="db_write_failed"):
        msvc.insert_health_snapshot("DEMO", "OK", {"x": 1})


def test_insert_health_snapshot_invalid_metrics_typeerror(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(msvc, "ensure_monitoring_tables", lambda: None)
    monkeypatch.setattr(msvc, "connect", _ConnectStub())

    with pytest.raises(TypeError):
        msvc.insert_health_snapshot("DEMO", "OK", {"bad": object()})  # type: ignore[arg-type]


def test_list_recent_alerts_clamps_limit_and_returns_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _ConnectStub()
    stub.fetchall_rows = [
        {
            "id": "a1",
            "account_mode": "DEMO",
            "rule_id": "r1",
            "severity": "LOW",
            "message": "m",
            "payload": {},
            "created_at": None,
        }
    ]
    monkeypatch.setattr(msvc, "ensure_monitoring_tables", lambda: None)
    monkeypatch.setattr(msvc, "connect", stub)

    rows = msvc.list_recent_alerts(limit=9999)
    assert stub.last_limit == 200
    assert len(rows) == 1
    assert rows[0]["rule_id"] == "r1"

    msvc.list_recent_alerts(limit=0)
    assert stub.last_limit == 1


def test_get_monitoring_summary_all_stubbed_no_db(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_metrics(mode: str) -> dict[str, Any]:
        return {
            "account_mode": mode,
            "equity_proxy": 1.0,
            "exposure_proxy": 0.0,
            "peak_equity_proxy_window": 10.0,
            "drawdown_proxy_pct": 0.0,
            "gross_buy_notional_filled_window": 0.0,
            "gross_sell_notional_filled_window": 0.0,
            "rejected_orders_window": 0,
            "filled_orders_window": 0,
            "open_orders_estimate": 0,
            "open_position_symbols": 0,
            "last_signal_at": None,
            "signal_age_seconds": 1.0,
            "signal_missing": False,
        }

    monkeypatch.setattr(msvc, "compute_operational_metrics", fake_metrics)
    monkeypatch.setattr(msvc, "get_kill_switch", lambda _m: {"active": False})

    out = msvc.get_monitoring_summary_all()
    assert set(out["modes"].keys()) == {"REAL", "DEMO"}
    assert out["modes"]["DEMO"]["bot_status"] == "OK"
    assert out["modes"]["REAL"]["metrics"]["account_mode"] == "REAL"


def test_get_monitoring_summary_all_degraded_on_stale_signal_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_metrics(mode: str) -> dict[str, Any]:
        return {
            "account_mode": mode,
            "equity_proxy": 1.0,
            "exposure_proxy": 0.0,
            "peak_equity_proxy_window": 10.0,
            "drawdown_proxy_pct": 0.0,
            "gross_buy_notional_filled_window": 0.0,
            "gross_sell_notional_filled_window": 0.0,
            "rejected_orders_window": 0,
            "filled_orders_window": 0,
            "open_orders_estimate": 0,
            "open_position_symbols": 0,
            "last_signal_at": "2020-01-01T00:00:00+00:00",
            "signal_age_seconds": 999_999.0,
            "signal_missing": False,
        }

    monkeypatch.setattr(msvc, "compute_operational_metrics", fake_metrics)
    monkeypatch.setattr(msvc, "get_kill_switch", lambda _m: {"active": False})
    monkeypatch.setattr(msvc.settings, "monitoring_signal_stale_minutes", 1)

    out = msvc.get_monitoring_summary_all()
    assert out["modes"]["DEMO"]["bot_status"] == "DEGRADED"
