from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo


def test_real_scan_only_scheduler_runs_one_grid_tick(monkeypatch):
    import app.services.automation_scheduler_service as svc

    now_local = datetime(2026, 5, 7, 10, 15, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
    calls: list[str] = []

    async def fake_to_thread(func, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        calls.append(getattr(func, "__name__", "unknown"))
        return None

    async def stop_after_tick(_seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(
        svc,
        "_scheduler_grid_snapshot",
        lambda: {"on_grid": True, "now_local": now_local, "next_grid_run_at": None},
    )
    monkeypatch.setattr(svc.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(svc.asyncio, "sleep", stop_after_tick)

    svc._runtime_real_scan_only_enabled = True
    svc._real_scan_only_last_slot_marker = None

    try:
        try:
            asyncio.run(svc._real_scan_only_scheduler_loop())
        except asyncio.CancelledError:
            pass

        assert calls == ["_run_real_scan_only_and_persist"]
        assert svc._real_scan_only_last_slot_marker == "2026-05-07-10:15"
    finally:
        svc._runtime_real_scan_only_enabled = False
        svc._real_scan_only_last_slot_marker = None


def test_demo_short_term_nav_uses_shared_session_cash(monkeypatch):
    import app.services.automation_scheduler_service as svc

    monkeypatch.setattr(svc, "get_demo_session_cash_balance", lambda session_id: 37_500_000.0)

    assert svc._short_term_allocated_nav_for_mode("DEMO", "demo-session") == 37_500_000.0


def test_demo_portfolio_review_schedule_times_parser():
    import app.services.automation_scheduler_service as svc

    assert svc._parse_demo_portfolio_review_schedule_times("17:00,12:00,17") == ((12, 0), (17, 0))
    assert svc._parse_demo_portfolio_review_schedule_times("bad,25:00") == ((12, 0), (17, 0))


def test_demo_portfolio_review_scheduler_runs_one_due_tick(monkeypatch):
    import app.services.automation_scheduler_service as svc

    now_local = datetime(2026, 5, 7, 12, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
    calls: list[dict] = []

    async def fake_to_thread(func, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        calls.append({"func": getattr(func, "__name__", "unknown"), "kwargs": kwargs})
        return {
            "session_id": "demo-1",
            "run_status": "COMPLETED",
            "applied_count": 1,
            "skipped_count": 0,
        }

    async def stop_after_tick(_seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(svc.settings, "demo_portfolio_review_scheduler_enabled", True)
    monkeypatch.setattr(svc.settings, "demo_portfolio_review_schedule_times_csv", "12:00,17:00")
    monkeypatch.setattr(svc.settings, "demo_portfolio_review_timezone", "Asia/Ho_Chi_Minh")
    monkeypatch.setattr(svc, "get_vn_market_holiday_dates", lambda: frozenset())
    monkeypatch.setattr(svc, "_demo_portfolio_review_scheduler_snapshot", lambda: {
        "enabled": True,
        "timezone": "Asia/Ho_Chi_Minh",
        "schedule_times": ["12:00", "17:00"],
        "now_local": now_local,
        "is_market_workday": True,
        "due": True,
        "marker": "2026-05-07-12:00",
    })
    monkeypatch.setattr(svc.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(svc.asyncio, "sleep", stop_after_tick)

    svc._active_demo_session_id = "demo-1"
    svc._demo_portfolio_review_run_markers.clear()

    try:
        try:
            asyncio.run(svc._demo_portfolio_review_scheduler_loop())
        except asyncio.CancelledError:
            pass

        assert calls == [
            {
                "func": "run_demo_portfolio_review_once",
                "kwargs": {
                    "demo_session_id": "demo-1",
                    "trigger_source": "scheduler",
                    "trigger_marker": "2026-05-07-12:00",
                },
            }
        ]
        assert "2026-05-07-12:00" in svc._demo_portfolio_review_run_markers
    finally:
        svc._active_demo_session_id = None
        svc._demo_portfolio_review_run_markers.clear()
