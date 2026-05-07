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
