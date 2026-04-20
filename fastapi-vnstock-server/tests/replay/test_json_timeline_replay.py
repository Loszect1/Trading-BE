"""
Replay tests driven by JSON fixtures under ``tests/fixtures/replay/``.

These mirror how a captured production timeline could be re-run in CI: deterministic
clock patches plus service calls, no network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.replay.replay_loader import load_replay_timeline
from tests.replay.replay_runner import run_trading_timeline_steps

_FIX = Path(__file__).resolve().parent.parent / "fixtures" / "replay"


@pytest.mark.postgres
def test_replay_from_json_t_plus_2_timeline(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    timeline = load_replay_timeline(_FIX / "t_plus_2_settlement_timeline.json")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=timeline, tcs_module=tcs)
