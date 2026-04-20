"""
Replay-style scenario: deterministic clock steps drive BUY → blocked SELL →
post-settlement SELL, asserting intermediate settlement snapshots.

This replays a scripted timeline (similar to a JSON event log) in-process.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.services.trading_core_service import check_settlement, place_order


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, 10, 0, tzinfo=timezone.utc)


@pytest.mark.postgres
def test_replay_t_plus_2_settlement_timeline(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    """
    Scripted steps (replay log):

    1) T0 Monday — BUY fills, inventory pending settlement.
    2) Same day — SELL blocked; settlement check shows zero available.
    3) After settle window — SELL fills.
    """
    import app.services.trading_core_service as tcs

    steps = [
        ("buy_day", _dt(2026, 1, 5)),
        ("sell_blocked_day", _dt(2026, 1, 6)),
        ("after_settle", _dt(2026, 1, 12)),
    ]
    clock = {"t": steps[0][1]}

    def _now():
        return clock["t"]

    monkeypatch.setattr(tcs, "_utc_now", _now)
    monkeypatch.setattr(tcs, "_settlement_effective_today", lambda: clock["t"].date())

    # Step 1
    buy = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTREP",
            "side": "BUY",
            "quantity": 40,
            "price": 8000.0,
            "idempotency_key": "idem-tstrep-buy",
            "auto_process": True,
        }
    )
    assert buy["status"] == "FILLED"

    st = check_settlement("DEMO", "TSTREP", 40)
    assert st["pass"] is False

    sell_fail = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTREP",
            "side": "SELL",
            "quantity": 40,
            "price": 8200.0,
            "idempotency_key": "idem-tstrep-sell-a",
            "auto_process": True,
        }
    )
    assert sell_fail["status"] == "REJECTED"

    # Step 2 — still before automatic availability
    clock["t"] = steps[1][1]
    st2 = check_settlement("DEMO", "TSTREP", 1)
    assert st2["available_qty"] == 0

    # Step 3 — calendar time past settle_date + roll
    clock["t"] = steps[2][1]
    st3 = check_settlement("DEMO", "TSTREP", 40)
    assert st3["pass"] is True
    assert st3["available_qty"] == 40

    sell_ok = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTREP",
            "side": "SELL",
            "quantity": 40,
            "price": 8200.0,
            "idempotency_key": "idem-tstrep-sell-b",
            "auto_process": True,
        }
    )
    assert sell_ok["status"] == "FILLED"
