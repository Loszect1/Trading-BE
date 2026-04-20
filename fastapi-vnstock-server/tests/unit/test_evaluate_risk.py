"""Pure unit tests for ``evaluate_risk`` (no database)."""

from __future__ import annotations

import pytest

from app.services.trading_core_service import evaluate_risk


def _base_payload(**overrides):
    base = {
        "stoploss_price": 90.0,
        "entry_price": 100.0,
        "nav": 1_000_000.0,
        "risk_per_trade": 0.01,
        "daily_new_orders": 0,
        "max_daily_new_orders": 10,
    }
    base.update(overrides)
    return base


def test_evaluate_risk_ok_suggested_size():
    r = evaluate_risk(_base_payload())
    assert r["pass"] is True
    assert r["reason"] == "ok"
    # distance 10, risk 1% of 1M = 10_000 / 10 = 1000
    assert r["suggested_size"] == 1000


def test_evaluate_risk_zero_stop_distance_rejected():
    r = evaluate_risk(_base_payload(stoploss_price=100.0, entry_price=100.0))
    assert r["pass"] is False
    assert r["reason"] == "invalid_stoploss_distance"
    assert r["suggested_size"] == 0


def test_evaluate_risk_daily_limit_no_longer_blocks():
    r = evaluate_risk(_base_payload(daily_new_orders=10, max_daily_new_orders=10))
    assert r["pass"] is True
    assert r["reason"] == "ok"


def test_evaluate_risk_size_too_small_rounds_to_zero():
    r = evaluate_risk(
        _base_payload(
            nav=100.0,
            risk_per_trade=0.001,
            entry_price=100.0,
            stoploss_price=90.0,
        )
    )
    assert r["suggested_size"] == 0
    assert r["pass"] is False
    assert r["reason"] == "size_too_small"


@pytest.mark.parametrize(
    "entry,stop,expected",
    [
        (100.0, 110.0, 1000),
        (100.0, 90.0, 1000),
    ],
)
def test_evaluate_risk_symmetric_distance(entry, stop, expected):
    r = evaluate_risk(_base_payload(entry_price=entry, stoploss_price=stop))
    assert r["suggested_size"] == expected
