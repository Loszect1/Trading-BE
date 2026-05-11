"""Pure unit tests for ``evaluate_risk`` (no database)."""

from __future__ import annotations

import pytest

from app.services.trading_core_service import evaluate_risk


def _base_payload(**overrides):
    base = {
        "stoploss_price": 90.0,
        "entry_price": 100.0,
        "nav": 100_000_000.0,
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
    # 100.0 is thousand-VND notation, so distance is 10,000 VND.
    # Risk 1% of 100M = 1,000,000 / 10,000 = 100 shares.
    assert r["suggested_size"] == 100
    assert r["suggested_lot_size"] == 100


def test_evaluate_risk_zero_stop_distance_rejected():
    r = evaluate_risk(_base_payload(stoploss_price=100.0, entry_price=100.0))
    assert r["pass"] is False
    assert r["reason"] == "invalid_buy_stoploss_geometry"
    assert r["suggested_size"] == 0


def test_evaluate_risk_daily_limit_blocks():
    r = evaluate_risk(_base_payload(daily_new_orders=10, max_daily_new_orders=10))
    assert r["pass"] is False
    assert r["reason"] == "max_daily_new_orders_reached"
    assert r["suggested_size"] == 0


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
    assert r["reason"] == "size_below_board_lot"


def test_evaluate_risk_rejects_buy_stop_above_entry():
    r = evaluate_risk(_base_payload(entry_price=100.0, stoploss_price=110.0))
    assert r["pass"] is False
    assert r["reason"] == "invalid_buy_stoploss_geometry"


def test_evaluate_risk_validates_reward_risk_when_take_profit_present():
    weak = evaluate_risk(_base_payload(entry_price=100.0, stoploss_price=90.0, take_profit_price=110.0))
    assert weak["pass"] is False
    assert weak["reason"] == "reward_risk_below_min"
    assert weak["reward_risk"] == pytest.approx(1.0)

    good = evaluate_risk(_base_payload(entry_price=100.0, stoploss_price=90.0, take_profit_price=120.0))
    assert good["pass"] is True
    assert good["reward_risk"] == pytest.approx(2.0)


def test_evaluate_risk_sell_geometry_is_directional():
    r = evaluate_risk(_base_payload(entry_price=100.0, stoploss_price=110.0, side="SELL", take_profit_price=80.0))
    assert r["pass"] is True
    assert r["reward_risk"] == pytest.approx(2.0)
