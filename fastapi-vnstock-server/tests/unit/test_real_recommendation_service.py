from __future__ import annotations

import pytest

from app.services import real_recommendation_service as service
from app.services.real_recommendation_service import preflight_real_recommendations


@pytest.fixture(autouse=True)
def _mock_real_preflight_db_snapshots(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "get_real_t2_settlement_pressure",
        lambda: {
            "available": True,
            "market_rule": "VN_T_PLUS_2",
            "total_pending_qty": 0,
            "total_pending_notional": 0.0,
            "next_settle_date": None,
            "by_symbol": {},
        },
    )
    monkeypatch.setattr(
        service,
        "get_real_account_preflight_context",
        lambda cash_base: {
            "available": True,
            "source": "test",
            "cash_base": float(cash_base),
            "daily_new_orders": 0,
            "orders_today": 0,
            "open_orders": 0,
            "open_buy_orders": 0,
            "max_daily_new_orders": int(service.settings.strategy_max_daily_new_orders),
            "open_buy_symbols": [],
            "symbol_exposure": {},
            "sector_exposure": {},
            "max_symbol_exposure_pct": float(service.settings.strategy_max_symbol_exposure_pct),
            "max_sector_exposure_pct": float(service.settings.strategy_max_sector_exposure_pct),
        },
    )


def test_preflight_real_recommendations_rejects_low_reward_risk() -> None:
    buyable, rejected = preflight_real_recommendations(
        [
            {
                "symbol": "AAA",
                "entry": 50.0,
                "take_profit": 52.0,
                "stop_loss": 48.0,
                "confidence": 70,
                "reason": "fixture",
            }
        ],
        available_cash_vnd=100_000_000.0,
    )

    assert buyable == []
    assert rejected[0]["risk_status"] == "REJECTED"
    assert rejected[0]["risk_reason"] == "reward_risk_below_min"
    assert rejected[0]["reward_risk"] < 1.5


def test_preflight_real_recommendations_marks_buyable_and_sizes_board_lot() -> None:
    buyable, rejected = preflight_real_recommendations(
        [
            {
                "symbol": "AAA",
                "entry": 50.0,
                "take_profit": 60.0,
                "stop_loss": 48.0,
                "confidence": 70,
                "reason": "fixture",
            }
        ],
        available_cash_vnd=100_000_000.0,
    )

    assert rejected == []
    assert buyable[0]["risk_status"] == "BUYABLE"
    assert buyable[0]["risk_reason"] == "ok"
    assert buyable[0]["suggested_quantity"] % 100 == 0
    assert buyable[0]["reward_risk"] >= 1.5


def test_preflight_real_recommendations_rejects_stale_data() -> None:
    buyable, rejected = preflight_real_recommendations(
        [
            {
                "symbol": "AAA",
                "entry": 50.0,
                "take_profit": 60.0,
                "stop_loss": 48.0,
                "confidence": 70,
                "reason": "fixture",
                "freshness": {"is_fresh": False, "latest_trading_date": "2026-01-01"},
            }
        ],
        available_cash_vnd=100_000_000.0,
    )

    assert buyable == []
    assert rejected[0]["risk_status"] == "REJECTED"
    assert rejected[0]["risk_reason"] == "stale_data"


def test_preflight_real_recommendations_rejects_high_t2_pressure(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "get_real_t2_settlement_pressure",
        lambda: {
            "available": True,
            "market_rule": "VN_T_PLUS_2",
            "total_pending_qty": 10_000,
            "total_pending_notional": 50_000_000.0,
            "next_settle_date": "2026-05-12",
            "by_symbol": {},
        },
    )

    buyable, rejected = preflight_real_recommendations(
        [
            {
                "symbol": "AAA",
                "entry": 50.0,
                "take_profit": 60.0,
                "stop_loss": 48.0,
                "confidence": 70,
                "reason": "fixture",
            }
        ],
        available_cash_vnd=100_000_000.0,
    )

    assert buyable == []
    assert rejected[0]["risk_status"] == "REJECTED"
    assert rejected[0]["risk_reason"] == "settlement_pressure_high"
    assert rejected[0]["settlement_pressure"]["market_rule"] == "VN_T_PLUS_2"
    assert rejected[0]["settlement_pressure"]["pending_notional_ratio"] == 0.5


def test_preflight_real_recommendations_rejects_same_symbol_pending_t2(monkeypatch) -> None:
    monkeypatch.setattr(service.settings, "strategy_t2_same_symbol_scale_in_allowed", False)
    monkeypatch.setattr(
        service,
        "get_real_t2_settlement_pressure",
        lambda: {
            "available": True,
            "market_rule": "VN_T_PLUS_2",
            "total_pending_qty": 2_000,
            "total_pending_notional": 10_000_000.0,
            "next_settle_date": "2026-05-12",
            "by_symbol": {
                "AAA": {
                    "pending_qty": 2_000,
                    "pending_notional": 10_000_000.0,
                    "next_settle_date": "2026-05-12",
                }
            },
        },
    )

    buyable, rejected = preflight_real_recommendations(
        [
            {
                "symbol": "AAA",
                "entry": 50.0,
                "take_profit": 60.0,
                "stop_loss": 48.0,
                "confidence": 70,
                "reason": "fixture",
                "setup_type": "BREAKOUT",
            }
        ],
        available_cash_vnd=100_000_000.0,
    )

    assert buyable == []
    assert rejected[0]["risk_status"] == "REJECTED"
    assert rejected[0]["risk_reason"] == "same_symbol_pending_t2"
    assert rejected[0]["settlement_pressure"]["symbol_pending_qty"] == 2_000


def test_preflight_real_recommendations_marks_unknown_cash_when_dnse_cash_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "get_real_cash_snapshot",
        lambda: {"available": False, "source": "config_cash", "reason": "dnse_balance_unavailable", "cash": None},
    )

    buyable, rejected = preflight_real_recommendations(
        [
            {
                "symbol": "AAA",
                "entry": 50.0,
                "take_profit": 60.0,
                "stop_loss": 48.0,
                "confidence": 70,
                "reason": "fixture",
            }
        ],
        settlement_pressure={
            "available": True,
            "total_pending_qty": 0,
            "total_pending_notional": 0.0,
            "by_symbol": {},
        },
        account_context={
            "available": True,
            "cash_base": 100_000_000.0,
            "daily_new_orders": 0,
            "max_daily_new_orders": 10,
            "open_buy_symbols": [],
            "symbol_exposure": {},
            "sector_exposure": {},
        },
    )

    assert buyable == []
    assert rejected[0]["risk_status"] == "PREFLIGHT_UNKNOWN_CASH"
    assert rejected[0]["risk_reason"] == "cash_snapshot_unavailable"
    assert rejected[0]["suggested_quantity"] == 0
