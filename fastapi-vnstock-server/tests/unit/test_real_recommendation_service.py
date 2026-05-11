from __future__ import annotations

from app.services import real_recommendation_service as service
from app.services.real_recommendation_service import preflight_real_recommendations


def test_preflight_real_recommendations_rejects_low_reward_risk() -> None:
    buyable, rejected = preflight_real_recommendations(
        [
            {
                "symbol": "AAA",
                "entry": 100.0,
                "take_profit": 104.0,
                "stop_loss": 97.0,
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
                "entry": 100.0,
                "take_profit": 108.0,
                "stop_loss": 96.0,
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
                "entry": 100.0,
                "take_profit": 108.0,
                "stop_loss": 96.0,
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
                "entry": 100.0,
                "take_profit": 108.0,
                "stop_loss": 96.0,
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
                "entry": 100.0,
                "take_profit": 108.0,
                "stop_loss": 96.0,
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
