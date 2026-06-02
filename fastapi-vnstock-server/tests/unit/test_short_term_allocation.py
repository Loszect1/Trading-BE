from __future__ import annotations

from app.services import short_term_automation_service as service
from app.services.short_term_automation_service import _allocate_quantities_by_score, _candidate_quality


def _buy_row(
    signal_id: str,
    *,
    symbol: str,
    confidence: float,
    composite: float,
    entry: float = 10_000.0,
    take_profit: float = 13_000.0,
    stoploss: float = 9_000.0,
    metadata_extra: dict | None = None,
) -> dict:
    metadata = {
        "signal_quality": {
            "composite_score_0_100": composite,
        },
    }
    if metadata_extra:
        metadata.update(metadata_extra)
    return {
        "id": signal_id,
        "symbol": symbol,
        "entry_price": entry,
        "take_profit_price": take_profit,
        "stoploss_price": stoploss,
        "confidence": confidence,
        "metadata": metadata,
    }


def test_allocate_quantities_by_score_uses_lot_100_and_prefers_higher_score(monkeypatch) -> None:
    monkeypatch.setattr(service.settings, "strategy_max_symbol_exposure_pct", 1.0)
    buy_rows = [
        _buy_row("s1", symbol="AAA", confidence=90, composite=90),
        _buy_row("s2", symbol="BBB", confidence=70, composite=70),
    ]
    out = _allocate_quantities_by_score(buy_rows, nav_total=10_000_000, lot_size=100)
    assert out["s1"] % 100 == 0
    assert out["s2"] % 100 == 0
    assert out["s1"] > out["s2"]


def test_allocate_quantities_by_score_zero_when_nav_too_small() -> None:
    buy_rows = [_buy_row("s1", symbol="AAA", confidence=100, composite=100, entry=50_000.0)]
    out = _allocate_quantities_by_score(buy_rows, nav_total=4_000_000, lot_size=100)
    assert out["s1"] == 0


def test_allocate_quantities_caps_one_symbol_to_symbol_cap(monkeypatch) -> None:
    monkeypatch.setattr(service.settings, "strategy_max_symbol_exposure_pct", 0.12)
    buy_rows = [_buy_row("s1", symbol="AAA", confidence=95, composite=95)]

    out = _allocate_quantities_by_score(buy_rows, nav_total=100_000_000, lot_size=100)

    assert out["s1"] == 1_200


def test_candidate_quality_caps_extreme_reward_risk() -> None:
    normal_rr = _candidate_quality(
        _buy_row("s1", symbol="AAA", confidence=80, composite=80, entry=10_000, stoploss=9_000, take_profit=13_000)
    )
    extreme_rr = _candidate_quality(
        _buy_row("s2", symbol="BBB", confidence=80, composite=80, entry=10_000, stoploss=9_900, take_profit=11_000)
    )

    assert normal_rr["reward_risk"] == 3.0
    assert extreme_rr["reward_risk"] == 10.0
    assert normal_rr["rr_score"] == extreme_rr["rr_score"] == 100.0
    assert normal_rr["quality_score"] == extreme_rr["quality_score"]


def test_experience_reduces_symbol_cap(monkeypatch) -> None:
    monkeypatch.setattr(service.settings, "strategy_max_symbol_exposure_pct", 0.12)
    weak_experience = {
        "experience_scoring_adjustment": {
            "applied": True,
            "confidence_adjustment": -12.0,
            "stoploss_ratio": 0.8,
        }
    }
    buy_rows = [_buy_row("s1", symbol="AAA", confidence=95, composite=95, metadata_extra=weak_experience)]

    out = _allocate_quantities_by_score(buy_rows, nav_total=100_000_000, lot_size=100)

    assert out["s1"] == 400


def test_experience_cooldown_zeroes_symbol_cap(monkeypatch) -> None:
    monkeypatch.setattr(service.settings, "strategy_max_symbol_exposure_pct", 0.12)
    cooldown = {
        "experience_buy_cooldown": {
            "applied": True,
            "cooldown_active": True,
        }
    }
    buy_rows = [_buy_row("s1", symbol="AAA", confidence=95, composite=95, metadata_extra=cooldown)]

    out = _allocate_quantities_by_score(buy_rows, nav_total=100_000_000, lot_size=100)

    assert out["s1"] == 0


def test_handle_buy_signal_uses_latest_market_price_for_demo_fill(monkeypatch) -> None:
    placed_payloads: list[dict] = []

    monkeypatch.setattr(service, "_refine_buy_levels_with_gpt", lambda _sig: None)
    monkeypatch.setattr(service, "_extract_latest_market_price", lambda _symbol: 4.1)

    def fake_place_order(payload: dict) -> dict:
        placed_payloads.append(payload)
        return {
            "id": "order-1",
            "status": "FILLED",
            "price": payload["price"],
        }

    monkeypatch.setattr(service, "place_order", fake_place_order)

    out = service._handle_one_buy_signal(
        sig={
            "id": "sig-1",
            "symbol": "DGT",
            "entry_price": 4.3,
            "take_profit_price": 5.02,
            "stoploss_price": 3.94,
        },
        account_mode="DEMO",
        available_cash=50_000_000,
        risk_per_trade=0.01,
        max_daily_new_orders=10,
        daily_new_orders=0,
        planned_quantity=100,
        demo_session_id="demo-test",
    )

    assert out["executed"] == 1
    assert placed_payloads[0]["price"] == 4100.0
    assert placed_payloads[0]["metadata"]["entry_price"] == 4300.0
    assert placed_payloads[0]["metadata"]["market_price_at_check"] == 4100.0


def test_handle_buy_signal_rejects_when_market_price_above_entry(monkeypatch) -> None:
    monkeypatch.setattr(service, "_refine_buy_levels_with_gpt", lambda _sig: None)
    monkeypatch.setattr(service, "_extract_latest_market_price", lambda _symbol: 4.4)

    out = service._handle_one_buy_signal(
        sig={
            "id": "sig-1",
            "symbol": "DGT",
            "entry_price": 4.3,
            "take_profit_price": 5.02,
            "stoploss_price": 3.94,
        },
        account_mode="DEMO",
        available_cash=50_000_000,
        risk_per_trade=0.01,
        max_daily_new_orders=10,
        daily_new_orders=0,
        planned_quantity=100,
        demo_session_id="demo-test",
    )

    assert out["executed"] == 0
    assert out["risk_rejected"] == 1
    assert out["detail"]["reason"] == "market_price_above_entry"
