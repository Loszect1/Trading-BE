from __future__ import annotations

from app.services import short_term_automation_service as service
from app.services.short_term_automation_service import _allocate_quantities_by_score


def test_allocate_quantities_by_score_uses_lot_100_and_prefers_higher_score() -> None:
    buy_rows = [
        {"id": "s1", "entry_price": 10.0, "confidence": 80},
        {"id": "s2", "entry_price": 10.0, "confidence": 20},
    ]
    out = _allocate_quantities_by_score(buy_rows, nav_total=100_000_000, lot_size=100)
    assert out["s1"] % 100 == 0
    assert out["s2"] % 100 == 0
    assert out["s1"] > out["s2"]


def test_allocate_quantities_by_score_zero_when_nav_too_small() -> None:
    buy_rows = [{"id": "s1", "entry_price": 50_000.0, "confidence": 100}]
    out = _allocate_quantities_by_score(buy_rows, nav_total=4_000_000, lot_size=100)
    assert out["s1"] == 0


def test_handle_buy_signal_uses_latest_market_price_for_demo_fill(monkeypatch) -> None:
    placed_payloads: list[dict] = []

    monkeypatch.setattr(service, "_refine_buy_levels_with_claude", lambda _sig: None)
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
    monkeypatch.setattr(service, "_refine_buy_levels_with_claude", lambda _sig: None)
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
