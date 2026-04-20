from __future__ import annotations

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
