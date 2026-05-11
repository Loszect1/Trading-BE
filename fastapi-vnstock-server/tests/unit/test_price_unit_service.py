from __future__ import annotations

from app.services.price_unit_service import normalize_price_fields_to_vnd, normalize_vn_price_to_vnd


def test_normalize_vn_price_to_vnd_converts_thousand_vnd_notation() -> None:
    assert normalize_vn_price_to_vnd(4.3) == 4300.0
    assert normalize_vn_price_to_vnd("42.05") == 42050.0


def test_normalize_vn_price_to_vnd_keeps_full_vnd_values() -> None:
    assert normalize_vn_price_to_vnd(4300) == 4300.0
    assert normalize_vn_price_to_vnd(42050) == 42050.0


def test_normalize_price_fields_to_vnd_only_updates_price_keys() -> None:
    payload = {"entry_price": 4.3, "take_profit_price": 5.02, "note": 4.3}

    out = normalize_price_fields_to_vnd(payload, ("entry_price", "take_profit_price"))

    assert out["entry_price"] == 4300.0
    assert out["take_profit_price"] == 5020.0
    assert out["note"] == 4.3
