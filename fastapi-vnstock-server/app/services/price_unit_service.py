from __future__ import annotations


def normalize_vn_price_to_vnd(price: float | int | str | None) -> float:
    """
    Normalize Vietnam stock prices to full VND.

    VN data sources commonly use thousand-VND notation: 4.3 means 4,300 VND,
    42.05 means 42,050 VND. Values already >= 1000 are treated as full VND.
    """
    try:
        value = float(price or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if value <= 0:
        return value
    return value * 1000.0 if value < 1000.0 else value


def normalize_price_fields_to_vnd(payload: dict, keys: tuple[str, ...]) -> dict:
    out = dict(payload)
    for key in keys:
        if key in out and out.get(key) is not None:
            out[key] = normalize_vn_price_to_vnd(out.get(key))
    return out
