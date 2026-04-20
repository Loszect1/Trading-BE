"""
Deterministic OHLCV-shaped rows for exercising ``signal_engine_service`` logic
without calling the real vnstock network.
"""

from __future__ import annotations

from typing import Any


def ohlcv_rows_short_term_buy_spike(*, bars: int = 50) -> list[dict[str, Any]]:
    """
    Build ``history`` rows that satisfy short-term scanner thresholds:

    - ``len(closes) >= 25`` and volumes aligned
    - Volume spike: latest / mean(prev 20 window used in code) >= 1.8
    - ``last_close > mean(closes[-20:])``
    """
    if bars < 50:
        raise ValueError("bars must be at least 50 for current short-term thresholds")

    rows: list[dict[str, Any]] = []
    for i in range(bars - 1):
        rows.append({"close": 10_000.0, "volume": 1_000.0, "time": f"2025-11-{i + 1:02d}"})
    rows.append({"close": 20_000.0, "volume": 3_000.0, "time": "2025-12-20"})
    return rows


def ohlcv_rows_short_term_hold_flat_volume(*, bars: int = 50) -> list[dict[str, Any]]:
    """Flat volume (no spike) so action stays HOLD while still passing min bar count."""
    if bars < 50:
        raise ValueError("bars must be at least 50")
    return [{"close": 10_000.0 + float(i), "volume": 1_000.0, "time": f"2025-11-{i + 1:02d}"} for i in range(bars)]

