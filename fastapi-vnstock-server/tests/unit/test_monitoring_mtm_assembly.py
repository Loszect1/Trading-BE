"""Deterministic tests for monitoring MTM assembly and float parsing (no DB, no network)."""

from __future__ import annotations

import pytest

from app.core import config
from app.services import monitoring_service as ms


def test_float_safe_parses_and_rejects_invalid() -> None:
    assert ms._float_safe("12.5") == 12.5
    assert ms._float_safe(7) == 7.0
    assert ms._float_safe(None) is None
    assert ms._float_safe("nope") is None
    assert ms._float_safe(float("nan")) is None
    assert ms._float_safe(float("inf")) is None


def test_compute_open_position_mtm_cost_basis_only_when_no_marks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ms, "_last_daily_close_mark", lambda _sym: (None, "stub_no_quote"))

    out = ms._compute_open_position_mtm(
        "DEMO",
        [
            {"symbol": "TSTA", "total_qty": 10, "avg_price": 100.0},
            {"symbol": "TSTB", "total_qty": 5, "avg_price": 200.0},
        ],
    )
    assert out["valuation_method"] == "COST_BASIS_ONLY"
    assert out["exposure_cost_basis_vnd"] == out["exposure_market_vnd"]
    assert out["unrealized_pnl_vnd"] == 0.0
    assert set(out["symbols_mark_failed"].keys()) == {"TSTA", "TSTB"}


def test_compute_open_position_mtm_partial_fallback_and_truncation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _mark(sym: str) -> tuple[float | None, str | None]:
        if sym.upper() == "TSTA":
            return (150.0, None)
        return (None, "stub_fail")

    monkeypatch.setattr(ms, "_last_daily_close_mark", _mark)
    monkeypatch.setattr(config.settings, "monitoring_dashboard_mtm_max_symbols", 1)

    out = ms._compute_open_position_mtm(
        "DEMO",
        [
            {"symbol": "TSTA", "total_qty": 10, "avg_price": 100.0},
            {"symbol": "TSTB", "total_qty": 20, "avg_price": 50.0},
        ],
    )
    assert out["valuation_method"] == "MARK_TO_MARKET_DAILY_CLOSE_PARTIAL_FALLBACK"
    assert "TSTA" in out["symbols_with_mark"]
    assert "TSTB" in out["symbols_truncated_from_mtm_fetch"]
    assert "TSTB" not in out["symbols_mark_failed"]
    assert out["unrealized_pnl_vnd"] == pytest.approx(500.0)


def test_build_account_monitoring_dashboard_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError, match="REAL or DEMO"):
        ms.build_account_monitoring_dashboard("paper")


def test_compute_open_position_mtm_full_marks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ms, "_last_daily_close_mark", lambda sym: (float(len(sym)), None))

    out = ms._compute_open_position_mtm(
        "DEMO",
        [{"symbol": "AB", "total_qty": 2, "avg_price": 1.0}],
    )
    assert out["valuation_method"] == "MARK_TO_MARKET_DAILY_CLOSE"
    assert out["unrealized_pnl_vnd"] == pytest.approx(2.0 * (2.0 - 1.0))
