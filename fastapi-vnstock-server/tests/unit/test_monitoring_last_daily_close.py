"""Unit tests for _last_daily_close_mark via VNStockApiService.call_quote mock (no network)."""

from __future__ import annotations

import pytest

from app.services import monitoring_service as ms


def test_last_daily_close_mark_empty_symbol() -> None:
    price, err = ms._last_daily_close_mark("  ")
    assert price is None
    assert err == "empty_symbol"


def test_last_daily_close_mark_exception_from_quote(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a, **_k):
        raise RuntimeError("quote_down")

    monkeypatch.setattr(ms._vnstock_api_service, "call_quote", boom)
    price, err = ms._last_daily_close_mark("VNM")
    assert price is None
    assert "quote_down" in (err or "")


def test_last_daily_close_mark_empty_history(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ms._vnstock_api_service, "call_quote", lambda *_a, **_k: [])
    price, err = ms._last_daily_close_mark("VNM")
    assert price is None
    assert err == "empty_quote_history"


def test_last_daily_close_mark_non_list_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ms._vnstock_api_service, "call_quote", lambda *_a, **_k: {})
    price, err = ms._last_daily_close_mark("VNM")
    assert price is None
    assert err == "empty_quote_history"


def test_last_daily_close_mark_no_valid_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ms._vnstock_api_service,
        "call_quote",
        lambda *_a, **_k: [{"close": None}, {"close": "x"}, {"close": -1}],
    )
    price, err = ms._last_daily_close_mark("VNM")
    assert price is None
    assert err == "no_valid_close_in_history"


def test_last_daily_close_mark_picks_last_positive_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ms._vnstock_api_service,
        "call_quote",
        lambda *_a, **_k: [
            {"close": 10.0},
            {"close": 12.5},
        ],
    )
    price, err = ms._last_daily_close_mark("VNM")
    assert err is None
    assert price == pytest.approx(12.5)


def test_last_daily_close_mark_skips_non_dict_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ms._vnstock_api_service,
        "call_quote",
        lambda *_a, **_k: ["bad", {"close": 9.0}],
    )
    price, err = ms._last_daily_close_mark("VNM")
    assert err is None
    assert price == pytest.approx(9.0)
