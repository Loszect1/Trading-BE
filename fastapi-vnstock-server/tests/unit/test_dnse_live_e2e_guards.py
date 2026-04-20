from __future__ import annotations

import os

import pytest

from tests.helpers import dnse_live_e2e_guards as g


def test_parse_symbol_allowlist() -> None:
    assert g.parse_symbol_allowlist(" VNM , SSI;abc ") == frozenset({"VNM", "SSI", "ABC"})


def test_live_e2e_master_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUN_DNSE_LIVE_TESTS", raising=False)
    monkeypatch.delenv("RUN_DNSE_LIVE_E2E", raising=False)
    assert g.live_e2e_master_switch() is False
    monkeypatch.setenv("RUN_DNSE_LIVE_TESTS", "1")
    monkeypatch.setenv("RUN_DNSE_LIVE_E2E", "0")
    assert g.live_e2e_master_switch() is False
    monkeypatch.setenv("RUN_DNSE_LIVE_E2E", "1")
    assert g.live_e2e_master_switch() is True


def test_resolve_e2e_symbol_and_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DNSE_LIVE_E2E_SYMBOL_ALLOWLIST", "VNM")
    monkeypatch.setenv("DNSE_LIVE_E2E_SYMBOL", "VNM")
    monkeypatch.setenv("DNSE_LIVE_E2E_MAX_QTY", "3")
    monkeypatch.setenv("DNSE_LIVE_E2E_MAX_PRICE", "10000")
    monkeypatch.setenv("DNSE_LIVE_E2E_QTY", "2")
    monkeypatch.setenv("DNSE_LIVE_E2E_LIMIT_PRICE", "9000")
    p, err = g.resolve_e2e_symbol_and_order()
    assert err is None
    assert p.symbol == "VNM"
    assert p.quantity == 2
    assert p.price == 9000.0

    monkeypatch.setenv("DNSE_LIVE_E2E_QTY", "99")
    _, err2 = g.resolve_e2e_symbol_and_order()
    assert err2 == "dnse_e2e_qty_out_of_bounds"

    monkeypatch.setenv("DNSE_LIVE_E2E_QTY", "1")
    monkeypatch.setenv("DNSE_LIVE_E2E_LIMIT_PRICE", "999999")
    _, err3 = g.resolve_e2e_symbol_and_order()
    assert err3 == "dnse_e2e_price_out_of_bounds"


def test_e2e_skip_reason_if_not_runnable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUN_DNSE_LIVE_TESTS", raising=False)
    monkeypatch.delenv("RUN_DNSE_LIVE_E2E", raising=False)
    assert g.e2e_skip_reason_if_not_runnable() is not None

    monkeypatch.setenv("RUN_DNSE_LIVE_TESTS", "1")
    monkeypatch.setenv("RUN_DNSE_LIVE_E2E", "1")
    monkeypatch.delenv("DNSE_LIVE_E2E_SYMBOL_ALLOWLIST", raising=False)
    assert g.e2e_skip_reason_if_not_runnable() == "dnse_e2e_allowlist_missing"


def test_load_e2e_hard_caps_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DNSE_LIVE_E2E_MAX_QTY", raising=False)
    monkeypatch.delenv("DNSE_LIVE_E2E_MAX_PRICE", raising=False)
    q, p = g.load_e2e_hard_caps()
    assert q == 1
    assert p == 50_000.0


def test_load_e2e_hard_caps_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DNSE_LIVE_E2E_MAX_QTY", "9999")
    monkeypatch.setenv("DNSE_LIVE_E2E_MAX_PRICE", "9e9")
    q, p = g.load_e2e_hard_caps()
    assert q == 100
    assert p == 1_000_000.0


def test_normalize_dnse_broker_row_contract() -> None:
    from tests.helpers.dnse_live_contract import normalize_dnse_broker_row

    n = normalize_dnse_broker_row({"orderId": "x1", "orderStatus": "PENDING_NEW"})
    assert n["broker_order_id"] == "x1"
    assert n["internal_status"] == "ACK"
