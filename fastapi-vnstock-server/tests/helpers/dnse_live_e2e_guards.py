"""
Strict opt-in gates + caps for optional DNSE live E2E (place / poll / cancel).

These checks run in tests only; they do not change production execution.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _truthy(name: str) -> bool:
    return os.getenv(name, "").strip() == "1"


def live_e2e_master_switch() -> bool:
    """Both switches must be set so old ``RUN_DNSE_LIVE_TESTS`` alone cannot trigger trading tests."""
    return _truthy("RUN_DNSE_LIVE_TESTS") and _truthy("RUN_DNSE_LIVE_E2E")


def parse_symbol_allowlist(raw: str | None) -> frozenset[str]:
    if not raw or not str(raw).strip():
        return frozenset()
    parts = [p.strip().upper() for p in str(raw).replace(";", ",").split(",")]
    return frozenset(p for p in parts if p)


@dataclass(frozen=True)
class DnseLiveE2EParams:
    symbol: str
    quantity: int
    price: float
    side: str


def load_e2e_hard_caps() -> tuple[int, float]:
    """
    Max quantity and max limit price allowed for E2E tests.

    Env:
    - ``DNSE_LIVE_E2E_MAX_QTY`` (default 1, hard ceiling 100)
    - ``DNSE_LIVE_E2E_MAX_PRICE`` (default 50_000 VND, hard ceiling 1_000_000)
    """
    raw_q = os.getenv("DNSE_LIVE_E2E_MAX_QTY")
    if raw_q is None or not str(raw_q).strip():
        max_qty = 1
    else:
        try:
            max_qty = int(str(raw_q).strip())
        except ValueError:
            max_qty = 1
    max_qty = max(1, min(max_qty, 100))

    raw_p = os.getenv("DNSE_LIVE_E2E_MAX_PRICE")
    if raw_p is None or not str(raw_p).strip():
        max_price = 50_000.0
    else:
        try:
            max_price = float(str(raw_p).strip())
        except ValueError:
            max_price = 50_000.0
    max_price = max(1.0, min(max_price, 1_000_000.0))
    return max_qty, max_price


def _parse_int_env(raw: str | None, default: int) -> int:
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _parse_float_env(raw: str | None, default: float) -> float:
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default


def resolve_e2e_symbol_and_order() -> tuple[DnseLiveE2EParams, str | None]:
    """
    Returns (params, error_reason).

    Required env:
    - ``DNSE_LIVE_E2E_SYMBOL_ALLOWLIST`` — comma-separated tickers (non-empty).
    - ``DNSE_LIVE_E2E_SYMBOL`` — ticker to use; must appear in allowlist.

    Optional:
    - ``DNSE_LIVE_E2E_QTY`` — must be <= ``DNSE_LIVE_E2E_MAX_QTY``
    - ``DNSE_LIVE_E2E_LIMIT_PRICE`` — must be <= ``DNSE_LIVE_E2E_MAX_PRICE``
    - ``DNSE_LIVE_E2E_SIDE`` — BUY (default) or SELL
    """
    allow = parse_symbol_allowlist(os.getenv("DNSE_LIVE_E2E_SYMBOL_ALLOWLIST"))
    if not allow:
        return (
            DnseLiveE2EParams(symbol="", quantity=0, price=0.0, side="BUY"),
            "dnse_e2e_allowlist_missing",
        )

    sym = (os.getenv("DNSE_LIVE_E2E_SYMBOL") or "").strip().upper()
    if not sym:
        return DnseLiveE2EParams(symbol="", quantity=0, price=0.0, side="BUY"), "dnse_e2e_symbol_missing"
    if sym not in allow:
        return DnseLiveE2EParams(symbol="", quantity=0, price=0.0, side="BUY"), "dnse_e2e_symbol_not_in_allowlist"

    max_qty, max_price = load_e2e_hard_caps()
    qty = _parse_int_env(os.getenv("DNSE_LIVE_E2E_QTY"), 1)
    if qty < 1 or qty > max_qty:
        return DnseLiveE2EParams(symbol=sym, quantity=qty, price=0.0, side="BUY"), "dnse_e2e_qty_out_of_bounds"

    price = _parse_float_env(os.getenv("DNSE_LIVE_E2E_LIMIT_PRICE"), 1.0)
    if price <= 0 or price > max_price:
        return (
            DnseLiveE2EParams(symbol=sym, quantity=qty, price=price, side="BUY"),
            "dnse_e2e_price_out_of_bounds",
        )

    side = (os.getenv("DNSE_LIVE_E2E_SIDE") or "BUY").strip().upper()
    if side not in {"BUY", "SELL"}:
        return DnseLiveE2EParams(symbol=sym, quantity=qty, price=price, side="BUY"), "dnse_e2e_side_invalid"

    return DnseLiveE2EParams(symbol=sym, quantity=qty, price=price, side=side), None


def e2e_skip_reason_if_not_runnable() -> str | None:
    if not live_e2e_master_switch():
        return "Set RUN_DNSE_LIVE_TESTS=1 and RUN_DNSE_LIVE_E2E=1 to enable DNSE live E2E (never default CI)."
    _, err = resolve_e2e_symbol_and_order()
    if err:
        return err
    return None
