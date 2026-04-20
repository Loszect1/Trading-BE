"""
Optional live DNSE E2E: place (no auto-poll) → order_detail snapshot → cancel.

**Danger:** sends real orders when all gates and tokens are valid.

Excluded from default CI via marker ``dnse_live``. Requires **both**
``RUN_DNSE_LIVE_TESTS=1`` and ``RUN_DNSE_LIVE_E2E=1``, plus allowlisted symbol
env (see README). If the broker rejects the place step, the test **skips**
(dry-run fallback) instead of failing.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from app.core.config import AppSettings
from app.services.execution.dnse_adapter import DnseLiveExecutionAdapter
from app.services.execution.dnse_token import validate_dnse_tokens_for_live
from app.services.execution.types import OrderExecutionContext
from tests.helpers.dnse_live_contract import assert_valid_e2e_order_row, normalize_dnse_broker_row
from tests.helpers.dnse_live_e2e_guards import e2e_skip_reason_if_not_runnable, resolve_e2e_symbol_and_order

pytestmark = [pytest.mark.dnse_live, pytest.mark.dnse_live_e2e]


def test_live_dnse_place_poll_cancel_e2e():
    gate = e2e_skip_reason_if_not_runnable()
    if gate:
        pytest.skip(gate)

    params, perr = resolve_e2e_symbol_and_order()
    assert perr is None

    cfg = AppSettings()
    ok, tok_reason = validate_dnse_tokens_for_live(cfg.dnse_access_token or "", cfg.dnse_trading_token or "")
    if not ok:
        pytest.skip(f"DNSE tokens not configured for live E2E: {tok_reason}")

    adapter = DnseLiveExecutionAdapter(settings=cfg)
    ctx = OrderExecutionContext(
        internal_order_id="pytest-dnse-live-e2e",
        account_mode="REAL",
        symbol=params.symbol,
        side=params.side,
        quantity=params.quantity,
        price=float(params.price),
        idempotency_key="pytest-dnse-live-e2e-idem",
        broker_order_id=None,
        order_metadata={"dnse_order_type": "LO"},
        current_status="NEW",
    )

    placed = adapter.place_order_without_poll(ctx)
    norm_place = normalize_dnse_broker_row(placed.broker_raw or {})
    assert_valid_e2e_order_row(norm_place, require_order_id=bool(placed.broker_order_id))

    if placed.internal_status == "REJECTED":
        pytest.skip(
            "DNSE live E2E dry-run: broker rejected place step "
            f"(reason={placed.reason!r}, normalized={norm_place!r})"
        )

    if not placed.broker_order_id:
        pytest.skip("DNSE live E2E dry-run: place accepted but broker order id missing")

    snap_ctx = replace(ctx, broker_order_id=placed.broker_order_id, current_status="ACK")
    polled = adapter.fetch_broker_order_snapshot(snap_ctx)
    norm_poll = normalize_dnse_broker_row(polled.broker_raw or {})
    assert_valid_e2e_order_row(norm_poll, require_order_id=False)

    cancelled = adapter.cancel_at_broker(snap_ctx)
    assert cancelled.internal_status in {"CANCELLED", "REJECTED"}, (
        f"unexpected cancel outcome: {cancelled.internal_status!r} reason={cancelled.reason!r}"
    )

    after = adapter.fetch_broker_order_snapshot(snap_ctx)
    norm_after = normalize_dnse_broker_row(after.broker_raw or {})
    assert_valid_e2e_order_row(norm_after, require_order_id=False)
    assert norm_after["internal_status"] in {"FILLED", "PARTIAL", "REJECTED", "CANCELLED", "ACK"}
