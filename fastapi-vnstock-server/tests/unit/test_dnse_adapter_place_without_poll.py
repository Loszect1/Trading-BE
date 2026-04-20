from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.core.config import AppSettings
from app.services.execution.dnse_adapter import DnseLiveExecutionAdapter
from app.services.execution.types import OrderExecutionContext


def _ctx() -> OrderExecutionContext:
    return OrderExecutionContext(
        internal_order_id="u1",
        account_mode="REAL",
        symbol="VNM",
        side="BUY",
        quantity=1,
        price=1000.0,
        idempotency_key=None,
        broker_order_id=None,
        order_metadata={},
        current_status="NEW",
    )


def test_place_order_without_poll_rests_ack() -> None:
    cfg = AppSettings()
    cfg.dnse_access_token = "a" * 32 + ".b.c"
    cfg.dnse_trading_token = "t" * 12
    cfg.dnse_sub_account = "SUB1"
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    raw = {"orderId": "oid-1", "orderStatus": "PENDING_NEW"}
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_place_order", return_value=raw):
            out = adapter.place_order_without_poll(_ctx())
    assert out.internal_status == "ACK"
    assert out.broker_order_id == "oid-1"
    assert out.reason is None


def test_place_order_without_poll_terminal_rejected() -> None:
    cfg = AppSettings()
    cfg.dnse_access_token = "a" * 32 + ".b.c"
    cfg.dnse_trading_token = "t" * 12
    cfg.dnse_sub_account = "SUB1"
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    raw = {"orderId": "oid-2", "orderStatus": "REJECTED"}
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_place_order", return_value=raw):
            out = adapter.place_order_without_poll(_ctx())
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_broker_rejected"


def test_fetch_broker_order_snapshot_maps_status() -> None:
    cfg = AppSettings()
    cfg.dnse_access_token = "a" * 32 + ".b.c"
    cfg.dnse_trading_token = "t" * 12
    cfg.dnse_sub_account = "SUB1"
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    ctx = OrderExecutionContext(
        internal_order_id="u1",
        account_mode="REAL",
        symbol="VNM",
        side="BUY",
        quantity=1,
        price=1000.0,
        idempotency_key=None,
        broker_order_id="oid-3",
        order_metadata={},
        current_status="ACK",
    )
    raw = {"status": "OPEN"}
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_order_detail", return_value=raw):
            out = adapter.fetch_broker_order_snapshot(ctx)
    assert out.internal_status == "ACK"
    assert out.broker_order_id == "oid-3"
