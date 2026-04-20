"""Unit tests for DnseLiveExecutionAdapter cancel + execute error paths (mocked Trade, no network)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.core.config import AppSettings
from app.services.execution.dnse_adapter import DnseLiveExecutionAdapter
from app.services.execution.types import OrderExecutionContext


def _valid_tokens_cfg() -> AppSettings:
    cfg = AppSettings()
    cfg.dnse_access_token = "a" * 32 + ".b.c"
    cfg.dnse_trading_token = "t" * 12
    cfg.dnse_sub_account = "SUB1"
    cfg.dnse_execution_timeout_seconds = 2.0
    cfg.dnse_execution_poll_attempts = 2
    cfg.dnse_execution_poll_interval_seconds = 0.05
    cfg.dnse_execution_place_retries = 0
    return cfg


def _ctx(*, broker_id: str | None = "brk-1", status: str = "ACK") -> OrderExecutionContext:
    return OrderExecutionContext(
        internal_order_id="ord-u1",
        account_mode="REAL",
        symbol="VNM",
        side="BUY",
        quantity=1,
        price=1000.0,
        idempotency_key=None,
        broker_order_id=broker_id,
        order_metadata={"dnse_asset_type": "stock"},
        current_status=status,
    )


def test_cancel_at_broker_token_gate_returns_gate_outcome() -> None:
    cfg = AppSettings()
    cfg.dnse_access_token = "a" * 32 + ".b.c"
    cfg.dnse_trading_token = ""
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    out = adapter.cancel_at_broker(_ctx())
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_trading_token_missing"


def test_cancel_at_broker_missing_sub_account() -> None:
    cfg = _valid_tokens_cfg()
    cfg.dnse_sub_account = ""
    cfg.dnse_default_sub_account = ""
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    out = adapter.cancel_at_broker(_ctx())
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_sub_account_missing"


def test_cancel_at_broker_missing_broker_order_id() -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    out = adapter.cancel_at_broker(_ctx(broker_id=None))
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_cancel_missing_broker_order_id"


def test_cancel_at_broker_timeout_maps_to_rejected() -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_cancel_order", side_effect=TimeoutError("slow")):
            out = adapter.cancel_at_broker(_ctx())
    assert out.internal_status == "REJECTED"
    assert out.broker_order_id == "brk-1"


def test_cancel_at_broker_exception_maps_to_rejected() -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_cancel_order", side_effect=RuntimeError("boom")):
            out = adapter.cancel_at_broker(_ctx())
    assert out.internal_status == "REJECTED"
    assert out.broker_order_id == "brk-1"


@pytest.mark.parametrize(
    ("raw", "expected_internal", "reason_substr"),
    [
        ({"orderStatus": "CANCELLED"}, "CANCELLED", None),
        ({"orderStatus": "REJECTED"}, "REJECTED", "dnse_cancel_rejected"),
        ({"orderStatus": "FILLED"}, "REJECTED", "dnse_cancel_ineligible_terminal"),
        ({"orderStatus": "OPEN"}, "REJECTED", "dnse_cancel_pending_or_ambiguous"),
    ],
)
def test_cancel_at_broker_status_branches(
    raw: dict, expected_internal: str, reason_substr: str | None
) -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_cancel_order", return_value=raw):
            out = adapter.cancel_at_broker(_ctx())
    assert out.internal_status == expected_internal
    if reason_substr:
        assert reason_substr in (out.reason or "")


def test_place_order_without_poll_missing_order_id_after_success() -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_place_order", return_value={"orderStatus": "PENDING_NEW"}):
            out = adapter.place_order_without_poll(_ctx(broker_id=None, status="NEW"))
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_place_no_order_id"


def test_execute_after_place_poll_timeout() -> None:
    cfg = _valid_tokens_cfg()
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    place_raw = {"orderId": "oid-t", "orderStatus": "PENDING_NEW"}
    ctx = _ctx(broker_id=None, status="NEW")
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_place_order", return_value=place_raw):
            with patch.object(adapter, "_poll_until_terminal", side_effect=TimeoutError("poll slow")):
                out = adapter.execute(ctx)
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_poll_timeout"


def test_fetch_broker_order_snapshot_timeout() -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    ctx = _ctx(broker_id="oid-snap", status="ACK")
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_order_detail", side_effect=TimeoutError("snap")):
            out = adapter.fetch_broker_order_snapshot(ctx)
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_timeout"


def test_execute_resume_poll_timeout() -> None:
    cfg = _valid_tokens_cfg()
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    ctx = _ctx(broker_id="oid-r", status="ACK")
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_poll_until_terminal", side_effect=TimeoutError("resume slow")):
            out = adapter.execute(ctx)
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_poll_timeout"


def test_execute_place_retries_then_filled() -> None:
    cfg = _valid_tokens_cfg()
    cfg.dnse_execution_place_retries = 2
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    ctx = _ctx(broker_id=None, status="NEW")
    terminal_place = {"orderId": "oid-retry", "orderStatus": "FILLED"}
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_place_order", side_effect=[RuntimeError("e1"), RuntimeError("e2"), terminal_place]):
            with patch("app.services.execution.dnse_adapter.time.sleep") as sl:
                out = adapter.execute(ctx)
    assert out.internal_status == "FILLED"
    assert out.broker_order_id == "oid-retry"
    assert sl.call_count == 2


def test_place_order_without_poll_place_retries_then_ack() -> None:
    cfg = _valid_tokens_cfg()
    cfg.dnse_execution_place_retries = 1
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    ctx = _ctx(broker_id=None, status="NEW")
    resting = {"orderId": "oid-np", "orderStatus": "PENDING_NEW"}
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_place_order", side_effect=[ConnectionError("net"), resting]):
            with patch("app.services.execution.dnse_adapter.time.sleep"):
                out = adapter.place_order_without_poll(ctx)
    assert out.internal_status == "ACK"
    assert out.broker_order_id == "oid-np"


def test_poll_until_terminal_exhausted_returns_pending_timeout() -> None:
    cfg = _valid_tokens_cfg()
    cfg.dnse_execution_poll_attempts = 3
    cfg.dnse_execution_poll_interval_seconds = 0.001
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    trade = MagicMock()
    ctx = _ctx(broker_id="oid-pend", status="NEW")
    pending_row = {"orderStatus": "OPEN"}
    with patch.object(adapter, "_order_detail", return_value=pending_row):
        with patch("app.services.execution.dnse_adapter.time.sleep") as sl:
            out = adapter._poll_until_terminal(trade, "oid-pend", ctx)
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_status_pending_timeout"
    assert "still pending after 3 polls" in out.log_messages[0]
    assert sl.call_count == 2


def test_readonly_list_sub_accounts_token_gate_raises() -> None:
    cfg = AppSettings()
    cfg.dnse_access_token = "a" * 32 + ".b.c"
    cfg.dnse_trading_token = ""
    adapter = DnseLiveExecutionAdapter(settings=cfg)
    with pytest.raises(RuntimeError, match="dnse_trading_token_missing"):
        adapter.readonly_list_sub_accounts()


def test_readonly_list_sub_accounts_success_rows() -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    df = MagicMock()
    df.empty = False
    df.to_dict.return_value = [{"subAccount": "SA1", "balance": 1}]
    with patch.object(adapter, "_token_gate", return_value=None):
        with patch.object(adapter, "_build_trade", return_value=MagicMock()):
            with patch.object(adapter, "_call_with_timeout", return_value=df):
                rows = adapter.readonly_list_sub_accounts()
    assert rows == [{"subAccount": "SA1", "balance": 1}]


def test_fetch_broker_order_snapshot_exception_maps_reason() -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    ctx = _ctx(broker_id="oid-snapx", status="ACK")
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_order_detail", side_effect=RuntimeError("db boom")):
            out = adapter.fetch_broker_order_snapshot(ctx)
    assert out.internal_status == "REJECTED"
    assert out.reason == "dnse_unknown_error"


@pytest.mark.parametrize(
    "raw,reason_substr",
    [
        ({"orderStatus": "PARTIALLY_FILLED"}, "dnse_cancel_ineligible_terminal"),
        ({"orderStatus": "UNKNOWN_OPEN"}, "dnse_cancel_pending_or_ambiguous"),
    ],
)
def test_cancel_at_broker_status_mapping_edges(raw: dict, reason_substr: str) -> None:
    adapter = DnseLiveExecutionAdapter(settings=_valid_tokens_cfg())
    with patch.object(adapter, "_build_trade", return_value=MagicMock()):
        with patch.object(adapter, "_cancel_order", return_value=raw):
            out = adapter.cancel_at_broker(_ctx())
    assert out.internal_status == "REJECTED"
    assert reason_substr in (out.reason or "")
