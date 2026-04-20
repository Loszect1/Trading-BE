from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.services.execution.broker_status import (
    build_dnse_reconcile_metadata_snapshot,
    dnse_row_to_dict,
    extract_dnse_order_id,
    extract_dnse_status_token,
    map_dnse_status_to_internal,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ({"orderId": "abc123"}, "abc123"),
        ({"order_id": "x"}, "x"),
        ({"id": "noid"}, "noid"),
        ({}, None),
    ],
)
def test_extract_dnse_order_id(raw: dict, expected: str | None) -> None:
    assert extract_dnse_order_id(raw) == expected


@pytest.mark.parametrize(
    "raw,expected_substr",
    [
        ({"orderStatus": "Filled"}, "FILLED"),
        ({"status": "PENDING_NEW"}, "PENDING_NEW"),
        ({"state": "working"}, "WORKING"),
    ],
)
def test_extract_dnse_status_token(raw: dict, expected_substr: str) -> None:
    assert expected_substr in (extract_dnse_status_token(raw) or "").upper()


@pytest.mark.parametrize(
    "token,internal",
    [
        ("FILLED", "FILLED"),
        ("MATCHED", "FILLED"),
        ("PARTIALLY_FILLED", "PARTIAL"),
        ("REJECTED", "REJECTED"),
        ("CANCELLED", "CANCELLED"),
        ("PENDING_NEW", "ACK"),
        ("OPEN", "ACK"),
        (None, "ACK"),
    ],
)
def test_map_dnse_status_to_internal(token: str | None, internal: str) -> None:
    assert map_dnse_status_to_internal(token) == internal


def test_map_dnse_status_unknown_token_maps_to_ack() -> None:
    assert map_dnse_status_to_internal("MYSTERIOUS_STATE") == "ACK"


def test_dnse_row_to_dict_handles_pandas_like_row() -> None:
    row = MagicMock()
    row.to_dict.return_value = {"a": 1, "b": 2}
    assert dnse_row_to_dict(row) == {"a": 1, "b": 2}


def test_dnse_row_to_dict_non_dict_returns_empty() -> None:
    assert dnse_row_to_dict("not-a-row") == {}


def test_build_dnse_reconcile_metadata_snapshot() -> None:
    raw = {"orderStatus": "PARTIALLY_FILLED", "matchedQty": 3, "leaveQty": 7, "orderQty": 10}
    snap = build_dnse_reconcile_metadata_snapshot(raw)
    assert snap["matched_qty"] == 3
    assert snap["leave_qty"] == 7
    assert snap["order_qty"] == 10
    assert "PARTIALLY" in (snap.get("broker_status") or "").upper()
