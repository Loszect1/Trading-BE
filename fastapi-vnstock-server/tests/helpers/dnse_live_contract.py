"""Shape checks for DNSE live responses (no secrets logged in assertions)."""

from __future__ import annotations

from typing import Any

from app.services.execution.broker_status import (
    build_dnse_reconcile_metadata_snapshot,
    extract_dnse_order_id,
    extract_dnse_status_token,
    map_dnse_status_to_internal,
)


def assert_sub_account_table_shape(rows: list[dict[str, Any]]) -> None:
    """Assert vnstock/DNSE sub_accounts payload looks like a non-empty account listing."""
    assert isinstance(rows, list)
    assert len(rows) >= 1
    for row in rows:
        assert isinstance(row, dict)
        lk = {str(k).lower() for k in row}
        assert any("sub" in k or "account" in k for k in lk), f"unexpected column set keys_sample={sorted(lk)[:8]}"


def normalize_dnse_broker_row(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Compact, assertion-friendly view of a broker row (ids + status + numeric snapshot only).

    Suitable for logging in tests without dumping the full broker payload.
    """
    st = extract_dnse_status_token(raw)
    oid = extract_dnse_order_id(raw)
    return {
        "broker_order_id": oid,
        "status_token": st,
        "internal_status": map_dnse_status_to_internal(st),
        "snapshot": build_dnse_reconcile_metadata_snapshot(raw),
    }


def assert_valid_e2e_order_row(normalized: dict[str, Any], *, require_order_id: bool) -> None:
    assert isinstance(normalized, dict)
    assert "internal_status" in normalized
    assert normalized["internal_status"] in {
        "FILLED",
        "PARTIAL",
        "REJECTED",
        "CANCELLED",
        "ACK",
    }
    if require_order_id:
        assert normalized.get("broker_order_id"), "expected broker_order_id in normalized row"
