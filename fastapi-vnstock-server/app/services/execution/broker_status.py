from __future__ import annotations

import re
from typing import Any


def _norm_token(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    s = re.sub(r"[\s\-]+", "_", s)
    return s


def dnse_row_to_dict(row: Any) -> dict[str, Any]:
    """Flatten a vnstock/pandas row into a plain dict with string keys."""
    if row is None:
        return {}
    if hasattr(row, "to_dict"):
        d = row.to_dict()
        return {str(k): v for k, v in d.items()}
    if isinstance(row, dict):
        return {str(k): v for k, v in row.items()}
    return {}


def extract_dnse_order_id(raw: dict[str, Any]) -> str | None:
    """Best-effort broker order id from DNSE place/detail payloads (flattened keys)."""

    def _pick(*keys: str) -> str | None:
        for want in keys:
            for rk, rv in raw.items():
                if str(rk).lower() == want.lower() and rv is not None and str(rv).strip():
                    return str(rv).strip()
        return None

    return _pick("orderId", "order_id", "orderNo", "order_no", "id", "clOrdId")


def extract_dnse_status_token(raw: dict[str, Any]) -> str | None:
    for key in raw.keys():
        lk = str(key).lower()
        if "status" in lk or lk == "state" or lk.endswith("_state"):
            tok = _norm_token(raw.get(key))
            if tok:
                return tok
    return None


def map_dnse_status_to_internal(status_token: str | None) -> str:
    """
    Map DNSE / vnstock-normalized status text to trading_core order status.

    Returns one of: FILLED, PARTIAL, REJECTED, CANCELLED, ACK
    """
    s = _norm_token(status_token) if status_token else None
    if not s:
        return "ACK"

    filled = {
        "FILLED",
        "MATCHED",
        "DONE",
        "COMPLETED",
        "FULLY_FILLED",
        "SUCCESS",
        "EXECUTED",
    }
    rejected = {"REJECTED", "FAILED", "REJ", "REJECT", "ERROR", "INVALID"}
    cancelled = {"CANCELLED", "CANCELED", "WITHDRAWN", "EXPIRED", "CANCELLED_BY_SYSTEM"}
    partial = {"PARTIALLY_FILLED", "PARTIAL", "PARTIAL_FILL", "PARTIAL_MATCH"}

    if s in filled:
        return "FILLED"
    if s in rejected:
        return "REJECTED"
    if s in cancelled:
        return "CANCELLED"
    if s in partial:
        return "PARTIAL"
    if s in {
        "NEW",
        "PENDING",
        "PENDING_NEW",
        "QUEUE",
        "QUEUED",
        "SENDING",
        "SENT",
        "WAITING",
        "WAITING_TO_SEND",
        "WAITING_ACK",
        "OPEN",
        "ACTIVE",
        "AWAITING",
        "WORKING",
        "READY",
        "APPROVED",
        "CONFIRMED",
    }:
        return "ACK"
    if s.startswith("PENDING") or s.startswith("WAITING"):
        return "ACK"
    return "ACK"


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def build_dnse_reconcile_metadata_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Compact, mostly-numeric snapshot from a broker row for JSONB order_metadata.
    Keys are normalized; values are primitives only.
    """
    snap: dict[str, Any] = {}
    for k, v in raw.items():
        lk = str(k).lower().replace(" ", "_")
        if lk in {
            "matchedqty",
            "matched_quantity",
            "executedqty",
            "executed_quantity",
            "filledqty",
            "filled_quantity",
            "cumqty",
            "cum_qty",
            "matchqty",
        }:
            n = _coerce_int(v)
            if n is not None:
                snap["matched_qty"] = n
        if lk in {
            "leaveqty",
            "leave_quantity",
            "leavesqty",
            "leaves_quantity",
            "outstandingqty",
            "remainingqty",
            "remaining_quantity",
        }:
            n = _coerce_int(v)
            if n is not None:
                snap["leave_qty"] = n
        if lk in {"orderqty", "order_quantity", "quantity", "qty"}:
            n = _coerce_int(v)
            if n is not None:
                snap["order_qty"] = n
    st = extract_dnse_status_token(raw)
    if st:
        snap["broker_status"] = st
    oid = extract_dnse_order_id(raw)
    if oid:
        snap["broker_order_id"] = oid
    return snap
