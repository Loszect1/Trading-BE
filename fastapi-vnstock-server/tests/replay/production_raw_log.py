"""
Parse **raw production trade log** captures (NDJSON / JSON list) into sanitized
events, then into replay ``steps`` via :func:`replay_loader.normalize_sanitized_trade_log_v1`.

Versioning, strict envelope + per-record validation, and migrations (including **v2→v1**)
live in :mod:`tests.replay.replay_schema_registry`; this module performs redaction and maps
canonical **v1** records into the existing replay runner.
"""

from __future__ import annotations

from typing import Any

from tests.replay.production_log_redact import redact_sensitive_fields
from tests.replay.replay_schema_registry import (
    CANONICAL_RAW_VERSION,
    REGISTERED_RAW_BUNDLE_VERSIONS,
    REGISTERED_RAW_NDJSON_HEADER_VERSIONS,
    canonical_raw_body_record_type,
    canonicalize_raw_lines,
    is_registered_raw_header,
)

# Backward-compatible alias used by tests and docs.
RAW_FORMAT_VERSION = CANONICAL_RAW_VERSION


def _as_sanitized_events(raw_lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for raw in raw_lines:
        rec = str(raw.get("record_type") or raw.get("kind") or "").strip().lower()
        if raw.get("role") == "header" or rec == "header":
            sym = str(raw.get("default_symbol") or raw.get("symbol") or "")
            acct = str(raw.get("account_mode") or "DEMO")
            events.append({"kind": "meta", "symbol": sym, "account_mode": acct})
            continue
        if rec == "meta":
            events.append(
                {
                    "kind": "meta",
                    "symbol": str(raw.get("symbol") or ""),
                    "account_mode": str(raw.get("account_mode") or "DEMO"),
                }
            )
            continue
        if not rec:
            raise ValueError(f"raw production record missing record_type: keys={list(raw)!r}")
        canon = canonical_raw_body_record_type(raw)
        if rec in {"clock", "set_clock"}:
            iso = raw.get("iso_utc") or raw.get("ts_utc") or raw.get("ts")
            if not iso:
                raise ValueError("raw clock record requires iso_utc / ts_utc / ts")
            ev = {"kind": "clock", "ts": str(iso)}
            if tid := raw.get("trace_id"):
                ev["trace_id"] = tid
            events.append(ev)
            continue
        if canon in {"check_settlement", "settlement_check"}:
            if "quantity" not in raw:
                raise ValueError("raw check_settlement requires quantity")
            if "expect" not in raw:
                raise ValueError("raw check_settlement requires expect")
            ev = {
                "kind": "check_settlement",
                "quantity": int(raw["quantity"]),
                "expect": dict(raw["expect"]),
            }
            if sym := raw.get("symbol"):
                ev["symbol"] = str(sym)
            if am := raw.get("account_mode"):
                ev["account_mode"] = str(am)
            events.append(ev)
            continue
        if canon == "place_order":
            if "expect" not in raw:
                raise ValueError("raw place_order requires expect")
            order = raw.get("order") or raw.get("payload")
            if not isinstance(order, dict):
                raise ValueError("raw place_order requires order|payload dict")
            events.append({"kind": "place_order", "payload": dict(order), "expect": dict(raw["expect"])})
            continue
        if canon == "cancel_order":
            if "expect" not in raw:
                raise ValueError("raw cancel_order requires expect")
            ev: dict[str, Any] = {
                "kind": "cancel_order",
                "reason": str(raw.get("reason") or "replay_cancel"),
                "expect": dict(raw["expect"]),
            }
            if raw.get("order_id"):
                ev["order_id"] = str(raw["order_id"])
            if raw.get("idempotency_key"):
                ev["idempotency_key"] = str(raw["idempotency_key"])
            if raw.get("account_mode"):
                ev["account_mode"] = str(raw["account_mode"])
            events.append(ev)
            continue
        if canon == "reconcile_order":
            if "expect" not in raw:
                raise ValueError("raw reconcile_order requires expect")
            ev2: dict[str, Any] = {"kind": "reconcile_order", "expect": dict(raw["expect"])}
            if raw.get("order_id"):
                ev2["order_id"] = str(raw["order_id"])
            if raw.get("idempotency_key"):
                ev2["idempotency_key"] = str(raw["idempotency_key"])
            if raw.get("account_mode"):
                ev2["account_mode"] = str(raw["account_mode"])
            events.append(ev2)
            continue
        if canon == "risk_event":
            if "expect" not in raw:
                raise ValueError("raw risk_event requires expect")
            events.append(
                {
                    "kind": "risk_event",
                    "account_mode": str(raw.get("account_mode") or "DEMO"),
                    "event_type": str(raw["event_type"]),
                    "symbol": str(raw["symbol"]) if raw.get("symbol") is not None else None,
                    "payload": dict(raw["payload"]) if isinstance(raw.get("payload"), dict) else {},
                    "expect": dict(raw["expect"]),
                }
            )
            continue
        if canon == "broker_snapshot":
            if "expect" not in raw:
                raise ValueError("raw broker_snapshot requires expect")
            events.append(
                {
                    "kind": "broker_snapshot",
                    "account_mode": str(raw.get("account_mode") or "DEMO"),
                    "expect": dict(raw["expect"]),
                }
            )
            continue
        raise ValueError(f"unknown raw production record_type: {rec!r}")
    return events


def normalize_raw_production_trade_log_v1(
    raw_lines: list[dict[str, Any]],
    *,
    declared_format_version: str | None = None,
) -> dict[str, Any]:
    """
    Redact, map raw records to sanitized events, return a ``timeline`` dict
    (``symbol`` + ``steps``) compatible with :func:`replay_runner.run_trading_timeline_steps`.

    When the stream declares a registered raw ``format_version`` (NDJSON header or
    ``declared_format_version`` from a JSON bundle), lines are canonicalized through
    :func:`replay_schema_registry.canonicalize_raw_lines` before redaction.
    """
    if not raw_lines:
        raise ValueError("raw production log: empty")

    head = raw_lines[0] if isinstance(raw_lines[0], dict) else {}
    fv_head = str(head.get("format_version") or "").strip() if isinstance(head, dict) else ""
    needs_registry = (
        is_registered_raw_header(head)
        if isinstance(head, dict)
        else False
    ) or (declared_format_version in REGISTERED_RAW_BUNDLE_VERSIONS)

    if needs_registry or fv_head in REGISTERED_RAW_NDJSON_HEADER_VERSIONS:
        lines = [dict(r) for r in raw_lines]
        if isinstance(lines[0], dict) and not str(lines[0].get("format_version") or "").strip():
            if declared_format_version:
                lines[0] = {**lines[0], "format_version": declared_format_version}
        ver = str(lines[0].get("format_version") or declared_format_version or "")
        lines = canonicalize_raw_lines(lines, declared_version=ver or None, context="raw_production")
        scrubbed = redact_sensitive_fields(lines)
    else:
        scrubbed = redact_sensitive_fields(raw_lines)

    if not scrubbed:
        raise ValueError("raw production log: empty")

    from tests.replay.replay_loader import normalize_sanitized_trade_log_v1

    return normalize_sanitized_trade_log_v1(_as_sanitized_events(scrubbed))


def is_raw_production_header(obj: dict[str, Any]) -> bool:
    """True when ``obj`` is the first line of a registered raw capture stream (any supported version)."""
    return is_registered_raw_header(obj)
