"""
Explicit registry of replay capture **format_version** strings, strict bundle validation,
and ordered migration hooks from older raw capture shapes to the canonical parser target.

The canonical raw production timeline parser remains
:func:`production_raw_log.normalize_raw_production_trade_log_v1` (v1). This module answers:

- Is this ``format_version`` a known raw-capture bundle / NDJSON header?
- What migrations must run before handing lines to the v1 normalizer?
- Are required bundle / envelope fields present (fail loud for CI and bad exports)?
- Do canonical v1 body rows satisfy **record-type contracts** (required fields per ``record_type``)?
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# --- Public version constants (single source of truth) --------------------------

RAW_PRODUCTION_TRADE_LOG_V1 = "raw_production_trade_log_v1"
"""Current on-disk / production-aligned raw capture schema consumed by the replay parser."""

RAW_PRODUCTION_TRADE_LOG_V0_COMPAT = "raw_production_trade_log_v0_compat"
"""
Deprecated pre-release shape kept **only** for migration tests and docs.

Historical captures used ``type`` instead of ``record_type`` on body lines.
"""

RAW_PRODUCTION_TRADE_LOG_V2 = "raw_production_trade_log_v2"
"""
Deploy-time capture schema **ahead** of the canonical v1 parser target.

Body lines may use ``event`` instead of ``record_type``, legacy ``type``, and may include
annotation rows (``capture_marker``, ``deploy_span``, ``trace_context``) that are **dropped**
when migrating to v1. The header may carry deploy-only keys (``deploy_schema``, …) stripped
during migration.
"""

SANITIZED_TRADE_LOG_V1 = "sanitized_trade_log_v1"

CANONICAL_RAW_VERSION = RAW_PRODUCTION_TRADE_LOG_V1

# v2-only rows removed before v1 normalization (not representable as sanitized events today).
V2_ANNOTATION_RECORD_TYPES: frozenset[str] = frozenset(
    {"capture_marker", "deploy_span", "trace_context"}
)

# Aliases from production capture / broker envelopes → canonical ``record_type`` names.
RAW_V1_RECORD_TYPE_ALIASES: dict[str, str] = {
    "order.place": "place_order",
    "order.cancel": "cancel_order",
    "order.reconcile": "reconcile_order",
    "risk.log": "risk_event",
    "monitoring.snapshot": "broker_snapshot",
}

# Body ``record_type`` / ``kind`` values allowed on **canonical v1** lines (line 0 = header).
# Includes aliases; :func:`canonical_raw_body_record_type` maps to the canonical name for contracts.
RAW_V1_BODY_RECORD_TYPES: frozenset[str] = frozenset(
    {
        "meta",
        "clock",
        "set_clock",
        "check_settlement",
        "settlement_check",
        "place_order",
        "order.place",
        "cancel_order",
        "order.cancel",
        "reconcile_order",
        "order.reconcile",
        "risk_event",
        "risk.log",
        "broker_snapshot",
        "monitoring.snapshot",
    }
)

RAW_V1_CANONICAL_BODY_RECORD_TYPES: frozenset[str] = frozenset(
    {
        "meta",
        "clock",
        "set_clock",
        "check_settlement",
        "settlement_check",
        "place_order",
        "cancel_order",
        "reconcile_order",
        "risk_event",
        "broker_snapshot",
    }
)


def canonical_raw_body_record_type(row: dict[str, Any]) -> str:
    """Map ``record_type`` / ``kind`` (lowered) through :data:`RAW_V1_RECORD_TYPE_ALIASES` to a canonical name."""
    raw = str(row.get("record_type") or row.get("kind") or "").strip().lower()
    return RAW_V1_RECORD_TYPE_ALIASES.get(raw, raw)

# All ``format_version`` values that mark the start of a **raw** NDJSON capture stream
# (first line is typically a header row).
REGISTERED_RAW_NDJSON_HEADER_VERSIONS: frozenset[str] = frozenset(
    {
        RAW_PRODUCTION_TRADE_LOG_V1,
        RAW_PRODUCTION_TRADE_LOG_V0_COMPAT,
        RAW_PRODUCTION_TRADE_LOG_V2,
    }
)

# Raw JSON bundles: top-level ``format_version`` plus ``records`` | ``lines`` | ``events``.
REGISTERED_RAW_BUNDLE_VERSIONS: frozenset[str] = frozenset(
    {
        RAW_PRODUCTION_TRADE_LOG_V1,
        RAW_PRODUCTION_TRADE_LOG_V0_COMPAT,
        RAW_PRODUCTION_TRADE_LOG_V2,
    }
)

RawLineList = list[dict[str, Any]]
MigrateFn = Callable[[RawLineList], RawLineList]


def migrate_raw_v0_compat_to_v1(lines: RawLineList) -> RawLineList:
    """
    Deterministic rewrite of ``raw_production_trade_log_v0_compat`` lines to v1.

    - Copies each dict (shallow) so callers keep their originals intact.
    - Promotes legacy ``type`` -> ``record_type`` when ``record_type`` is absent.
    - Sets the header's ``format_version`` to :data:`RAW_PRODUCTION_TRADE_LOG_V1`.
    """
    out: RawLineList = []
    for row in lines:
        r = dict(row)
        if "record_type" not in r and "type" in r:
            r["record_type"] = r.pop("type")
        out.append(r)
    if out and str(out[0].get("format_version") or "") == RAW_PRODUCTION_TRADE_LOG_V0_COMPAT:
        out[0] = dict(out[0])
        out[0]["format_version"] = RAW_PRODUCTION_TRADE_LOG_V1
    return out


def migrate_raw_v2_to_v1(lines: RawLineList) -> RawLineList:
    """
    Rewrite ``raw_production_trade_log_v2`` to canonical v1 for the existing parser.

    - Shallow-copies each dict.
    - Header: ``format_version`` -> v1; strips deploy-only keys
      ``deploy_schema``, ``capture_sdk``, ``schema_minor``.
    - Promotes ``event`` -> ``record_type`` when ``record_type`` / ``kind`` absent;
      promotes legacy ``type`` -> ``record_type`` when still missing.
    - Drops :data:`V2_ANNOTATION_RECORD_TYPES` body lines (replay timeline has no equivalent yet).
    """
    out: RawLineList = []
    for idx, row in enumerate(lines):
        r = dict(row)
        if idx == 0:
            for k in ("deploy_schema", "capture_sdk", "schema_minor"):
                r.pop(k, None)
            r["format_version"] = RAW_PRODUCTION_TRADE_LOG_V1
            out.append(r)
            continue
        if "record_type" not in r and "kind" not in r:
            if "event" in r:
                r["record_type"] = str(r.pop("event")).strip()
            elif "type" in r:
                r["record_type"] = str(r.pop("type")).strip()
        rec = str(r.get("record_type") or r.get("kind") or "").strip().lower()
        if rec in V2_ANNOTATION_RECORD_TYPES:
            continue
        if rec in {"clock", "set_clock"}:
            if not r.get("ts"):
                if r.get("iso_utc"):
                    r["ts"] = r["iso_utc"]
                elif r.get("ts_utc"):
                    r["ts"] = r["ts_utc"]
            r.pop("iso_utc", None)
            r.pop("ts_utc", None)
        out.append(r)
    return out


# Directed edges: (source_version, target_version, migrate_fn)
RAW_VERSION_MIGRATIONS: list[tuple[str, str, MigrateFn]] = [
    (RAW_PRODUCTION_TRADE_LOG_V0_COMPAT, RAW_PRODUCTION_TRADE_LOG_V1, migrate_raw_v0_compat_to_v1),
    (RAW_PRODUCTION_TRADE_LOG_V2, RAW_PRODUCTION_TRADE_LOG_V1, migrate_raw_v2_to_v1),
]


def _migration_step(from_ver: str) -> tuple[str, MigrateFn] | None:
    for src, dst, fn in RAW_VERSION_MIGRATIONS:
        if src == from_ver and dst != from_ver:
            return dst, fn
    return None


def canonicalize_raw_lines(
    lines: RawLineList,
    *,
    declared_version: str | None = None,
    context: str = "raw",
) -> RawLineList:
    """
    Walk migration edges until ``lines`` are tagged as :data:`CANONICAL_RAW_VERSION`.

    ``declared_version`` is the envelope version (NDJSON header or bundle top-level).
    After migration, runs :func:`validate_canonical_raw_v1_envelope` on the result.
    """
    if not lines:
        raise ValueError(f"{context}: raw production log is empty")

    ver = declared_version or str(lines[0].get("format_version") or "")
    if not ver:
        raise ValueError(f"{context}: raw capture missing format_version on header line")

    if ver not in REGISTERED_RAW_NDJSON_HEADER_VERSIONS and ver not in REGISTERED_RAW_BUNDLE_VERSIONS:
        known = ", ".join(sorted(REGISTERED_RAW_BUNDLE_VERSIONS))
        raise ValueError(f"{context}: unsupported raw production replay format_version {ver!r} (known: {known})")

    current: RawLineList = [dict(r) for r in lines]
    current_ver = ver
    seen: set[str] = set()
    while current_ver != CANONICAL_RAW_VERSION:
        if current_ver in seen:
            raise ValueError(f"{context}: migration loop detected at {current_ver!r}")
        seen.add(current_ver)
        step = _migration_step(current_ver)
        if step is None:
            raise ValueError(
                f"{context}: no migration path from {current_ver!r} to {CANONICAL_RAW_VERSION!r}"
            )
        nxt, fn = step
        current = fn(current)
        current_ver = nxt

    validate_canonical_raw_v1_envelope(current, context=context)
    validate_canonical_raw_v1_record_contracts(current, context=context)
    return current


def validate_canonical_raw_v1_envelope(lines: RawLineList, *, context: str = "raw") -> None:
    """
    Strict structural checks for **canonical** v1 lines *before* redaction / mapping.

    - Header (line 0) must declare :data:`RAW_PRODUCTION_TRADE_LOG_V1` and act as capture header
      (``role == \"header\"`` or ``record_type == \"header\"``).
    - Every row is a dict with ``record_type`` / header role (after migration), except header uses
      ``role`` + ``format_version`` as today in fixtures.
    """
    if not lines:
        raise ValueError(f"{context}: raw production log is empty")
    head = lines[0]
    fv = str(head.get("format_version") or "")
    if fv != RAW_PRODUCTION_TRADE_LOG_V1:
        raise ValueError(
            f"{context}: expected header format_version {RAW_PRODUCTION_TRADE_LOG_V1!r}, got {fv!r}"
        )
    is_hdr = head.get("role") == "header" or str(head.get("record_type") or "").strip().lower() == "header"
    if not is_hdr:
        raise ValueError(
            f"{context}: strict raw v1 requires first record to be a header "
            f"(role=header or record_type=header), keys={list(head)!r}"
        )


def _body_record_kind(row: dict[str, Any]) -> str:
    return str(row.get("record_type") or row.get("kind") or "").strip().lower()


def validate_canonical_raw_v1_record_contracts(lines: RawLineList, *, context: str = "raw") -> None:
    """
    Strict per-row contracts for canonical v1 **body** lines (index >= 1).

    Matches the minimum field requirements enforced later by
    :func:`production_raw_log.normalize_raw_production_trade_log_v1` so CI fails at a single,
    explicit schema boundary.
    """
    for i, row in enumerate(lines[1:], start=1):
        if not isinstance(row, dict):
            raise ValueError(f"{context}: record {i} must be an object, got {type(row).__name__}")
        kind = _body_record_kind(row)
        if not kind:
            raise ValueError(f"{context}: record {i} missing record_type/kind, keys={list(row)!r}")
        if kind == "header":
            raise ValueError(
                f"{context}: record {i} must not reuse header record_type=header "
                f"(only line 0 may be the capture header)"
            )
        if kind not in RAW_V1_BODY_RECORD_TYPES:
            raise ValueError(f"{context}: record {i} unsupported record_type/kind {kind!r}")
        canon = canonical_raw_body_record_type(row)
        if canon not in RAW_V1_CANONICAL_BODY_RECORD_TYPES:
            raise ValueError(f"{context}: record {i} unknown canonical record type after alias map {canon!r}")
        if canon in {"clock", "set_clock"}:
            if not (row.get("iso_utc") or row.get("ts_utc") or row.get("ts")):
                raise ValueError(
                    f"{context}: record {i} (clock) requires one of iso_utc, ts_utc, ts"
                )
            continue
        if canon in {"check_settlement", "settlement_check"}:
            if "quantity" not in row:
                raise ValueError(f"{context}: record {i} (check_settlement) requires quantity")
            if "expect" not in row:
                raise ValueError(f"{context}: record {i} (check_settlement) requires expect")
            continue
        if canon == "place_order":
            if "expect" not in row:
                raise ValueError(f"{context}: record {i} (place_order) requires expect")
            order = row.get("order") or row.get("payload")
            if not isinstance(order, dict):
                raise ValueError(f"{context}: record {i} (place_order) requires order|payload dict")
            continue
        if canon == "cancel_order":
            if "expect" not in row:
                raise ValueError(f"{context}: record {i} (cancel_order) requires expect")
            if not row.get("order_id") and not row.get("idempotency_key"):
                raise ValueError(
                    f"{context}: record {i} (cancel_order) requires order_id or idempotency_key"
                )
            continue
        if canon == "reconcile_order":
            if "expect" not in row:
                raise ValueError(f"{context}: record {i} (reconcile_order) requires expect")
            if not row.get("order_id") and not row.get("idempotency_key"):
                raise ValueError(
                    f"{context}: record {i} (reconcile_order) requires order_id or idempotency_key"
                )
            continue
        if canon == "risk_event":
            if "expect" not in row:
                raise ValueError(f"{context}: record {i} (risk_event) requires expect")
            if not str(row.get("event_type") or "").strip():
                raise ValueError(f"{context}: record {i} (risk_event) requires event_type")
            continue
        if canon == "broker_snapshot":
            if "expect" not in row:
                raise ValueError(f"{context}: record {i} (broker_snapshot) requires expect")
            continue
        # meta: no required fields


def extract_raw_bundle_record_array(doc: dict[str, Any]) -> list[Any]:
    """Return the list container for raw bundle documents (mutually exclusive keys)."""
    for key in ("records", "lines", "events"):
        if key in doc and doc[key] is not None:
            return doc[key]  # type: ignore[return-value]
    raise ValueError("raw production log bundle requires records|lines|events array")


def validate_raw_bundle_document(doc: dict[str, Any], *, path_hint: str = "") -> tuple[str, list[Any]]:
    """
    Validate a JSON **bundle** (dict) carrying a registered raw ``format_version``.

    Returns ``(format_version, records_list)`` for downstream canonicalization.
    """
    ctx = path_hint or "bundle"
    fv = str(doc.get("format_version") or "")
    if fv not in REGISTERED_RAW_BUNDLE_VERSIONS:
        known = ", ".join(sorted(REGISTERED_RAW_BUNDLE_VERSIONS))
        raise ValueError(f"{ctx}: unsupported raw bundle format_version {fv!r} (known: {known})")

    raw_lines = extract_raw_bundle_record_array(doc)
    if not isinstance(raw_lines, list):
        raise ValueError(f"{ctx}: records|lines|events must be a JSON array")

    if len(raw_lines) == 0:
        raise ValueError(f"{ctx}: raw bundle records array is empty")

    for i, row in enumerate(raw_lines):
        if not isinstance(row, dict):
            raise ValueError(f"{ctx}: record {i} must be an object, got {type(row).__name__}")

    inner_fv = raw_lines[0].get("format_version")
    if inner_fv is not None and str(inner_fv).strip() and str(inner_fv) != fv:
        raise ValueError(
            f"{ctx}: inner header format_version {inner_fv!r} must match bundle {fv!r} when present"
        )

    return fv, raw_lines


def is_registered_raw_header(obj: dict[str, Any]) -> bool:
    """True when ``obj`` is the first NDJSON line of a registered raw capture stream."""
    ver = str(obj.get("format_version") or "")
    return ver in REGISTERED_RAW_NDJSON_HEADER_VERSIONS
