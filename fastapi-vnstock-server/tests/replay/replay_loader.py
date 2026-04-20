"""
Load replay fixtures in multiple shapes and normalize to the timeline schema
expected by :func:`replay_runner.run_trading_timeline_steps`.

Supported inputs:

1. **Timeline JSON** (existing): ``{ "symbol": "...", "steps": [ { "op": ... }, ... ] }``
2. **NDJSON / .jsonl**: one JSON object per line; each object is a *step* with an ``op`` key
   (same objects as the ``steps`` array in (1)). Optional first-line metadata object with keys
   ``replay_meta`` + ``symbol`` is ignored except for ``symbol``.
3. **Sanitized trade log v1** (array or wrapped object): production-style records that are
   transformed into ``op`` steps — see :func:`normalize_sanitized_trade_log_v1`.
4. **Raw production trade log** (NDJSON / JSON bundle): capture-shaped lines with a
   registered ``format_version`` (see :mod:`tests.replay.replay_schema_registry`),
   ``record_type``, nested ``order`` payloads, and optional secrets. Canonical target is
   **v1**; older registered versions are migrated before parsing. See
   :func:`production_raw_log.normalize_raw_production_trade_log_v1` (applies
   :func:`production_log_redact.redact_sensitive_fields` before normalization).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tests.replay.production_raw_log import (
    is_raw_production_header,
    normalize_raw_production_trade_log_v1,
)
from tests.replay.replay_schema_registry import (
    REGISTERED_RAW_BUNDLE_VERSIONS,
    REGISTERED_RAW_NDJSON_HEADER_VERSIONS,
    validate_raw_bundle_document,
)
from tests.replay.replay_runner import load_json_document, load_ndjson_steps

_RAW_BUNDLE_PREFIX = "raw_production_trade_log"


def load_replay_timeline(path: Path) -> dict[str, Any]:
    """
    Auto-detect JSON timeline vs NDJSON step stream vs sanitized log v1.
    """
    suf = path.suffix.lower()
    if suf in {".ndjson", ".jsonl"}:
        steps = load_ndjson_steps(path)
        if steps and isinstance(steps[0], dict):
            fv0 = str(steps[0].get("format_version") or "")
            if fv0.startswith(_RAW_BUNDLE_PREFIX) and fv0 not in REGISTERED_RAW_NDJSON_HEADER_VERSIONS:
                known = ", ".join(sorted(REGISTERED_RAW_NDJSON_HEADER_VERSIONS))
                raise ValueError(
                    f"unsupported raw production NDJSON format_version {fv0!r} in {path} "
                    f"(known: {known})"
                )
        if steps and isinstance(steps[0], dict) and is_raw_production_header(steps[0]):
            return normalize_raw_production_trade_log_v1(steps)
        symbol = ""
        if steps and isinstance(steps[0], dict) and "replay_meta" in steps[0]:
            meta = steps[0]
            symbol = str(meta.get("symbol") or "")
            steps = steps[1:]
        return {"symbol": symbol, "steps": steps}

    doc = load_json_document(path)
    if isinstance(doc, dict):
        fv = str(doc.get("format_version") or "")
        raw_container_keys = ("records", "lines", "events")
        has_raw_container = any(
            k in doc and isinstance(doc.get(k), list) for k in raw_container_keys
        )
        if has_raw_container and fv.startswith(_RAW_BUNDLE_PREFIX):
            if fv not in REGISTERED_RAW_BUNDLE_VERSIONS:
                known = ", ".join(sorted(REGISTERED_RAW_BUNDLE_VERSIONS))
                raise ValueError(
                    f"unsupported raw production replay bundle format_version {fv!r} "
                    f"in {path} (known: {known})"
                )
            _, raw_lines = validate_raw_bundle_document(doc, path_hint=str(path))
            return normalize_raw_production_trade_log_v1(
                [dict(r) for r in raw_lines],
                declared_format_version=fv,
            )
    if isinstance(doc, list):
        return normalize_sanitized_trade_log_v1(doc)
    if isinstance(doc, dict) and doc.get("format_version") == "sanitized_trade_log_v1":
        return normalize_sanitized_trade_log_v1(doc["events"])
    if isinstance(doc, dict) and "steps" in doc:
        return doc
    raise ValueError(f"unsupported replay document: {path}")


def normalize_sanitized_trade_log_v1(events: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Map sanitized log-like records to internal ``steps``.

    Each event:

    - ``kind`` (required): ``clock`` | ``check_settlement`` | ``place_order`` |
      ``cancel_order`` | ``reconcile_order`` | ``risk_event`` | ``broker_snapshot``
    - ``ts`` / ``ts_utc`` / ``iso_utc``: ISO-8601 timestamp for ``clock`` (required for clock)
    - ``symbol``, ``account_mode``: optional defaults for later steps
    - ``quantity``, ``expect``: settlement check
    - ``payload``, ``expect``: place order

    This is intentionally strict so bad fixtures fail loudly in tests.
    """
    symbol_default = ""
    account_default = "DEMO"
    steps: list[dict[str, Any]] = []

    for ev in events:
        kind = str(ev.get("kind") or "").strip().lower()
        if kind in {"clock", "set_clock"}:
            iso = ev.get("iso_utc") or ev.get("ts_utc") or ev.get("ts")
            if not iso:
                raise ValueError("clock event requires iso_utc / ts_utc / ts")
            steps.append({"op": "set_clock", "iso_utc": str(iso)})
            continue
        if kind in {"check_settlement", "settlement_check"}:
            sym = str(ev.get("symbol") or symbol_default)
            if not sym:
                raise ValueError("check_settlement requires symbol (or prior default)")
            steps.append(
                {
                    "op": "check_settlement",
                    "account_mode": str(ev.get("account_mode") or account_default),
                    "symbol": sym,
                    "quantity": int(ev["quantity"]),
                    "expect": dict(ev["expect"]),
                }
            )
            continue
        if kind in {"place_order", "order.place"}:
            payload = dict(ev["payload"])
            if "symbol" not in payload and symbol_default:
                payload = {**payload, "symbol": symbol_default}
            steps.append({"op": "place_order", "payload": payload, "expect": dict(ev["expect"])})
            continue
        if kind == "cancel_order":
            if not ev.get("order_id") and not ev.get("idempotency_key"):
                raise ValueError("cancel_order requires order_id or idempotency_key")
            step: dict[str, Any] = {
                "op": "cancel_order",
                "reason": str(ev.get("reason") or "replay_cancel"),
                "expect": dict(ev["expect"]),
            }
            if ev.get("order_id"):
                step["order_id"] = str(ev["order_id"])
            if ev.get("idempotency_key"):
                step["idempotency_key"] = str(ev["idempotency_key"])
            if ev.get("account_mode"):
                step["account_mode"] = str(ev["account_mode"])
            steps.append(step)
            continue
        if kind == "reconcile_order":
            if not ev.get("order_id") and not ev.get("idempotency_key"):
                raise ValueError("reconcile_order requires order_id or idempotency_key")
            stp: dict[str, Any] = {"op": "reconcile_order", "expect": dict(ev["expect"])}
            if ev.get("order_id"):
                stp["order_id"] = str(ev["order_id"])
            if ev.get("idempotency_key"):
                stp["idempotency_key"] = str(ev["idempotency_key"])
            if ev.get("account_mode"):
                stp["account_mode"] = str(ev["account_mode"])
            steps.append(stp)
            continue
        if kind == "risk_event":
            steps.append(
                {
                    "op": "risk_event",
                    "account_mode": str(ev.get("account_mode") or account_default),
                    "event_type": str(ev["event_type"]),
                    "symbol": str(ev["symbol"]) if ev.get("symbol") is not None else None,
                    "payload": dict(ev["payload"]) if isinstance(ev.get("payload"), dict) else {},
                    "expect": dict(ev["expect"]),
                }
            )
            continue
        if kind == "broker_snapshot":
            steps.append(
                {
                    "op": "broker_snapshot",
                    "account_mode": str(ev.get("account_mode") or account_default),
                    "expect": dict(ev["expect"]),
                }
            )
            continue
        if kind == "meta":
            symbol_default = str(ev.get("symbol") or symbol_default)
            account_default = str(ev.get("account_mode") or account_default)
            continue
        raise ValueError(f"unknown sanitized log kind: {kind!r}")

    out_symbol = symbol_default
    return {"symbol": out_symbol, "steps": steps}
