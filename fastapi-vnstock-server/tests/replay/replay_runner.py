"""
Shared replay execution: timeline ``steps`` -> ``trading_core_service`` calls.

Used by JSON fixtures, NDJSON line logs, and sanitized production-style logs
after they are normalized to the same step schema.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from app.services.trading_core_service import check_settlement, place_order


def _resolve_order_id(tcs_module: Any, account_mode: str, step: dict[str, Any]) -> str:
    if step.get("order_id"):
        return str(step["order_id"])
    idem = step.get("idempotency_key")
    if not idem:
        raise AssertionError("step requires order_id or idempotency_key")
    idem_s = str(idem)
    for o in tcs_module.list_orders(account_mode, limit=500):
        if str(o.get("idempotency_key") or "") == idem_s:
            return str(o["id"])
    raise AssertionError(f"no order found for idempotency_key={idem_s!r}")


def parse_iso_utc(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def run_trading_timeline_steps(
    *,
    monkeypatch: pytest.MonkeyPatch,
    timeline: dict[str, Any],
    tcs_module: Any,
) -> None:
    """
    Drive ``check_settlement`` / ``place_order`` from a ``timeline`` dict:

    - ``symbol`` (optional; per-step payloads may include symbol)
    - ``steps``: list of operation dicts (``set_clock``, ``check_settlement``, ``place_order``)
    """
    symbol = str(timeline.get("symbol") or "")
    clock = {"t": parse_iso_utc("2026-01-01T10:00:00Z")}

    def _now():
        return clock["t"]

    monkeypatch.setattr(tcs_module, "_utc_now", _now)
    monkeypatch.setattr(tcs_module, "_settlement_effective_today", lambda: clock["t"].date())

    for step in timeline["steps"]:
        op = step["op"]
        if op == "set_clock":
            clock["t"] = parse_iso_utc(str(step["iso_utc"]))
            continue
        if op == "check_settlement":
            qty = int(step["quantity"])
            sym = str(step.get("symbol") or symbol)
            st = check_settlement(str(step.get("account_mode", "DEMO")), sym, qty)
            exp = step["expect"]
            assert st["pass"] is exp["pass"]
            assert int(st["available_qty"]) == int(exp["available_qty"])
            continue
        if op == "place_order":
            row = place_order(step["payload"])
            exp = step["expect"]
            assert str(row.get("status")).upper() == str(exp["status"]).upper()
            if "reason" in exp:
                assert str(row.get("reason")) == str(exp["reason"])
            continue
        if op == "cancel_order":
            am = str(step.get("account_mode") or "DEMO")
            oid = _resolve_order_id(tcs_module, am, step)
            out = tcs_module.cancel_order(oid, str(step.get("reason") or "replay_cancel"))
            exp = step["expect"]
            if "cancelled" in exp:
                assert out.get("cancelled") is exp["cancelled"]
            if "reason" in exp:
                assert str(out.get("reason")) == str(exp["reason"])
            if exp.get("order_status"):
                assert str((out.get("order") or {}).get("status")).upper() == str(exp["order_status"]).upper()
            continue
        if op == "reconcile_order":
            am = str(step.get("account_mode") or "DEMO")
            oid = _resolve_order_id(tcs_module, am, step)
            out = tcs_module.reconcile_order(oid)
            exp = step["expect"]
            if "reconciled" in exp:
                assert out.get("reconciled") is exp["reconciled"]
            if "reason" in exp:
                assert str(out.get("reason")) == str(exp["reason"])
            if "reason_contains" in exp:
                assert str(exp["reason_contains"]) in str(out.get("reason") or "")
            continue
        if op == "risk_event":
            am = str(step["account_mode"] or "DEMO")
            et = str(step["event_type"])
            sym = step.get("symbol")
            sym_s = str(sym) if sym is not None else None
            payload = step.get("payload") if isinstance(step.get("payload"), dict) else {}
            logged = tcs_module.log_risk_event(am, et, symbol=sym_s, payload=payload)
            exp = step["expect"]
            if "event_type" in exp:
                assert str(logged.get("event_type")) == str(exp["event_type"])
            if exp.get("verify_listed"):
                recent = tcs_module.list_risk_events(am, limit=50, event_type=et)
                assert any(str(r.get("id")) == str(logged.get("id")) for r in recent)
            continue
        if op == "broker_snapshot":
            am = str(step.get("account_mode") or "DEMO")
            summary = tcs_module.get_monitoring_summary(am)
            exp = step["expect"]
            for k in exp.get("contains_keys", ()):
                assert k in summary, f"missing summary key {k!r}"
            if "min_risk_events_last_7_days" in exp:
                assert int(summary["risk_events_last_7_days"]) >= int(exp["min_risk_events_last_7_days"])
            if "account_mode" in exp:
                assert str(summary.get("account_mode")) == str(exp["account_mode"])
            continue
        raise AssertionError(f"unknown op: {op}")


def load_json_document(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_ndjson_steps(path: Path) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        steps.append(json.loads(line))
    return steps
