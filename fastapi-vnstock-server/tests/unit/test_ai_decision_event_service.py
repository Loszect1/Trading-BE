from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def _unwrap_json(value: Any) -> Any:
    return getattr(value, "obj", value)


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self.db = db
        self._one: dict[str, Any] | None = None
        self._rows: list[dict[str, Any]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, query: str, params: dict[str, Any] | None = None) -> None:
        params = params or {}
        compact_query = " ".join(query.split())
        now = datetime.now(tz=timezone.utc)
        if "INSERT INTO ai_decision_events" in compact_query:
            idem = str(params["idempotency_key"])
            if idem in self.db.rows_by_idem:
                self._one = None
                return
            row = {
                "id": params["id"],
                "created_at": now,
                "updated_at": now,
                "workflow_type": params["workflow_type"],
                "account_mode": params["account_mode"],
                "symbol": params["symbol"],
                "strategy_type": params["strategy_type"],
                "source_type": params["source_type"],
                "source_id": params["source_id"],
                "session_id": params["session_id"],
                "idempotency_key": idem,
                "model": params["model"],
                "schema_version": params["schema_version"],
                "prompt_hash": params["prompt_hash"],
                "confidence": params["confidence"],
                "reuse_status": params["reuse_status"],
                "input_snapshot": _unwrap_json(params["input_snapshot"]),
                "llm_recommendation": _unwrap_json(params["llm_recommendation"]),
                "final_system_decision": _unwrap_json(params["final_system_decision"]),
                "guardrail_result": _unwrap_json(params["guardrail_result"]),
            }
            self.db.rows_by_idem[idem] = row
            self._one = row
            return
        if "WHERE idempotency_key = %(idempotency_key)s" in compact_query:
            self._one = self.db.rows_by_idem.get(str(params["idempotency_key"]))
            return
        if "UPDATE ai_decision_events" in compact_query:
            target = str(params["id"])
            self._one = None
            for row in self.db.rows_by_idem.values():
                if str(row["id"]) == target:
                    row["reuse_status"] = params["reuse_status"]
                    row["updated_at"] = now
                    self._one = row
                    return
        if "FROM ai_decision_events" in compact_query and "ORDER BY created_at DESC" in compact_query:
            rows = list(self.db.rows_by_idem.values())
            if "symbol IS NULL" in compact_query:
                rows = [row for row in rows if row.get("symbol") is None]
            if params.get("workflow_type"):
                rows = [row for row in rows if row.get("workflow_type") == params["workflow_type"]]
            if params.get("symbol"):
                rows = [row for row in rows if row.get("symbol") == params["symbol"]]
            if params.get("strategy_type"):
                rows = [
                    row
                    for row in rows
                    if row.get("strategy_type") == params["strategy_type"] or row.get("strategy_type") is None
                ]
            if "reuse_status = 'APPROVED'" in compact_query and "created_at >= NOW()" in compact_query:
                min_conf = float(params.get("min_recent_confidence") or 75)
                rows = [
                    row
                    for row in rows
                    if row.get("reuse_status") == "APPROVED"
                    or (row.get("reuse_status") == "NEW" and float(row.get("confidence") or 0) >= min_conf)
                ]
            elif "reuse_status = 'APPROVED'" in compact_query:
                rows = [row for row in rows if row.get("reuse_status") == "APPROVED"]
            self._rows = rows[: int(params.get("limit") or 50)]
            return
        self._one = None
        self._rows = []

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeConn:
    def __init__(self, db: "_FakeDb") -> None:
        self.db = db

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor(self.db)

    def commit(self) -> None:
        self.db.commits += 1


class _FakeDb:
    def __init__(self) -> None:
        self.rows_by_idem: dict[str, dict[str, Any]] = {}
        self.commits = 0

    def connect(self, *_args: Any, **_kwargs: Any) -> _FakeConn:
        return _FakeConn(self)


def test_record_ai_decision_event_is_idempotent(monkeypatch) -> None:
    import app.services.ai_decision_event_service as svc

    fake_db = _FakeDb()
    monkeypatch.setattr(svc, "connect", fake_db.connect)

    first = svc.record_ai_decision_event(
        workflow_type="signal_scoring",
        account_mode="demo",
        symbol="fpt",
        strategy_type="short_term",
        source_type="signal",
        source_id="sig-1",
        idempotency_key="same-key",
        prompt_text="prompt",
        confidence=82,
        input_snapshot={"raw": "x"},
        llm_recommendation={"score_commentary": "ok"},
        final_system_decision={"action": "BUY"},
        guardrail_result={"status": "PASS"},
    )
    second = svc.record_ai_decision_event(
        workflow_type="SIGNAL_SCORING",
        account_mode="DEMO",
        symbol="FPT",
        strategy_type="SHORT_TERM",
        source_type="signal",
        source_id="sig-1",
        idempotency_key="same-key",
        prompt_text="prompt",
        confidence=10,
        llm_recommendation={"score_commentary": "different"},
    )

    assert first["id"] == second["id"]
    assert len(fake_db.rows_by_idem) == 1
    assert first["symbol"] == "FPT"
    assert first["account_mode"] == "DEMO"
    assert first["reuse_status"] == "NEW"


def test_symbol_memory_excludes_rejected_and_expired(monkeypatch) -> None:
    import app.services.ai_decision_event_service as svc

    fake_db = _FakeDb()
    monkeypatch.setattr(svc, "connect", fake_db.connect)
    approved = svc.record_ai_decision_event(
        workflow_type="EXPERIENCE_ANALYSIS",
        symbol="FPT",
        strategy_type="SHORT_TERM",
        idempotency_key="approved",
        prompt_text="a",
        confidence=55,
        reuse_status="APPROVED",
        llm_recommendation={"root_cause": "weak_volume_confirmation"},
    )
    high_conf_new = svc.record_ai_decision_event(
        workflow_type="SIGNAL_SCORING",
        symbol="FPT",
        strategy_type="SHORT_TERM",
        idempotency_key="new",
        prompt_text="b",
        confidence=90,
        reuse_status="NEW",
        llm_recommendation={"score_commentary": "good memory"},
    )
    rejected = svc.record_ai_decision_event(
        workflow_type="SIGNAL_SCORING",
        symbol="FPT",
        strategy_type="SHORT_TERM",
        idempotency_key="rejected",
        prompt_text="c",
        confidence=99,
        reuse_status="REJECTED",
        llm_recommendation={"score_commentary": "do not use"},
    )
    expired = svc.record_ai_decision_event(
        workflow_type="SIGNAL_SCORING",
        symbol="FPT",
        strategy_type="SHORT_TERM",
        idempotency_key="expired",
        prompt_text="d",
        confidence=99,
        reuse_status="EXPIRED",
        llm_recommendation={"score_commentary": "do not use"},
    )

    rows = svc.get_symbol_ai_memory(symbol="FPT", strategy_type="SHORT_TERM", limit=10)
    ids = {row["id"] for row in rows}

    assert approved["id"] in ids
    assert high_conf_new["id"] in ids
    assert rejected["id"] not in ids
    assert expired["id"] not in ids
    summary = svc.summarize_reusable_ai_lessons(rows)
    assert all(item["reuse_status"] in {"APPROVED", "NEW"} for item in summary)


def test_update_ai_decision_reuse_status(monkeypatch) -> None:
    import app.services.ai_decision_event_service as svc

    fake_db = _FakeDb()
    monkeypatch.setattr(svc, "connect", fake_db.connect)
    row = svc.record_ai_decision_event(
        workflow_type="SIGNAL_SCORING",
        idempotency_key="status",
        prompt_text="prompt",
        llm_recommendation={"decision": "BUY"},
    )

    updated = svc.update_ai_decision_reuse_status(event_id=row["id"], reuse_status="approved")

    assert updated is not None
    assert updated["id"] == row["id"]
    assert updated["reuse_status"] == "APPROVED"


def test_global_ai_memory_uses_only_approved_symbolless_rows(monkeypatch) -> None:
    import app.services.ai_decision_event_service as svc

    fake_db = _FakeDb()
    monkeypatch.setattr(svc, "connect", fake_db.connect)
    approved = svc.record_ai_decision_event(
        workflow_type="MACRO_STRATEGY_MEMORY",
        strategy_type="LONG_TERM",
        source_type="manual_user_strategy_plan",
        source_id="balanced",
        idempotency_key="approved-global",
        confidence=75,
        reuse_status="APPROVED",
        prompt_text="approved",
        llm_recommendation={
            "title": "Balanced strategy",
            "summary": "Use as context.",
            "allocation": {"cash": "25-30"},
            "rules": ["No leverage"],
            "monitoring_metrics": ["CPI"],
            "invalidation_triggers": ["Inflation stress"],
            "assumptions": ["Mostly cash"],
        },
        final_system_decision={"scope": ["macro_gpt_analysis", "long_term_research"]},
    )
    svc.record_ai_decision_event(
        workflow_type="MACRO_STRATEGY_MEMORY",
        strategy_type="LONG_TERM",
        idempotency_key="new-global",
        confidence=99,
        reuse_status="NEW",
        prompt_text="new",
        llm_recommendation={"title": "Do not auto-use"},
    )
    svc.record_ai_decision_event(
        workflow_type="MACRO_STRATEGY_MEMORY",
        symbol="FPT",
        strategy_type="LONG_TERM",
        idempotency_key="symbol-memory",
        confidence=99,
        reuse_status="APPROVED",
        prompt_text="symbol",
        llm_recommendation={"title": "Symbol memory"},
    )

    rows = svc.get_global_ai_memory(limit=10)
    summary = svc.summarize_global_ai_memory(rows)

    assert [row["id"] for row in rows] == [approved["id"]]
    assert summary == [
        {
            "workflow_type": "MACRO_STRATEGY_MEMORY",
            "strategy_type": "LONG_TERM",
            "source_type": "manual_user_strategy_plan",
            "source_id": "balanced",
            "confidence": 75.0,
            "reuse_status": "APPROVED",
            "memory": {
                "title": "Balanced strategy",
                "summary": "Use as context.",
                "allocation": {"cash": "25-30"},
                "rules": ["No leverage"],
                "monitoring_metrics": ["CPI"],
                "invalidation_triggers": ["Inflation stress"],
                "assumptions": ["Mostly cash"],
            },
            "scope": ["macro_gpt_analysis", "long_term_research"],
        }
    ]
