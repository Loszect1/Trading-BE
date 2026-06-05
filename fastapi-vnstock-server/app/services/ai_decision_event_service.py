from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings

_REUSE_STATUSES = {"NEW", "APPROVED", "REJECTED", "EXPIRED"}
_MAX_TEXT_CHARS = 2000
_MAX_LIST_ITEMS = 40
_MAX_DICT_ITEMS = 80
_CONNECT_TIMEOUT_SECONDS = 2


def ensure_ai_decision_events_table() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS ai_decision_events (
        id UUID PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        workflow_type VARCHAR(64) NOT NULL,
        account_mode VARCHAR(10) NULL CHECK (account_mode IS NULL OR account_mode IN ('REAL', 'DEMO')),
        symbol VARCHAR(20) NULL,
        strategy_type VARCHAR(20) NULL CHECK (
            strategy_type IS NULL
            OR strategy_type IN ('SHORT_TERM', 'LONG_TERM', 'TECHNICAL', 'MAIL_SIGNAL')
        ),
        source_type VARCHAR(64) NULL,
        source_id TEXT NULL,
        session_id VARCHAR(128) NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        model TEXT NULL,
        schema_version VARCHAR(32) NOT NULL DEFAULT '1.0',
        prompt_hash TEXT NOT NULL,
        confidence NUMERIC(5,2) NULL CHECK (confidence IS NULL OR confidence >= 0 AND confidence <= 100),
        reuse_status VARCHAR(16) NOT NULL DEFAULT 'NEW' CHECK (reuse_status IN ('NEW', 'APPROVED', 'REJECTED', 'EXPIRED')),
        input_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        llm_recommendation JSONB NOT NULL DEFAULT '{}'::jsonb,
        final_system_decision JSONB NOT NULL DEFAULT '{}'::jsonb,
        guardrail_result JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE INDEX IF NOT EXISTS idx_ai_decision_events_workflow_created
        ON ai_decision_events(workflow_type, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ai_decision_events_symbol_strategy_created
        ON ai_decision_events(symbol, strategy_type, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ai_decision_events_mode_workflow_created
        ON ai_decision_events(account_mode, workflow_type, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ai_decision_events_source
        ON ai_decision_events(source_type, source_id);
    """
    with connect(settings.database_url, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def build_prompt_hash(prompt_text: str) -> str:
    return hashlib.sha256(str(prompt_text or "").encode("utf-8")).hexdigest()


def _compact_text(value: Any, max_chars: int = _MAX_TEXT_CHARS) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)] + "..."


def _json_safe(value: Any, *, depth: int = 0) -> Any:
    if depth >= 5:
        return _compact_text(value, 500)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for idx, (key, item) in enumerate(value.items()):
            if idx >= _MAX_DICT_ITEMS:
                out["_truncated"] = True
                break
            out[str(key)] = _json_safe(item, depth=depth + 1)
        return out
    if isinstance(value, (list, tuple, set)):
        rows = list(value)
        compacted = [_json_safe(item, depth=depth + 1) for item in rows[:_MAX_LIST_ITEMS]]
        if len(rows) > _MAX_LIST_ITEMS:
            compacted.append({"_truncated": True, "remaining_count": len(rows) - _MAX_LIST_ITEMS})
        return compacted
    if isinstance(value, str):
        return _compact_text(value)
    return value


def _normal_status(value: str | None) -> str:
    status = str(value or "NEW").strip().upper()
    return status if status in _REUSE_STATUSES else "NEW"


def _normal_optional_upper(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _coerce_confidence(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(100.0, parsed))


def _row_to_dict(row: Any) -> dict[str, Any]:
    data = dict(row or {})
    for key in ("id",):
        if data.get(key) is not None:
            data[key] = str(data[key])
    if data.get("confidence") is not None:
        data["confidence"] = float(data["confidence"])
    return data


def _derive_idempotency_key(
    *,
    workflow_type: str,
    source_type: str | None,
    source_id: str | None,
    symbol: str | None,
    strategy_type: str | None,
    prompt_hash: str,
) -> str:
    if source_type and source_id:
        return f"{workflow_type}:{source_type}:{source_id}:{prompt_hash}"
    identity = ":".join(part for part in (workflow_type, symbol or "", strategy_type or "", prompt_hash) if part)
    return identity or f"{workflow_type}:{prompt_hash}"


def record_ai_decision_event(
    *,
    workflow_type: str,
    llm_recommendation: dict[str, Any],
    final_system_decision: dict[str, Any] | None = None,
    guardrail_result: dict[str, Any] | None = None,
    input_snapshot: dict[str, Any] | None = None,
    account_mode: str | None = None,
    symbol: str | None = None,
    strategy_type: str | None = None,
    source_type: str | None = None,
    source_id: str | UUID | None = None,
    session_id: str | None = None,
    idempotency_key: str | None = None,
    model: str | None = None,
    schema_version: str = "1.0",
    prompt_text: str | None = None,
    prompt_hash: str | None = None,
    confidence: Any = None,
    reuse_status: str | None = "NEW",
) -> dict[str, Any]:
    ensure_ai_decision_events_table()
    workflow = str(workflow_type or "").strip().upper() or "UNKNOWN"
    source = str(source_type or "").strip() or None
    source_key = str(source_id).strip() if source_id is not None and str(source_id).strip() else None
    sym = _normal_optional_upper(symbol)
    strategy = _normal_optional_upper(strategy_type)
    prompt_digest = str(prompt_hash or "").strip() or build_prompt_hash(
        prompt_text
        or json.dumps(
            {
                "workflow_type": workflow,
                "input_snapshot": _json_safe(input_snapshot or {}),
                "llm_recommendation": _json_safe(llm_recommendation or {}),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    idem = str(idempotency_key or "").strip() or _derive_idempotency_key(
        workflow_type=workflow,
        source_type=source,
        source_id=source_key,
        symbol=sym,
        strategy_type=strategy,
        prompt_hash=prompt_digest,
    )

    params = {
        "id": uuid4(),
        "workflow_type": workflow,
        "account_mode": _normal_optional_upper(account_mode),
        "symbol": sym,
        "strategy_type": strategy,
        "source_type": source,
        "source_id": source_key,
        "session_id": str(session_id).strip() if session_id else None,
        "idempotency_key": idem,
        "model": str(model or settings.gpt_model or "").strip() or None,
        "schema_version": str(schema_version or "1.0").strip()[:32],
        "prompt_hash": prompt_digest,
        "confidence": _coerce_confidence(confidence),
        "reuse_status": _normal_status(reuse_status),
        "input_snapshot": Json(_json_safe(input_snapshot or {})),
        "llm_recommendation": Json(_json_safe(llm_recommendation or {})),
        "final_system_decision": Json(_json_safe(final_system_decision or {})),
        "guardrail_result": Json(_json_safe(guardrail_result or {})),
    }
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai_decision_events (
                    id, workflow_type, account_mode, symbol, strategy_type,
                    source_type, source_id, session_id, idempotency_key,
                    model, schema_version, prompt_hash, confidence, reuse_status,
                    input_snapshot, llm_recommendation, final_system_decision, guardrail_result
                ) VALUES (
                    %(id)s, %(workflow_type)s, %(account_mode)s, %(symbol)s, %(strategy_type)s,
                    %(source_type)s, %(source_id)s, %(session_id)s, %(idempotency_key)s,
                    %(model)s, %(schema_version)s, %(prompt_hash)s, %(confidence)s, %(reuse_status)s,
                    %(input_snapshot)s, %(llm_recommendation)s, %(final_system_decision)s, %(guardrail_result)s
                )
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING *;
                """,
                params,
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    SELECT *
                    FROM ai_decision_events
                    WHERE idempotency_key = %(idempotency_key)s
                    """,
                    {"idempotency_key": idem},
                )
                row = cur.fetchone()
        conn.commit()
    return _row_to_dict(row)


def list_recent_ai_decision_events(
    *,
    workflow_type: str | None = None,
    symbol: str | None = None,
    account_mode: str | None = None,
    reuse_status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    ensure_ai_decision_events_table()
    filters: list[str] = []
    params: dict[str, Any] = {"limit": max(1, min(int(limit), 200))}
    if workflow_type:
        filters.append("workflow_type = %(workflow_type)s")
        params["workflow_type"] = str(workflow_type).strip().upper()
    if symbol:
        filters.append("symbol = %(symbol)s")
        params["symbol"] = str(symbol).strip().upper()
    if account_mode:
        filters.append("account_mode = %(account_mode)s")
        params["account_mode"] = str(account_mode).strip().upper()
    if reuse_status:
        filters.append("reuse_status = %(reuse_status)s")
        params["reuse_status"] = _normal_status(reuse_status)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id::text AS id, created_at, updated_at, workflow_type, account_mode,
                       symbol, strategy_type, source_type, source_id, session_id,
                       idempotency_key, model, schema_version, prompt_hash, confidence,
                       reuse_status, input_snapshot, llm_recommendation,
                       final_system_decision, guardrail_result
                FROM ai_decision_events
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %(limit)s
                """,
                params,
            )
            rows = cur.fetchall()
    return [_row_to_dict(row) for row in rows]


def get_symbol_ai_memory(
    *,
    symbol: str,
    strategy_type: str | None = None,
    limit: int = 5,
    min_recent_confidence: float = 75.0,
) -> list[dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return []
    ensure_ai_decision_events_table()
    params: dict[str, Any] = {
        "symbol": sym,
        "limit": max(1, min(int(limit), 20)),
        "min_recent_confidence": max(0.0, min(100.0, float(min_recent_confidence))),
    }
    strategy_clause = ""
    if strategy_type:
        strategy_clause = "AND (strategy_type = %(strategy_type)s OR strategy_type IS NULL)"
        params["strategy_type"] = str(strategy_type).strip().upper()
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id::text AS id, created_at, workflow_type, account_mode, symbol,
                       strategy_type, source_type, source_id, model, confidence,
                       reuse_status, llm_recommendation, final_system_decision, guardrail_result
                FROM ai_decision_events
                WHERE symbol = %(symbol)s
                  {strategy_clause}
                  AND (
                    reuse_status = 'APPROVED'
                    OR (
                        reuse_status = 'NEW'
                        AND confidence IS NOT NULL
                        AND confidence >= %(min_recent_confidence)s
                        AND created_at >= NOW() - INTERVAL '14 days'
                    )
                  )
                ORDER BY created_at DESC
                LIMIT %(limit)s
                """,
                params,
            )
            rows = cur.fetchall()
    return [_row_to_dict(row) for row in rows]


def get_global_ai_memory(
    *,
    workflow_type: str | None = "MACRO_STRATEGY_MEMORY",
    strategy_type: str | None = "LONG_TERM",
    limit: int = 5,
) -> list[dict[str, Any]]:
    ensure_ai_decision_events_table()
    filters = [
        "symbol IS NULL",
        "reuse_status = 'APPROVED'",
    ]
    params: dict[str, Any] = {"limit": max(1, min(int(limit), 20))}
    if workflow_type:
        filters.append("workflow_type = %(workflow_type)s")
        params["workflow_type"] = str(workflow_type).strip().upper()
    if strategy_type:
        filters.append("(strategy_type = %(strategy_type)s OR strategy_type IS NULL)")
        params["strategy_type"] = str(strategy_type).strip().upper()
    where_clause = " AND ".join(filters)
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id::text AS id, created_at, workflow_type, account_mode, symbol,
                       strategy_type, source_type, source_id, model, confidence,
                       reuse_status, llm_recommendation, final_system_decision, guardrail_result
                FROM ai_decision_events
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT %(limit)s
                """,
                params,
            )
            rows = cur.fetchall()
    return [_row_to_dict(row) for row in rows]


def summarize_reusable_ai_lessons(rows: list[dict[str, Any]], *, max_items: int = 5) -> list[dict[str, Any]]:
    lessons: list[dict[str, Any]] = []
    for row in rows[: max(1, max_items)]:
        recommendation = row.get("llm_recommendation") if isinstance(row.get("llm_recommendation"), dict) else {}
        final_decision = row.get("final_system_decision") if isinstance(row.get("final_system_decision"), dict) else {}
        guardrail = row.get("guardrail_result") if isinstance(row.get("guardrail_result"), dict) else {}
        lessons.append(
            {
                "workflow_type": row.get("workflow_type"),
                "symbol": row.get("symbol"),
                "strategy_type": row.get("strategy_type"),
                "confidence": row.get("confidence"),
                "reuse_status": row.get("reuse_status"),
                "recommendation": {
                    key: recommendation.get(key)
                    for key in ("decision", "score_commentary", "rationale", "root_cause", "risk_notes")
                    if key in recommendation
                },
                "final_decision": {
                    key: final_decision.get(key)
                    for key in ("action", "run_status", "applied_count", "skipped_count", "confidence_adjustment_applied")
                    if key in final_decision
                },
                "guardrail": {
                    key: guardrail.get(key)
                    for key in ("status", "reason", "applied_count", "skipped_count")
                    if key in guardrail
                },
            }
        )
    return lessons


def summarize_global_ai_memory(rows: list[dict[str, Any]], *, max_items: int = 3) -> list[dict[str, Any]]:
    memories: list[dict[str, Any]] = []
    for row in rows[: max(1, max_items)]:
        recommendation = row.get("llm_recommendation") if isinstance(row.get("llm_recommendation"), dict) else {}
        final_decision = row.get("final_system_decision") if isinstance(row.get("final_system_decision"), dict) else {}
        memories.append(
            {
                "workflow_type": row.get("workflow_type"),
                "strategy_type": row.get("strategy_type"),
                "source_type": row.get("source_type"),
                "source_id": row.get("source_id"),
                "confidence": row.get("confidence"),
                "reuse_status": row.get("reuse_status"),
                "memory": {
                    key: recommendation.get(key)
                    for key in (
                        "title",
                        "summary",
                        "allocation",
                        "rules",
                        "monitoring_metrics",
                        "invalidation_triggers",
                        "assumptions",
                    )
                    if key in recommendation
                },
                "scope": final_decision.get("scope"),
            }
        )
    return memories


def update_ai_decision_reuse_status(*, event_id: str | UUID, reuse_status: str) -> dict[str, Any] | None:
    ensure_ai_decision_events_table()
    status = _normal_status(reuse_status)
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ai_decision_events
                SET reuse_status = %(reuse_status)s,
                    updated_at = NOW()
                WHERE id = %(id)s
                RETURNING *;
                """,
                {"id": event_id, "reuse_status": status},
            )
            row = cur.fetchone()
        conn.commit()
    return _row_to_dict(row) if row else None
