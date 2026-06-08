from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.ai_decision_event_service import ensure_ai_decision_events_table, _json_safe, _normal_status, _row_to_dict

MEMORY_WORKFLOW_TYPES = {
    "MACRO_STRATEGY_MEMORY",
    "RISK_MANAGEMENT_MEMORY",
    "TECHNICAL_PATTERN_MEMORY",
    "MARKET_STRUCTURE_MEMORY",
    "FUNDAMENTAL_THESIS_MEMORY",
    "OTHER_STRATEGY_CONTEXT",
}

DEFAULT_MEMORY_SCOPE = ["macro_gpt_analysis", "long_term_research"]
MAX_MEMORY_FILE_BYTES = 8 * 1024 * 1024
MAX_MEMORY_TOTAL_BYTES = 20 * 1024 * 1024
MAX_EXTRACTED_TEXT_CHARS = 20_000
MAX_EXCERPT_CHARS = 4_000
_CONNECT_TIMEOUT_SECONDS = 2

_TEXT_EXTENSIONS = (".txt", ".md", ".csv", ".tsv", ".json", ".log")
_IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tiff", ".tif")
_EVIDENCE_TABLE_READY = False
_EVIDENCE_TABLE_LOCK = threading.Lock()
_EVIDENCE_TABLE_ADVISORY_LOCK_ID = 915_202_606_071


def normalize_memory_workflow_type(value: str | None) -> str:
    workflow_type = str(value or "").strip().upper()
    return workflow_type if workflow_type in MEMORY_WORKFLOW_TYPES else "OTHER_STRATEGY_CONTEXT"


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _compact_text(value: Any, max_chars: int = MAX_EXCERPT_CHARS) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)] + "..."


def _file_hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def ensure_ai_memory_evidence_sources_table() -> None:
    global _EVIDENCE_TABLE_READY
    if _EVIDENCE_TABLE_READY:
        return
    with _EVIDENCE_TABLE_LOCK:
        if _EVIDENCE_TABLE_READY:
            return
        ensure_ai_decision_events_table()
        ddl = """
        CREATE TABLE IF NOT EXISTS ai_memory_evidence_sources (
            id UUID PRIMARY KEY,
            event_id UUID NOT NULL REFERENCES ai_decision_events(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            filename TEXT NOT NULL,
            content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
            file_sha256 TEXT NOT NULL,
            file_size_bytes INTEGER NOT NULL CHECK (file_size_bytes >= 0),
            extraction_method TEXT NOT NULL,
            extracted_text TEXT NOT NULL DEFAULT '',
            excerpt TEXT NOT NULL DEFAULT '',
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
            UNIQUE(event_id, file_sha256, filename)
        );

        CREATE INDEX IF NOT EXISTS idx_ai_memory_evidence_event_created
            ON ai_memory_evidence_sources(event_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ai_memory_evidence_hash
            ON ai_memory_evidence_sources(file_sha256);
        """
        with connect(settings.database_url, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (_EVIDENCE_TABLE_ADVISORY_LOCK_ID,))
                try:
                    cur.execute(ddl)
                finally:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (_EVIDENCE_TABLE_ADVISORY_LOCK_ID,))
            conn.commit()
        _EVIDENCE_TABLE_READY = True


def extract_memory_evidence_source(*, filename: str, content_type: str, raw: bytes) -> dict[str, Any]:
    safe_name = (filename or "unnamed").strip() or "unnamed"
    ctype = (content_type or "application/octet-stream").lower()
    lower_name = safe_name.lower()
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    method = "metadata_only"
    extracted_text = ""

    if len(raw) > MAX_MEMORY_FILE_BYTES:
        raise ValueError(f"{safe_name} exceeds max file size of {MAX_MEMORY_FILE_BYTES} bytes")

    if ctype.startswith("text/") or lower_name.endswith(_TEXT_EXTENSIONS):
        method = "text_decode"
        extracted_text = raw.decode("utf-8", errors="ignore")
        metadata["encoding"] = "utf-8"
    elif ctype == "application/pdf" or lower_name.endswith(".pdf"):
        method = "pdf_text"
        try:
            from pypdf import PdfReader

            reader = PdfReader(BytesIO(raw))
            metadata["page_count"] = len(reader.pages)
            page_text: list[str] = []
            for idx, page in enumerate(reader.pages[:20], start=1):
                text = page.extract_text() or ""
                if text.strip():
                    page_text.append(f"[page {idx}]\n{text}")
            extracted_text = "\n\n".join(page_text)
            if len(reader.pages) > 20:
                warnings.append("pdf_truncated_to_first_20_pages")
        except ImportError:
            warnings.append("pypdf_not_installed")
        except Exception as exc:
            warnings.append(f"pdf_extract_failed:{type(exc).__name__}")
    elif ctype.startswith("image/") or lower_name.endswith(_IMAGE_EXTENSIONS):
        method = "image_ocr"
        try:
            from PIL import Image
            import pytesseract

            with Image.open(BytesIO(raw)) as image:
                metadata["width"] = image.width
                metadata["height"] = image.height
                metadata["mode"] = image.mode
                extracted_text = pytesseract.image_to_string(image, lang="vie+eng")
                if not extracted_text.strip():
                    warnings.append("ocr_no_text_detected")
        except ImportError as exc:
            warnings.append(f"ocr_dependency_missing:{exc.name}")
        except Exception as exc:
            warnings.append(f"ocr_extract_failed:{type(exc).__name__}")
    else:
        warnings.append("unsupported_file_type_metadata_only")

    extracted_text = str(extracted_text or "").strip()
    if len(extracted_text) > MAX_EXTRACTED_TEXT_CHARS:
        extracted_text = extracted_text[:MAX_EXTRACTED_TEXT_CHARS]
        warnings.append("extracted_text_truncated")

    if not extracted_text:
        extracted_text = f"[{method}: no extracted text for {safe_name}]"

    return {
        "filename": safe_name,
        "kind": content_type or "application/octet-stream",
        "content_type": content_type or "application/octet-stream",
        "file_sha256": _file_hash(raw),
        "file_size_bytes": len(raw),
        "extraction_method": method,
        "extracted_text": extracted_text,
        "excerpt": _compact_text(extracted_text, MAX_EXCERPT_CHARS),
        "metadata": metadata,
        "warnings": warnings,
    }


def validate_memory_upload_budget(sources: list[dict[str, Any]]) -> None:
    total = sum(int(item.get("file_size_bytes") or 0) for item in sources)
    if total > MAX_MEMORY_TOTAL_BYTES:
        raise ValueError(f"memory uploads exceed max total size of {MAX_MEMORY_TOTAL_BYTES} bytes")


def persist_ai_memory_evidence_sources(event_id: str | UUID, sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not sources:
        return []
    ensure_ai_memory_evidence_sources_table()
    persisted: list[dict[str, Any]] = []
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            for source in sources:
                params = {
                    "id": uuid4(),
                    "event_id": event_id,
                    "filename": str(source.get("filename") or "unnamed"),
                    "content_type": str(source.get("content_type") or source.get("kind") or "application/octet-stream"),
                    "file_sha256": str(source.get("file_sha256") or ""),
                    "file_size_bytes": int(source.get("file_size_bytes") or 0),
                    "extraction_method": str(source.get("extraction_method") or "metadata_only"),
                    "extracted_text": str(source.get("extracted_text") or ""),
                    "excerpt": str(source.get("excerpt") or ""),
                    "metadata": Json(_json_safe(source.get("metadata") or {})),
                    "warnings": Json(_json_safe(source.get("warnings") or [])),
                }
                cur.execute(
                    """
                    INSERT INTO ai_memory_evidence_sources (
                        id, event_id, filename, content_type, file_sha256, file_size_bytes,
                        extraction_method, extracted_text, excerpt, metadata, warnings
                    ) VALUES (
                        %(id)s, %(event_id)s, %(filename)s, %(content_type)s, %(file_sha256)s,
                        %(file_size_bytes)s, %(extraction_method)s, %(extracted_text)s,
                        %(excerpt)s, %(metadata)s, %(warnings)s
                    )
                    ON CONFLICT (event_id, file_sha256, filename) DO UPDATE
                    SET content_type = EXCLUDED.content_type,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        extraction_method = EXCLUDED.extraction_method,
                        extracted_text = EXCLUDED.extracted_text,
                        excerpt = EXCLUDED.excerpt,
                        metadata = EXCLUDED.metadata,
                        warnings = EXCLUDED.warnings
                    RETURNING id::text AS id, event_id::text AS event_id, created_at, filename,
                              content_type, file_sha256, file_size_bytes, extraction_method,
                              extracted_text, excerpt, metadata, warnings
                    """,
                    params,
                )
                row = cur.fetchone()
                if row:
                    persisted.append(_row_to_dict(row))
        conn.commit()
    return persisted


def list_ai_memory_evidence_sources(event_id: str | UUID) -> list[dict[str, Any]]:
    ensure_ai_memory_evidence_sources_table()
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id::text AS id, event_id::text AS event_id, created_at, filename,
                       content_type, file_sha256, file_size_bytes, extraction_method,
                       extracted_text, excerpt, metadata, warnings
                FROM ai_memory_evidence_sources
                WHERE event_id = %(event_id)s
                ORDER BY created_at ASC, filename ASC
                """,
                {"event_id": event_id},
            )
            rows = cur.fetchall()
    return [_row_to_dict(row) for row in rows]


def list_ai_memory_events(
    *,
    reuse_status: str | None = None,
    workflow_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    ensure_ai_memory_evidence_sources_table()
    filters = [
        "e.symbol IS NULL",
        "(e.strategy_type = 'LONG_TERM' OR e.strategy_type IS NULL)",
        "e.workflow_type = ANY(%(memory_workflow_types)s)",
    ]
    params: dict[str, Any] = {
        "memory_workflow_types": sorted(MEMORY_WORKFLOW_TYPES),
        "limit": max(1, min(int(limit), 200)),
        "offset": max(0, int(offset)),
    }
    if reuse_status:
        filters.append("e.reuse_status = %(reuse_status)s")
        params["reuse_status"] = _normal_status(reuse_status)
    if workflow_type:
        filters.append("e.workflow_type = %(workflow_type)s")
        params["workflow_type"] = normalize_memory_workflow_type(workflow_type)
    where_clause = " AND ".join(filters)
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT e.id::text AS id, e.created_at, e.updated_at, e.workflow_type, e.account_mode,
                       e.symbol, e.strategy_type, e.source_type, e.source_id, e.session_id,
                       e.idempotency_key, e.model, e.schema_version, e.prompt_hash, e.confidence,
                       e.reuse_status, e.input_snapshot, e.llm_recommendation,
                       e.final_system_decision, e.guardrail_result,
                       COALESCE(COUNT(s.id), 0)::int AS evidence_count
                FROM ai_decision_events e
                LEFT JOIN ai_memory_evidence_sources s ON s.event_id = e.id
                WHERE {where_clause}
                GROUP BY e.id
                ORDER BY e.created_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                params,
            )
            rows = cur.fetchall()
    return [_attach_review_notes(_row_to_dict(row)) for row in rows]


def get_ai_memory_event(event_id: str | UUID) -> dict[str, Any] | None:
    ensure_ai_memory_evidence_sources_table()
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT e.id::text AS id, e.created_at, e.updated_at, e.workflow_type, e.account_mode,
                       e.symbol, e.strategy_type, e.source_type, e.source_id, e.session_id,
                       e.idempotency_key, e.model, e.schema_version, e.prompt_hash, e.confidence,
                       e.reuse_status, e.input_snapshot, e.llm_recommendation,
                       e.final_system_decision, e.guardrail_result,
                       COALESCE(COUNT(s.id), 0)::int AS evidence_count
                FROM ai_decision_events e
                LEFT JOIN ai_memory_evidence_sources s ON s.event_id = e.id
                WHERE e.id = %(event_id)s
                  AND e.symbol IS NULL
                  AND (e.strategy_type = 'LONG_TERM' OR e.strategy_type IS NULL)
                GROUP BY e.id
                """,
                {"event_id": event_id},
            )
            row = cur.fetchone()
    if not row:
        return None
    data = _attach_review_notes(_row_to_dict(row))
    data["evidence_sources"] = list_ai_memory_evidence_sources(event_id)
    return data


def review_ai_memory_event(
    *,
    event_id: str | UUID,
    reuse_status: str,
    workflow_type: str,
    llm_recommendation: dict[str, Any],
    final_system_decision: dict[str, Any] | None,
    guardrail_result: dict[str, Any] | None,
    review_notes: str | None = None,
) -> dict[str, Any] | None:
    ensure_ai_memory_evidence_sources_table()
    normalized_workflow = normalize_memory_workflow_type(workflow_type)
    normalized_status = _normal_status(reuse_status)
    recommendation = llm_recommendation if isinstance(llm_recommendation, dict) else {}
    final_decision = final_system_decision if isinstance(final_system_decision, dict) else {}
    guardrail = guardrail_result if isinstance(guardrail_result, dict) else {}
    final_decision.setdefault("scope", DEFAULT_MEMORY_SCOPE)
    final_decision["automatic_execution"] = False
    final_decision["deterministic_scoring_change"] = False
    guardrail.setdefault("status", "APPROVED_FOR_CONTEXT_ONLY")
    guardrail.setdefault("reason", "Approved memory is context only, not an execution signal.")
    review_patch = {
        "review_notes": _compact_text(review_notes or "", 2000),
        "reviewed_at": _utc_now_iso(),
        "reviewed_memory_workflow_type": normalized_workflow,
    }
    with connect(settings.database_url, row_factory=dict_row, connect_timeout=_CONNECT_TIMEOUT_SECONDS) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                UPDATE ai_decision_events
                SET workflow_type = %(workflow_type)s,
                    reuse_status = %(reuse_status)s,
                    llm_recommendation = %(llm_recommendation)s,
                    final_system_decision = %(final_system_decision)s,
                    guardrail_result = %(guardrail_result)s,
                    input_snapshot = COALESCE(input_snapshot, '{}'::jsonb) || %(review_patch)s::jsonb,
                    updated_at = NOW()
                WHERE id = %(event_id)s
                  AND symbol IS NULL
                  AND (strategy_type = 'LONG_TERM' OR strategy_type IS NULL)
                RETURNING id::text AS id, created_at, updated_at, workflow_type, account_mode,
                          symbol, strategy_type, source_type, source_id, session_id,
                          idempotency_key, model, schema_version, prompt_hash, confidence,
                          reuse_status, input_snapshot, llm_recommendation,
                          final_system_decision, guardrail_result
                """,
                {
                    "event_id": event_id,
                    "workflow_type": normalized_workflow,
                    "reuse_status": normalized_status,
                    "llm_recommendation": Json(_json_safe(recommendation)),
                    "final_system_decision": Json(_json_safe(final_decision)),
                    "guardrail_result": Json(_json_safe(guardrail)),
                    "review_patch": Json(_json_safe(review_patch)),
                },
            )
            row = cur.fetchone()
        conn.commit()
    if not row:
        return None
    data = _attach_review_notes(_row_to_dict(row))
    data["evidence_count"] = len(list_ai_memory_evidence_sources(event_id))
    return data


def get_approved_global_long_term_memory(limit: int = 20) -> list[dict[str, Any]]:
    return list_ai_memory_events(reuse_status="APPROVED", limit=limit, offset=0)


def summarize_grouped_global_ai_memory(rows: list[dict[str, Any]], *, max_items_per_category: int = 5) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {key: [] for key in sorted(MEMORY_WORKFLOW_TYPES)}
    for row in rows:
        category = normalize_memory_workflow_type(row.get("workflow_type"))
        bucket = grouped.setdefault(category, [])
        if len(bucket) >= max(1, int(max_items_per_category)):
            continue
        recommendation = row.get("llm_recommendation") if isinstance(row.get("llm_recommendation"), dict) else {}
        final_decision = row.get("final_system_decision") if isinstance(row.get("final_system_decision"), dict) else {}
        guardrail = row.get("guardrail_result") if isinstance(row.get("guardrail_result"), dict) else {}
        bucket.append(
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
                "guardrail": {
                    key: guardrail.get(key)
                    for key in ("status", "reason")
                    if key in guardrail
                },
            }
        )
    return {key: value for key, value in grouped.items() if value}


def build_approved_global_memory_version(limit: int = 200) -> str:
    rows = get_approved_global_long_term_memory(limit=limit)
    payload = [
        {
            "id": row.get("id"),
            "workflow_type": row.get("workflow_type"),
            "reuse_status": row.get("reuse_status"),
            "updated_at": str(row.get("updated_at") or row.get("created_at") or ""),
        }
        for row in rows
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _attach_review_notes(row: dict[str, Any]) -> dict[str, Any]:
    snapshot = row.get("input_snapshot") if isinstance(row.get("input_snapshot"), dict) else {}
    row["review_notes"] = snapshot.get("review_notes") if isinstance(snapshot.get("review_notes"), str) else None
    return row
