from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.signal_engine_service import run_technical


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def ensure_technical_scan_runs_table() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS technical_scan_runs (
        id UUID PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        run_status VARCHAR(32) NOT NULL,
        scanned INTEGER NOT NULL DEFAULT 0 CHECK (scanned >= 0),
        written INTEGER NOT NULL DEFAULT 0 CHECK (written >= 0),
        errors INTEGER NOT NULL DEFAULT 0 CHECK (errors >= 0),
        detail JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_technical_scan_runs_started
    ON technical_scan_runs(started_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def _persist_run_row(
    *,
    run_id: UUID,
    started_at: datetime,
    finished_at: datetime,
    run_status: str,
    scanned: int,
    written: int,
    errors: int,
    detail: dict[str, Any],
) -> None:
    ensure_technical_scan_runs_table()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO technical_scan_runs (
                    id, started_at, finished_at, run_status, scanned, written, errors, detail
                ) VALUES (
                    %(id)s, %(started_at)s, %(finished_at)s, %(run_status)s, %(scanned)s, %(written)s, %(errors)s, %(detail)s
                )
                """,
                {
                    "id": run_id,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "run_status": run_status,
                    "scanned": scanned,
                    "written": written,
                    "errors": errors,
                    "detail": Json(detail),
                },
            )
        conn.commit()


def run_technical_scan_cycle(*, limit_symbols: int = 0, exchange_scope: str = "ALL") -> dict[str, Any]:
    started_at = _utc_now()
    run_id = uuid4()
    detail: dict[str, Any] = {
        "limit_symbols": int(limit_symbols),
        "exchange_scope": str(exchange_scope).strip().upper() or "ALL",
    }

    try:
        rows = run_technical(limit_symbols=int(limit_symbols), exchange_scope=exchange_scope)
        scanned = len(rows)
        written = len(rows)
        run_status = "COMPLETED"
        finished_at = _utc_now()
        detail["strategy_type"] = "TECHNICAL"
        _persist_run_row(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            run_status=run_status,
            scanned=scanned,
            written=written,
            errors=0,
            detail=detail,
        )
        return {
            "success": True,
            "run_id": str(run_id),
            "run_status": run_status,
            "scanned": scanned,
            "written": written,
            "errors": 0,
            "detail": detail,
        }
    except Exception as exc:
        finished_at = _utc_now()
        detail["error"] = str(exc)
        _persist_run_row(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            run_status="FAILED",
            scanned=0,
            written=0,
            errors=1,
            detail=detail,
        )
        return {
            "success": False,
            "run_id": str(run_id),
            "run_status": "FAILED",
            "scanned": 0,
            "written": 0,
            "errors": 1,
            "detail": detail,
        }


def get_last_technical_scan_run() -> dict[str, Any] | None:
    ensure_technical_scan_runs_table()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, started_at, finished_at, run_status, scanned, written, errors, detail
                FROM technical_scan_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
    return dict(row) if row else None


def list_recent_technical_scan_runs(*, limit: int = 20) -> list[dict[str, Any]]:
    ensure_technical_scan_runs_table()
    safe_limit = max(1, min(int(limit), 200))
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, started_at, finished_at, run_status, scanned, written, errors, detail
                FROM technical_scan_runs
                ORDER BY started_at DESC
                LIMIT %(limit)s
                """,
                {"limit": safe_limit},
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]
