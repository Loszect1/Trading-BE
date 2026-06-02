from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.demo_trading_service import (
    get_active_scheduler_demo_session_id_from_db,
    get_demo_session_overview,
    normalize_demo_session_id,
    upsert_demo_symbol_exit_levels,
)
from app.services.gpt_service import GptService
from app.services.price_unit_service import normalize_vn_price_to_vnd

logger = logging.getLogger(__name__)

_gpt_service = GptService()

_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "portfolio_view": {"type": "string"},
        "risk_notes": {"type": "array", "items": {"type": "string"}},
        "adjustments": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "symbol": {"type": "string"},
                    "decision": {"type": "string", "enum": ["KEEP", "ADJUST"]},
                    "take_profit_price": {"type": ["number", "null"]},
                    "stoploss_price": {"type": ["number", "null"]},
                    "reason": {"type": "string"},
                    "confidence": {"type": ["number", "null"]},
                },
                "required": [
                    "symbol",
                    "decision",
                    "take_profit_price",
                    "stoploss_price",
                    "reason",
                    "confidence",
                ],
            },
        },
    },
    "required": ["portfolio_view", "risk_notes", "adjustments"],
}


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _to_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def _compact_text(value: Any, max_chars: int = 1200) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)] + "..."


def ensure_demo_portfolio_review_runs_table() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS demo_portfolio_review_runs (
        id UUID PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        run_status VARCHAR(32) NOT NULL,
        trigger_source VARCHAR(32) NOT NULL,
        trigger_marker VARCHAR(64),
        session_id VARCHAR(128),
        holdings_count INTEGER NOT NULL DEFAULT 0 CHECK (holdings_count >= 0),
        applied_count INTEGER NOT NULL DEFAULT 0 CHECK (applied_count >= 0),
        skipped_count INTEGER NOT NULL DEFAULT 0 CHECK (skipped_count >= 0),
        error TEXT,
        detail JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_demo_portfolio_review_runs_started
        ON demo_portfolio_review_runs(started_at DESC);
    CREATE INDEX IF NOT EXISTS idx_demo_portfolio_review_runs_session_started
        ON demo_portfolio_review_runs(session_id, started_at DESC);
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
    trigger_source: str,
    trigger_marker: str | None,
    session_id: str | None,
    holdings_count: int,
    applied_count: int,
    skipped_count: int,
    error: str | None,
    detail: dict[str, Any],
) -> None:
    ensure_demo_portfolio_review_runs_table()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO demo_portfolio_review_runs (
                    id, started_at, finished_at, run_status, trigger_source,
                    trigger_marker, session_id, holdings_count, applied_count,
                    skipped_count, error, detail
                ) VALUES (
                    %(id)s, %(started_at)s, %(finished_at)s, %(run_status)s, %(trigger_source)s,
                    %(trigger_marker)s, %(session_id)s, %(holdings_count)s, %(applied_count)s,
                    %(skipped_count)s, %(error)s, %(detail)s
                )
                ON CONFLICT (id) DO UPDATE
                SET started_at = EXCLUDED.started_at,
                    finished_at = EXCLUDED.finished_at,
                    run_status = EXCLUDED.run_status,
                    trigger_source = EXCLUDED.trigger_source,
                    trigger_marker = EXCLUDED.trigger_marker,
                    session_id = EXCLUDED.session_id,
                    holdings_count = EXCLUDED.holdings_count,
                    applied_count = EXCLUDED.applied_count,
                    skipped_count = EXCLUDED.skipped_count,
                    error = EXCLUDED.error,
                    detail = EXCLUDED.detail
                """,
                {
                    "id": run_id,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "run_status": run_status,
                    "trigger_source": trigger_source,
                    "trigger_marker": trigger_marker,
                    "session_id": session_id,
                    "holdings_count": int(holdings_count),
                    "applied_count": int(applied_count),
                    "skipped_count": int(skipped_count),
                    "error": error,
                    "detail": Json(_json_safe(detail)),
                },
            )
        conn.commit()


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return dict(row)


def get_last_demo_portfolio_review_run() -> dict[str, Any] | None:
    ensure_demo_portfolio_review_runs_table()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id::text AS id, started_at, finished_at, run_status, trigger_source,
                       trigger_marker, session_id, holdings_count, applied_count,
                       skipped_count, error, detail
                FROM demo_portfolio_review_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
    return _row_to_dict(row) if row else None


def list_recent_demo_portfolio_review_runs(
    *,
    limit: int = 20,
    session_id: str | None = None,
) -> list[dict[str, Any]]:
    ensure_demo_portfolio_review_runs_table()
    safe_limit = max(1, min(int(limit), 200))
    sid = str(session_id or "").strip()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if sid:
                cur.execute(
                    """
                    SELECT id::text AS id, started_at, finished_at, run_status, trigger_source,
                           trigger_marker, session_id, holdings_count, applied_count,
                           skipped_count, error, detail
                    FROM demo_portfolio_review_runs
                    WHERE session_id = %(session_id)s
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"session_id": sid, "limit": safe_limit},
                )
            else:
                cur.execute(
                    """
                    SELECT id::text AS id, started_at, finished_at, run_status, trigger_source,
                           trigger_marker, session_id, holdings_count, applied_count,
                           skipped_count, error, detail
                    FROM demo_portfolio_review_runs
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": safe_limit},
                )
            rows = cur.fetchall()
    return [_row_to_dict(row) for row in rows]


def _resolve_session_id(demo_session_id: str | None) -> str | None:
    requested = str(demo_session_id or "").strip()
    if requested:
        return normalize_demo_session_id(requested)
    active = get_active_scheduler_demo_session_id_from_db()
    if not active:
        return None
    return normalize_demo_session_id(active)


def _holding_for_prompt(row: dict[str, Any]) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").strip().upper()
    qty = int(row.get("quantity") or 0)
    avg_price = normalize_vn_price_to_vnd(row.get("average_buy_price") or 0.0)
    position_value = float(row.get("position_value") or 0.0)
    latest_price = normalize_vn_price_to_vnd(position_value / qty) if qty > 0 and position_value > 0 else avg_price
    current_tp = row.get("take_profit_price")
    current_sl = row.get("stoploss_price")
    unrealized_pct = ((latest_price - avg_price) / avg_price * 100.0) if avg_price > 0 else None
    return {
        "symbol": symbol,
        "quantity": qty,
        "settled_quantity": int(row.get("settled_quantity") or 0),
        "pending_settlement_quantity": int(row.get("pending_settlement_quantity") or 0),
        "average_buy_price": round(avg_price, 2),
        "latest_price": round(latest_price, 2),
        "unrealized_pct": round(unrealized_pct, 4) if unrealized_pct is not None else None,
        "current_take_profit_price": normalize_vn_price_to_vnd(current_tp) if current_tp else None,
        "current_stoploss_price": normalize_vn_price_to_vnd(current_sl) if current_sl else None,
        "position_value": round(position_value, 2),
        "is_t2_sell_allowed": bool(row.get("is_t2_sell_allowed")),
        "opened_at": row.get("opened_at"),
        "strategy_code": str(row.get("strategy_code") or "SHORT_TERM").strip().upper(),
    }


def _portfolio_prompt_payload(overview: dict[str, Any]) -> dict[str, Any]:
    holdings = [_holding_for_prompt(row) for row in list(overview.get("holdings") or [])]
    return {
        "session_id": str(overview.get("session_id") or ""),
        "cash_balance": round(float(overview.get("cash_balance") or 0.0), 2),
        "stock_value": round(float(overview.get("stock_value") or 0.0), 2),
        "total_assets": round(float(overview.get("total_assets") or 0.0), 2),
        "realized_pnl": round(float(overview.get("realized_pnl") or 0.0), 2),
        "tp_slot_pct": float(overview.get("tp_slot_pct") or 0.3),
        "generated_at": _utc_now().isoformat(),
        "holdings": holdings,
    }


def _build_review_prompt(payload: dict[str, Any]) -> str:
    return (
        "Review the active DEMO Vietnam stock portfolio below. "
        "Return JSON only using the provided schema. You may recommend KEEP or ADJUST per symbol. "
        "Only recommend ADJUST when the new TP/SL improves risk control for the current holding. "
        "Use full VND price values, not thousand-VND shorthand.\n\n"
        "Rules:\n"
        "- DEMO only; do not recommend broker orders or REAL-account actions.\n"
        "- TP must be above latest_price and stoploss must be below latest_price.\n"
        "- Do not lower/loosen an existing stoploss; keep or raise it only.\n"
        "- Respect T+2: pending settlement affects sellability but not TP/SL storage.\n"
        "- Prefer CMT-style evidence: trend, support/resistance, momentum, volume/liquidity if visible.\n"
        "- If evidence is insufficient, return KEEP with a short reason.\n\n"
        f"Portfolio JSON:\n{json.dumps(_json_safe(payload), ensure_ascii=False, separators=(',', ':'))}"
    )


def _parse_codex_output(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty_codex_output")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("codex_output_not_object")
    return parsed


def _validate_adjustment(
    adjustment: dict[str, Any],
    holding_by_symbol: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    symbol = str(adjustment.get("symbol") or "").strip().upper()
    if not symbol or symbol not in holding_by_symbol:
        return None, {"symbol": symbol or None, "reason": "symbol_not_in_holdings", "raw": adjustment}

    holding = holding_by_symbol[symbol]
    decision = str(adjustment.get("decision") or "").strip().upper()
    if decision != "ADJUST":
        return None, {"symbol": symbol, "reason": "keep", "codex_reason": adjustment.get("reason")}

    latest_price = _to_float(holding.get("latest_price")) or 0.0
    current_tp = _to_float(holding.get("current_take_profit_price"))
    current_sl = _to_float(holding.get("current_stoploss_price"))
    next_tp_raw = _to_float(adjustment.get("take_profit_price"))
    next_sl_raw = _to_float(adjustment.get("stoploss_price"))
    next_tp = normalize_vn_price_to_vnd(next_tp_raw) if next_tp_raw is not None else 0.0
    next_sl = normalize_vn_price_to_vnd(next_sl_raw) if next_sl_raw is not None else 0.0

    if latest_price <= 0:
        return None, {"symbol": symbol, "reason": "missing_latest_price", "latest_price": latest_price}
    if next_tp <= 0 or next_sl <= 0:
        return None, {"symbol": symbol, "reason": "invalid_empty_levels", "take_profit": next_tp, "stoploss": next_sl}
    if not (next_sl < latest_price < next_tp):
        return None, {
            "symbol": symbol,
            "reason": "invalid_level_geometry",
            "latest_price": latest_price,
            "take_profit": next_tp,
            "stoploss": next_sl,
        }
    if current_sl is not None and current_sl > 0 and next_sl + 1e-9 < current_sl:
        return None, {
            "symbol": symbol,
            "reason": "stoploss_would_loosen",
            "current_stoploss": current_sl,
            "proposed_stoploss": next_sl,
        }
    if (
        current_tp is not None
        and current_sl is not None
        and abs(next_tp - current_tp) < 1.0
        and abs(next_sl - current_sl) < 1.0
    ):
        return None, {"symbol": symbol, "reason": "unchanged", "take_profit": next_tp, "stoploss": next_sl}

    return (
        {
            "symbol": symbol,
            "take_profit_price": float(next_tp),
            "stoploss_price": float(next_sl),
            "latest_price": float(latest_price),
            "previous_take_profit_price": current_tp,
            "previous_stoploss_price": current_sl,
            "reason": _compact_text(adjustment.get("reason"), 600),
            "confidence": _to_float(adjustment.get("confidence")),
        },
        {},
    )


def run_demo_portfolio_review_once(
    *,
    demo_session_id: str | None = None,
    trigger_source: str = "manual",
    trigger_marker: str | None = None,
) -> dict[str, Any]:
    run_id = uuid4()
    started_at = _utc_now()
    finished_at = started_at
    run_status = "FAILED"
    sid: str | None = None
    holdings_count = 0
    applied: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    error: str | None = None
    detail: dict[str, Any] = {}

    try:
        sid = _resolve_session_id(demo_session_id)
        if not sid:
            run_status = "SKIPPED"
            error = "missing_active_demo_session"
            detail = {"reason": error}
            return {
                "success": True,
                "run_id": str(run_id),
                "run_status": run_status,
                "session_id": sid,
                "holdings_count": 0,
                "applied_count": 0,
                "skipped_count": 0,
                "detail": detail,
                "error": error,
            }

        overview = get_demo_session_overview(sid)
        payload = _portfolio_prompt_payload(overview)
        holdings = list(payload.get("holdings") or [])
        holdings_count = len(holdings)
        if not holdings:
            run_status = "SKIPPED"
            error = "no_demo_holdings"
            detail = {"session_id": sid, "reason": error, "portfolio": payload}
            return {
                "success": True,
                "run_id": str(run_id),
                "run_status": run_status,
                "session_id": sid,
                "holdings_count": 0,
                "applied_count": 0,
                "skipped_count": 0,
                "detail": detail,
                "error": error,
            }

        raw = _gpt_service.generate_text(
            prompt=_build_review_prompt(payload),
            system_prompt=(
                "You are a Vietnam equities portfolio risk reviewer. "
                "Apply Vietnam market mechanics, risk management, and technical-analysis discipline. "
                "Return only valid JSON."
            ),
            model=settings.gpt_model,
            max_tokens=int(settings.demo_portfolio_review_max_tokens),
            temperature=0.1,
            output_schema=_OUTPUT_SCHEMA,
        )
        parsed = _parse_codex_output(raw)
        holding_by_symbol = {str(row.get("symbol") or "").upper(): row for row in holdings}
        for adjustment in list(parsed.get("adjustments") or []):
            if not isinstance(adjustment, dict):
                skipped.append({"reason": "adjustment_not_object", "raw": adjustment})
                continue
            valid, reject = _validate_adjustment(adjustment, holding_by_symbol)
            if not valid:
                skipped.append(reject)
                continue
            try:
                upsert_demo_symbol_exit_levels(
                    session_id=sid,
                    symbol=str(valid["symbol"]),
                    take_profit_price=float(valid["take_profit_price"]),
                    stoploss_price=float(valid["stoploss_price"]),
                )
                applied.append(valid)
            except Exception as exc:
                skipped.append(
                    {
                        "symbol": valid.get("symbol"),
                        "reason": "apply_failed",
                        "error": _compact_text(str(exc), 300),
                    }
                )

        run_status = "COMPLETED"
        detail = {
            "session_id": sid,
            "portfolio": payload,
            "portfolio_view": parsed.get("portfolio_view"),
            "risk_notes": parsed.get("risk_notes") if isinstance(parsed.get("risk_notes"), list) else [],
            "applied": applied,
            "skipped": skipped,
        }
        return {
            "success": True,
            "run_id": str(run_id),
            "run_status": run_status,
            "session_id": sid,
            "holdings_count": holdings_count,
            "applied_count": len(applied),
            "skipped_count": len(skipped),
            "detail": detail,
            "error": None,
        }
    except Exception as exc:
        error = _compact_text(str(exc), 1200)
        run_status = "FAILED"
        detail = {
            "session_id": sid,
            "applied": applied,
            "skipped": skipped,
            "error": error,
        }
        logger.exception("demo_portfolio_review_failed", extra={"session_id": sid, "trigger_source": trigger_source})
        return {
            "success": False,
            "run_id": str(run_id),
            "run_status": run_status,
            "session_id": sid,
            "holdings_count": holdings_count,
            "applied_count": len(applied),
            "skipped_count": len(skipped),
            "detail": detail,
            "error": error,
        }
    finally:
        finished_at = _utc_now()
        try:
            _persist_run_row(
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                run_status=run_status,
                trigger_source=str(trigger_source or "manual")[:32],
                trigger_marker=str(trigger_marker)[:64] if trigger_marker else None,
                session_id=sid,
                holdings_count=holdings_count,
                applied_count=len(applied),
                skipped_count=len(skipped),
                error=error,
                detail=detail,
            )
        except Exception as persist_exc:
            logger.warning(
                "demo_portfolio_review_persist_failed",
                extra={"run_id": str(run_id), "error": str(persist_exc)},
            )
