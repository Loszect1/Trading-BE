from __future__ import annotations

import logging
from typing import cast
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from psycopg import connect
from psycopg.rows import dict_row

from app.core.config import settings
from app.schemas.automation import (
    SchedulerDemoSessionRequest,
    SchedulerStateRow,
    SchedulerStatusResponse,
    SchedulerToggleRequest,
    ShortTermAutomationRunRow,
    ShortTermCycleRunRequest,
    ShortTermCycleRunResponse,
    TechnicalAutomationRunRow,
    TechnicalCycleRunRequest,
    TechnicalCycleRunResponse,
)
from app.services.automation_scheduler_service import (
    get_active_scheduler_demo_session_id,
    get_automation_scheduler_status,
    get_persisted_scheduler_states,
    set_automation_scheduler_demo_session_id,
    set_automation_scheduler_enabled,
)
from app.services.short_term_automation_service import (
    get_short_term_async_job,
    get_last_short_term_automation_run,
    launch_short_term_production_cycle_async,
    list_recent_short_term_automation_runs,
    run_short_term_production_cycle,
)
from app.services.technical_automation_service import (
    get_last_technical_scan_run,
    list_recent_technical_scan_runs,
    run_technical_scan_cycle,
)
from app.services.redis_cache import RedisCacheService
from app.services.mail_signal_scheduler_service import (
    get_latest_mail_signals,
    get_latest_mail_signal_entry_run,
    get_today_mail_signals,
)

logger = logging.getLogger(__name__)
_redis_cache = RedisCacheService()

router = APIRouter(prefix="/automation", tags=["automation"])


@router.post("/short-term/run-cycle", response_model=ShortTermCycleRunResponse)
def post_short_term_run_cycle(body: ShortTermCycleRunRequest = ShortTermCycleRunRequest()) -> ShortTermCycleRunResponse:
    """
    Run one full short-term cycle: scanner → BUY picks → risk sizing → execution (DEMO/REAL core).
    Manual calls default to `enforce_vn_scan_schedule=false` so operators can trigger off-grid.
    """
    payload = body
    try:
        normalized_scope = str(payload.exchange_scope).upper()
        should_queue_async = bool(payload.async_for_heavy) and normalized_scope == "ALL" and int(payload.limit_symbols) <= 0
        if should_queue_async:
            raw = launch_short_term_production_cycle_async(
                limit_symbols=payload.limit_symbols,
                exchange_scope=payload.exchange_scope,
                account_mode=payload.account_mode,
                nav=payload.nav,
                risk_per_trade=payload.risk_per_trade,
                max_daily_new_orders=payload.max_daily_new_orders,
                enforce_vn_scan_schedule=payload.enforce_vn_scan_schedule,
                demo_session_id=payload.demo_session_id,
            )
            return ShortTermCycleRunResponse.model_validate(raw)
        raw: dict[str, Any] = run_short_term_production_cycle(
            limit_symbols=payload.limit_symbols,
            exchange_scope=payload.exchange_scope,
            account_mode=payload.account_mode,
            nav=payload.nav,
            risk_per_trade=payload.risk_per_trade,
            max_daily_new_orders=payload.max_daily_new_orders,
            enforce_vn_scan_schedule=payload.enforce_vn_scan_schedule,
            demo_session_id=payload.demo_session_id,
        )
        return ShortTermCycleRunResponse.model_validate(raw)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("automation.short_term_run_cycle_failed")
        raise HTTPException(status_code=500, detail=f"Short-term automation cycle error: {exc}") from exc


@router.get("/short-term/async-job/{job_id}")
def get_short_term_async_job_status(job_id: str) -> dict[str, Any]:
    """Read in-memory status for a queued manual cycle."""
    try:
        row = get_short_term_async_job(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Async job not found: {job_id}")
        return {"success": True, "data": row}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("automation.short_term_async_job_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read async job: {exc}") from exc


@router.get("/short-term/last-run")
def get_short_term_last_run() -> dict[str, Any]:
    """Most recent persisted automation summary (any outcome, including skipped)."""
    try:
        row = get_last_short_term_automation_run()
        if row is None:
            return {"success": True, "data": None}
        data = ShortTermAutomationRunRow.model_validate(row).model_dump(mode="json")
        return {"success": True, "data": data}
    except Exception as exc:
        logger.exception("automation.short_term_last_run_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read last automation run: {exc}") from exc


@router.get("/short-term/runs")
def get_short_term_runs(
    limit: int = Query(default=20, ge=1, le=200),
    account_mode: Literal["REAL", "DEMO"] | None = Query(default=None),
) -> dict[str, Any]:
    """Recent persisted automation runs for FE/backend operations visibility."""
    try:
        rows = list_recent_short_term_automation_runs(limit=limit, account_mode=account_mode)
        data = [ShortTermAutomationRunRow.model_validate(row).model_dump(mode="json") for row in rows]
        return {"success": True, "data": data, "limit": limit, "account_mode": account_mode}
    except Exception as exc:
        logger.exception("automation.short_term_runs_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read automation runs: {exc}") from exc


@router.post("/technical/run-cycle", response_model=TechnicalCycleRunResponse)
def post_technical_run_cycle(body: TechnicalCycleRunRequest = TechnicalCycleRunRequest()) -> TechnicalCycleRunResponse:
    """
    Run one technical-analysis scan cycle and persist summary for operations tracking.
    """
    try:
        raw: dict[str, Any] = run_technical_scan_cycle(
            limit_symbols=body.limit_symbols,
            exchange_scope=body.exchange_scope,
        )
        return TechnicalCycleRunResponse.model_validate(raw)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("automation.technical_run_cycle_failed")
        raise HTTPException(status_code=500, detail=f"Technical automation cycle error: {exc}") from exc


@router.get("/technical/last-run")
def get_technical_last_run() -> dict[str, Any]:
    """Most recent persisted technical scan summary."""
    try:
        row = get_last_technical_scan_run()
        if row is None:
            return {"success": True, "data": None}
        data = TechnicalAutomationRunRow.model_validate(row).model_dump(mode="json")
        return {"success": True, "data": data}
    except Exception as exc:
        logger.exception("automation.technical_last_run_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read last technical run: {exc}") from exc


@router.get("/technical/runs")
def get_technical_runs(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
    """Recent persisted technical scan runs for operations visibility."""
    try:
        rows = list_recent_technical_scan_runs(limit=limit)
        data = [TechnicalAutomationRunRow.model_validate(row).model_dump(mode="json") for row in rows]
        return {"success": True, "data": data, "limit": limit}
    except Exception as exc:
        logger.exception("automation.technical_runs_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read technical runs: {exc}") from exc


@router.get("/short-term/cache-stats")
def get_short_term_cache_stats() -> dict[str, Any]:
    """
    Redis cache stats for short-term symbol liquidity gate keys.
    Key format: scan:liquidity:{exchange}:{symbol}
    """
    try:
        keys = _redis_cache.scan_keys("scan:liquidity:*", limit=500_000)
        per_exchange: dict[str, int] = {}
        for key in keys:
            parts = key.split(":")
            if len(parts) < 4:
                continue
            exchange = str(parts[2]).upper()
            per_exchange[exchange] = int(per_exchange.get(exchange, 0)) + 1
        return {
            "success": True,
            "data": {
                "total_keys": len(keys),
                "per_exchange": per_exchange,
                "key_pattern": "scan:liquidity:{exchange}:{symbol}",
            },
        }
    except Exception as exc:
        logger.exception("automation.short_term_cache_stats_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read short-term cache stats: {exc}") from exc


@router.get("/short-term/cache-eligible")
def get_short_term_cache_eligible(
    limit: int = Query(default=300, ge=1, le=5000),
    exchange_scope: Literal["ALL", "HOSE", "HNX", "UPCOM"] = Query(default="ALL"),
) -> dict[str, Any]:
    """
    Read Redis liquidity cache rows where both gate flags are true:
    - eligible_liquidity == true
    - eligible_spike == true
    """
    def _is_true_flag(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value == 1
        if isinstance(value, str):
            normalized = value.strip().lower()
            return normalized in {"1", "true", "yes", "y", "on"}
        return False

    try:
        keys = _redis_cache.scan_keys("scan:liquidity:*", limit=500_000)
        items: list[dict[str, Any]] = []
        for key in keys:
            parts = str(key).split(":", 3)
            if len(parts) != 4:
                continue
            _, domain, raw_exchange, raw_symbol = parts
            if domain != "liquidity":
                continue
            exchange = str(raw_exchange).strip().upper()
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            if exchange_scope != "ALL" and exchange != exchange_scope:
                continue
            payload = _redis_cache.get_json(str(key))
            if not isinstance(payload, dict):
                continue
            if not _is_true_flag(payload.get("eligible_liquidity", False)):
                continue
            if not _is_true_flag(payload.get("eligible_spike", False)):
                continue
            items.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "baseline_vol": payload.get("baseline_vol"),
                    "latest_vol": payload.get("latest_vol"),
                    "spike_ratio": payload.get("spike_ratio"),
                    "eligible_liquidity": True,
                    "eligible_spike": True,
                    "redis_key": str(key),
                }
            )

        items.sort(key=lambda row: (str(row.get("exchange", "")), str(row.get("symbol", ""))))
        total = len(items)
        limited = cast(list[dict[str, Any]], items[:limit])
        return {
            "success": True,
            "data": limited,
            "meta": {
                "exchange_scope": exchange_scope,
                "limit": limit,
                "total_matched": total,
                "returned": len(limited),
            },
        }
    except Exception as exc:
        logger.exception("automation.short_term_cache_eligible_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read short-term cache eligible rows: {exc}") from exc


@router.get("/scheduler/status", response_model=SchedulerStatusResponse)
def get_scheduler_status(account_mode: Literal["REAL", "DEMO"] = Query(default="DEMO")) -> SchedulerStatusResponse:
    try:
        return SchedulerStatusResponse.model_validate(get_automation_scheduler_status(account_mode))
    except Exception as exc:
        logger.exception("automation.scheduler_status_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read scheduler status: {exc}") from exc


@router.post("/scheduler/toggle", response_model=SchedulerStatusResponse)
async def post_scheduler_toggle(body: SchedulerToggleRequest) -> SchedulerStatusResponse:
    try:
        result = await set_automation_scheduler_enabled(body.account_mode, body.enabled)
        return SchedulerStatusResponse.model_validate(result)
    except Exception as exc:
        logger.exception("automation.scheduler_toggle_failed")
        raise HTTPException(status_code=500, detail=f"Failed to toggle scheduler: {exc}") from exc


@router.post("/scheduler/demo-session")
def post_scheduler_demo_session(body: SchedulerDemoSessionRequest) -> dict[str, Any]:
    """Set active DEMO session id used by scheduler-triggered DEMO cycles."""
    try:
        active = set_automation_scheduler_demo_session_id(body.demo_session_id)
        return {"success": True, "data": {"active_demo_session_id": active}}
    except Exception as exc:
        logger.exception("automation.scheduler_demo_session_set_failed")
        raise HTTPException(status_code=500, detail=f"Failed to set scheduler demo session: {exc}") from exc


@router.get("/scheduler/demo-session")
def get_scheduler_demo_session() -> dict[str, Any]:
    """Read active DEMO session id used by scheduler-triggered DEMO cycles."""
    try:
        return {"success": True, "data": {"active_demo_session_id": get_active_scheduler_demo_session_id()}}
    except Exception as exc:
        logger.exception("automation.scheduler_demo_session_read_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read scheduler demo session: {exc}") from exc


@router.get("/scheduler/state")
def get_scheduler_state_rows() -> dict[str, Any]:
    """Read persisted scheduler enabled state directly from DB for REAL/DEMO."""
    try:
        rows = get_persisted_scheduler_states()
        data = [SchedulerStateRow.model_validate(row).model_dump(mode="json") for row in rows]
        return {"success": True, "data": data}
    except Exception as exc:
        logger.exception("automation.scheduler_state_rows_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read scheduler state rows: {exc}") from exc


@router.get("/mail-signals/today")
def get_mail_signals_today() -> dict[str, Any]:
    """
    Read today's mail->Claude analyzed picks from Redis key signals:mail:daily:YYYY-MM-DD.
    """
    try:
        row = get_today_mail_signals()
        return {"success": True, "data": row}
    except Exception as exc:
        logger.exception("automation.mail_signals_today_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read today mail signals: {exc}") from exc


@router.get("/mail-signals/latest")
def get_mail_signals_latest() -> dict[str, Any]:
    """
    Read latest mail->Claude analyzed picks from Redis key signals:mail:daily:YYYY-MM-DD.
    """
    try:
        row = get_latest_mail_signals()
        return {"success": True, "data": row}
    except Exception as exc:
        logger.exception("automation.mail_signals_latest_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read latest mail signals: {exc}") from exc


@router.get("/mail-signals/entry-run/latest")
def get_mail_signals_entry_run_latest() -> dict[str, Any]:
    """
    Read latest entry scheduler run log from Redis (includes executed orders list).
    """
    try:
        row = get_latest_mail_signal_entry_run()
        return {"success": True, "data": row}
    except Exception as exc:
        logger.exception("automation.mail_signals_entry_run_latest_failed")
        raise HTTPException(status_code=500, detail=f"Failed to read latest mail entry run: {exc}") from exc
