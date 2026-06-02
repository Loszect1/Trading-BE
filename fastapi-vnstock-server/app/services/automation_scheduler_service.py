from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import Any, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg import connect
from psycopg.rows import dict_row

from app.core.config import get_vn_market_holiday_dates, settings
from app.services.alert_dispatcher_service import dispatch_alert_to_channels
from app.services.demo_trading_service import (
    get_demo_session_cash_balance,
    run_demo_auto_exit_cycle,
)
from app.services.short_term_automation_service import run_short_term_production_cycle
from app.services.short_term_automation_service import mark_stale_short_term_running_runs
from app.services.short_term_scan_schedule import (
    is_instant_on_short_term_scan_grid,
    next_run_datetimes,
)
from app.services.redis_cache import RedisCacheService
from app.services.signal_engine_service import (
    refresh_short_term_liquidity_cache_from_db,
    scan_and_persist_symbol_news_from_liquidity_cache,
    warm_daily_volume_for_saved_symbols,
    warm_short_term_liquidity_cache,
)
from app.services.real_recommendation_scan_service import run_real_recommendations_scan
from app.services.demo_portfolio_review_service import run_demo_portfolio_review_once

logger = logging.getLogger(__name__)

AccountMode = Literal["REAL", "DEMO"]
_ACCOUNT_MODES: tuple[AccountMode, ...] = ("REAL", "DEMO")
# Backend internal short-term scheduler must support REAL as well so
# that toggling "auto schedule" for REAL actually triggers the same grid loop as DEMO.
_BACKEND_SCHEDULER_SUPPORTED_MODES: tuple[AccountMode, ...] = ("REAL", "DEMO")
# Internal scheduler: one grid tick runs three persisted cycles in strict board order.
_SHORT_TERM_SCHEDULER_EXCHANGE_SCOPES: tuple[str, ...] = ("HOSE", "HNX", "UPCOM")
_loop_tasks: dict[AccountMode, asyncio.Task | None] = {mode: None for mode in _ACCOUNT_MODES}
_scheduler_last_slot_markers: dict[AccountMode, str | None] = {mode: None for mode in _ACCOUNT_MODES}
_cache_warm_task: asyncio.Task | None = None
_last_cache_warm_local_date: date | None = None
_post_close_refresh_run_markers: set[str] = set()
_POST_CLOSE_REFRESH_LOCK_KEY = "lock:short-term:post-close-refresh"
_POST_CLOSE_REFRESH_LOCK_TTL_SECONDS = 1800

# REAL scan-only scheduler persistence + execution.
_real_scan_only_loop_task: asyncio.Task | None = None
_real_scan_only_last_slot_marker: str | None = None
_runtime_real_scan_only_enabled: bool = False
_demo_auto_exit_loop_task: asyncio.Task | None = None
_demo_auto_exit_last_slot_marker: str | None = None
_demo_portfolio_review_loop_task: asyncio.Task | None = None
_demo_portfolio_review_run_markers: set[str] = set()
_redis_cache = RedisCacheService()
_runtime_enabled: dict[AccountMode, bool] = {
    mode: bool(settings.automation_short_term_scheduler_enabled) for mode in _ACCOUNT_MODES
}
_active_demo_session_id: str | None = None


def _log_task_state(task_name: str, status: str, **detail: Any) -> None:
    logger.warning("scheduler_task_state | task=%s | status=%s | detail=%s", task_name, status, detail or {})


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _real_scan_only_slot_marker(now_local: datetime) -> str:
    # Unique per VN slot grid boundary.
    return f"{now_local.date().isoformat()}-{now_local.hour:02d}:{now_local.minute:02d}"


def _parse_demo_portfolio_review_schedule_times(raw: str | None = None) -> tuple[tuple[int, int], ...]:
    source = str(raw if raw is not None else settings.demo_portfolio_review_schedule_times_csv).strip()
    if not source:
        return ((12, 0), (17, 0))
    parsed: list[tuple[int, int]] = []
    for token in source.split(","):
        value = token.strip()
        if not value:
            continue
        parts = value.split(":", 1)
        try:
            hour = int(parts[0].strip())
            minute = int(parts[1].strip()) if len(parts) == 2 else 0
        except ValueError:
            logger.warning(
                "demo_portfolio_review_schedule_invalid_token",
                extra={"token": value, "raw": source},
            )
            continue
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            parsed.append((hour, minute))
        else:
            logger.warning(
                "demo_portfolio_review_schedule_out_of_range",
                extra={"token": value, "raw": source},
            )
    if not parsed:
        return ((12, 0), (17, 0))
    return tuple(sorted(set(parsed)))


def _is_vn_market_workday(day: date) -> bool:
    return day.weekday() < 5 and day not in get_vn_market_holiday_dates()


def _demo_portfolio_review_slot_marker(now_local: datetime) -> str:
    return f"{now_local.date().isoformat()}-{now_local.hour:02d}:{now_local.minute:02d}"


def _demo_portfolio_review_scheduler_snapshot(now_local: datetime | None = None) -> dict[str, Any]:
    tz = ZoneInfo(settings.demo_portfolio_review_timezone)
    local_now = now_local.astimezone(tz) if now_local is not None else datetime.now(tz=tz)
    schedule_times = _parse_demo_portfolio_review_schedule_times()
    marker = _demo_portfolio_review_slot_marker(local_now)
    is_workday = _is_vn_market_workday(local_now.date())
    due = (
        bool(settings.demo_portfolio_review_scheduler_enabled)
        and is_workday
        and (local_now.hour, local_now.minute) in schedule_times
        and marker not in _demo_portfolio_review_run_markers
    )
    return {
        "enabled": bool(settings.demo_portfolio_review_scheduler_enabled),
        "timezone": settings.demo_portfolio_review_timezone,
        "schedule_times": [f"{hour:02d}:{minute:02d}" for hour, minute in schedule_times],
        "now_local": local_now,
        "is_market_workday": is_workday,
        "due": due,
        "marker": marker,
    }


def _is_vn_market_open(now_local: datetime) -> bool:
    if now_local.weekday() >= 5:
        return False
    minute_of_day = now_local.hour * 60 + now_local.minute
    in_morning = (9 * 60) <= minute_of_day <= (11 * 60 + 30)
    in_afternoon = (13 * 60) <= minute_of_day <= (14 * 60 + 45)
    return in_morning or in_afternoon


async def _demo_auto_exit_scheduler_loop() -> None:
    global _demo_auto_exit_last_slot_marker
    tz = ZoneInfo(settings.short_term_scan_timezone)
    logger.info("demo_auto_exit_scheduler_started", extra={"timezone": settings.short_term_scan_timezone})
    while True:
        try:
            if not bool(_runtime_enabled.get("DEMO")):
                logger.info("demo_auto_exit_scheduler_disabled_by_state")
                return
            session_id = (_active_demo_session_id or "").strip()
            if not session_id:
                await asyncio.sleep(10)
                continue
            now_local = datetime.now(tz=tz)
            if not _is_vn_market_open(now_local):
                await asyncio.sleep(10)
                continue
            marker = _real_scan_only_slot_marker(now_local)
            if marker == _demo_auto_exit_last_slot_marker:
                await asyncio.sleep(10)
                continue
            result = await asyncio.to_thread(run_demo_auto_exit_cycle, session_id)
            _demo_auto_exit_last_slot_marker = marker
            _log_task_state(
                "demo_auto_exit_scheduler",
                "FINISHED",
                marker=marker,
                session_id=session_id,
                scanned=int(result.get("scanned") or 0),
                executed=int(result.get("executed") or 0),
            )
            logger.warning(
                "demo_auto_exit_cycle_finished",
                extra={
                    "session_id": session_id,
                    "marker": marker,
                    "scanned": int(result.get("scanned") or 0),
                    "executed": int(result.get("executed") or 0),
                },
            )
        except Exception as exc:
            _log_task_state("demo_auto_exit_scheduler", "FAILED", error=str(exc))
            logger.warning("demo_auto_exit_scheduler_tick_failed", extra={"error": str(exc)})
        await asyncio.sleep(10)


async def _start_demo_auto_exit_scheduler() -> None:
    global _demo_auto_exit_loop_task
    if _demo_auto_exit_loop_task and not _demo_auto_exit_loop_task.done():
        return
    _demo_auto_exit_loop_task = asyncio.create_task(
        _demo_auto_exit_scheduler_loop(),
        name="demo-auto-exit-scheduler",
    )
    _log_task_state("demo_auto_exit_scheduler", "STARTED")
    logger.warning("demo_auto_exit_scheduler_task_started")


async def _stop_demo_auto_exit_scheduler() -> None:
    global _demo_auto_exit_loop_task, _demo_auto_exit_last_slot_marker
    if _demo_auto_exit_loop_task and not _demo_auto_exit_loop_task.done():
        _demo_auto_exit_loop_task.cancel()
        try:
            await _demo_auto_exit_loop_task
        except asyncio.CancelledError:
            pass
    _demo_auto_exit_loop_task = None
    _demo_auto_exit_last_slot_marker = None
    _log_task_state("demo_auto_exit_scheduler", "STOPPED")
    logger.warning("demo_auto_exit_scheduler_task_stopped")


async def _demo_portfolio_review_scheduler_loop() -> None:
    poll_seconds = 30
    logger.info(
        "demo_portfolio_review_scheduler_started",
        extra={
            "timezone": settings.demo_portfolio_review_timezone,
            "schedule_times": list(_demo_portfolio_review_scheduler_snapshot()["schedule_times"]),
            "poll_seconds": poll_seconds,
        },
    )
    while True:
        try:
            snapshot = _demo_portfolio_review_scheduler_snapshot()
            if bool(snapshot["due"]):
                marker = str(snapshot["marker"])
                result = await asyncio.to_thread(
                    run_demo_portfolio_review_once,
                    demo_session_id=_active_demo_session_id,
                    trigger_source="scheduler",
                    trigger_marker=marker,
                )
                _demo_portfolio_review_run_markers.add(marker)
                _log_task_state(
                    "demo_portfolio_review_scheduler",
                    "FINISHED",
                    marker=marker,
                    session_id=result.get("session_id"),
                    run_status=result.get("run_status"),
                    applied_count=int(result.get("applied_count") or 0),
                    skipped_count=int(result.get("skipped_count") or 0),
                )
                logger.warning(
                    "demo_portfolio_review_scheduler_completed",
                    extra={
                        "marker": marker,
                        "session_id": result.get("session_id"),
                        "run_status": result.get("run_status"),
                        "applied_count": int(result.get("applied_count") or 0),
                        "skipped_count": int(result.get("skipped_count") or 0),
                    },
                )
            if len(_demo_portfolio_review_run_markers) > 20:
                today_prefix = datetime.now(tz=ZoneInfo(settings.demo_portfolio_review_timezone)).date().isoformat()
                _demo_portfolio_review_run_markers.intersection_update(
                    {m for m in _demo_portfolio_review_run_markers if str(m).startswith(today_prefix)}
                )
        except Exception as exc:
            _log_task_state("demo_portfolio_review_scheduler", "FAILED", error=str(exc))
            logger.warning("demo_portfolio_review_scheduler_failed", extra={"error": str(exc)})
        await asyncio.sleep(poll_seconds)


async def _start_demo_portfolio_review_scheduler() -> None:
    global _demo_portfolio_review_loop_task
    if not bool(settings.demo_portfolio_review_scheduler_enabled):
        logger.info("demo_portfolio_review_scheduler_disabled")
        return
    if _demo_portfolio_review_loop_task and not _demo_portfolio_review_loop_task.done():
        return
    _demo_portfolio_review_loop_task = asyncio.create_task(
        _demo_portfolio_review_scheduler_loop(),
        name="demo-portfolio-review-scheduler",
    )
    _log_task_state("demo_portfolio_review_scheduler", "STARTED")
    logger.warning("demo_portfolio_review_scheduler_task_started")


async def _stop_demo_portfolio_review_scheduler() -> None:
    global _demo_portfolio_review_loop_task
    if _demo_portfolio_review_loop_task and not _demo_portfolio_review_loop_task.done():
        _demo_portfolio_review_loop_task.cancel()
        try:
            await _demo_portfolio_review_loop_task
        except asyncio.CancelledError:
            pass
    _demo_portfolio_review_loop_task = None
    _log_task_state("demo_portfolio_review_scheduler", "STOPPED")
    logger.warning("demo_portfolio_review_scheduler_task_stopped")


def get_demo_portfolio_review_scheduler_status() -> dict[str, Any]:
    snapshot = _demo_portfolio_review_scheduler_snapshot()
    running = _demo_portfolio_review_loop_task is not None and not _demo_portfolio_review_loop_task.done()
    return {
        "enabled": bool(snapshot["enabled"]),
        "running": running,
        "poll_seconds": 30,
        "timezone": snapshot["timezone"],
        "schedule_times": snapshot["schedule_times"],
        "now_local": snapshot["now_local"],
        "is_market_workday": bool(snapshot["is_market_workday"]),
        "due": bool(snapshot["due"]),
        "active_demo_session_id": _active_demo_session_id,
    }


def _run_real_scan_only_and_persist(slot_marker: str | None = None) -> dict[str, Any]:
    payload = run_real_recommendations_scan(
        exchange_scope="ALL",
        limit_symbols=0,
        slot_marker=slot_marker,
        source="scheduler",
    )
    return {
        "generated_at": payload.get("generated_at"),
        "scanned": payload.get("scanned"),
        "short_term_count": payload.get("short_term_count"),
        "rejected_count": payload.get("rejected_count"),
        "watch_count": payload.get("watch_count"),
        "mail_signal_count": payload.get("mail_signal_count"),
    }


def _is_backend_scheduler_supported(account_mode: AccountMode) -> bool:
    return account_mode in _BACKEND_SCHEDULER_SUPPORTED_MODES


def _short_term_allocated_nav() -> float:
    total = float(settings.strategy_total_cash_vnd)
    pct = float(settings.strategy_alloc_short_term_pct)
    nav = total * pct
    return max(1_000_000.0, nav)


def _short_term_allocated_nav_for_mode(account_mode: AccountMode, demo_session_id: str | None) -> float:
    if str(account_mode).upper() == "DEMO":
        cash = get_demo_session_cash_balance(demo_session_id)
        if cash is not None and cash > 0:
            return max(1_000_000.0, cash)
    return _short_term_allocated_nav()


def _format_scheduler_batch_telegram_message(
    *,
    account_mode: AccountMode,
    trigger_batch_id: str,
    runs: list[dict[str, Any]],
    demo_session_id: str | None = None,
) -> str:
    lines: list[str] = [
        "Auto Trading Scheduler Batch",
        f"account_mode={account_mode}",
        f"trigger_batch_id={trigger_batch_id}",
    ]
    if account_mode == "DEMO":
        lines.append(f"demo_session_id={demo_session_id or '-'}")
    for row in runs:
        scope = str(row.get("exchange_scope") or "-")
        run_status = str(row.get("run_status") or "-")
        scanned = int(row.get("scanned") or 0)
        buy_candidates = int(row.get("buy_candidates") or 0)
        executed = int(row.get("executed") or 0)
        errors = int(row.get("errors") or 0)
        lines.append(
            f"{scope}: status={run_status}, scanned={scanned}, buy={buy_candidates}, executed={executed}, errors={errors}"
        )
    return "\n".join(lines)


def _dispatch_scheduler_batch_notification(
    *,
    account_mode: AccountMode,
    trigger_batch_id: str,
    runs: list[dict[str, Any]],
    demo_session_id: str | None = None,
) -> None:
    message = _format_scheduler_batch_telegram_message(
        account_mode=account_mode,
        trigger_batch_id=trigger_batch_id,
        runs=runs,
        demo_session_id=demo_session_id,
    )
    report = dispatch_alert_to_channels(message)
    logger.warning(
        "short_term_scheduler_batch_notification_dispatched",
        extra={
            "account_mode": account_mode,
            "scheduler_trigger_batch_id": trigger_batch_id,
            "delivery": report,
        },
    )


def _scheduler_grid_snapshot() -> dict[str, Any]:
    tz = ZoneInfo(settings.short_term_scan_timezone)
    now_local = datetime.now(tz=tz)
    holiday_dates = get_vn_market_holiday_dates()
    on_grid = is_instant_on_short_term_scan_grid(
        now_local,
        interval_minutes=settings.short_term_scan_interval_minutes,
        holiday_dates=holiday_dates,
    )
    next_grid_run_at: datetime | None = None
    if not on_grid:
        upcoming = next_run_datetimes(
            after=now_local,
            count=1,
            interval_minutes=settings.short_term_scan_interval_minutes,
            timezone_name=settings.short_term_scan_timezone,
            holiday_dates=holiday_dates,
        )
        if upcoming:
            next_grid_run_at = upcoming[0]
    return {
        "on_grid": on_grid,
        "now_local": now_local,
        "next_grid_run_at": next_grid_run_at,
    }


def _resolve_post_close_refresh_hours() -> tuple[int, ...]:
    raw = str(settings.automation_short_term_post_close_refresh_hours_csv or "").strip()
    if raw == "":
        return (int(settings.automation_short_term_post_close_refresh_hour),)

    parsed_hours: list[int] = []
    for token in raw.split(","):
        value = token.strip()
        if value == "":
            continue
        try:
            hour = int(value)
        except ValueError:
            logger.warning(
                "short_term_post_close_refresh_hours_csv_invalid_token",
                extra={"token": value, "raw": raw},
            )
            continue
        if 0 <= hour <= 23:
            parsed_hours.append(hour)
        else:
            logger.warning(
                "short_term_post_close_refresh_hours_csv_out_of_range",
                extra={"token": value, "raw": raw},
            )

    if not parsed_hours:
        fallback_hour = int(settings.automation_short_term_post_close_refresh_hour)
        logger.warning(
            "short_term_post_close_refresh_hours_csv_empty_after_parse_use_fallback",
            extra={"fallback_hour": fallback_hour, "raw": raw},
        )
        return (fallback_hour,)

    # Deduplicate and keep deterministic execution order.
    return tuple(sorted(set(parsed_hours)))


def run_short_term_post_close_refresh_once() -> dict[str, Any]:
    """
    Run one post-close refresh cycle on demand.
    Shared by scheduler slot (e.g. 16:00) and manual trigger endpoint.
    """
    token = str(uuid4())
    if not _redis_cache.acquire_lock(
        _POST_CLOSE_REFRESH_LOCK_KEY,
        token,
        ttl_seconds=_POST_CLOSE_REFRESH_LOCK_TTL_SECONDS,
    ):
        return {
            "skipped": True,
            "reason": "post_close_refresh_already_running",
            "lock_key": _POST_CLOSE_REFRESH_LOCK_KEY,
        }
    try:
        volume_stats = warm_daily_volume_for_saved_symbols(70)
        redis_stats = refresh_short_term_liquidity_cache_from_db(30, "ALL")
        news_stats = scan_and_persist_symbol_news_from_liquidity_cache(
            exchange_scope="ALL",
            max_symbols=0,
            per_symbol_news_limit=20,
        )
        return {
            "skipped": False,
            "volume_stats": volume_stats,
            "redis_stats": redis_stats,
            "news_stats": news_stats,
        }
    finally:
        _redis_cache.release_lock(_POST_CLOSE_REFRESH_LOCK_KEY, token)


def _ensure_automation_scheduler_state_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS automation_scheduler_state (
        account_mode VARCHAR(10) PRIMARY KEY CHECK (account_mode IN ('REAL', 'DEMO')),
        enabled BOOLEAN NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def _ensure_automation_scheduler_demo_context_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS automation_scheduler_demo_context (
        id SMALLINT PRIMARY KEY CHECK (id = 1),
        demo_session_id VARCHAR(128) NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def _ensure_real_scan_only_state_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS automation_real_scan_only_state (
        id SMALLINT PRIMARY KEY CHECK (id = 1),
        enabled BOOLEAN NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def _load_runtime_real_scan_only_enabled_from_db() -> None:
    global _runtime_real_scan_only_enabled
    _ensure_real_scan_only_state_table()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO automation_real_scan_only_state (id, enabled)
                VALUES (1, FALSE)
                ON CONFLICT (id) DO NOTHING
                """
            )
            cur.execute(
                """
                SELECT enabled
                FROM automation_real_scan_only_state
                WHERE id = 1
                """
            )
            row = cur.fetchone() or {}
        conn.commit()

    _runtime_real_scan_only_enabled = bool(row.get("enabled"))


def _persist_runtime_real_scan_only_enabled(enabled: bool) -> None:
    _ensure_real_scan_only_state_table()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO automation_real_scan_only_state (id, enabled)
                VALUES (1, %(enabled)s)
                ON CONFLICT (id)
                DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = NOW()
                """,
                {"enabled": bool(enabled)},
            )
        conn.commit()


def _load_active_demo_session_from_db() -> None:
    global _active_demo_session_id
    _ensure_automation_scheduler_demo_context_table()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO automation_scheduler_demo_context (id, demo_session_id, updated_at)
                VALUES (1, NULL, NOW())
                ON CONFLICT (id) DO NOTHING
                """
            )
            cur.execute(
                """
                SELECT demo_session_id
                FROM automation_scheduler_demo_context
                WHERE id = 1
                """
            )
            row = cur.fetchone() or {}
        conn.commit()
    raw = row.get("demo_session_id")
    sid = str(raw).strip() if raw else ""
    _active_demo_session_id = sid or None


def _persist_active_demo_session_id(demo_session_id: str | None) -> None:
    _ensure_automation_scheduler_demo_context_table()
    normalized = str(demo_session_id).strip() if demo_session_id else None
    if normalized == "":
        normalized = None
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO automation_scheduler_demo_context (id, demo_session_id, updated_at)
                VALUES (1, %(demo_session_id)s, NOW())
                ON CONFLICT (id)
                DO UPDATE SET demo_session_id = EXCLUDED.demo_session_id, updated_at = NOW()
                """,
                {"demo_session_id": normalized},
            )
        conn.commit()


def get_active_scheduler_demo_session_id() -> str | None:
    return _active_demo_session_id


def _load_runtime_enabled_from_db() -> None:
    _ensure_automation_scheduler_state_table()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            for mode in _ACCOUNT_MODES:
                cur.execute(
                    """
                    INSERT INTO automation_scheduler_state (account_mode, enabled, updated_at)
                    VALUES (%(account_mode)s, %(enabled)s, NOW())
                    ON CONFLICT (account_mode) DO NOTHING
                    """,
                    {
                        "account_mode": mode,
                        "enabled": bool(settings.automation_short_term_scheduler_enabled),
                    },
                )
            cur.execute(
                """
                SELECT account_mode, enabled
                FROM automation_scheduler_state
                WHERE account_mode IN ('REAL', 'DEMO')
                """
            )
            rows = cur.fetchall()
        conn.commit()

    for row in rows:
        account_mode = str(row.get("account_mode", "")).upper()
        if account_mode in _runtime_enabled:
            enabled = bool(row.get("enabled"))
            if not _is_backend_scheduler_supported(account_mode):  # type: ignore[arg-type]
                enabled = False
            _runtime_enabled[account_mode] = enabled


def _persist_runtime_enabled(account_mode: AccountMode, enabled: bool) -> None:
    _ensure_automation_scheduler_state_table()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO automation_scheduler_state (account_mode, enabled, updated_at)
                VALUES (%(account_mode)s, %(enabled)s, NOW())
                ON CONFLICT (account_mode)
                DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = NOW()
                """,
                {"account_mode": account_mode, "enabled": bool(enabled)},
            )
        conn.commit()


def get_persisted_scheduler_states() -> list[dict]:
    _load_runtime_enabled_from_db()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT account_mode, enabled, updated_at
                FROM automation_scheduler_state
                WHERE account_mode IN ('REAL', 'DEMO')
                ORDER BY account_mode
                """
            )
            rows = cur.fetchall()
        conn.commit()
    return [dict(row) for row in rows]


def _run_short_term_scheduler_on_grid_batch(
    account_mode: AccountMode,
    demo_session_id: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """
    Synchronous worker for asyncio.to_thread: HOSE → HNX → UPCOM production cycles.
    Each scope acquires/releases the advisory lock separately (no overlap, strict order).
    """
    trigger_batch_id = str(uuid4())
    total = len(_SHORT_TERM_SCHEDULER_EXCHANGE_SCOPES)
    results: list[dict[str, Any]] = []
    scheduler_limit_symbols = int(settings.automation_short_term_scheduler_limit_symbols)
    for idx, scope in enumerate(_SHORT_TERM_SCHEDULER_EXCHANGE_SCOPES, start=1):
        try:
            raw = run_short_term_production_cycle(
                limit_symbols=scheduler_limit_symbols,
                exchange_scope=scope,
                account_mode=account_mode,
                nav=_short_term_allocated_nav_for_mode(account_mode, demo_session_id),
                risk_per_trade=0.01,
                max_daily_new_orders=10,
                enforce_vn_scan_schedule=True,
                demo_session_id=demo_session_id if account_mode == "DEMO" else None,
                scheduler_trigger_batch_id=trigger_batch_id,
                scheduler_sequence_index=idx,
                scheduler_sequence_total=total,
                # Keep timeouts ON in scheduler so one stuck upstream call
                # cannot starve the advisory lock for a whole grid.
                scheduler_disable_timeouts=False,
            )
            results.append(raw)
        except Exception as exc:
            logger.warning(
                "short_term_scheduler_exchange_cycle_failed",
                extra={
                    "account_mode": account_mode,
                    "exchange_scope": scope,
                    "scheduler_trigger_batch_id": trigger_batch_id,
                    "scheduler_sequence_index": idx,
                    "error": str(exc),
                },
            )
            results.append(
                {
                    "success": False,
                    "run_id": None,
                    "run_status": "FAILED",
                    "scanned": 0,
                    "buy_candidates": 0,
                    "risk_rejected": 0,
                    "executed": 0,
                    "execution_rejected": 0,
                    "errors": 1,
                    "detail": {
                        "account_mode": account_mode,
                        "exchange_scope": scope,
                        "scheduler_trigger_batch_id": trigger_batch_id,
                        "scheduler_sequence_index": idx,
                        "scheduler_sequence_total": total,
                        "error": str(exc),
                    },
                }
            )
    return trigger_batch_id, results


async def _scheduler_loop(account_mode: AccountMode) -> None:
    poll_seconds = settings.automation_short_term_scheduler_poll_seconds
    logger.info(
        "short_term_scheduler_started",
        extra={
            "account_mode": account_mode,
            "poll_seconds": poll_seconds,
            "timezone": settings.short_term_scan_timezone,
        },
    )
    while True:
        try:
            grid = _scheduler_grid_snapshot()
            on_grid = bool(grid["on_grid"])
            logger.warning(
                "short_term_scheduler_tick",
                extra={
                    "account_mode": account_mode,
                    "on_grid": on_grid,
                    "next_grid_run_at": (
                        grid["next_grid_run_at"].isoformat()
                        if isinstance(grid["next_grid_run_at"], datetime)
                        else None
                    ),
                },
            )
            if on_grid:
                now_local = grid["now_local"]
                marker = _real_scan_only_slot_marker(now_local) if isinstance(now_local, datetime) else None
                if marker and marker == _scheduler_last_slot_markers.get(account_mode):
                    logger.warning(
                        "short_term_scheduler_tick_skipped_duplicate_slot",
                        extra={"account_mode": account_mode, "marker": marker},
                    )
                    await asyncio.sleep(poll_seconds)
                    continue
                if marker:
                    _scheduler_last_slot_markers[account_mode] = marker

                demo_session_id = _active_demo_session_id if account_mode == "DEMO" else None
                trigger_batch_id, batch_results = await asyncio.to_thread(
                    _run_short_term_scheduler_on_grid_batch,
                    account_mode,
                    demo_session_id,
                )
                run_summaries = [
                    {
                        "exchange_scope": (r.get("detail") or {}).get("exchange_scope"),
                        "run_id": r.get("run_id"),
                        "run_status": r.get("run_status"),
                        "scanned": r.get("scanned"),
                        "buy_candidates": r.get("buy_candidates"),
                        "executed": r.get("executed"),
                        "errors": r.get("errors"),
                    }
                    for r in batch_results
                ]
                logger.warning(
                    "short_term_scheduler_batch_result",
                    extra={
                        "account_mode": account_mode,
                        "scheduler_trigger_batch_id": trigger_batch_id,
                        "demo_session_id": demo_session_id,
                        "runs": run_summaries,
                    },
                )
                _log_task_state(
                    f"short_term_scheduler_{account_mode.lower()}",
                    "FINISHED",
                    scheduler_trigger_batch_id=trigger_batch_id,
                    demo_session_id=demo_session_id,
                )
                try:
                    _dispatch_scheduler_batch_notification(
                        account_mode=account_mode,
                        trigger_batch_id=trigger_batch_id,
                        runs=run_summaries,
                        demo_session_id=demo_session_id,
                    )
                except Exception as notify_exc:
                    logger.warning(
                        "short_term_scheduler_batch_notification_failed",
                        extra={
                            "account_mode": account_mode,
                            "scheduler_trigger_batch_id": trigger_batch_id,
                            "error": str(notify_exc),
                        },
                    )
            else:
                logger.warning(
                    "short_term_scheduler_tick_skipped_off_grid",
                    extra={
                        "account_mode": account_mode,
                        "now_local": grid["now_local"].isoformat(),
                        "next_grid_run_at": (
                            grid["next_grid_run_at"].isoformat()
                            if isinstance(grid["next_grid_run_at"], datetime)
                            else None
                        ),
                    },
                )
        except Exception as exc:
            _log_task_state(f"short_term_scheduler_{account_mode.lower()}", "FAILED", error=str(exc))
            logger.warning(
                "short_term_scheduler_tick_failed",
                extra={"account_mode": account_mode, "error": str(exc)},
            )
        await asyncio.sleep(poll_seconds)


async def _cache_warm_loop() -> None:
    global _last_cache_warm_local_date
    poll_seconds = 30
    tz = ZoneInfo(settings.short_term_scan_timezone)
    warm_hour = int(settings.automation_short_term_cache_warm_hour)
    warm_minute = int(settings.automation_short_term_cache_warm_minute)
    post_close_hours = _resolve_post_close_refresh_hours()
    post_close_minute = int(settings.automation_short_term_post_close_refresh_minute)
    logger.info(
        "short_term_cache_warm_scheduler_started",
        extra={
            "timezone": settings.short_term_scan_timezone,
            "warm_hour": warm_hour,
            "warm_minute": warm_minute,
            "post_close_hours": list(post_close_hours),
            "post_close_minute": post_close_minute,
            "poll_seconds": poll_seconds,
        },
    )
    while True:
        try:
            now_local = datetime.now(tz=tz)
            today = now_local.date()
            is_weekday = today.weekday() < 5
            is_holiday = today in get_vn_market_holiday_dates()
            should_run = (
                bool(settings.automation_short_term_cache_warm_enabled)
                and is_weekday
                and not is_holiday
                and now_local.hour == warm_hour
                and now_local.minute == warm_minute
                and _last_cache_warm_local_date != today
            )
            if should_run:
                stats = await asyncio.to_thread(
                    warm_short_term_liquidity_cache,
                    0,
                    "ALL",
                )
                _last_cache_warm_local_date = today
                logger.warning(
                    "short_term_cache_warm_completed",
                    extra={"stats": stats},
                )
                _log_task_state("short_term_cache_warm_scheduler", "FINISHED", stage="warm")

            # Keep only today's markers to avoid in-memory growth over long uptime.
            today_prefix = f"{today.isoformat()}-"
            stale_markers = [marker for marker in _post_close_refresh_run_markers if not marker.startswith(today_prefix)]
            for marker in stale_markers:
                _post_close_refresh_run_markers.discard(marker)

            current_slot_marker = f"{today.isoformat()}-{now_local.hour:02d}:{now_local.minute:02d}"
            should_refresh_post_close = (
                bool(settings.automation_short_term_post_close_refresh_enabled)
                and is_weekday
                and not is_holiday
                and now_local.hour in post_close_hours
                and now_local.minute == post_close_minute
                and current_slot_marker not in _post_close_refresh_run_markers
            )
            if should_refresh_post_close:
                refresh_result = await asyncio.to_thread(run_short_term_post_close_refresh_once)
                _post_close_refresh_run_markers.add(current_slot_marker)
                logger.warning(
                    "short_term_post_close_refresh_completed",
                    extra={
                        "slot_marker": current_slot_marker,
                        "volume_stats": refresh_result.get("volume_stats"),
                        "redis_stats": refresh_result.get("redis_stats"),
                        "news_stats": refresh_result.get("news_stats"),
                    },
                )
                _log_task_state(
                    "short_term_cache_warm_scheduler",
                    "FINISHED",
                    stage="post_close_refresh",
                    marker=current_slot_marker,
                )
        except Exception as exc:
            _log_task_state("short_term_cache_warm_scheduler", "FAILED", error=str(exc))
            logger.warning(
                "short_term_cache_warm_failed",
                extra={"error": str(exc)},
            )
        await asyncio.sleep(poll_seconds)


async def _real_scan_only_scheduler_loop() -> None:
    global _real_scan_only_last_slot_marker

    poll_seconds = settings.automation_short_term_scheduler_poll_seconds
    logger.info(
        "real_scan_only_scheduler_started",
        extra={"poll_seconds": poll_seconds, "timezone": settings.short_term_scan_timezone},
    )
    while True:
        try:
            if not bool(_runtime_real_scan_only_enabled):
                logger.info("real_scan_only_scheduler_disabled_by_state")
                return

            grid = _scheduler_grid_snapshot()
            if bool(grid["on_grid"]):
                now_local = grid["now_local"]
                if isinstance(now_local, datetime):
                    marker = _real_scan_only_slot_marker(now_local)
                    if marker != _real_scan_only_last_slot_marker:
                        _log_task_state("real_scan_only_scheduler", "RUNNING", marker=marker)
                        summary = await asyncio.to_thread(_run_real_scan_only_and_persist, marker)
                        _real_scan_only_last_slot_marker = marker
                        _log_task_state("real_scan_only_scheduler", "FINISHED", marker=marker, **summary)
        except Exception as exc:
            _log_task_state("real_scan_only_scheduler", "FAILED", error=str(exc))
            logger.warning(
                "real_scan_only_scheduler_tick_failed",
                extra={"error": str(exc)},
            )
        await asyncio.sleep(poll_seconds)


async def _start_real_scan_only_scheduler() -> None:
    global _real_scan_only_loop_task
    if not bool(_runtime_real_scan_only_enabled):
        return
    if _real_scan_only_loop_task and not _real_scan_only_loop_task.done():
        return
    _real_scan_only_loop_task = asyncio.create_task(
        _real_scan_only_scheduler_loop(),
        name="real-scan-only-scheduler",
    )
    _log_task_state("real_scan_only_scheduler", "STARTED")
    logger.warning("real_scan_only_scheduler_task_started")


async def _stop_real_scan_only_scheduler() -> None:
    global _real_scan_only_last_slot_marker, _real_scan_only_loop_task

    if _real_scan_only_loop_task and not _real_scan_only_loop_task.done():
        _real_scan_only_loop_task.cancel()
        try:
            await _real_scan_only_loop_task
        except asyncio.CancelledError:
            pass
    _real_scan_only_loop_task = None
    _real_scan_only_last_slot_marker = None
    _log_task_state("real_scan_only_scheduler", "STOPPED")
    logger.warning("real_scan_only_scheduler_task_stopped")


def get_real_scan_only_scheduler_status() -> dict:
    running = _real_scan_only_loop_task is not None and not _real_scan_only_loop_task.done()
    grid = _scheduler_grid_snapshot()
    return {
        "account_mode": "REAL",
        "mode": "SCAN_ONLY",
        "enabled": bool(_runtime_real_scan_only_enabled),
        "running": running,
        "poll_seconds": settings.automation_short_term_scheduler_poll_seconds,
        "interval_minutes": settings.short_term_scan_interval_minutes,
        "timezone": settings.short_term_scan_timezone,
        "on_grid": bool(grid["on_grid"]),
        "now_local": grid["now_local"],
        "next_grid_run_at": grid["next_grid_run_at"],
        "active_demo_session_id": None,
    }


async def set_real_scan_only_scheduler_enabled(enabled: bool) -> dict:
    global _runtime_real_scan_only_enabled
    enabled = bool(enabled)

    # Mutual exclusivity: scan-only scheduler vs production scheduler for REAL.
    if enabled:
        _runtime_real_scan_only_enabled = True
        _persist_runtime_real_scan_only_enabled(True)
        if _runtime_enabled.get("REAL"):
            _runtime_enabled["REAL"] = False
            _persist_runtime_enabled("REAL", False)
            await _stop_mode_scheduler("REAL")
    else:
        _runtime_real_scan_only_enabled = False
        _persist_runtime_real_scan_only_enabled(False)

    if enabled:
        await _start_real_scan_only_scheduler()
    else:
        await _stop_real_scan_only_scheduler()

    return get_real_scan_only_scheduler_status()


async def start_automation_scheduler() -> None:
    try:
        # This deployment runs a single backend scheduler instance. On startup,
        # any RUNNING row older than the immediate boot window is orphaned by a
        # previous process and should not stay visible as an active Automation Log.
        marked_stale = await asyncio.to_thread(mark_stale_short_term_running_runs, older_than_minutes=1)
        if marked_stale:
            _log_task_state("short_term_scheduler_stale_recovery", "FINISHED", updated=marked_stale)
    except Exception as exc:
        logger.warning("short_term_scheduler_stale_recovery_failed", extra={"error": str(exc)})

    try:
        _load_runtime_enabled_from_db()
    except Exception as exc:
        logger.warning("short_term_scheduler_state_load_failed", extra={"error": str(exc)})
    try:
        _load_active_demo_session_from_db()
    except Exception as exc:
        logger.warning("short_term_scheduler_demo_context_load_failed", extra={"error": str(exc)})

    try:
        _load_runtime_real_scan_only_enabled_from_db()
    except Exception as exc:
        logger.warning("real_scan_only_scheduler_state_load_failed", extra={"error": str(exc)})

    # Scan-only mode takes precedence over production mode for REAL.
    if bool(_runtime_real_scan_only_enabled):
        if _runtime_enabled.get("REAL"):
            _runtime_enabled["REAL"] = False
            _persist_runtime_enabled("REAL", False)
        await _stop_mode_scheduler("REAL")

    for mode in _ACCOUNT_MODES:
        if not _is_backend_scheduler_supported(mode):
            _runtime_enabled[mode] = False
            _persist_runtime_enabled(mode, False)
            logger.info("short_term_scheduler_backend_disabled_for_mode", extra={"account_mode": mode})
            await _stop_mode_scheduler(mode)
            continue
        if _runtime_enabled[mode]:
            await _start_mode_scheduler(mode)
        else:
            logger.info("short_term_scheduler_disabled", extra={"account_mode": mode})

    if bool(_runtime_real_scan_only_enabled):
        await _start_real_scan_only_scheduler()

    await _start_cache_warm_scheduler()
    await _start_demo_portfolio_review_scheduler()


async def stop_automation_scheduler() -> None:
    for mode in _ACCOUNT_MODES:
        await _stop_mode_scheduler(mode)
    await _stop_demo_auto_exit_scheduler()
    await _stop_real_scan_only_scheduler()
    await _stop_cache_warm_scheduler()
    await _stop_demo_portfolio_review_scheduler()


async def _start_mode_scheduler(account_mode: AccountMode) -> None:
    if not _is_backend_scheduler_supported(account_mode):
        _runtime_enabled[account_mode] = False
        _persist_runtime_enabled(account_mode, False)
        logger.info("short_term_scheduler_backend_disabled_for_mode", extra={"account_mode": account_mode})
        return
    task = _loop_tasks[account_mode]
    if task and not task.done():
        return
    _loop_tasks[account_mode] = asyncio.create_task(
        _scheduler_loop(account_mode),
        name=f"short-term-automation-scheduler-{account_mode.lower()}",
    )
    if account_mode == "DEMO":
        await _start_demo_auto_exit_scheduler()
    _log_task_state(f"short_term_scheduler_{account_mode.lower()}", "STARTED")
    logger.warning("short_term_scheduler_task_started", extra={"account_mode": account_mode})


async def _stop_mode_scheduler(account_mode: AccountMode) -> None:
    global _scheduler_last_slot_markers

    task = _loop_tasks[account_mode]
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _loop_tasks[account_mode] = None
    _scheduler_last_slot_markers[account_mode] = None
    if account_mode == "DEMO":
        await _stop_demo_auto_exit_scheduler()
    _log_task_state(f"short_term_scheduler_{account_mode.lower()}", "STOPPED")
    logger.warning("short_term_scheduler_task_stopped", extra={"account_mode": account_mode})


async def _start_cache_warm_scheduler() -> None:
    global _cache_warm_task
    if not (
        bool(settings.automation_short_term_cache_warm_enabled)
        or bool(settings.automation_short_term_post_close_refresh_enabled)
    ):
        logger.info("short_term_cache_warm_scheduler_disabled_all_features")
        return
    if _cache_warm_task and not _cache_warm_task.done():
        return
    _cache_warm_task = asyncio.create_task(
        _cache_warm_loop(),
        name="short-term-cache-warm-scheduler",
    )
    _log_task_state("short_term_cache_warm_scheduler", "STARTED")
    logger.warning("short_term_cache_warm_scheduler_task_started")


async def _stop_cache_warm_scheduler() -> None:
    global _cache_warm_task
    if _cache_warm_task and not _cache_warm_task.done():
        _cache_warm_task.cancel()
        try:
            await _cache_warm_task
        except asyncio.CancelledError:
            pass
    _cache_warm_task = None
    _log_task_state("short_term_cache_warm_scheduler", "STOPPED")
    logger.warning("short_term_cache_warm_scheduler_task_stopped")


def get_automation_scheduler_status(account_mode: AccountMode) -> dict:
    running = _loop_tasks[account_mode] is not None and not _loop_tasks[account_mode].done()
    enabled = bool(_runtime_enabled[account_mode]) and _is_backend_scheduler_supported(account_mode)
    grid = _scheduler_grid_snapshot()
    return {
        "account_mode": account_mode,
        "enabled": enabled,
        "running": running,
        "poll_seconds": settings.automation_short_term_scheduler_poll_seconds,
        "interval_minutes": settings.short_term_scan_interval_minutes,
        "timezone": settings.short_term_scan_timezone,
        "on_grid": bool(grid["on_grid"]),
        "now_local": grid["now_local"],
        "next_grid_run_at": grid["next_grid_run_at"],
        "active_demo_session_id": _active_demo_session_id if account_mode == "DEMO" else None,
    }


async def set_automation_scheduler_enabled(account_mode: AccountMode, enabled: bool) -> dict:
    global _runtime_real_scan_only_enabled

    if not _is_backend_scheduler_supported(account_mode):
        _runtime_enabled[account_mode] = False
        _persist_runtime_enabled(account_mode, False)
        await _stop_mode_scheduler(account_mode)
        logger.info(
            "short_term_scheduler_backend_disabled_for_mode",
            extra={"account_mode": account_mode, "requested_enabled": bool(enabled)},
        )
        return get_automation_scheduler_status(account_mode)

    # Mutual exclusivity: enabling production scheduler for REAL disables scan-only.
    if account_mode == "REAL" and bool(enabled) and bool(_runtime_real_scan_only_enabled):
        _runtime_real_scan_only_enabled = False
        _persist_runtime_real_scan_only_enabled(False)
        await _stop_real_scan_only_scheduler()

    _runtime_enabled[account_mode] = bool(enabled)
    _persist_runtime_enabled(account_mode, _runtime_enabled[account_mode])
    logger.warning(
        "short_term_scheduler_toggle_requested",
        extra={"account_mode": account_mode, "enabled": _runtime_enabled[account_mode]},
    )
    if _runtime_enabled[account_mode]:
        await _start_mode_scheduler(account_mode)
    else:
        await _stop_mode_scheduler(account_mode)
    return get_automation_scheduler_status(account_mode)


def set_automation_scheduler_demo_session_id(demo_session_id: str | None) -> str | None:
    global _active_demo_session_id
    normalized = str(demo_session_id).strip() if demo_session_id else None
    if normalized == "":
        normalized = None
    _active_demo_session_id = normalized
    _persist_active_demo_session_id(_active_demo_session_id)
    logger.warning(
        "short_term_scheduler_demo_session_set",
        extra={"demo_session_id": _active_demo_session_id},
    )
    return _active_demo_session_id
