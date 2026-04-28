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
from app.services.demo_trading_service import get_demo_session_cash_balance, get_demo_strategy_remaining_cash
from app.services.short_term_automation_service import run_short_term_production_cycle
from app.services.short_term_scan_schedule import (
    is_instant_on_short_term_scan_grid,
    next_run_datetimes,
)
from app.services.signal_engine_service import (
    refresh_short_term_liquidity_cache_from_db,
    scan_and_persist_symbol_news_from_liquidity_cache,
    warm_daily_volume_for_saved_symbols,
    warm_short_term_liquidity_cache,
)

logger = logging.getLogger(__name__)

AccountMode = Literal["REAL", "DEMO"]
_ACCOUNT_MODES: tuple[AccountMode, ...] = ("REAL", "DEMO")
_BACKEND_SCHEDULER_SUPPORTED_MODES: tuple[AccountMode, ...] = ("DEMO",)
# Internal scheduler: one grid tick runs three persisted cycles in strict board order.
_SHORT_TERM_SCHEDULER_EXCHANGE_SCOPES: tuple[str, ...] = ("HOSE", "HNX", "UPCOM")
_loop_tasks: dict[AccountMode, asyncio.Task | None] = {mode: None for mode in _ACCOUNT_MODES}
_cache_warm_task: asyncio.Task | None = None
_last_cache_warm_local_date: date | None = None
_post_close_refresh_run_markers: set[str] = set()
_runtime_enabled: dict[AccountMode, bool] = {
    mode: bool(settings.automation_short_term_scheduler_enabled) for mode in _ACCOUNT_MODES
}
_active_demo_session_id: str | None = None


def _is_backend_scheduler_supported(account_mode: AccountMode) -> bool:
    return account_mode in _BACKEND_SCHEDULER_SUPPORTED_MODES


def _short_term_allocated_nav() -> float:
    total = float(settings.strategy_total_cash_vnd)
    pct = float(settings.strategy_alloc_short_term_pct)
    nav = total * pct
    return max(1_000_000.0, nav)


def _short_term_allocated_nav_for_mode(account_mode: AccountMode, demo_session_id: str | None) -> float:
    if str(account_mode).upper() == "DEMO":
        cash = get_demo_strategy_remaining_cash(str(demo_session_id or ""), "SHORT_TERM")
        if cash > 0:
            return max(1_000_000.0, cash)
        cash = get_demo_session_cash_balance(demo_session_id)
        if cash is not None and cash > 0:
            alloc = cash * float(settings.strategy_alloc_short_term_pct)
            return max(1_000_000.0, alloc)
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
    for idx, scope in enumerate(_SHORT_TERM_SCHEDULER_EXCHANGE_SCOPES, start=1):
        try:
            raw = run_short_term_production_cycle(
                limit_symbols=0,
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
                volume_stats = await asyncio.to_thread(
                    warm_daily_volume_for_saved_symbols,
                    30,
                )
                redis_stats = await asyncio.to_thread(
                    refresh_short_term_liquidity_cache_from_db,
                    30,
                    "ALL",
                )
                news_stats = await asyncio.to_thread(
                    scan_and_persist_symbol_news_from_liquidity_cache,
                    exchange_scope="ALL",
                    max_symbols=0,
                    per_symbol_news_limit=20,
                )
                _post_close_refresh_run_markers.add(current_slot_marker)
                logger.warning(
                    "short_term_post_close_refresh_completed",
                    extra={
                        "slot_marker": current_slot_marker,
                        "volume_stats": volume_stats,
                        "redis_stats": redis_stats,
                        "news_stats": news_stats,
                    },
                )
        except Exception as exc:
            logger.warning(
                "short_term_cache_warm_failed",
                extra={"error": str(exc)},
            )
        await asyncio.sleep(poll_seconds)


async def start_automation_scheduler() -> None:
    try:
        _load_runtime_enabled_from_db()
    except Exception as exc:
        logger.warning("short_term_scheduler_state_load_failed", extra={"error": str(exc)})
    try:
        _load_active_demo_session_from_db()
    except Exception as exc:
        logger.warning("short_term_scheduler_demo_context_load_failed", extra={"error": str(exc)})

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
    await _start_cache_warm_scheduler()


async def stop_automation_scheduler() -> None:
    for mode in _ACCOUNT_MODES:
        await _stop_mode_scheduler(mode)
    await _stop_cache_warm_scheduler()


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
    logger.warning("short_term_scheduler_task_started", extra={"account_mode": account_mode})


async def _stop_mode_scheduler(account_mode: AccountMode) -> None:
    task = _loop_tasks[account_mode]
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _loop_tasks[account_mode] = None
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
    if not _is_backend_scheduler_supported(account_mode):
        _runtime_enabled[account_mode] = False
        _persist_runtime_enabled(account_mode, False)
        await _stop_mode_scheduler(account_mode)
        logger.info(
            "short_term_scheduler_backend_disabled_for_mode",
            extra={"account_mode": account_mode, "requested_enabled": bool(enabled)},
        )
        return get_automation_scheduler_status(account_mode)
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
