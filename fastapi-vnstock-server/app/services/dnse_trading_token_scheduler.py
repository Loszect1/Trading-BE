from __future__ import annotations

import asyncio
import logging

from app.core.config import settings
from app.services.dnse_trading_token_refresh import refresh_dnse_trading_token_sync

logger = logging.getLogger(__name__)

_refresh_task: asyncio.Task | None = None


async def _refresh_loop() -> None:
    while True:
        try:
            if settings.dnse_trading_token_refresh_enabled:
                await asyncio.to_thread(refresh_dnse_trading_token_sync, settings)
        except Exception as exc:
            logger.warning(
                "dnse_trading_token_refresh_tick_failed",
                extra={"error_type": type(exc).__name__, "error": str(exc)},
            )
        interval = max(60, int(settings.dnse_trading_token_refresh_interval_seconds))
        await asyncio.sleep(interval)


async def start_dnse_trading_token_refresh_scheduler() -> None:
    global _refresh_task
    if not settings.dnse_trading_token_refresh_enabled:
        logger.info("dnse_trading_token_refresh_scheduler_disabled")
        return
    if _refresh_task and not _refresh_task.done():
        return
    _refresh_task = asyncio.create_task(_refresh_loop(), name="dnse-trading-token-refresh")
    logger.info(
        "dnse_trading_token_refresh_scheduler_started",
        extra={
            "interval_seconds": settings.dnse_trading_token_refresh_interval_seconds,
            "timeout_seconds": settings.dnse_trading_token_refresh_timeout_seconds,
        },
    )


async def stop_dnse_trading_token_refresh_scheduler() -> None:
    global _refresh_task
    if _refresh_task and not _refresh_task.done():
        _refresh_task.cancel()
        try:
            await _refresh_task
        except asyncio.CancelledError:
            pass
    _refresh_task = None


def get_dnse_trading_token_refresh_scheduler_status() -> dict:
    running = _refresh_task is not None and not _refresh_task.done()
    return {
        "enabled": bool(settings.dnse_trading_token_refresh_enabled),
        "running": running,
        "interval_seconds": settings.dnse_trading_token_refresh_interval_seconds,
        "timeout_seconds": settings.dnse_trading_token_refresh_timeout_seconds,
    }
