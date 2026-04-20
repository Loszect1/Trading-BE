from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING

from app.services.dnse_trading_token_store import set_cached_dnse_trading_token
from app.services.execution.dnse_token import (
    normalize_dnse_error,
    validate_dnse_access_token_for_live,
    validate_dnse_tokens_for_live,
)

if TYPE_CHECKING:
    from app.core.config import AppSettings

logger = logging.getLogger(__name__)

DnseTradingTokenFetcher = Callable[[], str]


def default_fetch_dnse_trading_token_from_broker(settings: AppSettings) -> str:
    """Blocking call into vnstock Trade.get_trading_token (same pattern as HTTP router)."""
    from vnstock.connector.dnse import Trade

    access = (settings.dnse_access_token or "").strip()
    if not access:
        raise ValueError("dnse_access_token_missing")

    trade = Trade()
    trade.token = access
    otp = (settings.dnse_refresh_otp or "").strip()
    smart = bool(settings.dnse_trading_token_refresh_smart_otp)
    if not smart and not otp:
        raise ValueError("dnse_refresh_otp_missing")

    trading_token = trade.get_trading_token(otp=otp, smart_otp=smart)
    if not trading_token:
        raise RuntimeError("dnse_trading_token_empty")
    return str(trading_token).strip()


def _call_fetch_with_timeout(fetch: DnseTradingTokenFetcher, timeout_sec: float) -> str:
    timeout_sec = max(0.5, float(timeout_sec))
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(fetch)
        try:
            return fut.result(timeout=timeout_sec).strip()
        except FuturesTimeoutError as e:
            raise TimeoutError(f"DNSE trading token refresh exceeded {timeout_sec}s") from e


def refresh_dnse_trading_token_sync(
    settings: AppSettings,
    *,
    fetch: DnseTradingTokenFetcher | None = None,
) -> tuple[bool, str | None]:
    """
    Fetch a new trading token and update the process-wide cache on success.

    Returns (ok, failure_reason_code). Never logs or returns secret values.
    On failure, the previous cache is left unchanged.
    """
    if not settings.dnse_trading_token_refresh_enabled:
        return False, "dnse_refresh_disabled"

    fetcher = fetch or (lambda: default_fetch_dnse_trading_token_from_broker(settings))

    access = (settings.dnse_access_token or "").strip()
    ok_access, access_reason = validate_dnse_access_token_for_live(access)
    if not ok_access:
        logger.warning(
            "dnse_trading_token_refresh_skipped",
            extra={"reason": access_reason or "dnse_access_token_invalid"},
        )
        return False, access_reason or "dnse_access_token_invalid"

    timeout = float(settings.dnse_trading_token_refresh_timeout_seconds)
    try:
        new_token = _call_fetch_with_timeout(fetcher, timeout)
    except Exception as exc:
        reason = normalize_dnse_error(exc)
        logger.warning(
            "dnse_trading_token_refresh_failed",
            extra={"reason": reason, "error_type": type(exc).__name__},
        )
        return False, reason

    ok_pair, pair_reason = validate_dnse_tokens_for_live(access, new_token)
    if not ok_pair:
        logger.warning(
            "dnse_trading_token_refresh_rejected_new_token",
            extra={"reason": pair_reason or "dnse_trading_token_invalid"},
        )
        return False, pair_reason or "dnse_trading_token_invalid"

    set_cached_dnse_trading_token(new_token)
    logger.info("dnse_trading_token_refresh_ok")
    return True, None
