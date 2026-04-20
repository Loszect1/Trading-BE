from __future__ import annotations

import re
from typing import Any


def validate_dnse_access_token_for_live(access_token: str) -> tuple[bool, str | None]:
    """Validate JWT/access token only (used before trading-token refresh)."""
    access = (access_token or "").strip()
    if not access:
        return False, "dnse_access_token_missing"
    if len(access) < 16:
        return False, "dnse_access_token_too_short"
    if "." in access:
        parts = access.split(".")
        if len(parts) < 3 or any(not p for p in parts[:3]):
            return False, "dnse_access_token_malformed_jwt"
    if re.search(r"[\r\n\0]", access):
        return False, "dnse_token_invalid_characters"
    return True, None


def validate_dnse_tokens_for_live(access_token: str, trading_token: str) -> tuple[bool, str | None]:
    """
    Explicit pre-flight validation for server-side DNSE credentials.
    Returns (ok, reason_code) where reason_code is safe to persist (no secrets).
    """
    access = (access_token or "").strip()
    trading = (trading_token or "").strip()
    if not access and not trading:
        return False, "dnse_tokens_missing"
    if not trading:
        return False, "dnse_trading_token_missing"
    ok_a, reason_a = validate_dnse_access_token_for_live(access)
    if not ok_a:
        return False, reason_a
    if len(trading) < 4:
        return False, "dnse_trading_token_too_short"
    if re.search(r"[\r\n\0]", trading):
        return False, "dnse_token_invalid_characters"
    return True, None


def normalize_dnse_error(exc: BaseException | str) -> str:
    """
    Map exceptions / message strings to a small stable reason code for logs and order.reason.
    Never includes token values.
    """
    if isinstance(exc, str):
        msg = exc.lower()
        if "timeout" in msg:
            return "dnse_timeout"
        if "connection" in msg or "connect" in msg:
            return "dnse_network_error"
        return "dnse_unknown_error"
    if isinstance(exc, TimeoutError):
        return "dnse_timeout"
    name = type(exc).__name__.lower()
    if "timeout" in name:
        return "dnse_timeout"
    msg = str(exc).lower()
    if "connection" in msg or "remote" in msg or "refused" in msg:
        return "dnse_network_error"
    if "401" in msg or "unauthorized" in msg:
        return "dnse_unauthorized"
    if "403" in msg or "forbidden" in msg:
        return "dnse_forbidden"
    if "404" in msg or "not found" in msg:
        return "dnse_not_found"
    return "dnse_unknown_error"
