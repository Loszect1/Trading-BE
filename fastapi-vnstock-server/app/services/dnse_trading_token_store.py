from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.core.config import AppSettings

_lock = threading.RLock()
_cached_trading_token: str = ""


def get_cached_dnse_trading_token() -> str:
    with _lock:
        return _cached_trading_token


def set_cached_dnse_trading_token(token: str) -> None:
    with _lock:
        global _cached_trading_token
        _cached_trading_token = (token or "").strip()


def clear_cached_dnse_trading_token() -> None:
    set_cached_dnse_trading_token("")


def get_runtime_dnse_trading_token(settings: AppSettings) -> str:
    """
    Prefer in-memory refreshed token; fall back to env-backed settings value.
    Thread-safe read of the cache; settings is read without holding the lock.
    """
    cached = get_cached_dnse_trading_token().strip()
    if cached:
        return cached
    return (settings.dnse_trading_token or "").strip()
