from __future__ import annotations

import pytest

from app.core import config as cfg
from app.services import dnse_trading_token_store as token_store
from app.services.dnse_trading_token_refresh import refresh_dnse_trading_token_sync


@pytest.fixture(autouse=True)
def _clear_token_cache() -> None:
    token_store.clear_cached_dnse_trading_token()
    yield
    token_store.clear_cached_dnse_trading_token()


def test_refresh_disabled_returns_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cfg.settings, "dnse_trading_token_refresh_enabled", False)
    ok, reason = refresh_dnse_trading_token_sync(cfg.settings, fetch=lambda: "anytradingtokenlong")
    assert ok is False
    assert reason == "dnse_refresh_disabled"


def test_refresh_success_updates_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cfg.settings, "dnse_trading_token_refresh_enabled", True)
    monkeypatch.setattr(cfg.settings, "dnse_access_token", "a" * 20)
    monkeypatch.setattr(cfg.settings, "dnse_trading_token", "legacyfromenv")
    ok, reason = refresh_dnse_trading_token_sync(cfg.settings, fetch=lambda: "newtradingtokenfrommock")
    assert ok is True
    assert reason is None
    assert token_store.get_cached_dnse_trading_token() == "newtradingtokenfrommock"
    assert token_store.get_runtime_dnse_trading_token(cfg.settings) == "newtradingtokenfrommock"


def test_refresh_failure_keeps_previous_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cfg.settings, "dnse_trading_token_refresh_enabled", True)
    monkeypatch.setattr(cfg.settings, "dnse_access_token", "a" * 20)
    token_store.set_cached_dnse_trading_token("cachedgoodtoken")
    ok, reason = refresh_dnse_trading_token_sync(cfg.settings, fetch=lambda: _raise_timeout())
    assert ok is False
    assert reason == "dnse_timeout"
    assert token_store.get_cached_dnse_trading_token() == "cachedgoodtoken"


def test_refresh_rejects_short_new_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cfg.settings, "dnse_trading_token_refresh_enabled", True)
    monkeypatch.setattr(cfg.settings, "dnse_access_token", "a" * 20)
    token_store.set_cached_dnse_trading_token("stillvalidcached")
    ok, reason = refresh_dnse_trading_token_sync(cfg.settings, fetch=lambda: "abc")
    assert ok is False
    assert reason == "dnse_trading_token_too_short"
    assert token_store.get_cached_dnse_trading_token() == "stillvalidcached"


def test_refresh_skips_invalid_access(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cfg.settings, "dnse_trading_token_refresh_enabled", True)
    monkeypatch.setattr(cfg.settings, "dnse_access_token", "short")
    calls: list[int] = []

    def _fetch() -> str:
        calls.append(1)
        return "neverused"

    ok, reason = refresh_dnse_trading_token_sync(cfg.settings, fetch=_fetch)
    assert ok is False
    assert reason == "dnse_access_token_too_short"
    assert calls == []


def test_runtime_prefers_cache_over_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cfg.settings, "dnse_trading_token", "fromsettings")
    token_store.set_cached_dnse_trading_token("fromcache")
    assert token_store.get_runtime_dnse_trading_token(cfg.settings) == "fromcache"


def _raise_timeout() -> str:
    raise TimeoutError("simulated")
