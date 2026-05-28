from __future__ import annotations

import app.services.signal_engine_service as sigsvc


def test_scan_universe_filters_only_eligible_liquidity_and_spike(
    monkeypatch,
) -> None:
    keys = [
        "scan:liquidity:HOSE:AAA",
        "scan:liquidity:HNX:BBB",
        "scan:liquidity:UPCOM:CCC",
        "scan:liquidity:HOSE:DDD",
    ]
    payload_by_key = {
        "scan:liquidity:HOSE:AAA": {"eligible_liquidity": True, "eligible_spike": True},
        "scan:liquidity:HNX:BBB": {"eligible_liquidity": True, "eligible_spike": False},
        "scan:liquidity:UPCOM:CCC": {"eligible_liquidity": False, "eligible_spike": True},
        "scan:liquidity:HOSE:DDD": {"eligible_liquidity": True, "eligible_spike": True},
    }

    monkeypatch.setattr(sigsvc._redis_cache, "scan_keys", lambda pattern, limit=500_000: keys)
    monkeypatch.setattr(sigsvc._redis_cache, "get_json", lambda key: payload_by_key.get(key))

    out = sigsvc._get_scan_symbol_exchange_pairs_from_liquidity_cache(
        limit_total=0,
        exchange_scope="ALL",
    )

    assert out == [("AAA", "HOSE"), ("DDD", "HOSE")]


def test_scan_universe_treats_missing_flags_as_not_eligible(
    monkeypatch,
) -> None:
    keys = [
        "scan:liquidity:HOSE:AAA",
        "scan:liquidity:HNX:BBB",
    ]
    payload_by_key = {
        "scan:liquidity:HOSE:AAA": {"eligible_liquidity": True},
        "scan:liquidity:HNX:BBB": {"eligible_spike": True},
    }

    monkeypatch.setattr(sigsvc._redis_cache, "scan_keys", lambda pattern, limit=500_000: keys)
    monkeypatch.setattr(sigsvc._redis_cache, "get_json", lambda key: payload_by_key.get(key))

    out = sigsvc._get_scan_symbol_exchange_pairs_from_liquidity_cache(
        limit_total=0,
        exchange_scope="ALL",
    )

    assert out == []


def test_short_term_liquidity_rejects_irregular_volume_history(monkeypatch) -> None:
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_avg_daily_volume", 20_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_latest_volume", 10_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_liquidity_regular_window_sessions", 20)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_regular_session_volume", 10_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_active_sessions", 12)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_max_zero_volume_sessions", 2)

    volumes = [0.0] * 8 + [5_000.0] * 4 + [50_000.0] * 8
    detail = sigsvc._short_term_liquidity_detail(
        baseline_vol=24_000.0,
        latest_vol=50_000.0,
        volumes=volumes,
    )

    assert detail["eligible_liquidity"] is False
    assert detail["reason"] == "irregular_volume_history"
    assert detail["active_sessions"] == 8
    assert detail["zero_volume_sessions"] == 8


def test_short_term_liquidity_accepts_regular_volume_history(monkeypatch) -> None:
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_avg_daily_volume", 20_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_latest_volume", 10_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_liquidity_regular_window_sessions", 20)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_regular_session_volume", 10_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_active_sessions", 12)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_max_zero_volume_sessions", 2)

    volumes = [15_000.0] * 14 + [8_000.0] * 6

    assert sigsvc._is_short_term_liquid_enough(
        22_000.0,
        15_000.0,
        volumes=volumes,
    )


def test_short_term_liquidity_reason_keeps_absolute_volume_failure(monkeypatch) -> None:
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_avg_daily_volume", 20_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_latest_volume", 10_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_liquidity_regular_window_sessions", 20)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_regular_session_volume", 10_000.0)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_min_active_sessions", 12)
    monkeypatch.setattr(sigsvc.settings, "short_term_scan_max_zero_volume_sessions", 2)

    detail = sigsvc._short_term_liquidity_detail(
        86_300.0,
        0.0,
        volumes=[25_000.0] * 19 + [0.0],
    )

    assert detail["eligible_liquidity"] is False
    assert detail["reason"] == "latest_volume_below_floor"
    assert detail["regularity_reason"] == "ok"


def test_cached_symbol_liquidity_status_rejects_low_or_irregular_cache(monkeypatch) -> None:
    key = "scan:liquidity:HNX:BNA"
    payload_by_key = {
        key: {
            "symbol": "BNA",
            "exchange": "HNX",
            "baseline_vol": 86_300.0,
            "latest_vol": 0.0,
            "eligible_liquidity": False,
            "eligible_spike": False,
            "regular_liquidity": False,
            "liquidity_detail": {"reason": "latest_volume_below_floor"},
        }
    }

    monkeypatch.setattr(sigsvc._redis_cache, "scan_keys", lambda pattern, limit=20: [key])
    monkeypatch.setattr(sigsvc._redis_cache, "get_many_json", lambda keys: payload_by_key)
    monkeypatch.setattr(sigsvc._redis_cache, "get_json", lambda key: payload_by_key.get(key))

    status = sigsvc.get_cached_symbol_liquidity_status("bna")

    assert status["symbol"] == "BNA"
    assert status["eligible_liquidity"] is False
    assert status["reason"] == "low_or_irregular_liquidity"
