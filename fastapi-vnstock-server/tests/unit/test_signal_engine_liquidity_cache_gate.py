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
