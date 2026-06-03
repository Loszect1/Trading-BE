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


def test_average_true_range_uses_high_low_gap() -> None:
    rows = [
        {"close": 100.0, "high": 101.0, "low": 99.0, "volume": 10_000.0},
        {"close": 108.0, "high": 112.0, "low": 105.0, "volume": 12_000.0},
        {"close": 110.0, "high": 111.0, "low": 107.0, "volume": 12_000.0},
    ]

    out = sigsvc._average_true_range_metadata(
        ohlcv_rows=rows,
        closes=[100.0, 108.0, 110.0],
        last_close=110.0,
        period=14,
    )

    assert out["method"] == "true_range"
    assert out["high_low_samples"] == 2
    assert out["atr14"] == 8.0
    assert out["atrp14"] > 7.0


def test_market_regime_risk_off_when_benchmark_and_breadth_are_weak(monkeypatch) -> None:
    monkeypatch.setattr(sigsvc.settings, "short_term_risk_off_size_multiplier", 0.5)
    closes = [120.0 - i * 0.4 for i in range(70)]
    breadth = {"available": True, "above_ma20_ratio": 0.3, "above_ma50_ratio": 0.25, "advance_5d_ratio": 0.35}

    regime = sigsvc._market_regime_metadata(closes, breadth)

    assert regime["regime"] == "RISK_OFF"
    assert regime["size_multiplier"] == 0.5


def test_short_term_entry_gate_hard_rejects_rsi_chase(monkeypatch) -> None:
    monkeypatch.setattr(sigsvc.settings, "short_term_rsi_hard_reject", 80.0)
    closes = [100.0 + i for i in range(70)]
    volumes = [100_000.0] * 69 + [250_000.0]

    passed, meta = sigsvc._short_term_entry_gate(
        closes=closes,
        volumes=volumes,
        spike=2.5,
        last_close=closes[-1],
        ema20_proxy=sum(closes[-20:]) / 20.0,
        min_spike_ratio=1.5,
        rsi_min=50.0,
        rsi_max=78.0,
    )

    assert passed is False
    assert meta["rsi_hard_reject"] is True


def test_setup_validation_blocks_risk_off_non_leader(monkeypatch) -> None:
    monkeypatch.setattr(sigsvc.settings, "short_term_risk_off_min_relative_strength_rank_pct", 60.0)
    setup = {"setup_type": "BREAKOUT"}
    gate_meta = {"rsi14": 62.0, "distance_from_ema20_pct": 3.0}
    levels = {"target_rr": 2.0, "atr": {"atrp14": 3.5}}
    market_regime = {"regime": "RISK_OFF", "regime_key": "risk_off"}
    relative_strength = {"relative_strength_rank_pct": 45.0, "is_leading_benchmark": False}

    validation = sigsvc._setup_validation_metadata(
        setup=setup,
        gate_meta=gate_meta,
        levels=levels,
        market_regime=market_regime,
        relative_strength=relative_strength,
        sector_breadth={},
    )

    assert validation["buyable"] is False
    assert "risk_off_requires_relative_strength_leader" in validation["reasons"]
