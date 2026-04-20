import json
import time
from typing import Any, Callable

from app.services import short_term_automation_service as auto_svc
from app.services import signal_engine_service as sig_svc


def _timed_call(metrics: dict[str, dict[str, float]], name: str, fn: Callable[..., Any], *args, **kwargs) -> Any:
    started = time.perf_counter()
    try:
        return fn(*args, **kwargs)
    finally:
        elapsed = time.perf_counter() - started
        bucket = metrics.setdefault(name, {"seconds": 0.0, "calls": 0})
        bucket["seconds"] += elapsed
        bucket["calls"] += 1


def profile_cycle_scan_wrapper() -> dict[str, Any]:
    original = auto_svc._run_scan_with_timeout_fallback
    wrapper_metrics = {"seconds": 0.0, "calls": 0}

    def timed_wrapper(*args, **kwargs):
        started = time.perf_counter()
        out = original(*args, **kwargs)
        wrapper_metrics["seconds"] += time.perf_counter() - started
        wrapper_metrics["calls"] += 1
        return out

    auto_svc._run_scan_with_timeout_fallback = timed_wrapper
    try:
        started_total = time.perf_counter()
        result = auto_svc.run_short_term_production_cycle(
            limit_symbols=1,
            exchange_scope="HOSE",
            account_mode="DEMO",
            nav=100_000_000.0,
            risk_per_trade=0.01,
            max_daily_new_orders=10,
            enforce_vn_scan_schedule=False,
        )
        total_seconds = time.perf_counter() - started_total
    finally:
        auto_svc._run_scan_with_timeout_fallback = original

    return {
        "cycle_total_seconds": round(total_seconds, 4),
        "scan_wrapper_seconds": round(wrapper_metrics["seconds"], 4),
        "scan_wrapper_calls": int(wrapper_metrics["calls"]),
        "run_status": result.get("run_status"),
        "scanned": result.get("scanned"),
        "buy_candidates": result.get("buy_candidates"),
        "executed": result.get("executed"),
        "errors": result.get("errors"),
        "scan_mode": (result.get("detail") or {}).get("scan_mode"),
    }


def profile_direct_scan_substeps() -> dict[str, Any]:
    metrics: dict[str, dict[str, float]] = {}

    originals = {
        "fetch_news_snapshot_for_scoring": sig_svc.fetch_news_snapshot_for_scoring,
        "default_news_aggregate_callable": sig_svc.default_news_aggregate_callable,
        "fetch_symbol_daily_closes": sig_svc.fetch_symbol_daily_closes,
        "_get_scan_symbol_exchange_pairs_from_liquidity_cache": sig_svc._get_scan_symbol_exchange_pairs_from_liquidity_cache,
        "_read_liquidity_gate_cache": sig_svc._read_liquidity_gate_cache,
        "_get_close_and_volume": sig_svc._get_close_and_volume,
        "_write_liquidity_gate_cache": sig_svc._write_liquidity_gate_cache,
        "score_short_term_technical": sig_svc.score_short_term_technical,
        "score_news_for_symbol": sig_svc.score_news_for_symbol,
        "score_macro_fundamental_proxy": sig_svc.score_macro_fundamental_proxy,
        "build_signal_quality_metadata": sig_svc.build_signal_quality_metadata,
        "_insert_signal": sig_svc._insert_signal,
    }

    sig_svc.fetch_news_snapshot_for_scoring = lambda *a, **k: _timed_call(
        metrics, "fetch_news_snapshot_for_scoring", originals["fetch_news_snapshot_for_scoring"], *a, **k
    )
    sig_svc.default_news_aggregate_callable = lambda *a, **k: _timed_call(
        metrics, "default_news_aggregate_callable", originals["default_news_aggregate_callable"], *a, **k
    )
    sig_svc.fetch_symbol_daily_closes = lambda *a, **k: _timed_call(
        metrics, "fetch_symbol_daily_closes", originals["fetch_symbol_daily_closes"], *a, **k
    )
    sig_svc._get_scan_symbol_exchange_pairs_from_liquidity_cache = lambda *a, **k: _timed_call(
        metrics,
        "_get_scan_symbol_exchange_pairs_from_liquidity_cache",
        originals["_get_scan_symbol_exchange_pairs_from_liquidity_cache"],
        *a,
        **k,
    )
    sig_svc._read_liquidity_gate_cache = lambda *a, **k: _timed_call(
        metrics, "_read_liquidity_gate_cache", originals["_read_liquidity_gate_cache"], *a, **k
    )
    sig_svc._get_close_and_volume = lambda *a, **k: _timed_call(
        metrics, "_get_close_and_volume", originals["_get_close_and_volume"], *a, **k
    )
    sig_svc._write_liquidity_gate_cache = lambda *a, **k: _timed_call(
        metrics, "_write_liquidity_gate_cache", originals["_write_liquidity_gate_cache"], *a, **k
    )
    sig_svc.score_short_term_technical = lambda *a, **k: _timed_call(
        metrics, "score_short_term_technical", originals["score_short_term_technical"], *a, **k
    )
    sig_svc.score_news_for_symbol = lambda *a, **k: _timed_call(
        metrics, "score_news_for_symbol", originals["score_news_for_symbol"], *a, **k
    )
    sig_svc.score_macro_fundamental_proxy = lambda *a, **k: _timed_call(
        metrics, "score_macro_fundamental_proxy", originals["score_macro_fundamental_proxy"], *a, **k
    )
    sig_svc.build_signal_quality_metadata = lambda *a, **k: _timed_call(
        metrics, "build_signal_quality_metadata", originals["build_signal_quality_metadata"], *a, **k
    )
    sig_svc._insert_signal = lambda *a, **k: _timed_call(metrics, "_insert_signal", originals["_insert_signal"], *a, **k)

    try:
        started_total = time.perf_counter()
        result = sig_svc.run_short_term_scan_batch(limit_symbols=1, exchange_scope="HOSE")
        total_seconds = time.perf_counter() - started_total
    finally:
        for name, fn in originals.items():
            setattr(sig_svc, name, fn)

    steps = []
    for name, stat in metrics.items():
        seconds = stat["seconds"]
        calls = int(stat["calls"])
        steps.append(
            {
                "name": name,
                "seconds": round(seconds, 4),
                "calls": calls,
                "avg_ms": round((seconds / calls) * 1000.0, 2) if calls > 0 else 0.0,
            }
        )
    steps.sort(key=lambda item: item["seconds"], reverse=True)

    return {
        "scan_total_seconds": round(total_seconds, 4),
        "scanned": int(result.get("scanned") or 0),
        "signals_written": len(result.get("signals") or []),
        "skipped_insufficient_data": int(result.get("skipped_insufficient_data") or 0),
        "skipped_low_liquidity": int(result.get("skipped_low_liquidity") or 0),
        "skipped_no_volume_spike": int(result.get("skipped_no_volume_spike") or 0),
        "steps": steps,
    }


def main() -> None:
    cycle = profile_cycle_scan_wrapper()
    direct = profile_direct_scan_substeps()
    payload = {"cycle_scan": cycle, "direct_scan_breakdown": direct}
    print(json.dumps(payload, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
