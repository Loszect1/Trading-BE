import json
import time

from app.services import short_term_automation_service as svc


def main() -> None:
    metrics = {
        "scan_seconds": 0.0,
        "risk_seconds": 0.0,
        "place_order_seconds": 0.0,
        "buy_evaluations": 0,
        "orders_attempted": 0,
    }

    orig_scan = svc._run_scan_with_timeout_fallback
    orig_risk = svc.evaluate_risk
    orig_place = svc.place_order

    def timed_scan(*args, **kwargs):
        started = time.perf_counter()
        output = orig_scan(*args, **kwargs)
        metrics["scan_seconds"] += time.perf_counter() - started
        return output

    def timed_risk(*args, **kwargs):
        started = time.perf_counter()
        output = orig_risk(*args, **kwargs)
        metrics["risk_seconds"] += time.perf_counter() - started
        metrics["buy_evaluations"] += 1
        return output

    def timed_place(*args, **kwargs):
        started = time.perf_counter()
        output = orig_place(*args, **kwargs)
        metrics["place_order_seconds"] += time.perf_counter() - started
        metrics["orders_attempted"] += 1
        return output

    svc._run_scan_with_timeout_fallback = timed_scan
    svc.evaluate_risk = timed_risk
    svc.place_order = timed_place

    started_total = time.perf_counter()
    result = svc.run_short_term_production_cycle(
        limit_symbols=1,
        exchange_scope="HOSE",
        account_mode="DEMO",
        nav=100_000_000.0,
        risk_per_trade=0.01,
        max_daily_new_orders=10,
        enforce_vn_scan_schedule=False,
    )
    total_seconds = time.perf_counter() - started_total

    payload = {
        "total_seconds": round(total_seconds, 4),
        "step_seconds": {
            "scan_seconds": round(metrics["scan_seconds"], 4),
            "risk_seconds": round(metrics["risk_seconds"], 4),
            "place_order_seconds": round(metrics["place_order_seconds"], 4),
        },
        "counts": {
            "buy_evaluations": metrics["buy_evaluations"],
            "orders_attempted": metrics["orders_attempted"],
        },
        "result": {
            "run_id": result.get("run_id"),
            "run_status": result.get("run_status"),
            "scanned": result.get("scanned"),
            "buy_candidates": result.get("buy_candidates"),
            "executed": result.get("executed"),
            "risk_rejected": result.get("risk_rejected"),
            "execution_rejected": result.get("execution_rejected"),
            "errors": result.get("errors"),
            "scan_mode": (result.get("detail") or {}).get("scan_mode"),
        },
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
