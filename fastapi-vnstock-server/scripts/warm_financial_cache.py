from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict

CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.vnstock_api_service import VNStockApiService


def main() -> None:
    svc = VNStockApiService()
    exchanges = ("HOSE", "HNX", "UPCOM")
    methods = ("ratio", "income_statement", "balance_sheet", "cash_flow")
    stats: dict[str, int] = defaultdict(int)
    started = time.time()

    for exchange in exchanges:
        rows = svc.call_listing("symbols_by_exchange", method_kwargs={"exchange": exchange})
        symbols: list[str] = []
        seen: set[str] = set()
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol", "")).strip().upper()
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)
                symbols.append(symbol)

        print(f"[warmup] exchange={exchange} symbols={len(symbols)}", flush=True)
        for idx, symbol in enumerate(symbols, start=1):
            for method_name in methods:
                try:
                    svc.call_financial(
                        method_name,
                        source="VCI",
                        symbol=symbol,
                        period="quarter",
                        get_all=True,
                        show_log=False,
                    )
                    stats["ok"] += 1
                except Exception as exc:
                    stats["err"] += 1
                    if stats["err"] <= 20:
                        print(
                            f"[warmup][err] ex={exchange} sym={symbol} "
                            f"method={method_name} err={type(exc).__name__}: {exc}",
                            flush=True,
                        )
                # Keep under community plan threshold (60 req/minute) with headroom.
                time.sleep(1.15)

            stats["symbols_done"] += 1
            if idx % 20 == 0:
                elapsed = time.time() - started
                print(
                    f"[warmup][progress] ex={exchange} {idx}/{len(symbols)} symbols, "
                    f"elapsed={elapsed:.1f}s, ok={stats['ok']}, err={stats['err']}",
                    flush=True,
                )

    elapsed = time.time() - started
    print(
        json.dumps(
            {
                "status": "done",
                "elapsed_seconds": round(elapsed, 1),
                "symbols_done": stats["symbols_done"],
                "financial_calls_ok": stats["ok"],
                "financial_calls_err": stats["err"],
            },
            ensure_ascii=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
