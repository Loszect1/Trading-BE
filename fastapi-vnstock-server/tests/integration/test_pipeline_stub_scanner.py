"""
Integration-style flow: stubbed scanner batch → risk → ``place_order`` via
``run_short_term_production_cycle`` (real advisory lock + DB persistence).

VN real scanner/signal paths that call vnstock are not executed here.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import app.services.short_term_automation_service as stauto


@pytest.mark.postgres
def test_short_term_cycle_stub_scanner_runs_pipeline(trading_db: str):
    fake_batch = {
        "signals": [
            {
                "id": "replay-sig-1",
                "symbol": "TSTPIPE",
                "action": "BUY",
                "entry_price": 10000.0,
                "stoploss_price": 9500.0,
            },
        ],
        "scanned": 1,
        "skipped_insufficient_data": 0,
    }

    with patch.object(stauto, "run_short_term_scan_batch", return_value=fake_batch):
        result = stauto.run_short_term_production_cycle(
            limit_symbols=5,
            account_mode="DEMO",
            nav=100_000_000.0,
            risk_per_trade=0.01,
            max_daily_new_orders=50,
            enforce_vn_scan_schedule=False,
        )

    assert result["success"] is True
    assert result["run_status"] in {"COMPLETED", "PARTIAL"}
    assert result["scanned"] == 1
    assert result["buy_candidates"] == 1
    assert result["executed"] >= 1
    assert result["risk_rejected"] == 0

    last = stauto.get_last_short_term_automation_run()
    assert last is not None
    assert last["run_status"] in {"COMPLETED", "PARTIAL"}
    detail = last.get("detail") or {}
    executions = detail.get("executions") or []
    assert any(e.get("outcome") == "executed" for e in executions)
