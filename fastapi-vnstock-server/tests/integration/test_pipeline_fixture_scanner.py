"""
Pipeline coverage with **real** ``run_short_term_scan_batch`` logic and DB inserts.

Only the vnstock HTTP/SDK layer is replaced by ``FixtureVNStockApi`` + synthetic OHLCV
(no outbound market data calls).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import app.services.short_term_automation_service as stauto
import app.services.signal_engine_service as sigeng
from tests.helpers.fixture_vnstock_api import FixtureVNStockApi
from tests.helpers.synthetic_market_data import ohlcv_rows_short_term_buy_spike, ohlcv_rows_short_term_hold_flat_volume


@pytest.mark.postgres
def test_run_short_term_scan_batch_uses_real_signal_rules_with_fixture_api(trading_db: str):
    sym = "TSTBUY1"
    fake_api = FixtureVNStockApi(
        symbols_by_exchange={
            "HOSE": [{"symbol": sym}],
            "HNX": [],
            "UPCOM": [],
        },
        history_by_symbol={sym: ohlcv_rows_short_term_buy_spike()},
    )
    with patch.object(sigeng, "vnstock_api_service", fake_api):
        batch = sigeng.run_short_term_scan_batch(limit_symbols=1)

    assert batch["scanned"] == 1
    assert batch["skipped_insufficient_data"] == 0
    signals = batch["signals"]
    assert len(signals) == 1
    assert str(signals[0].get("action")).upper() == "BUY"
    assert str(signals[0].get("symbol")).upper() == sym


@pytest.mark.postgres
def test_run_short_term_scan_batch_hold_when_no_volume_spike(trading_db: str):
    sym = "TSTHOLD1"
    fake_api = FixtureVNStockApi(
        symbols_by_exchange={
            "HOSE": [{"symbol": sym}],
            "HNX": [],
            "UPCOM": [],
        },
        history_by_symbol={sym: ohlcv_rows_short_term_hold_flat_volume()},
    )
    with patch.object(sigeng, "vnstock_api_service", fake_api):
        batch = sigeng.run_short_term_scan_batch(limit_symbols=1)

    assert batch["scanned"] == 1
    signals = batch["signals"]
    assert len(signals) == 1
    assert str(signals[0].get("action")).upper() == "HOLD"


@pytest.mark.postgres
def test_short_term_production_cycle_fixture_scanner_risk_then_execution(trading_db: str):
    sym = "TSTPIPE2"
    fake_api = FixtureVNStockApi(
        symbols_by_exchange={
            "HOSE": [{"symbol": sym}],
            "HNX": [],
            "UPCOM": [],
        },
        history_by_symbol={sym: ohlcv_rows_short_term_buy_spike()},
    )
    with patch.object(sigeng, "vnstock_api_service", fake_api):
        result = stauto.run_short_term_production_cycle(
            limit_symbols=1,
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
    executions = (result.get("detail") or {}).get("executions") or []
    assert any(e.get("outcome") == "executed" for e in executions)


@pytest.mark.postgres
def test_short_term_production_cycle_risk_rejects_when_daily_cap_zero(trading_db: str):
    sym = "TSTPIPE3"
    fake_api = FixtureVNStockApi(
        symbols_by_exchange={
            "HOSE": [{"symbol": sym}],
            "HNX": [],
            "UPCOM": [],
        },
        history_by_symbol={sym: ohlcv_rows_short_term_buy_spike()},
    )
    with patch.object(sigeng, "vnstock_api_service", fake_api):
        result = stauto.run_short_term_production_cycle(
            limit_symbols=1,
            account_mode="DEMO",
            nav=100_000_000.0,
            risk_per_trade=0.01,
            max_daily_new_orders=0,
            enforce_vn_scan_schedule=False,
        )

    assert result["buy_candidates"] == 1
    assert result["risk_rejected"] == 1
    assert result["executed"] == 0
    executions = (result.get("detail") or {}).get("executions") or []
    assert any(e.get("outcome") == "risk_rejected" for e in executions)
