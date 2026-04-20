"""
HTTP-level integration against trading core routes (sub-app, no scheduler).

Covers: ``/risk/evaluate``, ``/risk/check-settlement``, ``/execution/place``,
order event timeline. Requires PostgreSQL (``trading_db`` fixture).
"""

from __future__ import annotations

import pytest


@pytest.mark.postgres
def test_http_risk_evaluate_and_reject_buy_on_risk(trading_api_client):
    resp = trading_api_client.post(
        "/risk/evaluate",
        json={
            "symbol": "TSTHTTP",
            "nav": 1_000_000,
            "risk_per_trade": 0.01,
            "entry_price": 100.0,
            "stoploss_price": 100.0,
            "daily_new_orders": 0,
            "max_daily_new_orders": 10,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["pass"] is False

    place = trading_api_client.post(
        "/execution/place",
        json={
            "account_mode": "DEMO",
            "symbol": "TSTHTTP",
            "side": "BUY",
            "quantity": 100,
            "price": 100.0,
            "stoploss_price": 100.0,
            "nav": 1_000_000,
            "risk_per_trade": 0.01,
            "idempotency_key": "idem-tsthttp-buy-fail",
        },
    )
    assert place.status_code == 200
    pl = place.json()
    assert pl["success"] is False
    assert "risk_reject" in str(pl["data"].get("reason", ""))


@pytest.mark.postgres
def test_http_buy_then_order_events_then_settlement_guard_on_sell(trading_api_client):
    place = trading_api_client.post(
        "/execution/place",
        json={
            "account_mode": "DEMO",
            "symbol": "TSTSELL",
            "side": "BUY",
            "quantity": 50,
            "price": 20000.0,
            "stoploss_price": 19000.0,
            "nav": 100_000_000.0,
            "risk_per_trade": 0.01,
            "idempotency_key": "idem-tstsell-buy-1",
        },
    )
    assert place.status_code == 200
    pl = place.json()
    assert pl["success"] is True
    order_id = pl["data"]["id"]
    assert pl["data"]["status"] == "FILLED"

    ev = trading_api_client.get(f"/orders/{order_id}/events")
    assert ev.status_code == 200
    events = ev.json()["data"]
    statuses = [e["status"] for e in events]
    assert statuses[:4] == ["NEW", "SENT", "ACK", "FILLED"]

    chk = trading_api_client.post(
        "/risk/check-settlement",
        json={"account_mode": "DEMO", "symbol": "TSTSELL", "quantity": 50},
    )
    assert chk.status_code == 200
    assert chk.json()["data"]["pass"] is False
    assert chk.json()["data"]["available_qty"] == 0

    sell = trading_api_client.post(
        "/execution/place",
        json={
            "account_mode": "DEMO",
            "symbol": "TSTSELL",
            "side": "SELL",
            "quantity": 50,
            "price": 21000.0,
            "idempotency_key": "idem-tstsell-sell-blocked",
        },
    )
    assert sell.status_code == 200
    sj = sell.json()
    assert sj["success"] is False
    assert sj["data"]["reason"] == "settlement_guard_failed"
