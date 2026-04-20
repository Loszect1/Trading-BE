"""
Service-level tests: settlement guard, FIFO sell consumption, idempotent place_order.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.services.execution.demo_adapter import DemoExecutionAdapter
from app.services.execution.types import ExecutionOutcome, OrderExecutionContext
from app.services.trading_core_service import (
    cancel_order,
    check_settlement,
    get_order_events,
    place_order,
    process_order,
    reconcile_order,
)
from psycopg import connect
from psycopg.rows import dict_row


@pytest.mark.postgres
def test_sell_rejected_when_pending_settlement(trading_db: str):
    buy = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTPEND",
            "side": "BUY",
            "quantity": 80,
            "price": 15000.0,
            "idempotency_key": "idem-tstpend-buy",
            "auto_process": True,
        }
    )
    assert buy["status"] == "FILLED"

    st = check_settlement("DEMO", "TSTPEND", 80)
    assert st["pass"] is False
    assert st["pending_settlement_qty"] == 80

    sell = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTPEND",
            "side": "SELL",
            "quantity": 80,
            "price": 16000.0,
            "idempotency_key": "idem-tstpend-sell",
            "auto_process": True,
        }
    )
    assert sell["status"] == "REJECTED"
    assert sell["reason"] == "settlement_guard_failed"


@pytest.mark.postgres
def test_place_order_idempotency_returns_same_row(trading_db: str):
    payload = {
        "account_mode": "DEMO",
        "symbol": "TSTIDEM",
        "side": "BUY",
        "quantity": 10,
        "price": 5000.0,
        "idempotency_key": "idem-tstidem-same",
        "auto_process": False,
    }
    first = place_order(payload)
    second = place_order(payload)
    assert first["id"] == second["id"]
    process_order(first["id"])
    third = place_order(payload)
    assert third["id"] == first["id"]


@pytest.mark.postgres
def test_real_execution_demo_adapter_fills(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.core.config as cfg

    monkeypatch.setattr(cfg.settings, "real_execution_adapter", "demo")
    buy = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTRDEMO",
            "side": "BUY",
            "quantity": 5,
            "price": 11000.0,
            "idempotency_key": "idem-tstrdemo-real-buy",
            "auto_process": True,
        }
    )
    assert buy["status"] == "FILLED"
    ev = get_order_events(buy["id"])
    assert [e["status"] for e in ev] == ["NEW", "SENT", "ACK", "FILLED"]


@pytest.mark.postgres
def test_real_dnse_live_missing_tokens_rejects(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.core.config as cfg

    monkeypatch.setattr(cfg.settings, "real_execution_adapter", "dnse_live")
    monkeypatch.setattr(cfg.settings, "dnse_access_token", "")
    monkeypatch.setattr(cfg.settings, "dnse_trading_token", "")
    monkeypatch.setattr(cfg.settings, "dnse_sub_account", "0000123456")

    row = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTDNSE",
            "side": "BUY",
            "quantity": 1,
            "price": 10000.0,
            "idempotency_key": "idem-tstdnse-miss-tok",
            "auto_process": False,
        }
    )
    assert row["status"] == "NEW"
    out = process_order(row["id"])
    assert out["processed"] is True
    assert out["order"]["status"] == "REJECTED"
    assert out["order"]["reason"] == "dnse_tokens_missing"
    ev = get_order_events(row["id"])
    assert "SENT" in [e["status"] for e in ev]
    assert "REJECTED" in [e["status"] for e in ev]


@pytest.mark.postgres
def test_real_dnse_partial_persists_reconcile_metadata(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.core.config as cfg
    import app.services.trading_core_service as tcs

    monkeypatch.setattr(cfg.settings, "real_execution_adapter", "dnse_live")

    class FakeDnse:
        name = "dnse_live"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
            return ExecutionOutcome(
                internal_status="PARTIAL",
                reason=None,
                broker_order_id="BRK-P1",
                broker_raw={"orderStatus": "PARTIALLY_FILLED", "matchedQty": 4, "leaveQty": 6},
                log_messages=[],
            )

    def _adapter(mode: str):
        return FakeDnse() if mode == "REAL" else DemoExecutionAdapter()

    monkeypatch.setattr(tcs, "get_execution_adapter", _adapter)

    row = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTPMETA",
            "side": "BUY",
            "quantity": 10,
            "price": 9000.0,
            "idempotency_key": "idem-tstpmeta-partial",
            "auto_process": False,
        }
    )
    out = process_order(row["id"])
    assert out["processed"] is True
    assert out["order"]["status"] == "PARTIAL"
    with connect(trading_db, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT order_metadata FROM orders_core WHERE id = %(id)s::uuid",
                {"id": row["id"]},
            )
            meta_row = cur.fetchone()
    assert meta_row is not None
    meta = meta_row["order_metadata"]
    assert meta.get("dnse_last_reconcile", {}).get("matched_qty") == 4
    assert meta["dnse_last_reconcile"].get("leave_qty") == 6

    rec = reconcile_order(row["id"])
    assert rec.get("reconcile") is True


@pytest.mark.postgres
def test_real_dnse_cancel_invokes_broker_when_broker_id(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.core.config as cfg
    import app.services.trading_core_service as tcs

    monkeypatch.setattr(cfg.settings, "real_execution_adapter", "dnse_live")

    class FakeDnse:
        name = "dnse_live"

        def cancel_at_broker(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
            assert ctx.broker_order_id == "BRK-CX1"
            return ExecutionOutcome(
                internal_status="CANCELLED",
                reason=None,
                broker_order_id=ctx.broker_order_id,
                broker_raw={"orderStatus": "CANCELLED"},
                log_messages=["cancelled"],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda mode: FakeDnse() if mode == "REAL" else DemoExecutionAdapter())

    row = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTCXOK",
            "side": "BUY",
            "quantity": 5,
            "price": 8000.0,
            "idempotency_key": "idem-tstcxok",
            "auto_process": False,
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'PARTIAL', broker_order_id = %(bid)s
                WHERE id = %(id)s::uuid
                """,
                {"id": row["id"], "bid": "BRK-CX1"},
            )
        conn.commit()

    result = cancel_order(row["id"], "test_cancel")
    assert result["cancelled"] is True
    assert result["order"]["status"] == "CANCELLED"


@pytest.mark.postgres
def test_real_dnse_cancel_fails_closed_when_broker_rejects(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.core.config as cfg
    import app.services.trading_core_service as tcs

    monkeypatch.setattr(cfg.settings, "real_execution_adapter", "dnse_live")

    class FakeDnse:
        name = "dnse_live"

        def cancel_at_broker(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_cancel_pending_or_ambiguous",
                broker_order_id=ctx.broker_order_id,
                broker_raw={"orderStatus": "PENDING_CANCEL"},
                log_messages=["still pending"],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda mode: FakeDnse() if mode == "REAL" else DemoExecutionAdapter())

    row = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTCXBAD",
            "side": "BUY",
            "quantity": 3,
            "price": 7000.0,
            "idempotency_key": "idem-tstcxbad",
            "auto_process": False,
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'ACK', broker_order_id = %(bid)s
                WHERE id = %(id)s::uuid
                """,
                {"id": row["id"], "bid": "BRK-CX2"},
            )
        conn.commit()

    result = cancel_order(row["id"], "test_cancel")
    assert result["cancelled"] is False
    assert result["reason"] == "dnse_cancel_pending_or_ambiguous"

    with connect(trading_db, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM orders_core WHERE id = %(id)s::uuid", {"id": row["id"]})
            st = cur.fetchone()["status"]
    assert st == "ACK"


@pytest.mark.postgres
def test_settlement_roll_allows_sell(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    """After simulated settlement date, SELL path reaches FILLED and consumes lots."""
    buy_day = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)
    after_settle = datetime(2026, 1, 12, 9, 0, tzinfo=timezone.utc)

    def _clock():
        return buy_day

    import app.services.trading_core_service as tcs

    monkeypatch.setattr(tcs, "_utc_now", _clock)
    monkeypatch.setattr(tcs, "_settlement_effective_today", lambda: _clock().date())

    buy = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTROLL",
            "side": "BUY",
            "quantity": 30,
            "price": 12000.0,
            "idempotency_key": "idem-tstroll-buy",
            "auto_process": True,
        }
    )
    assert buy["status"] == "FILLED"

    monkeypatch.setattr(tcs, "_utc_now", lambda: after_settle)
    monkeypatch.setattr(tcs, "_settlement_effective_today", lambda: after_settle.date())

    sell = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTROLL",
            "side": "SELL",
            "quantity": 30,
            "price": 13000.0,
            "idempotency_key": "idem-tstroll-sell",
            "auto_process": True,
        }
    )
    assert sell["status"] == "FILLED"
    ev = get_order_events(sell["id"])
    assert [e["status"] for e in ev] == ["NEW", "SENT", "ACK", "FILLED"]
