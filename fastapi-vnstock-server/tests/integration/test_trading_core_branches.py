"""Deterministic trading_core_service branches: adapters via mocks, DB via trading_db (no broker network)."""

from __future__ import annotations

import json
from datetime import date
from uuid import uuid4

import pytest

from psycopg import connect
from psycopg.rows import dict_row

from app.services.execution.types import ExecutionOutcome, OrderExecutionContext
from app.services.trading_core_service import (
    cancel_order,
    get_monitoring_summary,
    get_order_events,
    get_settlement_rows,
    list_orders,
    list_risk_events,
    log_risk_event,
    place_order,
    process_order,
    reconcile_order,
    set_kill_switch,
)


@pytest.mark.postgres
def test_process_order_adapter_exception_rejects_without_risk_log_on_demo(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.trading_core_service as tcs

    class BoomAdapter:
        name = "demo"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            raise RuntimeError("adapter boom")

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: BoomAdapter())

    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTBOOM",
            "side": "BUY",
            "quantity": 2,
            "price": 5000.0,
            "idempotency_key": "idem-tstboom-demo",
            "auto_process": False,
        }
    )
    out = process_order(row["id"])
    assert out["processed"] is True
    assert out["order"]["status"] == "REJECTED"
    assert out["order"]["reason"] == "execution_adapter_exception"

    recent = list_risk_events("DEMO", limit=20, event_type="execution_adapter_exception")
    assert recent == []


@pytest.mark.postgres
def test_process_order_adapter_exception_logs_risk_on_real(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.core.config as cfg
    import app.services.trading_core_service as tcs

    monkeypatch.setattr(cfg.settings, "real_execution_adapter", "demo")

    class BoomAdapter:
        name = "demo"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            raise RuntimeError("adapter boom real")

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: BoomAdapter())

    row = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTBOOMR",
            "side": "BUY",
            "quantity": 1,
            "price": 6000.0,
            "idempotency_key": "idem-tstboomr",
            "auto_process": False,
        }
    )
    out = process_order(row["id"])
    assert out["order"]["status"] == "REJECTED"
    recent = list_risk_events("REAL", limit=10, event_type="execution_adapter_exception")
    assert len(recent) >= 1
    assert recent[0]["payload"].get("order_id") == row["id"]


@pytest.mark.postgres
def test_process_order_unexpected_adapter_status_rejected(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.trading_core_service as tcs

    class WeirdAdapter:
        name = "demo"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            return ExecutionOutcome(
                internal_status="MYSTERY",
                reason=None,
                broker_order_id="x",
                broker_raw={"k": "v"},
                log_messages=[],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: WeirdAdapter())

    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTWEIRD",
            "side": "BUY",
            "quantity": 1,
            "price": 4000.0,
            "idempotency_key": "idem-tstweird",
            "auto_process": False,
        }
    )
    out = process_order(row["id"])
    assert out["processed"] is True
    assert out["order"]["status"] == "REJECTED"
    assert out["order"]["reason"] == "execution_unexpected_status"


@pytest.mark.postgres
def test_reconcile_order_skips_terminal_fill(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTREC",
            "side": "BUY",
            "quantity": 3,
            "price": 3000.0,
            "idempotency_key": "idem-tstrec",
            "auto_process": True,
        }
    )
    assert row["status"] == "FILLED"
    out = reconcile_order(row["id"])
    assert out.get("reconciled") is False
    assert "not_eligible" in str(out.get("reason", ""))


@pytest.mark.postgres
def test_cancel_order_demo_before_send(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTCXD",
            "side": "BUY",
            "quantity": 4,
            "price": 2500.0,
            "idempotency_key": "idem-tstcxd",
            "auto_process": False,
        }
    )
    assert row["status"] == "NEW"
    result = cancel_order(row["id"], "user_cancel_test")
    assert result["cancelled"] is True
    assert result["order"]["status"] == "CANCELLED"


@pytest.mark.postgres
def test_get_monitoring_summary_includes_portfolio_and_risk_count(trading_db: str) -> None:
    log_risk_event("DEMO", "UNIT_TEST_PROBE", symbol="TSTSUM", payload={"ok": True})
    summary = get_monitoring_summary("DEMO")
    assert summary["account_mode"] == "DEMO"
    assert "portfolio" in summary
    assert "orders_by_status" in summary
    assert summary["risk_events_last_7_days"] >= 1
    assert "generated_at" in summary


@pytest.mark.postgres
def test_process_order_idempotent_when_already_filled(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTPROC2",
            "side": "BUY",
            "quantity": 2,
            "price": 8000.0,
            "idempotency_key": "idem-tstproc2",
            "auto_process": True,
        }
    )
    first = process_order(row["id"])
    assert first["processed"] is False
    assert first["reason"] == "already_filled"
    ev = get_order_events(row["id"])
    assert ev[-1]["status"] == "FILLED"


@pytest.mark.postgres
def test_sell_rejected_settlement_guard_when_lots_not_yet_available(trading_db: str) -> None:
    sym = "TSTSELL1"
    buy = place_order(
        {
            "account_mode": "DEMO",
            "symbol": sym,
            "side": "BUY",
            "quantity": 5,
            "price": 1000.0,
            "idempotency_key": "idem-tstsell1-buy",
            "auto_process": True,
        }
    )
    assert buy["status"] == "FILLED"

    sell = place_order(
        {
            "account_mode": "DEMO",
            "symbol": sym,
            "side": "SELL",
            "quantity": 1,
            "price": 1100.0,
            "idempotency_key": "idem-tstsell1-sell",
            "auto_process": False,
        }
    )
    out = process_order(sell["id"])
    assert out["order"]["status"] == "REJECTED"
    assert out["order"]["reason"] == "settlement_guard_failed"


@pytest.mark.postgres
def test_sell_rejected_fifo_when_guard_passes_but_no_lots(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.trading_core_service as tcs

    monkeypatch.setattr(
        tcs,
        "check_settlement",
        lambda *_a, **_k: {"pass": True, "available_qty": 99, "pending_settlement_qty": 0},
    )

    sell = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTSELL2",
            "side": "SELL",
            "quantity": 3,
            "price": 5000.0,
            "idempotency_key": "idem-tstsell2",
            "auto_process": False,
        }
    )
    out = process_order(sell["id"])
    assert out["order"]["status"] == "REJECTED"
    assert out["order"]["reason"] == "sell_fifo_consume_failed"


@pytest.mark.postgres
def test_process_order_does_not_hold_transaction_during_settlement_guard(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.trading_core_service as tcs

    class FillAdapter:
        name = "demo"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            return ExecutionOutcome(internal_status="FILLED", broker_order_id="brk-no-deadlock")

    def fail_roll_settlement(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("process_order must not call _roll_settlement before check_settlement")

    seen_statuses: list[str] = []

    def settlement_guard(_account_mode: str, _symbol: str, _quantity: int) -> dict[str, object]:
        with connect(trading_db, row_factory=dict_row) as check_conn:
            with check_conn.cursor() as cur:
                cur.execute("SELECT status FROM orders_core WHERE id = %(id)s::uuid", {"id": sell["id"]})
                row = cur.fetchone()
        seen_statuses.append(str(row["status"]) if row else "")
        return {"pass": False, "available_qty": 0, "pending_settlement_qty": 0}

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: FillAdapter())
    monkeypatch.setattr(tcs, "_roll_settlement", fail_roll_settlement)
    monkeypatch.setattr(tcs, "check_settlement", settlement_guard)

    sell = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTNODL",
            "side": "SELL",
            "quantity": 3,
            "price": 5000.0,
            "idempotency_key": "idem-tstnodl",
            "auto_process": False,
        }
    )

    out = process_order(sell["id"])

    assert seen_statuses == ["ACK"]
    assert out["order"]["status"] == "REJECTED"
    assert out["order"]["reason"] == "settlement_guard_failed"


@pytest.mark.postgres
def test_partial_then_filled_merges_reconcile_metadata(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.trading_core_service as tcs

    class SeqAdapter:
        name = "demo"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
            if str(ctx.current_status).upper() == "PARTIAL":
                return ExecutionOutcome(
                    internal_status="FILLED",
                    reason=None,
                    broker_order_id=ctx.broker_order_id or "brk-seq",
                    broker_raw={"matchedQty": 5},
                    log_messages=["filled_after_partial"],
                )
            return ExecutionOutcome(
                internal_status="PARTIAL",
                reason=None,
                broker_order_id="brk-seq",
                broker_raw={"matchedQty": 2, "leaveQty": 3, "orderStatus": "PARTIAL"},
                log_messages=["partial"],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: SeqAdapter())

    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTPART",
            "side": "BUY",
            "quantity": 4,
            "price": 7000.0,
            "idempotency_key": "idem-tstpart",
            "auto_process": False,
            "metadata": {"dnse_order_type": "LO", "dnse_asset_type": "stock"},
        }
    )
    first = process_order(row["id"])
    assert first["order"]["status"] == "PARTIAL"
    meta1 = first["order"].get("order_metadata") or {}
    assert "dnse_last_reconcile" in meta1

    second = process_order(row["id"])
    assert second["order"]["status"] == "FILLED"


@pytest.mark.postgres
def test_reconcile_order_runs_process_for_open_order(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.trading_core_service as tcs

    calls: list[str] = []

    class OncePartialThenFill:
        name = "demo"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
            calls.append(str(ctx.current_status).upper())
            if len(calls) >= 2:
                return ExecutionOutcome(
                    internal_status="FILLED",
                    reason=None,
                    broker_order_id="brk-rec",
                    broker_raw={},
                    log_messages=[],
                )
            return ExecutionOutcome(
                internal_status="PARTIAL",
                reason=None,
                broker_order_id="brk-rec",
                broker_raw={"leaveQty": 1},
                log_messages=[],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: OncePartialThenFill())

    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTREC2",
            "side": "BUY",
            "quantity": 2,
            "price": 1500.0,
            "idempotency_key": "idem-tstrec2",
            "auto_process": False,
        }
    )
    process_order(row["id"])
    rec = reconcile_order(row["id"])
    assert rec.get("reconcile") is True
    assert rec["order"]["status"] == "FILLED"


@pytest.mark.postgres
def test_cancel_order_real_dnse_live_broker_reject_logs_risk(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.core.config as cfg
    import app.services.trading_core_service as tcs

    monkeypatch.setattr(cfg.settings, "real_execution_adapter", "dnse_live")

    class LiveCancelReject:
        name = "dnse_live"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            return ExecutionOutcome(
                internal_status="ACK",
                reason=None,
                broker_order_id="brk-cancel-1",
                broker_raw={},
                log_messages=[],
            )

        def cancel_at_broker(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_cancel_rejected",
                broker_order_id="brk-cancel-1",
                broker_raw={"orderStatus": "REJECTED"},
                log_messages=["no"],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: LiveCancelReject())

    row = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTCXR",
            "side": "BUY",
            "quantity": 1,
            "price": 10000.0,
            "idempotency_key": "idem-tstcxr",
            "auto_process": False,
            "metadata": {"dnse_order_type": "LO", "dnse_asset_type": "stock"},
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'ACK', broker_order_id = %(bid)s,
                    order_metadata = %(meta)s::jsonb
                WHERE id = %(oid)s::uuid
                """,
                {"bid": "brk-cancel-1", "meta": json.dumps({"unit": "forced_ack"}), "oid": row["id"]},
            )
        conn.commit()

    out = cancel_order(row["id"], "test_cancel_broker")
    assert out["cancelled"] is False
    assert out.get("broker_cancel_attempted") is True

    recent = list_risk_events("REAL", limit=20, event_type="execution_dnse_cancel_failed")
    assert any(e.get("payload", {}).get("order_id") == row["id"] for e in recent)


@pytest.mark.postgres
def test_list_orders_and_settlement_rows_include_metadata(trading_db: str) -> None:
    meta = {"dnse_order_type": "LO", "note": "unit-list"}
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTLIST",
            "side": "BUY",
            "quantity": 2,
            "price": 3000.0,
            "idempotency_key": "idem-tstlist",
            "auto_process": True,
            "metadata": meta,
        }
    )
    orders = list_orders("DEMO", limit=50)
    found = next((o for o in orders if o["id"] == row["id"]), None)
    assert found is not None
    assert (found.get("order_metadata") or {}).get("note") == "unit-list"

    rows = get_settlement_rows("DEMO", symbol="TSTLIST")
    assert any(r["symbol"] == "TSTLIST" for r in rows)
    all_rows = get_settlement_rows("DEMO")
    assert any(r["symbol"] == "TSTLIST" for r in all_rows)


@pytest.mark.postgres
def test_process_order_real_dnse_reject_logs_execution_dnse_reject(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.trading_core_service as tcs

    class RejectDnse:
        name = "dnse_live"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="broker_risk_check",
                broker_order_id="brk-rej-1",
                broker_raw={"orderStatus": "REJECTED", "note": "x"},
                log_messages=["dnse-msg"],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: RejectDnse())

    row = place_order(
        {
            "account_mode": "REAL",
            "symbol": "TSTDNSEREJ",
            "side": "BUY",
            "quantity": 1,
            "price": 9000.0,
            "idempotency_key": "idem-tstdnserej",
            "auto_process": False,
        }
    )
    out = process_order(row["id"])
    assert out["processed"] is True
    assert out["order"]["status"] == "REJECTED"
    assert out["order"]["reason"] == "broker_risk_check"
    assert out["order"].get("broker_order_id") == "brk-rej-1"

    recent = list_risk_events("REAL", limit=30, event_type="execution_dnse_reject")
    hit = next((e for e in recent if e.get("payload", {}).get("order_id") == row["id"]), None)
    assert hit is not None
    assert hit["payload"].get("broker_order_id") == "brk-rej-1"
    assert hit["payload"].get("messages") == ["dnse-msg"]


@pytest.mark.postgres
def test_cancel_order_demo_after_sent_without_broker(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTCANSENT",
            "side": "BUY",
            "quantity": 1,
            "price": 1000.0,
            "idempotency_key": "idem-tstcansent",
            "auto_process": False,
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'SENT', broker_order_id = NULL
                WHERE id = %(oid)s::uuid
                """,
                {"oid": row["id"]},
            )
        conn.commit()

    result = cancel_order(row["id"], "cancel_while_sent")
    assert result["cancelled"] is True
    assert result["order"]["status"] == "CANCELLED"


@pytest.mark.postgres
def test_cancel_order_demo_after_sent_with_broker(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTCANSENTB",
            "side": "BUY",
            "quantity": 1,
            "price": 1000.0,
            "idempotency_key": "idem-tstcansentb",
            "auto_process": False,
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'SENT', broker_order_id = %(bid)s
                WHERE id = %(oid)s::uuid
                """,
                {"oid": row["id"], "bid": "demo-brk-sent"},
            )
        conn.commit()

    result = cancel_order(row["id"], "cancel_sent_with_broker_demo")
    assert result["cancelled"] is True
    assert result["order"]["status"] == "CANCELLED"


@pytest.mark.postgres
def test_cancel_order_demo_after_ack(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTCANACK",
            "side": "BUY",
            "quantity": 1,
            "price": 1000.0,
            "idempotency_key": "idem-tstcanack",
            "auto_process": False,
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'ACK', broker_order_id = %(bid)s
                WHERE id = %(oid)s::uuid
                """,
                {"oid": row["id"], "bid": "demo-brk-ack"},
            )
        conn.commit()

    result = cancel_order(row["id"], "cancel_while_ack")
    assert result["cancelled"] is True
    assert result["order"]["status"] == "CANCELLED"


@pytest.mark.postgres
def test_process_order_sent_without_broker_emits_retry_event_then_fills(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.trading_core_service as tcs

    class DemoFill:
        name = "demo"

        def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:  # noqa: ARG002
            return ExecutionOutcome(
                internal_status="FILLED",
                reason=None,
                broker_order_id="demo-fill-after-sent",
                broker_raw={},
                log_messages=[],
            )

    monkeypatch.setattr(tcs, "get_execution_adapter", lambda _mode: DemoFill())

    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTSENTRE",
            "side": "BUY",
            "quantity": 1,
            "price": 2000.0,
            "idempotency_key": "idem-tstsentre",
            "auto_process": False,
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'SENT', broker_order_id = NULL
                WHERE id = %(oid)s::uuid
                """,
                {"oid": row["id"]},
            )
        conn.commit()

    out = process_order(row["id"])
    assert out["order"]["status"] == "FILLED"
    ev = get_order_events(row["id"])
    msgs = [e.get("message") for e in ev]
    assert any(m == "Execution retry after prior SENT without broker id" for m in msgs)


@pytest.mark.postgres
def test_process_order_ack_with_broker_resumes_to_filled(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTACKRES",
            "side": "BUY",
            "quantity": 1,
            "price": 3000.0,
            "idempotency_key": "idem-tstackres",
            "auto_process": False,
        }
    )
    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE orders_core
                SET status = 'ACK', broker_order_id = %(bid)s
                WHERE id = %(oid)s::uuid
                """,
                {"oid": row["id"], "bid": "demo-resume-brk"},
            )
        conn.commit()

    out = process_order(row["id"])
    assert out["processed"] is True
    assert out["order"]["status"] == "FILLED"


@pytest.mark.postgres
def test_roll_settlement_unlocks_lots_for_check_settlement(
    trading_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.trading_core_service as tcs

    sym = "TSTROLL"
    buy = place_order(
        {
            "account_mode": "DEMO",
            "symbol": sym,
            "side": "BUY",
            "quantity": 4,
            "price": 1000.0,
            "idempotency_key": "idem-tstroll-buy",
            "auto_process": True,
        }
    )
    assert buy["status"] == "FILLED"

    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE position_lots SET settle_date = %s, available_qty = 0 WHERE symbol = %s AND account_mode = 'DEMO'",
                (date(2020, 1, 3), sym),
            )
        conn.commit()

    monkeypatch.setattr(tcs, "_settlement_effective_today", lambda: date(2025, 6, 1))
    settled = tcs.check_settlement("DEMO", sym, 4)
    assert settled["pass"] is True
    assert settled["available_qty"] == 4


@pytest.mark.postgres
def test_sell_fifo_consumes_oldest_lot_first(trading_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.trading_core_service as tcs

    monkeypatch.setattr(tcs, "_settlement_effective_today", lambda: date(2025, 6, 15))

    sym = "TSTFIFO"
    place_order(
        {
            "account_mode": "DEMO",
            "symbol": sym,
            "side": "BUY",
            "quantity": 2,
            "price": 1000.0,
            "idempotency_key": "idem-tstfifo-a",
            "auto_process": True,
        }
    )
    place_order(
        {
            "account_mode": "DEMO",
            "symbol": sym,
            "side": "BUY",
            "quantity": 2,
            "price": 1100.0,
            "idempotency_key": "idem-tstfifo-b",
            "auto_process": True,
        }
    )

    with connect(trading_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE position_lots
                SET settle_date = %(sd)s, available_qty = qty, buy_trade_date = %(bd)s
                WHERE symbol = %(sym)s AND account_mode = 'DEMO' AND qty = 2 AND avg_price < 1050
                """,
                {"sd": date(2020, 1, 1), "bd": date(2024, 1, 2), "sym": sym},
            )
            cur.execute(
                """
                UPDATE position_lots
                SET settle_date = %(sd)s, available_qty = qty, buy_trade_date = %(bd)s
                WHERE symbol = %(sym)s AND account_mode = 'DEMO' AND qty = 2 AND avg_price >= 1050
                """,
                {"sd": date(2020, 1, 1), "bd": date(2024, 6, 1), "sym": sym},
            )
        conn.commit()

    sell = place_order(
        {
            "account_mode": "DEMO",
            "symbol": sym,
            "side": "SELL",
            "quantity": 3,
            "price": 1200.0,
            "idempotency_key": "idem-tstfifo-sell",
            "auto_process": False,
        }
    )
    out = process_order(sell["id"])
    assert out["order"]["status"] == "FILLED"

    with connect(trading_db, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT buy_trade_date, available_qty, qty
                FROM position_lots
                WHERE account_mode = 'DEMO' AND symbol = %(sym)s
                ORDER BY buy_trade_date ASC
                """,
                {"sym": sym},
            )
            rows = cur.fetchall()

    assert len(rows) == 2
    assert int(rows[0]["available_qty"]) == 0
    assert int(rows[0]["qty"]) == 2
    assert int(rows[1]["available_qty"]) == 1
    assert int(rows[1]["qty"]) == 2


@pytest.mark.postgres
def test_process_order_reconcile_cancel_not_found(trading_db: str) -> None:
    missing = str(uuid4())
    assert process_order(missing) == {"processed": False, "reason": "order_not_found"}
    assert reconcile_order(missing) == {"reconciled": False, "reason": "order_not_found"}
    assert cancel_order(missing, "probe") == {"cancelled": False, "reason": "order_not_found"}


@pytest.mark.postgres
def test_cancel_order_cannot_cancel_terminal(trading_db: str) -> None:
    row = place_order(
        {
            "account_mode": "DEMO",
            "symbol": "TSTNOCAN",
            "side": "BUY",
            "quantity": 1,
            "price": 1000.0,
            "idempotency_key": "idem-tstnocan",
            "auto_process": True,
        }
    )
    assert row["status"] == "FILLED"
    out = cancel_order(row["id"], "should_fail")
    assert out["cancelled"] is False
    assert "cannot_cancel" in str(out.get("reason", ""))


@pytest.mark.postgres
def test_place_order_idempotent_returns_existing(trading_db: str) -> None:
    payload = {
        "account_mode": "DEMO",
        "symbol": "TSTIDEM",
        "side": "BUY",
        "quantity": 1,
        "price": 2000.0,
        "idempotency_key": "idem-tstidem-core",
        "auto_process": False,
    }
    first = place_order(payload)
    second = place_order(payload)
    assert first["id"] == second["id"]


@pytest.mark.postgres
def test_place_order_rejected_when_kill_switch_active(trading_db: str) -> None:
    set_kill_switch("DEMO", True, "unit_kill_probe")
    try:
        row = place_order(
            {
                "account_mode": "DEMO",
                "symbol": "TSTKILL",
                "side": "BUY",
                "quantity": 1,
                "price": 1000.0,
                "idempotency_key": "idem-tstkill",
                "auto_process": False,
            }
        )
        assert row.get("id") is None
        assert row["status"] == "REJECTED"
        assert row["reason"] == "kill_switch_active"
    finally:
        set_kill_switch("DEMO", False, None)
