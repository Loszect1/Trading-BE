from __future__ import annotations

import logging
import threading
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.execution import get_execution_adapter
from app.services.execution.broker_status import build_dnse_reconcile_metadata_snapshot
from app.services.execution.types import OrderExecutionContext
from app.services.vn_market_holiday_calendar import add_vn_trading_days_live, vn_market_local_today

logger = logging.getLogger(__name__)
_trading_core_tables_ready = False
_trading_core_tables_lock = threading.Lock()


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _settlement_effective_today() -> date:
    """Calendar 'today' for VN settlement roll and BUY trade-date; monkeypatch in tests."""
    return vn_market_local_today()


def ensure_trading_core_tables() -> None:
    global _trading_core_tables_ready
    if _trading_core_tables_ready:
        return
    with _trading_core_tables_lock:
        if _trading_core_tables_ready:
            return
        query = """
    CREATE TABLE IF NOT EXISTS orders_core (
        id UUID PRIMARY KEY,
        account_mode VARCHAR(10) NOT NULL CHECK (account_mode IN ('REAL', 'DEMO')),
        symbol VARCHAR(20) NOT NULL,
        side VARCHAR(10) NOT NULL CHECK (side IN ('BUY', 'SELL')),
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        price DOUBLE PRECISION NOT NULL CHECK (price > 0),
        status VARCHAR(20) NOT NULL,
        reason TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS position_lots (
        id UUID PRIMARY KEY,
        account_mode VARCHAR(10) NOT NULL CHECK (account_mode IN ('REAL', 'DEMO')),
        symbol VARCHAR(20) NOT NULL,
        buy_order_id UUID NOT NULL REFERENCES orders_core(id),
        buy_trade_date DATE NOT NULL,
        settle_date DATE NOT NULL,
        qty INTEGER NOT NULL CHECK (qty > 0),
        available_qty INTEGER NOT NULL CHECK (available_qty >= 0),
        avg_price DOUBLE PRECISION NOT NULL CHECK (avg_price > 0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS risk_events (
        id UUID PRIMARY KEY,
        account_mode VARCHAR(10) NOT NULL CHECK (account_mode IN ('REAL', 'DEMO')),
        symbol VARCHAR(20),
        event_type VARCHAR(50) NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
        with connect(settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                cur.execute("ALTER TABLE orders_core ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128);")
                cur.execute(
                    "ALTER TABLE orders_core ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();"
                )
                cur.execute("ALTER TABLE orders_core ADD COLUMN IF NOT EXISTS broker_order_id VARCHAR(128);")
                cur.execute(
                    "ALTER TABLE orders_core ADD COLUMN IF NOT EXISTS order_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;"
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_orders_core_broker_order_id
                    ON orders_core(broker_order_id)
                    WHERE broker_order_id IS NOT NULL;
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_core_idempotency_key
                    ON orders_core(idempotency_key)
                    WHERE idempotency_key IS NOT NULL;
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS order_events (
                        id UUID PRIMARY KEY,
                        order_id UUID NOT NULL REFERENCES orders_core(id) ON DELETE CASCADE,
                        status VARCHAR(20) NOT NULL,
                        message TEXT,
                        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trading_kill_switch (
                        account_mode VARCHAR(10) PRIMARY KEY CHECK (account_mode IN ('REAL', 'DEMO')),
                        active BOOLEAN NOT NULL DEFAULT FALSE,
                        reason TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
                cur.execute(
                    "INSERT INTO trading_kill_switch (account_mode, active, reason) VALUES ('REAL', FALSE, NULL) "
                    "ON CONFLICT (account_mode) DO NOTHING;"
                )
                cur.execute(
                    "INSERT INTO trading_kill_switch (account_mode, active, reason) VALUES ('DEMO', FALSE, NULL) "
                    "ON CONFLICT (account_mode) DO NOTHING;"
                )
            conn.commit()
        _trading_core_tables_ready = True


def _append_order_event(conn, order_id: str, status: str, message: str, payload: dict[str, Any] | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO order_events (id, order_id, status, message, payload, created_at)
            VALUES (%(id)s, %(order_id)s::uuid, %(status)s, %(message)s, %(payload)s, NOW())
            """,
            {
                "id": uuid4(),
                "order_id": order_id,
                "status": status,
                "message": message,
                "payload": Json(payload or {}),
            },
        )


def _set_order_status(conn, order_id: str, status: str, reason: str | None = None) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            UPDATE orders_core
            SET status = %(status)s, reason = COALESCE(%(reason)s, reason), updated_at = NOW()
            WHERE id = %(order_id)s::uuid
            RETURNING id::text AS id, account_mode, symbol, side, quantity, price, status, reason, idempotency_key,
                broker_order_id, order_metadata, created_at, updated_at
            """,
            {"order_id": order_id, "status": status, "reason": reason},
        )
        row = cur.fetchone()
    return dict(row or {})


def _trim_broker_payload(raw: dict[str, Any], limit: int = 48) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for i, (k, v) in enumerate(raw.items()):
        if i >= limit:
            out["_truncated"] = True
            break
        key = str(k)
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[key] = v
        else:
            out[key] = str(v)[:500]
    return out


def _persist_broker_order_id(conn, order_id: str, broker_order_id: str | None) -> None:
    if not broker_order_id:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders_core
            SET broker_order_id = %(broker_order_id)s, updated_at = NOW()
            WHERE id = %(order_id)s::uuid AND (broker_order_id IS NULL OR broker_order_id = %(broker_order_id)s)
            """,
            {"order_id": order_id, "broker_order_id": broker_order_id},
        )


def _merge_order_metadata(conn, order_id: str, patch: dict[str, Any]) -> None:
    if not patch:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders_core
            SET order_metadata = order_metadata || %(patch)s::jsonb, updated_at = NOW()
            WHERE id = %(order_id)s::uuid
            """,
            {"order_id": order_id, "patch": Json(patch)},
        )


def _roll_settlement(conn, account_mode: str) -> None:
    today = _settlement_effective_today()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE position_lots
            SET available_qty = qty, updated_at = NOW()
            WHERE account_mode = %(account_mode)s AND settle_date <= %(today)s AND available_qty < qty
            """,
            {"account_mode": account_mode, "today": today},
        )


def evaluate_risk(payload: dict[str, Any]) -> dict[str, Any]:
    stoploss = float(payload["stoploss_price"])
    entry = float(payload["entry_price"])
    nav = float(payload["nav"])
    risk_per_trade = float(payload["risk_per_trade"])
    daily_new_orders = int(payload["daily_new_orders"])
    max_daily_new_orders = int(payload["max_daily_new_orders"])
    if max_daily_new_orders > 0 and daily_new_orders >= max_daily_new_orders:
        return {
            "pass": False,
            "reason": "max_daily_new_orders_reached",
            "suggested_size": 0,
        }
    distance = abs(entry - stoploss)
    if distance <= 0:
        return {"pass": False, "reason": "invalid_stoploss_distance", "suggested_size": 0}
    suggested_size = int((nav * risk_per_trade) / distance)
    return {"pass": suggested_size > 0, "reason": "ok" if suggested_size > 0 else "size_too_small", "suggested_size": max(0, suggested_size)}


def check_settlement(account_mode: str, symbol: str, quantity: int) -> dict[str, Any]:
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _roll_settlement(conn, account_mode)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(available_qty), 0) AS available_qty, COALESCE(SUM(qty - available_qty), 0) AS pending_settlement_qty
                FROM position_lots
                WHERE account_mode = %(account_mode)s AND symbol = %(symbol)s AND qty > 0
                """,
                {"account_mode": account_mode, "symbol": symbol},
            )
            row = cur.fetchone() or {}
    available_qty = int(row.get("available_qty", 0))
    pending_qty = int(row.get("pending_settlement_qty", 0))
    return {"pass": available_qty >= quantity, "available_qty": available_qty, "pending_settlement_qty": pending_qty}


def get_kill_switch(account_mode: str) -> dict[str, Any]:
    ensure_trading_core_tables()
    mode = str(account_mode).upper()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT account_mode, active, reason, updated_at
                FROM trading_kill_switch
                WHERE account_mode = %(account_mode)s
                """,
                {"account_mode": mode},
            )
            row = cur.fetchone()
    return dict(row or {"account_mode": mode, "active": False, "reason": None, "updated_at": None})


def set_kill_switch(account_mode: str, active: bool, reason: str | None = None) -> dict[str, Any]:
    ensure_trading_core_tables()
    mode = str(account_mode).upper()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trading_kill_switch (account_mode, active, reason, updated_at)
                VALUES (%(account_mode)s, %(active)s, %(reason)s, NOW())
                ON CONFLICT (account_mode) DO UPDATE SET
                    active = EXCLUDED.active,
                    reason = EXCLUDED.reason,
                    updated_at = NOW()
                RETURNING account_mode, active, reason, updated_at
                """,
                {"account_mode": mode, "active": bool(active), "reason": reason},
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row or {})


def is_kill_switch_active(account_mode: str) -> bool:
    row = get_kill_switch(account_mode)
    return bool(row.get("active"))


def log_risk_event(
    account_mode: str,
    event_type: str,
    *,
    symbol: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_trading_core_tables()
    event_id = uuid4()
    mode = str(account_mode).upper()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO risk_events (id, account_mode, symbol, event_type, payload, created_at)
                VALUES (%(id)s, %(account_mode)s, %(symbol)s, %(event_type)s, %(payload)s, NOW())
                RETURNING id::text AS id, account_mode, symbol, event_type, payload, created_at
                """,
                {
                    "id": event_id,
                    "account_mode": mode,
                    "symbol": symbol.upper() if symbol else None,
                    "event_type": event_type,
                    "payload": Json(payload or {}),
                },
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row or {})


def list_risk_events(
    account_mode: str | None = None,
    *,
    limit: int = 100,
    event_type: str | None = None,
) -> list[dict[str, Any]]:
    ensure_trading_core_tables()
    safe_limit = max(1, min(limit, 500))
    filters: list[str] = []
    params: dict[str, Any] = {"limit": safe_limit}
    if account_mode:
        filters.append("account_mode = %(account_mode)s")
        params["account_mode"] = str(account_mode).upper()
    if event_type:
        filters.append("event_type = %(event_type)s")
        params["event_type"] = event_type
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
        SELECT id::text AS id, account_mode, symbol, event_type, payload, created_at
        FROM risk_events
        {where}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def place_order(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_trading_core_tables()
    account_mode = str(payload["account_mode"]).upper()
    symbol = str(payload["symbol"]).upper()
    side = str(payload["side"]).upper()
    quantity = int(payload["quantity"])
    price = float(payload["price"])
    idempotency_key = (payload.get("idempotency_key") or "").strip() or None
    auto_process = bool(payload.get("auto_process", True))
    order_metadata = payload.get("metadata") or {}
    if not isinstance(order_metadata, dict):
        order_metadata = {}
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if idempotency_key:
                cur.execute(
                    """
                    SELECT id::text AS id, account_mode, symbol, side, quantity, price, status, reason, idempotency_key,
                        broker_order_id, order_metadata, created_at, updated_at
                    FROM orders_core WHERE idempotency_key = %(idempotency_key)s
                    """,
                    {"idempotency_key": idempotency_key},
                )
                existing = cur.fetchone()
                if existing:
                    return dict(existing)
        if is_kill_switch_active(account_mode):
            return {
                "id": None,
                "account_mode": account_mode,
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": price,
                "status": "REJECTED",
                "reason": "kill_switch_active",
                "idempotency_key": idempotency_key,
                "created_at": _utc_now(),
                "updated_at": _utc_now(),
            }
        with conn.cursor() as cur:
            order_id = uuid4()
            cur.execute(
                """
                INSERT INTO orders_core (
                    id, account_mode, symbol, side, quantity, price, status, reason, idempotency_key, order_metadata, created_at, updated_at
                ) VALUES (
                    %(id)s, %(account_mode)s, %(symbol)s, %(side)s, %(quantity)s, %(price)s, 'NEW', NULL, %(idempotency_key)s,
                    %(order_metadata)s::jsonb, NOW(), NOW()
                )
                RETURNING id::text AS id, account_mode, symbol, side, quantity, price, status, reason, idempotency_key,
                    broker_order_id, order_metadata, created_at, updated_at
                """,
                {
                    "id": order_id,
                    "account_mode": account_mode,
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "price": price,
                    "idempotency_key": idempotency_key,
                    "order_metadata": Json(order_metadata),
                },
            )
            created = cur.fetchone()
            _append_order_event(conn, str(order_id), "NEW", "Order created", {"symbol": symbol, "side": side})
        conn.commit()
    row = dict(created or {})
    if auto_process and row.get("id"):
        return process_order(row["id"])
    return row


def process_order(order_id: str) -> dict[str, Any]:
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, account_mode, symbol, side, quantity, price, status, reason, idempotency_key,
                    broker_order_id, order_metadata
                FROM orders_core WHERE id = %(order_id)s::uuid
                """,
                {"order_id": order_id},
            )
            order = cur.fetchone()
        if not order:
            return {"processed": False, "reason": "order_not_found"}
        st = str(order["status"]).upper()
        if st in {"FILLED", "REJECTED", "CANCELLED"}:
            return {"processed": False, "reason": f"already_{st.lower()}", "order": dict(order)}

        account_mode = str(order["account_mode"])
        symbol = str(order["symbol"])
        side = str(order["side"])
        quantity = int(order["quantity"])
        price = float(order["price"])
        idem = order.get("idempotency_key")
        idem = str(idem) if idem is not None else None
        broker_id_row = order.get("broker_order_id")
        broker_id_row = str(broker_id_row).strip() if broker_id_row else None
        meta = order.get("order_metadata") or {}
        if not isinstance(meta, dict):
            meta = {}

        adapter = get_execution_adapter(account_mode)
        resume = st in {"ACK", "PARTIAL"} and broker_id_row

        if st == "NEW":
            _set_order_status(conn, order_id, "SENT")
            _append_order_event(
                conn,
                order_id,
                "SENT",
                "Order sent to execution adapter",
                {"adapter": getattr(adapter, "name", type(adapter).__name__)},
            )
        elif st == "SENT" and not broker_id_row and not resume:
            _append_order_event(
                conn,
                order_id,
                "SENT",
                "Execution retry after prior SENT without broker id",
                {"adapter": getattr(adapter, "name", type(adapter).__name__)},
            )

        ctx = OrderExecutionContext(
            internal_order_id=order_id,
            account_mode=account_mode,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            idempotency_key=idem,
            broker_order_id=broker_id_row,
            order_metadata=meta,
            current_status=st,
        )

        try:
            outcome = adapter.execute(ctx)
        except Exception as exc:
            logger.exception("Execution adapter crashed order_id=%s", order_id)
            if account_mode == "REAL":
                log_risk_event(
                    account_mode,
                    "execution_adapter_exception",
                    symbol=symbol,
                    payload={"order_id": order_id, "error": str(exc)},
                )
            final_order = _set_order_status(conn, order_id, "REJECTED", "execution_adapter_exception")
            _append_order_event(
                conn,
                order_id,
                "REJECTED",
                "Execution adapter raised",
                {"error": str(exc)},
            )
            conn.commit()
            return {"processed": True, "order": final_order}

        trimmed = _trim_broker_payload(outcome.broker_raw or {})

        if outcome.internal_status == "REJECTED":
            reason = outcome.reason or "execution_rejected"
            if outcome.broker_order_id:
                _persist_broker_order_id(conn, order_id, outcome.broker_order_id)
            final_order = _set_order_status(conn, order_id, "REJECTED", reason)
            if account_mode == "REAL" and getattr(adapter, "name", "") == "dnse_live":
                log_risk_event(
                    account_mode,
                    "execution_dnse_reject",
                    symbol=symbol,
                    payload={
                        "order_id": order_id,
                        "reason": reason,
                        "broker_order_id": outcome.broker_order_id,
                        "messages": outcome.log_messages,
                    },
                )
            _append_order_event(
                conn,
                order_id,
                "REJECTED",
                "Broker or adapter rejected order",
                {"reason": reason, "broker": trimmed, "messages": outcome.log_messages},
            )
            conn.commit()
            return {"processed": True, "order": final_order}

        if outcome.internal_status == "CANCELLED":
            if outcome.broker_order_id:
                _persist_broker_order_id(conn, order_id, outcome.broker_order_id)
            final_order = _set_order_status(conn, order_id, "CANCELLED", outcome.reason)
            _append_order_event(
                conn,
                order_id,
                "CANCELLED",
                "Broker reported cancelled",
                {"broker": trimmed, "messages": outcome.log_messages},
            )
            conn.commit()
            return {"processed": True, "order": final_order}

        if outcome.internal_status == "PARTIAL":
            if outcome.broker_order_id:
                _persist_broker_order_id(conn, order_id, outcome.broker_order_id)
            snap = build_dnse_reconcile_metadata_snapshot(outcome.broker_raw or {})
            _merge_order_metadata(
                conn,
                order_id,
                {
                    "dnse_last_reconcile": {
                        "at": _utc_now().isoformat(),
                        "path": "partial_or_poll",
                        **snap,
                    }
                },
            )
            _set_order_status(conn, order_id, "ACK")
            _append_order_event(
                conn,
                order_id,
                "ACK",
                "Order acknowledged (partial fill at broker)",
                {"broker": trimmed, "messages": outcome.log_messages},
            )
            final_order = _set_order_status(conn, order_id, "PARTIAL", outcome.reason)
            _append_order_event(
                conn,
                order_id,
                "PARTIAL",
                "Partial fill; settlement not applied until fully filled",
                {"broker": trimmed, "messages": outcome.log_messages},
            )
            conn.commit()
            return {"processed": True, "order": final_order}

        if outcome.internal_status != "FILLED":
            if outcome.broker_order_id:
                _persist_broker_order_id(conn, order_id, outcome.broker_order_id)
            msg = f"Unexpected adapter status: {outcome.internal_status}"
            final_order = _set_order_status(conn, order_id, "REJECTED", "execution_unexpected_status")
            _append_order_event(conn, order_id, "REJECTED", msg, {"broker": trimmed})
            conn.commit()
            return {"processed": True, "order": final_order}

        if outcome.broker_order_id:
            _persist_broker_order_id(conn, order_id, outcome.broker_order_id)
        _set_order_status(conn, order_id, "ACK")
        _append_order_event(
            conn,
            order_id,
            "ACK",
            "Order acknowledged",
            {"broker": trimmed, "messages": outcome.log_messages},
        )

        _roll_settlement(conn, account_mode)

        final_status = "FILLED"
        final_reason = None
        if side == "SELL":
            settlement_result = check_settlement(account_mode, symbol, quantity)
            if not settlement_result["pass"]:
                final_status = "REJECTED"
                final_reason = "settlement_guard_failed"
            else:
                remaining = quantity
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, available_qty FROM position_lots
                        WHERE account_mode = %(account_mode)s AND symbol = %(symbol)s AND available_qty > 0
                        ORDER BY buy_trade_date ASC, created_at ASC
                        """,
                        {"account_mode": account_mode, "symbol": symbol},
                    )
                    lots = cur.fetchall()
                    for lot in lots:
                        if remaining <= 0:
                            break
                        consume = min(int(lot["available_qty"]), remaining)
                        cur.execute(
                            "UPDATE position_lots SET available_qty = available_qty - %(consume)s, updated_at = NOW() WHERE id = %(lot_id)s",
                            {"consume": consume, "lot_id": lot["id"]},
                        )
                        remaining -= consume
                if remaining > 0:
                    final_status = "REJECTED"
                    final_reason = "sell_fifo_consume_failed"
        else:
            buy_trade_date = _settlement_effective_today()
            settle_date = add_vn_trading_days_live(buy_trade_date, 2)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO position_lots (
                        id, account_mode, symbol, buy_order_id, buy_trade_date, settle_date, qty, available_qty, avg_price, created_at, updated_at
                    ) VALUES (
                        %(id)s, %(account_mode)s, %(symbol)s, %(buy_order_id)s, %(buy_trade_date)s, %(settle_date)s, %(qty)s, %(available_qty)s, %(avg_price)s, NOW(), NOW()
                    )
                    """,
                    {
                        "id": uuid4(),
                        "account_mode": account_mode,
                        "symbol": symbol,
                        "buy_order_id": order_id,
                        "buy_trade_date": buy_trade_date,
                        "settle_date": settle_date,
                        "qty": quantity,
                        "available_qty": 0,
                        "avg_price": price,
                    },
                )

        final_order = _set_order_status(conn, order_id, final_status, final_reason)
        _append_order_event(
            conn,
            order_id,
            final_status,
            "Order processed",
            {"reason": final_reason, "broker": trimmed},
        )
        conn.commit()
    return {"processed": True, "order": final_order}


def get_positions(account_mode: str) -> list[dict[str, Any]]:
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _roll_settlement(conn, account_mode)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, SUM(qty)::int AS total_qty, SUM(available_qty)::int AS available_qty, SUM(qty - available_qty)::int AS pending_settlement_qty, AVG(avg_price)::float8 AS avg_price
                FROM position_lots
                WHERE account_mode = %(account_mode)s
                GROUP BY symbol
                HAVING SUM(qty) > 0
                ORDER BY symbol
                """,
                {"account_mode": account_mode},
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_settlement_rows(account_mode: str, symbol: str | None = None) -> list[dict[str, Any]]:
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _roll_settlement(conn, account_mode)
        with conn.cursor() as cur:
            if symbol:
                cur.execute(
                    """
                    SELECT symbol, buy_trade_date, settle_date, qty, available_qty, (qty - available_qty) AS pending_settlement_qty, avg_price
                    FROM position_lots
                    WHERE account_mode = %(account_mode)s AND symbol = %(symbol)s
                    ORDER BY buy_trade_date DESC, created_at DESC
                    """,
                    {"account_mode": account_mode, "symbol": symbol},
                )
            else:
                cur.execute(
                    """
                    SELECT symbol, buy_trade_date, settle_date, qty, available_qty, (qty - available_qty) AS pending_settlement_qty, avg_price
                    FROM position_lots
                    WHERE account_mode = %(account_mode)s
                    ORDER BY buy_trade_date DESC, created_at DESC
                    """,
                    {"account_mode": account_mode},
                )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_portfolio_summary(account_mode: str) -> dict[str, Any]:
    positions = get_positions(account_mode)
    return {
        "account_mode": account_mode,
        "total_symbols": len(positions),
        "total_qty": sum(int(p["total_qty"]) for p in positions),
        "total_available_qty": sum(int(p["available_qty"]) for p in positions),
        "total_pending_settlement_qty": sum(int(p["pending_settlement_qty"]) for p in positions),
    }


def list_orders(account_mode: str, limit: int = 100) -> list[dict[str, Any]]:
    ensure_trading_core_tables()
    safe_limit = max(1, min(limit, 500))
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, account_mode, symbol, side, quantity, price, status, reason, idempotency_key,
                    broker_order_id, order_metadata, created_at, updated_at
                FROM orders_core
                WHERE account_mode = %(account_mode)s
                ORDER BY created_at DESC
                LIMIT %(limit)s
                """,
                {"account_mode": account_mode, "limit": safe_limit},
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


def cancel_order(order_id: str, reason: str) -> dict[str, Any]:
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, account_mode, symbol, side, quantity, price, status, broker_order_id,
                    idempotency_key, order_metadata
                FROM orders_core WHERE id = %(order_id)s::uuid
                """,
                {"order_id": order_id},
            )
            row = cur.fetchone()
            if not row:
                return {"cancelled": False, "reason": "order_not_found"}
            st = str(row["status"]).upper()
            if st in {"FILLED", "REJECTED", "CANCELLED"}:
                return {"cancelled": False, "reason": f"cannot_cancel_{str(row['status']).lower()}"}

            account_mode = str(row["account_mode"])
            broker_id_row = row.get("broker_order_id")
            broker_id_row = str(broker_id_row).strip() if broker_id_row else None
            meta = row.get("order_metadata") or {}
            if not isinstance(meta, dict):
                meta = {}

            adapter = get_execution_adapter(account_mode)
            cancel_fn = getattr(adapter, "cancel_at_broker", None)
            need_broker_cancel = (
                account_mode == "REAL"
                and getattr(adapter, "name", "") == "dnse_live"
                and bool(broker_id_row)
                and callable(cancel_fn)
                and st in {"SENT", "ACK", "PARTIAL"}
            )

            broker_trimmed: dict[str, Any] = {}
            if need_broker_cancel and cancel_fn is not None:
                idem = row.get("idempotency_key")
                idem_s = str(idem) if idem is not None else None
                ctx = OrderExecutionContext(
                    internal_order_id=str(row["id"]),
                    account_mode=account_mode,
                    symbol=str(row["symbol"]),
                    side=str(row["side"]),
                    quantity=int(row["quantity"]),
                    price=float(row["price"]),
                    idempotency_key=idem_s,
                    broker_order_id=broker_id_row,
                    order_metadata=meta,
                    current_status=st,
                )
                outcome = cancel_fn(ctx)
                if outcome.internal_status != "CANCELLED":
                    if account_mode == "REAL":
                        log_risk_event(
                            account_mode,
                            "execution_dnse_cancel_failed",
                            symbol=str(row["symbol"]),
                            payload={
                                "order_id": order_id,
                                "reason": outcome.reason,
                                "broker_order_id": broker_id_row,
                                "messages": outcome.log_messages,
                            },
                        )
                    return {
                        "cancelled": False,
                        "reason": outcome.reason or "broker_cancel_failed",
                        "broker_cancel_attempted": True,
                        "messages": outcome.log_messages,
                        "order": dict(row),
                    }
                broker_trimmed = _trim_broker_payload(outcome.broker_raw or {})

            updated = _set_order_status(conn, order_id, "CANCELLED", reason)
            payload: dict[str, Any] = {"reason": reason}
            if broker_trimmed:
                payload["broker_cancel"] = True
                payload["broker"] = broker_trimmed
            _append_order_event(conn, order_id, "CANCELLED", "Order cancelled", payload)
        conn.commit()
    return {"cancelled": True, "order": dict(updated or {})}


def reconcile_order(order_id: str) -> dict[str, Any]:
    """Poll broker state for open / partially filled REAL orders (primarily dnse_live)."""
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id::text AS id, status FROM orders_core WHERE id = %(order_id)s::uuid",
                {"order_id": order_id},
            )
            order = cur.fetchone()
    if not order:
        return {"reconciled": False, "reason": "order_not_found"}
    st = str(order["status"]).upper()
    if st not in {"NEW", "SENT", "ACK", "PARTIAL"}:
        return {"reconciled": False, "reason": f"not_eligible_{st.lower()}", "order": dict(order)}
    result = process_order(order_id)
    return {**result, "reconcile": True}


def get_order_events(order_id: str) -> list[dict[str, Any]]:
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, order_id::text AS order_id, status, message, payload, created_at
                FROM order_events
                WHERE order_id = %(order_id)s::uuid
                ORDER BY created_at ASC
                """,
                {"order_id": order_id},
            )
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_monitoring_summary(account_mode: str) -> dict[str, Any]:
    portfolio = get_portfolio_summary(account_mode)
    ensure_trading_core_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COUNT(*)::int AS count
                FROM orders_core
                WHERE account_mode = %(account_mode)s
                GROUP BY status
                """,
                {"account_mode": account_mode},
            )
            status_rows = cur.fetchall()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int AS c
                FROM risk_events
                WHERE account_mode = %(account_mode)s
                  AND created_at >= NOW() - INTERVAL '7 days'
                """,
                {"account_mode": account_mode},
            )
            risk_row = cur.fetchone() or {}
    orders_by_status = {str(r["status"]): int(r["count"]) for r in status_rows}
    return {
        "account_mode": account_mode,
        "portfolio": portfolio,
        "orders_by_status": orders_by_status,
        "risk_events_last_7_days": int(risk_row.get("c", 0)),
        "generated_at": _utc_now().isoformat(),
    }
