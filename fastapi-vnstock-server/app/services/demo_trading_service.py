from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from psycopg import connect
from psycopg.rows import dict_row

from app.core.config import settings
from app.schemas.auto_trading import DEMO_INITIAL_BALANCE_VND, DemoTradeRequest


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


_SESSION_ID_PATTERN = re.compile(r"^[\w\-]{1,128}$")


def normalize_demo_session_id(raw: str | None) -> str:
    """Return a safe session key; invalid input falls back to 'default'."""
    if raw is None:
        return "default"
    candidate = raw.strip()
    if not candidate or not _SESSION_ID_PATTERN.fullmatch(candidate):
        return "default"
    return candidate


@dataclass
class _OpenLot:
    id: str
    quantity: int
    unit_cost: float
    opened_at: datetime


@dataclass
class _DemoPosition:
    symbol: str
    lots: list[_OpenLot]
    opened_at: datetime

    @property
    def quantity(self) -> int:
        return sum(lot.quantity for lot in self.lots)

    @property
    def average_cost(self) -> float:
        qty = self.quantity
        if qty <= 0:
            return 0.0
        return sum(lot.quantity * lot.unit_cost for lot in self.lots) / qty


@dataclass(frozen=True)
class DemoTradeExecutionResult:
    trade_id: str
    session_id: str
    side: Literal["BUY", "SELL"]
    symbol: str
    quantity: int
    price: float
    cash_after: float
    position_snapshot: dict[str, Any] | None
    realized_pnl_on_trade: float
    cumulative_realized_pnl: float
    experience_candidate: dict[str, Any] | None


def _create_demo_session_id() -> str:
    return f"demo-{uuid4()}"


def ensure_demo_trading_tables() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS demo_sessions (
        session_id VARCHAR(128) PRIMARY KEY,
        initial_balance DOUBLE PRECISION NOT NULL CHECK (initial_balance >= 0),
        cash_balance DOUBLE PRECISION NOT NULL CHECK (cash_balance >= 0),
        realized_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS demo_positions (
        session_id VARCHAR(128) NOT NULL REFERENCES demo_sessions(session_id) ON DELETE CASCADE,
        symbol VARCHAR(20) NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity >= 0),
        average_cost DOUBLE PRECISION NOT NULL CHECK (average_cost >= 0),
        opened_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (session_id, symbol)
    );

    CREATE TABLE IF NOT EXISTS demo_open_lots (
        id UUID PRIMARY KEY,
        session_id VARCHAR(128) NOT NULL REFERENCES demo_sessions(session_id) ON DELETE CASCADE,
        symbol VARCHAR(20) NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_cost DOUBLE PRECISION NOT NULL CHECK (unit_cost > 0),
        opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_demo_open_lots_session_symbol_opened
        ON demo_open_lots (session_id, symbol, opened_at ASC, created_at ASC);

    CREATE TABLE IF NOT EXISTS demo_trades (
        id UUID PRIMARY KEY,
        trade_id VARCHAR(140) NOT NULL UNIQUE,
        session_id VARCHAR(128) NOT NULL REFERENCES demo_sessions(session_id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        side VARCHAR(10) NOT NULL CHECK (side IN ('BUY', 'SELL')),
        symbol VARCHAR(20) NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        price DOUBLE PRECISION NOT NULL CHECK (price > 0),
        notional DOUBLE PRECISION NOT NULL CHECK (notional >= 0),
        realized_pnl_on_trade DOUBLE PRECISION NOT NULL,
        cash_after DOUBLE PRECISION NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_demo_trades_session_created
        ON demo_trades (session_id, created_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def create_demo_session() -> str:
    ensure_demo_trading_tables()
    session_id = _create_demo_session_id()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO demo_sessions (session_id, initial_balance, cash_balance, realized_pnl, created_at, updated_at)
                VALUES (%(session_id)s, %(initial_balance)s, %(cash_balance)s, 0, NOW(), NOW())
                """,
                {
                    "session_id": session_id,
                    "initial_balance": float(DEMO_INITIAL_BALANCE_VND),
                    "cash_balance": float(DEMO_INITIAL_BALANCE_VND),
                },
            )
        conn.commit()
    return session_id


def list_demo_sessions(*, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    ensure_demo_trading_tables()
    safe_limit = max(1, min(limit, 200))
    safe_offset = max(0, offset)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT COUNT(*)::int AS c FROM demo_sessions")
            total_row = cur.fetchone() or {}
            cur.execute(
                """
                SELECT session_id, initial_balance, cash_balance, realized_pnl, created_at, updated_at
                FROM demo_sessions
                ORDER BY created_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                {"limit": safe_limit, "offset": safe_offset},
            )
            rows = cur.fetchall()
        conn.commit()
    return {
        "items": [dict(row) for row in rows],
        "total": int(total_row.get("c", 0)),
        "limit": safe_limit,
        "offset": safe_offset,
    }


def delete_demo_session(session_id: str) -> bool:
    """
    Delete one demo session and all related rows across DB tables that reference this
    demo session id explicitly or through known JSON metadata.
    Core demo tables (sessions/positions/lots/trades) are removed via FK cascade.
    Returns True when a session row was deleted, otherwise False.
    """
    ensure_demo_trading_tables()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS automation_scheduler_demo_context (
                    id SMALLINT PRIMARY KEY CHECK (id = 1),
                    demo_session_id VARCHAR(128) NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            # Clear scheduler context if it points to the session being deleted.
            cur.execute(
                """
                UPDATE automation_scheduler_demo_context
                SET demo_session_id = NULL, updated_at = NOW()
                WHERE id = 1 AND demo_session_id = %(session_id)s
                """,
                {"session_id": session_id},
            )

            # Generic cleanup for any table that has an explicit demo_session_id column.
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.columns
                WHERE column_name = 'demo_session_id'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                """,
            )
            ref_tables = cur.fetchall() or []
            for table_schema, table_name in ref_tables:
                if not table_schema or not table_name:
                    continue
                safe_schema = str(table_schema).replace('"', '""')
                safe_table = str(table_name).replace('"', '""')
                cur.execute(
                    f'DELETE FROM "{safe_schema}"."{safe_table}" WHERE demo_session_id = %(session_id)s',
                    {"session_id": session_id},
                )

            # Related rows where session id is stored in JSON payloads.
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'short_term_automation_runs'
                )
                """,
            )
            has_short_term_runs = bool((cur.fetchone() or [False])[0])
            if has_short_term_runs:
                cur.execute(
                    """
                    DELETE FROM short_term_automation_runs
                    WHERE detail->>'demo_session_id' = %(session_id)s
                    """,
                    {"session_id": session_id},
                )
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'experience'
                )
                """,
            )
            has_experience = bool((cur.fetchone() or [False])[0])
            if has_experience:
                cur.execute(
                    """
                    DELETE FROM experience
                    WHERE account_mode = 'DEMO'
                      AND (
                        market_context->>'demo_session_id' = %(session_id)s
                        OR trade_id LIKE %(trade_prefix)s
                      )
                    """,
                    {"session_id": session_id, "trade_prefix": f"demo-{session_id}-%"},
                )

            # Delete root session row last; cascades demo_positions/open_lots/trades.
            cur.execute(
                """
                DELETE FROM demo_sessions
                WHERE session_id = %(session_id)s
                """,
                {"session_id": session_id},
            )
            deleted = int(cur.rowcount or 0) > 0
        conn.commit()
    return deleted


def _ensure_demo_session_exists(conn: Any, session_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO demo_sessions (session_id, initial_balance, cash_balance, realized_pnl, created_at, updated_at)
            VALUES (%(session_id)s, %(initial_balance)s, %(cash_balance)s, 0, NOW(), NOW())
            ON CONFLICT (session_id) DO NOTHING
            """,
            {
                "session_id": session_id,
                "initial_balance": float(DEMO_INITIAL_BALANCE_VND),
                "cash_balance": float(DEMO_INITIAL_BALANCE_VND),
            },
        )


def _next_trade_id(conn: Any, session_id: str) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*)::int FROM demo_trades WHERE session_id = %(session_id)s", {"session_id": session_id})
        count = int((cur.fetchone() or [0])[0])
    safe_session = session_id.replace("/", "_")
    return f"demo-{safe_session}-{count + 1}"


def _load_position_with_lots(conn: Any, session_id: str, symbol: str) -> _DemoPosition | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT symbol, quantity, average_cost, opened_at
            FROM demo_positions
            WHERE session_id = %(session_id)s AND symbol = %(symbol)s
            """,
            {"session_id": session_id, "symbol": symbol},
        )
        row = cur.fetchone()
        if not row:
            return None
        cur.execute(
            """
            SELECT id::text AS id, quantity, unit_cost, opened_at
            FROM demo_open_lots
            WHERE session_id = %(session_id)s AND symbol = %(symbol)s
            ORDER BY opened_at ASC, created_at ASC
            """,
            {"session_id": session_id, "symbol": symbol},
        )
        lots_rows = cur.fetchall()
    lots = [
        _OpenLot(id=str(item["id"]), quantity=int(item["quantity"]), unit_cost=float(item["unit_cost"]), opened_at=item["opened_at"])
        for item in lots_rows
    ]
    return _DemoPosition(symbol=str(row["symbol"]), lots=lots, opened_at=row["opened_at"])


def _upsert_position(conn: Any, session_id: str, position: _DemoPosition) -> None:
    qty = position.quantity
    avg = position.average_cost
    with conn.cursor() as cur:
        if qty <= 0:
            cur.execute(
                "DELETE FROM demo_positions WHERE session_id = %(session_id)s AND symbol = %(symbol)s",
                {"session_id": session_id, "symbol": position.symbol},
            )
            return
        cur.execute(
            """
            INSERT INTO demo_positions (session_id, symbol, quantity, average_cost, opened_at, updated_at)
            VALUES (%(session_id)s, %(symbol)s, %(quantity)s, %(average_cost)s, %(opened_at)s, NOW())
            ON CONFLICT (session_id, symbol) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                average_cost = EXCLUDED.average_cost,
                opened_at = EXCLUDED.opened_at,
                updated_at = NOW()
            """,
            {
                "session_id": session_id,
                "symbol": position.symbol,
                "quantity": qty,
                "average_cost": avg,
                "opened_at": position.opened_at,
            },
        )


def _append_trade_history(
    conn: Any,
    *,
    trade_id: str,
    session_id: str,
    side: str,
    symbol: str,
    quantity: int,
    price: float,
    notional: float,
    realized_pnl_on_trade: float,
    cash_after: float,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO demo_trades (
                id, trade_id, session_id, created_at, side, symbol, quantity, price,
                notional, realized_pnl_on_trade, cash_after
            ) VALUES (
                %(id)s, %(trade_id)s, %(session_id)s, NOW(), %(side)s, %(symbol)s, %(quantity)s, %(price)s,
                %(notional)s, %(realized_pnl_on_trade)s, %(cash_after)s
            )
            """,
            {
                "id": uuid4(),
                "trade_id": trade_id,
                "session_id": session_id,
                "side": side,
                "symbol": symbol,
                "quantity": quantity,
                "price": price,
                "notional": notional,
                "realized_pnl_on_trade": realized_pnl_on_trade,
                "cash_after": cash_after,
            },
        )


def execute_demo_trade(session_id: str, body: DemoTradeRequest) -> DemoTradeExecutionResult:
    ensure_demo_trading_tables()
    symbol = body.symbol
    side = body.side
    qty = body.quantity
    price = body.price
    now = _utc_now()

    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, session_id)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT session_id, cash_balance, realized_pnl
                FROM demo_sessions
                WHERE session_id = %(session_id)s
                FOR UPDATE
                """,
                {"session_id": session_id},
            )
            session_row = cur.fetchone()
        if not session_row:
            raise ValueError("DEMO_SESSION_NOT_FOUND")
        cash_balance = float(session_row["cash_balance"])
        realized_pnl_total = float(session_row["realized_pnl"])
        trade_id = _next_trade_id(conn, session_id)

        if side == "BUY":
            gross = qty * price
            if gross > cash_balance + 1e-9:
                raise ValueError("INSUFFICIENT_CASH")

            position = _load_position_with_lots(conn, session_id, symbol)
            if position is None:
                position = _DemoPosition(symbol=symbol, lots=[], opened_at=now)
            if position.quantity == 0:
                position.opened_at = now
                position.lots.clear()
            position.lots.append(_OpenLot(id=str(uuid4()), quantity=qty, unit_cost=price, opened_at=now))

            new_cash = cash_balance - gross
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE demo_sessions
                    SET cash_balance = %(cash_balance)s, updated_at = NOW()
                    WHERE session_id = %(session_id)s
                    """,
                    {"cash_balance": new_cash, "session_id": session_id},
                )
                cur.execute(
                    """
                    INSERT INTO demo_open_lots (id, session_id, symbol, quantity, unit_cost, opened_at, created_at)
                    VALUES (%(id)s::uuid, %(session_id)s, %(symbol)s, %(quantity)s, %(unit_cost)s, %(opened_at)s, NOW())
                    """,
                    {
                        "id": position.lots[-1].id,
                        "session_id": session_id,
                        "symbol": symbol,
                        "quantity": qty,
                        "unit_cost": price,
                        "opened_at": now,
                    },
                )
            _upsert_position(conn, session_id, position)
            _append_trade_history(
                conn,
                trade_id=trade_id,
                session_id=session_id,
                side=side,
                symbol=symbol,
                quantity=qty,
                price=price,
                notional=gross,
                realized_pnl_on_trade=0.0,
                cash_after=new_cash,
            )
            conn.commit()

            snap = {
                "symbol": symbol,
                "quantity": position.quantity,
                "average_cost": position.average_cost,
                "opened_at": position.opened_at,
            }
            return DemoTradeExecutionResult(
                trade_id=trade_id,
                session_id=session_id,
                side=side,
                symbol=symbol,
                quantity=qty,
                price=price,
                cash_after=new_cash,
                position_snapshot=snap,
                realized_pnl_on_trade=0.0,
                cumulative_realized_pnl=realized_pnl_total,
                experience_candidate=None,
            )

        # SELL
        position = _load_position_with_lots(conn, session_id, symbol)
        if position is None or position.quantity < qty:
            raise ValueError("INSUFFICIENT_POSITION")

        opened_at = position.opened_at
        avg_before = position.average_cost
        remaining = qty
        realized_on_trade = 0.0
        for lot in position.lots:
            if remaining <= 0:
                break
            take = min(lot.quantity, remaining)
            realized_on_trade += (price - lot.unit_cost) * take
            lot.quantity -= take
            remaining -= take
        position.lots = [lot for lot in position.lots if lot.quantity > 0]
        if remaining > 0:
            raise ValueError("INSUFFICIENT_POSITION")

        new_cash = cash_balance + qty * price
        new_realized_total = realized_pnl_total + realized_on_trade
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE demo_sessions
                SET cash_balance = %(cash_balance)s, realized_pnl = %(realized_pnl)s, updated_at = NOW()
                WHERE session_id = %(session_id)s
                """,
                {
                    "cash_balance": new_cash,
                    "realized_pnl": new_realized_total,
                    "session_id": session_id,
                },
            )
            cur.execute(
                "DELETE FROM demo_open_lots WHERE session_id = %(session_id)s AND symbol = %(symbol)s",
                {"session_id": session_id, "symbol": symbol},
            )
            for lot in position.lots:
                cur.execute(
                    """
                    INSERT INTO demo_open_lots (id, session_id, symbol, quantity, unit_cost, opened_at, created_at)
                    VALUES (%(id)s::uuid, %(session_id)s, %(symbol)s, %(quantity)s, %(unit_cost)s, %(opened_at)s, NOW())
                    """,
                    {
                        "id": lot.id,
                        "session_id": session_id,
                        "symbol": symbol,
                        "quantity": lot.quantity,
                        "unit_cost": lot.unit_cost,
                        "opened_at": lot.opened_at,
                    },
                )
        _upsert_position(conn, session_id, position)
        _append_trade_history(
            conn,
            trade_id=trade_id,
            session_id=session_id,
            side=side,
            symbol=symbol,
            quantity=qty,
            price=price,
            notional=qty * price,
            realized_pnl_on_trade=realized_on_trade,
            cash_after=new_cash,
        )
        conn.commit()

        fully_closed = position.quantity == 0

        snap = None
        if not fully_closed:
            snap = {
                "symbol": symbol,
                "quantity": position.quantity,
                "average_cost": position.average_cost,
                "opened_at": position.opened_at,
            }

        experience_candidate: dict[str, Any] | None = None
        if fully_closed and realized_on_trade < 0:
            cost_basis = avg_before * qty if avg_before > 0 else 0.0
            pnl_pct = (realized_on_trade / cost_basis * 100.0) if cost_basis > 1e-9 else 0.0
            experience_candidate = {
                "trade_id": trade_id,
                "account_mode": "DEMO",
                "symbol": symbol,
                "strategy_type": body.strategy_type,
                "entry_time": opened_at,
                "exit_time": _utc_now(),
                "pnl_value": realized_on_trade,
                "pnl_percent": pnl_pct,
                "market_context": {
                    **(body.market_context or {}),
                    "demo_session_id": session_id,
                    "exit_price": price,
                    "quantity": qty,
                    "average_entry": avg_before,
                    "rr_realized": float(body.market_context.get("rr_realized", 0.0)),
                },
            }

        return DemoTradeExecutionResult(
            trade_id=trade_id,
            session_id=session_id,
            side=side,
            symbol=symbol,
            quantity=qty,
            price=price,
                cash_after=new_cash,
            position_snapshot=snap,
            realized_pnl_on_trade=realized_on_trade,
                cumulative_realized_pnl=new_realized_total,
            experience_candidate=experience_candidate,
        )


def get_demo_account_snapshot(
    session_id: str,
    mark_prices: dict[str, float] | None,
    *,
    history_limit: int = 50,
    history_offset: int = 0,
) -> dict[str, Any]:
    ensure_demo_trading_tables()
    marks = {k.strip().upper(): float(v) for k, v in (mark_prices or {}).items() if v is not None}
    safe_limit = max(1, min(history_limit, 200))
    safe_offset = max(0, history_offset)

    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, session_id)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT session_id, cash_balance, realized_pnl FROM demo_sessions WHERE session_id = %(session_id)s",
                {"session_id": session_id},
            )
            session_row = cur.fetchone()
            cur.execute(
                """
                SELECT symbol, quantity, average_cost, opened_at
                FROM demo_positions
                WHERE session_id = %(session_id)s
                ORDER BY symbol
                """,
                {"session_id": session_id},
            )
            positions_rows = cur.fetchall()
            cur.execute(
                "SELECT COUNT(*)::int AS c FROM demo_trades WHERE session_id = %(session_id)s",
                {"session_id": session_id},
            )
            total_row = cur.fetchone() or {}
            cur.execute(
                """
                SELECT trade_id, created_at, side, symbol, quantity, price, notional, realized_pnl_on_trade, cash_after
                FROM demo_trades
                WHERE session_id = %(session_id)s
                ORDER BY created_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                {"session_id": session_id, "limit": safe_limit, "offset": safe_offset},
            )
            trade_rows = cur.fetchall()
        conn.commit()

    positions_out: list[dict[str, Any]] = []
    unrealized = 0.0
    market_value = 0.0
    present_symbols: set[str] = set()
    for row in positions_rows:
        sym = str(row["symbol"])
        qty = int(row["quantity"])
        avg = float(row["average_cost"])
        mark = marks.get(sym, avg)
        present_symbols.add(sym)
        if qty > 0:
            unrealized += (mark - avg) * qty
            market_value += qty * mark
        positions_out.append(
            {
                "symbol": sym,
                "quantity": qty,
                "average_cost": avg,
                "opened_at": row["opened_at"],
            }
        )
    marks_used = {s: marks[s] for s in marks if s in present_symbols}
    cash = float((session_row or {}).get("cash_balance", float(DEMO_INITIAL_BALANCE_VND)))
    realized = float((session_row or {}).get("realized_pnl", 0.0))
    equity = cash + market_value
    return {
        "session_id": session_id,
        "cash_balance": cash,
        "positions": positions_out,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "equity_approx_vnd": equity,
        "marks_used": marks_used,
        "trade_history": [dict(item) for item in trade_rows],
        "trade_history_total": int(total_row.get("c", 0)),
        "trade_history_limit": safe_limit,
        "trade_history_offset": safe_offset,
    }


def get_demo_session_overview(session_id: str) -> dict[str, Any]:
    """
    Return compact DB-backed demo session overview for operations UI/API.
    """
    ensure_demo_trading_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, session_id)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    s.session_id,
                    s.cash_balance,
                    s.realized_pnl,
                    s.created_at,
                    s.updated_at,
                    COALESCE((
                        SELECT COUNT(*)::int
                        FROM demo_trades t
                        WHERE t.session_id = s.session_id
                    ), 0) AS trade_count
                FROM demo_sessions s
                WHERE s.session_id = %(session_id)s
                """,
                {"session_id": session_id},
            )
            session_row = cur.fetchone()
            cur.execute(
                """
                SELECT symbol, quantity, average_cost, opened_at
                FROM demo_positions
                WHERE session_id = %(session_id)s
                ORDER BY symbol
                """,
                {"session_id": session_id},
            )
            holdings_rows = cur.fetchall()
        conn.commit()

    if not session_row:
        raise ValueError("DEMO_SESSION_NOT_FOUND")

    holdings = [
        {
            "symbol": str(row["symbol"]),
            "quantity": int(row["quantity"]),
            "average_buy_price": float(row["average_cost"]),
            "opened_at": row["opened_at"],
        }
        for row in holdings_rows
    ]
    holdings_count = len(holdings)
    trade_count = int(session_row.get("trade_count") or 0)

    return {
        "session_id": str(session_row["session_id"]),
        "is_active": bool(trade_count > 0 or holdings_count > 0),
        "cash_balance": float(session_row["cash_balance"]),
        "realized_pnl": float(session_row["realized_pnl"]),
        "trade_count": trade_count,
        "holdings_count": holdings_count,
        "holdings": holdings,
        "created_at": session_row["created_at"],
        "updated_at": session_row["updated_at"],
    }


def get_active_scheduler_demo_session_id_from_db() -> str | None:
    """
    Read current scheduler DEMO session from automation_scheduler_demo_context.
    Returns None when table/row is missing or value is blank.
    """
    try:
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT demo_session_id
                    FROM automation_scheduler_demo_context
                    WHERE id = 1
                    """
                )
                row = cur.fetchone() or {}
        raw = row.get("demo_session_id")
        if not raw:
            return None
        sid = str(raw).strip()
        return sid or None
    except Exception:
        return None


def get_demo_session_cash_balance(session_id: str | None) -> float | None:
    """
    Resolve DEMO cash balance for a specific session id.
    Returns None when session id is missing or lookup fails.
    """
    sid = normalize_demo_session_id(session_id)
    if not sid:
        return None
    try:
        snap = get_demo_account_snapshot(sid, mark_prices={})
    except Exception:
        return None
    try:
        cash = float(snap.get("cash_balance") or 0.0)
    except Exception:
        return None
    if cash <= 0:
        return None
    return cash
