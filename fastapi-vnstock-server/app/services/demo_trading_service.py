from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.schemas.auto_trading import DEMO_INITIAL_BALANCE_VND, DemoTradeRequest
from app.services.trading_core_service import roll_settlement
from app.services.vn_market_holiday_calendar import add_vn_trading_days_live, vn_market_local_today
from app.services.vnstock_api_service import VNStockApiService


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


_vnstock_api = VNStockApiService()
_VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
_DEMO_TP_SLOT_PCT_DEFAULT = 0.3
_DEMO_TP_SLOT_PCT_MIN = 0.1
_DEMO_TP_SLOT_PCT_MAX = 0.9


def _last_daily_close_mark(symbol: str) -> float | None:
    try:
        rows = _vnstock_api.call_quote(
            "history",
            source="VCI",
            symbol=symbol,
            method_kwargs={"interval": "1D", "count_back": 3},
        )
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        close_raw = row.get("close")
        try:
            close = float(close_raw)
        except (TypeError, ValueError):
            continue
        # VN market convention: some sources return price in "nghin dong" (e.g. 42.65 -> 42,650 VND).
        if 0 < close < 1000:
            close *= 1000.0
        if close > 0:
            return close
    return None


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


def _demo_lot_settle_date(opened_at: datetime) -> date:
    local_trade_date = opened_at.astimezone(_VN_TZ).date()
    return add_vn_trading_days_live(local_trade_date, 2)


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

    CREATE TABLE IF NOT EXISTS demo_strategy_cash (
        session_id VARCHAR(128) NOT NULL REFERENCES demo_sessions(session_id) ON DELETE CASCADE,
        strategy_code VARCHAR(20) NOT NULL CHECK (strategy_code IN ('SHORT_TERM', 'MAIL_SIGNAL', 'UNALLOCATED')),
        cash_value DOUBLE PRECISION NOT NULL CHECK (cash_value >= 0),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (session_id, strategy_code)
    );

    CREATE TABLE IF NOT EXISTS demo_session_exit_config (
        session_id VARCHAR(128) PRIMARY KEY REFERENCES demo_sessions(session_id) ON DELETE CASCADE,
        tp_slot_pct DOUBLE PRECISION NOT NULL CHECK (tp_slot_pct >= 0.1 AND tp_slot_pct <= 0.9),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS demo_symbol_exit_levels (
        session_id VARCHAR(128) NOT NULL REFERENCES demo_sessions(session_id) ON DELETE CASCADE,
        symbol VARCHAR(20) NOT NULL,
        take_profit_price DOUBLE PRECISION NOT NULL CHECK (take_profit_price > 0),
        stoploss_price DOUBLE PRECISION NOT NULL CHECK (stoploss_price > 0),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (session_id, symbol)
    );
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def _strategy_alloc_defaults(total_cash: float) -> dict[str, float]:
    base_cash = max(0.0, float(total_cash))
    short_pct = max(0.0, min(1.0, float(settings.strategy_alloc_short_term_pct)))
    mail_pct = max(0.0, min(1.0, float(settings.strategy_alloc_mail_signal_pct)))
    short_cash = base_cash * short_pct
    mail_cash = base_cash * mail_pct
    return {
        "SHORT_TERM": short_cash,
        "MAIL_SIGNAL": mail_cash,
        "UNALLOCATED": max(0.0, base_cash - short_cash - mail_cash),
    }


def _ensure_demo_strategy_cash_rows(conn: Any, session_id: str, total_cash: float) -> None:
    defaults = _strategy_alloc_defaults(total_cash)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT strategy_code, cash_value
            FROM demo_strategy_cash
            WHERE session_id = %(session_id)s
            """,
            {"session_id": session_id},
        )
        rows = cur.fetchall() or []
        if rows:
            return
        for strategy_code, cash_value in defaults.items():
            cur.execute(
                """
                INSERT INTO demo_strategy_cash (session_id, strategy_code, cash_value, updated_at)
                VALUES (%(session_id)s, %(strategy_code)s, %(cash_value)s, NOW())
                ON CONFLICT (session_id, strategy_code)
                DO UPDATE SET cash_value = EXCLUDED.cash_value, updated_at = NOW()
                """,
                {
                    "session_id": session_id,
                    "strategy_code": strategy_code,
                    "cash_value": float(cash_value),
                },
            )


def _sync_demo_strategy_cash_from_orders(session_id: str) -> None:
    sid = normalize_demo_session_id(session_id)
    ensure_demo_trading_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, sid)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT initial_balance, cash_balance
                FROM demo_sessions
                WHERE session_id = %(session_id)s
                """,
                {"session_id": sid},
            )
            session_row = cur.fetchone() or {}
            initial_balance = float(session_row.get("initial_balance") or DEMO_INITIAL_BALANCE_VND)
            total_cash = float(session_row.get("cash_balance") or 0.0)
            _ensure_demo_strategy_cash_rows(conn, sid, initial_balance)

        used = _strategy_used_notional_for_session(sid)
        defaults = _strategy_alloc_defaults(initial_balance)
        short_remaining = max(0.0, float(defaults.get("SHORT_TERM") or 0.0) - float(used.get("SHORT_TERM") or 0.0))
        mail_remaining = max(0.0, float(defaults.get("MAIL_SIGNAL") or 0.0) - float(used.get("MAIL_SIGNAL") or 0.0))
        unallocated_remaining = max(0.0, float(total_cash) - short_remaining - mail_remaining)

        with conn.cursor() as cur:
            for strategy_code, cash_value in {
                "SHORT_TERM": short_remaining,
                "MAIL_SIGNAL": mail_remaining,
                "UNALLOCATED": unallocated_remaining,
            }.items():
                cur.execute(
                    """
                    UPDATE demo_strategy_cash
                    SET cash_value = %(cash_value)s, updated_at = NOW()
                    WHERE session_id = %(session_id)s AND strategy_code = %(strategy_code)s
                    """,
                    {
                        "session_id": sid,
                        "strategy_code": strategy_code,
                        "cash_value": float(cash_value),
                    },
                )
        conn.commit()


def _normalize_strategy_code(raw: str | None) -> str:
    code = str(raw or "").strip().upper()
    if code in {"SHORT_TERM", "MAIL_SIGNAL", "UNALLOCATED"}:
        return code
    return "SHORT_TERM"


def _get_strategy_cash_value(conn: Any, session_id: str, strategy_code: str) -> float:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT cash_value
            FROM demo_strategy_cash
            WHERE session_id = %(session_id)s AND strategy_code = %(strategy_code)s
            FOR UPDATE
            """,
            {"session_id": session_id, "strategy_code": strategy_code},
        )
        row = cur.fetchone() or {}
    return max(0.0, float(row.get("cash_value") or 0.0))


def _apply_strategy_cash_delta(conn: Any, session_id: str, strategy_code: str, delta: float) -> None:
    current = _get_strategy_cash_value(conn, session_id, strategy_code)
    next_value = max(0.0, current + float(delta))
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE demo_strategy_cash
            SET cash_value = %(cash_value)s, updated_at = NOW()
            WHERE session_id = %(session_id)s AND strategy_code = %(strategy_code)s
            """,
            {
                "session_id": session_id,
                "strategy_code": strategy_code,
                "cash_value": next_value,
            },
        )


def _strategy_used_notional_for_session(session_id: str) -> dict[str, float]:
    used: dict[str, float] = {"SHORT_TERM": 0.0, "MAIL_SIGNAL": 0.0}
    sid = normalize_demo_session_id(session_id)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    CASE
                        WHEN COALESCE(order_metadata->>'strategy_code', '') IN ('SHORT_TERM', 'MAIL_SIGNAL')
                            THEN COALESCE(order_metadata->>'strategy_code', '')
                        WHEN COALESCE(order_metadata->>'source', '') IN ('short_term_schedule_entry', 'short_term_schedule_exit')
                            THEN 'SHORT_TERM'
                        WHEN COALESCE(order_metadata->>'source', '') LIKE 'mail_signal_%%'
                            THEN 'MAIL_SIGNAL'
                        ELSE NULL
                    END AS strategy_code,
                    side,
                    SUM(quantity * price)::double precision AS notional_sum
                FROM orders_core
                WHERE account_mode = 'DEMO'
                  AND status = 'FILLED'
                  AND order_metadata->>'demo_session_id' = %(session_id)s
                GROUP BY 1, 2
                """,
                {"session_id": sid},
            )
            for row in cur.fetchall() or []:
                strategy_code = str(row.get("strategy_code") or "").strip().upper()
                side = str(row.get("side") or "").strip().upper()
                notional = float(row.get("notional_sum") or 0.0)
                if strategy_code not in used:
                    continue
                if side == "BUY":
                    used[strategy_code] += notional
                elif side == "SELL":
                    used[strategy_code] -= notional
    for key in list(used.keys()):
        used[key] = max(0.0, used[key])
    return used


def _get_demo_positions_from_core(session_id: str) -> list[dict[str, Any]]:
    sid = normalize_demo_session_id(session_id)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH fills AS (
                    SELECT
                        symbol,
                        side,
                        SUM(quantity)::bigint AS qty_sum,
                        SUM(quantity * price)::double precision AS notional_sum
                    FROM orders_core
                    WHERE account_mode = 'DEMO'
                      AND status = 'FILLED'
                      AND order_metadata->>'demo_session_id' = %(session_id)s
                    GROUP BY symbol, side
                ),
                per_symbol AS (
                    SELECT
                        symbol,
                        COALESCE(SUM(CASE WHEN side = 'BUY' THEN qty_sum ELSE 0 END), 0)::bigint AS buy_qty,
                        COALESCE(SUM(CASE WHEN side = 'SELL' THEN qty_sum ELSE 0 END), 0)::bigint AS sell_qty,
                        COALESCE(SUM(CASE WHEN side = 'BUY' THEN notional_sum ELSE 0 END), 0)::double precision AS buy_notional
                    FROM fills
                    GROUP BY symbol
                )
                SELECT
                    symbol,
                    (buy_qty - sell_qty)::bigint AS total_qty,
                    CASE WHEN buy_qty > 0 THEN (buy_notional / buy_qty)::double precision ELSE 0::double precision END AS avg_price
                FROM per_symbol
                WHERE (buy_qty - sell_qty) > 0
                ORDER BY symbol
                """,
                {"session_id": sid},
            )
            rows = cur.fetchall() or []
    return [dict(row) for row in rows]


def _get_demo_position_exit_levels_from_core(session_id: str) -> dict[str, tuple[float, float]]:
    sid = normalize_demo_session_id(session_id)
    out: dict[str, tuple[float, float]] = {}
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (symbol)
                    symbol,
                    order_metadata
                FROM orders_core
                WHERE account_mode = 'DEMO'
                  AND status = 'FILLED'
                  AND side = 'BUY'
                  AND order_metadata->>'demo_session_id' = %(session_id)s
                ORDER BY symbol, created_at DESC
                """,
                {"session_id": sid},
            )
            rows = cur.fetchall() or []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        meta = row.get("order_metadata")
        if not isinstance(meta, dict):
            continue
        try:
            tp = float(meta.get("take_profit_price") or 0.0)
            sl = float(meta.get("stoploss_price") or 0.0)
        except (TypeError, ValueError):
            continue
        if tp > 0 and sl > 0:
            out[symbol] = (tp, sl)
    return out


def _get_demo_position_exit_levels_overrides(session_id: str) -> dict[str, tuple[float, float]]:
    sid = normalize_demo_session_id(session_id)
    out: dict[str, tuple[float, float]] = {}
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT symbol, take_profit_price, stoploss_price
                FROM demo_symbol_exit_levels
                WHERE session_id = %(session_id)s
                """,
                {"session_id": sid},
            )
            rows = cur.fetchall() or []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        try:
            tp = float(row.get("take_profit_price") or 0.0)
            sl = float(row.get("stoploss_price") or 0.0)
        except (TypeError, ValueError):
            continue
        if symbol and tp > 0 and sl > 0:
            out[symbol] = (tp, sl)
    return out


def _get_demo_symbol_settlement_snapshot(session_id: str) -> dict[str, dict[str, Any]]:
    sid = normalize_demo_session_id(session_id)
    out: dict[str, dict[str, Any]] = {}
    # Keep `available_qty` current before reading settlement snapshot.
    # Without this, `settled_quantity` may remain 0 even after `settle_date` has passed.
    try:
        roll_settlement("DEMO")
    except Exception:
        pass
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    pl.symbol,
                    COALESCE(SUM(pl.available_qty), 0)::bigint AS settled_quantity,
                    COALESCE(SUM(pl.qty - pl.available_qty), 0)::bigint AS pending_settlement_quantity,
                    MIN(pl.settle_date) AS next_settle_date
                FROM position_lots pl
                JOIN orders_core o ON o.id = pl.buy_order_id
                WHERE pl.account_mode = 'DEMO'
                  AND o.account_mode = 'DEMO'
                  AND o.status = 'FILLED'
                  AND COALESCE(o.order_metadata->>'demo_session_id', '') = %(session_id)s
                GROUP BY pl.symbol
                """,
                {"session_id": sid},
            )
            rows = cur.fetchall() or []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        settled_quantity = int(row.get("settled_quantity") or 0)
        pending_quantity = int(row.get("pending_settlement_quantity") or 0)
        settle_date = row.get("next_settle_date")
        if settled_quantity <= 0 and pending_quantity <= 0:
            continue
        out[symbol] = {
            "settled_quantity": max(0, settled_quantity),
            "pending_settlement_quantity": max(0, pending_quantity),
            "next_settle_date": settle_date if isinstance(settle_date, date) else None,
        }
    return out


def _get_demo_cash_balance_from_core(session_id: str) -> float:
    """
    Derive DEMO cash from filled execution orders (single source of truth).
    """
    sid = normalize_demo_session_id(session_id)
    buy_notional = 0.0
    sell_notional = 0.0
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(quantity * price) FILTER (WHERE side = 'BUY'), 0)::double precision AS buy_notional,
                    COALESCE(SUM(quantity * price) FILTER (WHERE side = 'SELL'), 0)::double precision AS sell_notional
                FROM orders_core
                WHERE account_mode = 'DEMO'
                  AND status = 'FILLED'
                  AND order_metadata->>'demo_session_id' = %(session_id)s
                """,
                {"session_id": sid},
            )
            row = cur.fetchone() or {}
            buy_notional = float(row.get("buy_notional") or 0.0)
            sell_notional = float(row.get("sell_notional") or 0.0)
    derived = float(DEMO_INITIAL_BALANCE_VND) + sell_notional - buy_notional
    return max(0.0, derived)


def _get_demo_trade_history_from_core(
    session_id: str,
    *,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    sid = normalize_demo_session_id(session_id)
    safe_limit = max(1, min(int(limit), 200))
    safe_offset = max(0, int(offset))
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int AS c
                FROM orders_core
                WHERE account_mode = 'DEMO'
                  AND status = 'FILLED'
                  AND COALESCE(order_metadata->>'demo_session_id', '') = %(session_id)s
                """,
                {"session_id": sid},
            )
            total_row = cur.fetchone() or {}
            cur.execute(
                """
                SELECT
                    id::text AS trade_id,
                    created_at,
                    side,
                    symbol,
                    quantity,
                    price,
                    (quantity * price)::double precision AS notional,
                    0::double precision AS realized_pnl_on_trade,
                    0::double precision AS cash_after
                FROM orders_core
                WHERE account_mode = 'DEMO'
                  AND status = 'FILLED'
                  AND COALESCE(order_metadata->>'demo_session_id', '') = %(session_id)s
                ORDER BY created_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                {"session_id": sid, "limit": safe_limit, "offset": safe_offset},
            )
            rows = cur.fetchall() or []
    return ([dict(row) for row in rows], int(total_row.get("c", 0)))


def get_demo_strategy_remaining_cash(session_id: str, strategy_code: str) -> float:
    sid = normalize_demo_session_id(session_id)
    strategy = str(strategy_code).strip().upper()
    if strategy not in {"SHORT_TERM", "MAIL_SIGNAL"}:
        return 0.0
    ensure_demo_trading_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, sid)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT initial_balance FROM demo_sessions WHERE session_id = %(session_id)s", {"session_id": sid})
            session_row = cur.fetchone() or {}
            initial_balance = float(session_row.get("initial_balance") or DEMO_INITIAL_BALANCE_VND)
            _ensure_demo_strategy_cash_rows(conn, sid, initial_balance)
            cur.execute(
                """
                SELECT cash_value
                FROM demo_strategy_cash
                WHERE session_id = %(session_id)s AND strategy_code = %(strategy_code)s
                """,
                {"session_id": sid, "strategy_code": strategy},
            )
            alloc_row = cur.fetchone() or {}
        conn.commit()
    return max(0.0, float(alloc_row.get("cash_value") or 0.0))


def _clamp_demo_tp_slot_pct(value: float) -> float:
    return min(_DEMO_TP_SLOT_PCT_MAX, max(_DEMO_TP_SLOT_PCT_MIN, float(value)))


def get_demo_session_tp_slot_pct(session_id: str) -> float:
    sid = normalize_demo_session_id(session_id)
    ensure_demo_trading_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, sid)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO demo_session_exit_config (session_id, tp_slot_pct, updated_at)
                VALUES (%(session_id)s, %(tp_slot_pct)s, NOW())
                ON CONFLICT (session_id) DO NOTHING
                """,
                {"session_id": sid, "tp_slot_pct": _DEMO_TP_SLOT_PCT_DEFAULT},
            )
            cur.execute(
                """
                SELECT tp_slot_pct
                FROM demo_session_exit_config
                WHERE session_id = %(session_id)s
                """,
                {"session_id": sid},
            )
            row = cur.fetchone() or {}
        conn.commit()
    return _clamp_demo_tp_slot_pct(float(row.get("tp_slot_pct") or _DEMO_TP_SLOT_PCT_DEFAULT))


def upsert_demo_session_tp_slot_pct(session_id: str, tp_slot_pct: float) -> float:
    sid = normalize_demo_session_id(session_id)
    next_pct = _clamp_demo_tp_slot_pct(tp_slot_pct)
    ensure_demo_trading_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, sid)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO demo_session_exit_config (session_id, tp_slot_pct, updated_at)
                VALUES (%(session_id)s, %(tp_slot_pct)s, NOW())
                ON CONFLICT (session_id)
                DO UPDATE SET tp_slot_pct = EXCLUDED.tp_slot_pct, updated_at = NOW()
                """,
                {"session_id": sid, "tp_slot_pct": next_pct},
            )
        conn.commit()
    return next_pct


def upsert_demo_symbol_exit_levels(session_id: str, symbol: str, take_profit_price: float, stoploss_price: float) -> None:
    sid = normalize_demo_session_id(session_id)
    sym = str(symbol or "").strip().upper()
    tp = float(take_profit_price)
    sl = float(stoploss_price)
    if not sym:
        raise ValueError("INVALID_SYMBOL")
    if tp <= 0 or sl <= 0:
        raise ValueError("INVALID_EXIT_LEVELS")
    ensure_demo_trading_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, sid)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO demo_symbol_exit_levels (session_id, symbol, take_profit_price, stoploss_price, updated_at)
                VALUES (%(session_id)s, %(symbol)s, %(take_profit_price)s, %(stoploss_price)s, NOW())
                ON CONFLICT (session_id, symbol)
                DO UPDATE SET
                    take_profit_price = EXCLUDED.take_profit_price,
                    stoploss_price = EXCLUDED.stoploss_price,
                    updated_at = NOW()
                """,
                {
                    "session_id": sid,
                    "symbol": sym,
                    "take_profit_price": tp,
                    "stoploss_price": sl,
                },
            )
        conn.commit()


def delete_demo_symbol_exit_levels(session_id: str, symbol: str) -> None:
    sid = normalize_demo_session_id(session_id)
    sym = str(symbol or "").strip().upper()
    if not sym:
        return
    ensure_demo_trading_tables()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM demo_symbol_exit_levels
                WHERE session_id = %(session_id)s AND symbol = %(symbol)s
                """,
                {"session_id": sid, "symbol": sym},
            )
        conn.commit()


def transfer_demo_strategy_cash_from_unallocated(
    session_id: str, to_strategy: str, amount_vnd: float, from_strategy: str = "UNALLOCATED"
) -> dict[str, Any]:
    sid = normalize_demo_session_id(session_id)
    target = str(to_strategy).strip().upper()
    source = str(from_strategy).strip().upper()
    amount = float(amount_vnd)
    if target not in {"SHORT_TERM", "MAIL_SIGNAL", "UNALLOCATED"}:
        raise ValueError("INVALID_TARGET_STRATEGY")
    if source not in {"UNALLOCATED", "SHORT_TERM", "MAIL_SIGNAL"}:
        raise ValueError("INVALID_SOURCE_STRATEGY")
    if amount <= 0:
        raise ValueError("INVALID_TRANSFER_AMOUNT")
    ensure_demo_trading_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        _ensure_demo_session_exists(conn, sid)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT cash_balance FROM demo_sessions WHERE session_id = %(session_id)s FOR UPDATE", {"session_id": sid})
            session_row = cur.fetchone() or {}
            total_cash = float(session_row.get("cash_balance") or 0.0)
            _ensure_demo_strategy_cash_rows(conn, sid, total_cash)
            if target == "UNALLOCATED" and source == "UNALLOCATED":
                cur.execute(
                    """
                    UPDATE demo_sessions
                    SET initial_balance = initial_balance + %(amount)s,
                        cash_balance = cash_balance + %(amount)s,
                        updated_at = NOW()
                    WHERE session_id = %(session_id)s
                    """,
                    {"amount": amount, "session_id": sid},
                )
                cur.execute(
                    """
                    UPDATE demo_strategy_cash
                    SET cash_value = cash_value + %(amount)s, updated_at = NOW()
                    WHERE session_id = %(session_id)s AND strategy_code = 'UNALLOCATED'
                    """,
                    {"amount": amount, "session_id": sid},
                )
                conn.commit()
                return {"session_id": sid, "transferred_to": target, "amount_vnd": amount}
            if source == target:
                raise ValueError("INVALID_TRANSFER_SAME_STRATEGY")
            cur.execute(
                """
                SELECT strategy_code, cash_value
                FROM demo_strategy_cash
                WHERE session_id = %(session_id)s
                  AND strategy_code IN (%(source)s, %(target)s)
                FOR UPDATE
                """,
                {"session_id": sid, "source": source, "target": target},
            )
            rows = {str(r["strategy_code"]).upper(): float(r["cash_value"]) for r in (cur.fetchall() or [])}
            source_cash = max(0.0, float(rows.get(source) or 0.0))
            current_target = max(0.0, float(rows.get(target) or 0.0))
            if amount > source_cash + 1e-9:
                raise ValueError("INSUFFICIENT_UNALLOCATED_CASH")
            cur.execute(
                """
                UPDATE demo_strategy_cash
                SET cash_value = %(cash_value)s, updated_at = NOW()
                WHERE session_id = %(session_id)s AND strategy_code = %(strategy_code)s
                """,
                {"session_id": sid, "strategy_code": source, "cash_value": max(0.0, source_cash - amount)},
            )
            cur.execute(
                """
                UPDATE demo_strategy_cash
                SET cash_value = %(cash_value)s, updated_at = NOW()
                WHERE session_id = %(session_id)s AND strategy_code = %(strategy_code)s
                """,
                {"session_id": sid, "strategy_code": target, "cash_value": current_target + amount},
            )
        conn.commit()
    return {"session_id": sid, "transferred_to": target, "amount_vnd": amount}


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
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT COUNT(*)::int FROM demo_trades WHERE session_id = %(session_id)s", {"session_id": session_id})
        row = cur.fetchone() or {}
        count = int(row.get("count", 0))
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


def _append_orders_core_fill(
    conn: Any,
    *,
    session_id: str,
    trade_id: str,
    side: str,
    symbol: str,
    quantity: int,
    price: float,
    metadata: dict[str, Any] | None = None,
) -> None:
    payload = dict(metadata or {})
    payload["demo_session_id"] = session_id
    if not payload.get("source"):
        payload["source"] = "demo_trading_service"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders_core (
                id, account_mode, symbol, side, quantity, price, status, reason,
                idempotency_key, order_metadata, created_at, updated_at
            ) VALUES (
                %(id)s, 'DEMO', %(symbol)s, %(side)s, %(quantity)s, %(price)s, 'FILLED', NULL,
                %(idempotency_key)s, %(order_metadata)s::jsonb, NOW(), NOW()
            )
            """,
            {
                "id": uuid4(),
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": price,
                "idempotency_key": f"demo-trade:{session_id}:{trade_id}",
                "order_metadata": Json(payload),
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
            initial_balance = float((session_row or {}).get("initial_balance") or DEMO_INITIAL_BALANCE_VND)
            _ensure_demo_strategy_cash_rows(conn, session_id, initial_balance)
        if not session_row:
            raise ValueError("DEMO_SESSION_NOT_FOUND")
        cash_balance = float(session_row["cash_balance"])
        realized_pnl_total = float(session_row["realized_pnl"])
        trade_id = _next_trade_id(conn, session_id)
        strategy_code = _normalize_strategy_code(body.strategy_type)

        if side == "BUY":
            gross = qty * price
            if gross > cash_balance + 1e-9:
                raise ValueError("INSUFFICIENT_CASH")
            strategy_cash_before = _get_strategy_cash_value(conn, session_id, strategy_code)
            if gross > strategy_cash_before + 1e-9:
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
            _apply_strategy_cash_delta(conn, session_id, strategy_code, -gross)
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
            _append_orders_core_fill(
                conn,
                session_id=session_id,
                trade_id=trade_id,
                side=side,
                symbol=symbol,
                quantity=qty,
                price=price,
                metadata={
                    "source": str((body.market_context or {}).get("source") or "demo_trading_service"),
                    "strategy_code": str(body.strategy_type or "SHORT_TERM"),
                    "entry_price": float(price),
                    "take_profit_price": float((body.market_context or {}).get("take_profit_price") or 0.0),
                    "stoploss_price": float((body.market_context or {}).get("stoploss_price") or 0.0),
                },
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
            core_qty = 0
            core_avg = 0.0
            for core_row in _get_demo_positions_from_core(session_id):
                if str(core_row.get("symbol") or "").strip().upper() != symbol:
                    continue
                core_qty = int(core_row.get("total_qty") or 0)
                core_avg = float(core_row.get("avg_price") or 0.0)
                break
            if core_qty < qty or core_avg <= 0:
                raise ValueError("INSUFFICIENT_POSITION")
            position = _DemoPosition(
                symbol=symbol,
                lots=[_OpenLot(id=str(uuid4()), quantity=core_qty, unit_cost=core_avg, opened_at=now - timedelta(days=7))],
                opened_at=now - timedelta(days=7),
            )
        today_local = vn_market_local_today()
        settlement_snapshot = _get_demo_symbol_settlement_snapshot(session_id).get(symbol, {})
        settled_quantity = int(settlement_snapshot.get("settled_quantity") or 0)
        if settled_quantity < qty:
            raise ValueError("INSUFFICIENT_SETTLED_POSITION")

        opened_at = position.opened_at
        avg_before = position.average_cost
        remaining = qty
        realized_on_trade = 0.0
        for lot in position.lots:
            if remaining <= 0:
                break
            settle_date = _demo_lot_settle_date(lot.opened_at)
            if settle_date > today_local:
                continue
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
        _apply_strategy_cash_delta(conn, session_id, strategy_code, float(qty) * float(price))
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
        _append_orders_core_fill(
            conn,
            session_id=session_id,
            trade_id=trade_id,
            side=side,
            symbol=symbol,
            quantity=qty,
            price=price,
            metadata={
                "source": str((body.market_context or {}).get("source") or "demo_trading_service"),
                "strategy_code": str(body.strategy_type or "SHORT_TERM"),
                "trigger": (body.market_context or {}).get("trigger"),
                "take_profit_price": float((body.market_context or {}).get("take_profit_price") or 0.0),
                "stoploss_price": float((body.market_context or {}).get("stoploss_price") or 0.0),
                "sell_mode": (body.market_context or {}).get("sell_mode"),
                "sell_quantity": int(qty),
            },
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
        if fully_closed:
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
                    "prediction_outcome": "correct" if realized_on_trade >= 0 else "wrong",
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
                SELECT symbol, opened_at
                FROM demo_positions
                WHERE session_id = %(session_id)s
                ORDER BY symbol
                """,
                {"session_id": session_id},
            )
            position_opened_rows = cur.fetchall()
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

    opened_at_by_symbol = {str(row["symbol"]).strip().upper(): row["opened_at"] for row in position_opened_rows}
    fallback_opened_at = _utc_now()
    core_positions_rows = _get_demo_positions_from_core(session_id)
    positions_out: list[dict[str, Any]] = []
    unrealized = 0.0
    market_value = 0.0
    present_symbols: set[str] = set()
    for row in core_positions_rows:
        sym = str(row["symbol"])
        qty = int(row["total_qty"])
        avg = float(row["avg_price"])
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
                "opened_at": opened_at_by_symbol.get(sym, fallback_opened_at),
            }
        )
    marks_used = {s: marks[s] for s in marks if s in present_symbols}
    cash = _get_demo_cash_balance_from_core(session_id)
    realized = float((session_row or {}).get("realized_pnl", 0.0))
    trade_history_out = [dict(item) for item in trade_rows]
    trade_history_total = int(total_row.get("c", 0))
    if not trade_history_out and trade_history_total == 0:
        trade_history_out, trade_history_total = _get_demo_trade_history_from_core(
            session_id,
            limit=safe_limit,
            offset=safe_offset,
        )
    equity = cash + market_value
    return {
        "session_id": session_id,
        "cash_balance": cash,
        "positions": positions_out,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "equity_approx_vnd": equity,
        "marks_used": marks_used,
        "trade_history": trade_history_out,
        "trade_history_total": trade_history_total,
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
                    s.initial_balance,
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
                SELECT symbol, opened_at
                FROM demo_positions
                WHERE session_id = %(session_id)s
                ORDER BY symbol
                """,
                {"session_id": session_id},
            )
            holdings_opened_rows = cur.fetchall()
        conn.commit()

    if not session_row:
        raise ValueError("DEMO_SESSION_NOT_FOUND")

    opened_at_by_symbol = {str(row["symbol"]).strip().upper(): row["opened_at"] for row in holdings_opened_rows}
    fallback_opened_at = session_row["created_at"]
    core_positions_rows = _get_demo_positions_from_core(session_id)
    exit_levels_by_symbol = _get_demo_position_exit_levels_from_core(session_id)
    override_exit_levels_by_symbol = _get_demo_position_exit_levels_overrides(session_id)
    settlement_by_symbol = _get_demo_symbol_settlement_snapshot(session_id)
    tp_slot_pct = get_demo_session_tp_slot_pct(session_id)
    holdings: list[dict[str, Any]] = []
    stock_value = 0.0
    for row in core_positions_rows:
        symbol = str(row["symbol"])
        quantity = int(row["total_qty"])
        average_buy_price = float(row["avg_price"])
        fallback_tp = average_buy_price * 1.04 if average_buy_price > 0 else 0.0
        fallback_sl = average_buy_price * 0.97 if average_buy_price > 0 else 0.0
        tp, sl = override_exit_levels_by_symbol.get(
            symbol.strip().upper(),
            exit_levels_by_symbol.get(symbol.strip().upper(), (fallback_tp, fallback_sl)),
        )
        # Mark-to-market for overview totals: use last daily close, fallback to cost basis.
        mark = _last_daily_close_mark(symbol)
        used_price = mark if mark is not None and mark > 0 else average_buy_price
        position_value = float(quantity) * float(used_price)
        stock_value += position_value
        settlement = settlement_by_symbol.get(symbol.strip().upper(), {})
        settled_quantity = int(settlement.get("settled_quantity") or 0)
        pending_settlement_quantity = int(settlement.get("pending_settlement_quantity") or 0)
        next_settle_date = settlement.get("next_settle_date")
        holdings.append(
            {
                "symbol": symbol,
                "quantity": quantity,
                "average_buy_price": average_buy_price,
                "position_value": position_value,
                "take_profit_price": tp if tp > 0 else None,
                "stoploss_price": sl if sl > 0 else None,
                "settled_quantity": max(0, settled_quantity),
                "pending_settlement_quantity": max(0, pending_settlement_quantity),
                "next_settle_date": (
                    datetime(
                        next_settle_date.year,
                        next_settle_date.month,
                        next_settle_date.day,
                        tzinfo=_VN_TZ,
                    )
                    if isinstance(next_settle_date, date)
                    else None
                ),
                "is_t2_sell_allowed": bool(settled_quantity > 0),
                "opened_at": opened_at_by_symbol.get(symbol.strip().upper(), fallback_opened_at),
            }
        )
    holdings_count = len(holdings)
    trade_count = int(session_row.get("trade_count") or 0)
    cash_balance = _get_demo_cash_balance_from_core(session_id)
    total_assets = max(0.0, cash_balance + stock_value)
    strategy_used_notional = _strategy_used_notional_for_session(str(session_row["session_id"]))
    with connect(settings.database_url, row_factory=dict_row) as conn_alloc:
        with conn_alloc.cursor(row_factory=dict_row) as cur_alloc:
            _ensure_demo_strategy_cash_rows(conn_alloc, str(session_row["session_id"]), float(session_row["initial_balance"]))
            cur_alloc.execute(
                """
                SELECT strategy_code, cash_value
                FROM demo_strategy_cash
                WHERE session_id = %(session_id)s
                """,
                {"session_id": str(session_row["session_id"])},
            )
            alloc_rows = {str(r["strategy_code"]).upper(): float(r["cash_value"]) for r in (cur_alloc.fetchall() or [])}
        conn_alloc.commit()
    short_term_cash = max(0.0, float(alloc_rows.get("SHORT_TERM") or 0.0))
    mail_signal_cash = max(0.0, float(alloc_rows.get("MAIL_SIGNAL") or 0.0))
    unallocated_cash = max(0.0, float(alloc_rows.get("UNALLOCATED") or 0.0))
    alloc_total = short_term_cash + mail_signal_cash + unallocated_cash
    alloc_short_term_pct = (short_term_cash / alloc_total) if alloc_total > 0 else 0.0
    alloc_mail_signal_pct = (mail_signal_cash / alloc_total) if alloc_total > 0 else 0.0
    alloc_unallocated_pct = (unallocated_cash / alloc_total) if alloc_total > 0 else 0.0
    strategy_cash_overview = [
        {
            "strategy_code": "SHORT_TERM",
            "allocation_pct": alloc_short_term_pct,
            "cash_value": round(short_term_cash, 6),
            "used_cash_value": round(float(strategy_used_notional.get("SHORT_TERM") or 0.0), 6),
            "remaining_cash_value": round(short_term_cash, 6),
        },
        {
            "strategy_code": "MAIL_SIGNAL",
            "allocation_pct": alloc_mail_signal_pct,
            "cash_value": round(mail_signal_cash, 6),
            "used_cash_value": round(float(strategy_used_notional.get("MAIL_SIGNAL") or 0.0), 6),
            "remaining_cash_value": round(mail_signal_cash, 6),
        },
    ]
    if unallocated_cash > 0:
        strategy_cash_overview.append(
            {
                "strategy_code": "UNALLOCATED",
                "allocation_pct": max(0.0, alloc_unallocated_pct),
                "cash_value": round(unallocated_cash, 6),
                "used_cash_value": 0.0,
                "remaining_cash_value": round(unallocated_cash, 6),
            }
        )

    return {
        "session_id": str(session_row["session_id"]),
        "is_active": bool(trade_count > 0 or holdings_count > 0),
        "initial_balance": float(session_row["initial_balance"]),
        "cash_balance": cash_balance,
        "stock_value": float(stock_value),
        "total_assets": float(total_assets),
        "realized_pnl": float(session_row["realized_pnl"]),
        "trade_count": trade_count,
        "holdings_count": holdings_count,
        "holdings": holdings,
        "tp_slot_pct": float(tp_slot_pct),
        "strategy_cash_overview": strategy_cash_overview,
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


def _resolve_exit_trigger(last_price: float, take_profit: float, stoploss: float) -> str | None:
    if last_price <= 0 or take_profit <= 0 or stoploss <= 0:
        return None
    if last_price >= take_profit:
        return "take_profit_hit"
    if last_price <= stoploss:
        return "stoploss_hit"
    return None


def run_demo_auto_exit_cycle(session_id: str) -> dict[str, Any]:
    sid = normalize_demo_session_id(session_id)
    overview = get_demo_session_overview(sid)
    holdings = list(overview.get("holdings") or [])
    if not holdings:
        return {"session_id": sid, "scanned": 0, "executed": 0}
    tp_slot_pct = float(overview.get("tp_slot_pct") or _DEMO_TP_SLOT_PCT_DEFAULT)
    executed = 0
    scanned = 0
    details: list[dict[str, Any]] = []
    for row in holdings:
        symbol = str(row.get("symbol") or "").strip().upper()
        sellable_qty = int(row.get("settled_quantity") or 0)
        quantity_total = int(row.get("quantity") or 0)
        tp = float(row.get("take_profit_price") or 0.0)
        sl = float(row.get("stoploss_price") or 0.0)
        if not symbol or sellable_qty < 100:
            continue
        mark = _last_daily_close_mark(symbol)
        last = float(mark or 0.0)
        trigger = _resolve_exit_trigger(last, tp, sl)
        scanned += 1
        if not trigger:
            continue
        slot_qty = max(100, int((((sellable_qty * tp_slot_pct) + 99) // 100) * 100))
        sell_qty = sellable_qty if trigger == "stoploss_hit" else min(sellable_qty, slot_qty)
        if sell_qty <= 0:
            continue
        try:
            result = execute_demo_trade(
                sid,
                DemoTradeRequest(
                    side="SELL",
                    symbol=symbol,
                    quantity=sell_qty,
                    price=last,
                    strategy_type="SHORT_TERM",
                    market_context={
                        "source": "demo_auto_sell_threshold",
                        "trigger": trigger,
                        "take_profit_price": tp,
                        "stoploss_price": sl,
                        "sell_mode": "full_exit" if trigger == "stoploss_hit" else "take_profit_slot",
                        "take_profit_slot_pct_applied": 1.0 if trigger == "stoploss_hit" else float(tp_slot_pct),
                        "sell_quantity": int(sell_qty),
                        "remaining_quantity": max(0, int(quantity_total - sell_qty)),
                    },
                ),
            )
            executed += 1
            realized = float(result.realized_pnl_on_trade)
            next_tp_slot_pct = _clamp_demo_tp_slot_pct(
                float(tp_slot_pct) - 0.05 if realized >= 0 else float(tp_slot_pct) + 0.05
            )
            tp_slot_pct = upsert_demo_session_tp_slot_pct(sid, next_tp_slot_pct)
            remaining_qty = max(0, int(quantity_total - sell_qty))
            if trigger == "take_profit_hit" and remaining_qty >= 100:
                next_tp = last * 1.03
                next_sl = last * 0.985
                upsert_demo_symbol_exit_levels(
                    session_id=sid,
                    symbol=symbol,
                    take_profit_price=next_tp,
                    stoploss_price=next_sl,
                )
            else:
                delete_demo_symbol_exit_levels(sid, symbol)
            details.append(
                {
                    "symbol": symbol,
                    "trigger": trigger,
                    "sell_quantity": sell_qty,
                    "price": last,
                }
            )
        except Exception as exc:
            details.append({"symbol": symbol, "trigger": trigger, "error": str(exc)})
    return {"session_id": sid, "scanned": scanned, "executed": executed, "details": details}
