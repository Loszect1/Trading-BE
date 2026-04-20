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

CREATE INDEX IF NOT EXISTS idx_orders_core_mode_created ON orders_core (account_mode, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_position_lots_mode_symbol ON position_lots (account_mode, symbol);
CREATE INDEX IF NOT EXISTS idx_position_lots_settle_date ON position_lots (settle_date);
CREATE INDEX IF NOT EXISTS idx_risk_events_mode_created ON risk_events (account_mode, created_at DESC);
