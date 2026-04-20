CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY,
    strategy_type VARCHAR(20) NOT NULL CHECK (strategy_type IN ('SHORT_TERM', 'LONG_TERM', 'TECHNICAL')),
    symbol VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL CHECK (action IN ('BUY', 'SELL', 'HOLD')),
    entry_price DOUBLE PRECISION,
    take_profit_price DOUBLE PRECISION,
    stoploss_price DOUBLE PRECISION,
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 100),
    reason TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signals_strategy_created
ON signals(strategy_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_signals_symbol_created
ON signals(symbol, created_at DESC);
