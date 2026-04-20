CREATE TABLE IF NOT EXISTS experience (
    id UUID PRIMARY KEY,
    trade_id TEXT NOT NULL,
    account_mode VARCHAR(10) NOT NULL CHECK (account_mode IN ('REAL', 'DEMO')),
    symbol VARCHAR(20) NOT NULL,
    strategy_type VARCHAR(20) NOT NULL CHECK (strategy_type IN ('SHORT_TERM', 'LONG_TERM', 'TECHNICAL')),
    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ NOT NULL,
    pnl_value DOUBLE PRECISION NOT NULL,
    pnl_percent DOUBLE PRECISION NOT NULL,
    market_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    root_cause TEXT NOT NULL,
    mistake_tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    improvement_action TEXT NOT NULL,
    confidence_after_review NUMERIC(5,2) NOT NULL,
    reviewed_by VARCHAR(10) NOT NULL CHECK (reviewed_by IN ('BOT', 'USER')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_experience_symbol ON experience (symbol);
CREATE INDEX IF NOT EXISTS idx_experience_mode ON experience (account_mode);
CREATE INDEX IF NOT EXISTS idx_experience_strategy_type ON experience (strategy_type);
CREATE INDEX IF NOT EXISTS idx_experience_created_at ON experience (created_at DESC);
