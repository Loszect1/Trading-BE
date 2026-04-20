CREATE TABLE IF NOT EXISTS short_term_automation_runs (
    id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_status VARCHAR(32) NOT NULL,
    scanned INTEGER NOT NULL DEFAULT 0 CHECK (scanned >= 0),
    buy_candidates INTEGER NOT NULL DEFAULT 0 CHECK (buy_candidates >= 0),
    risk_rejected INTEGER NOT NULL DEFAULT 0 CHECK (risk_rejected >= 0),
    executed INTEGER NOT NULL DEFAULT 0 CHECK (executed >= 0),
    execution_rejected INTEGER NOT NULL DEFAULT 0 CHECK (execution_rejected >= 0),
    errors INTEGER NOT NULL DEFAULT 0 CHECK (errors >= 0),
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_short_term_automation_runs_started
ON short_term_automation_runs(started_at DESC);
