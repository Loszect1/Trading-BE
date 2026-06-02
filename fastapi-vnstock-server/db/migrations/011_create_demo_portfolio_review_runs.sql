CREATE TABLE IF NOT EXISTS demo_portfolio_review_runs (
    id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_status VARCHAR(32) NOT NULL,
    trigger_source VARCHAR(32) NOT NULL,
    trigger_marker VARCHAR(64),
    session_id VARCHAR(128),
    holdings_count INTEGER NOT NULL DEFAULT 0 CHECK (holdings_count >= 0),
    applied_count INTEGER NOT NULL DEFAULT 0 CHECK (applied_count >= 0),
    skipped_count INTEGER NOT NULL DEFAULT 0 CHECK (skipped_count >= 0),
    error TEXT,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_demo_portfolio_review_runs_started
ON demo_portfolio_review_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_demo_portfolio_review_runs_session_started
ON demo_portfolio_review_runs(session_id, started_at DESC);
