CREATE TABLE IF NOT EXISTS ai_decision_events (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    workflow_type VARCHAR(64) NOT NULL,
    account_mode VARCHAR(10) NULL CHECK (account_mode IS NULL OR account_mode IN ('REAL', 'DEMO')),
    symbol VARCHAR(20) NULL,
    strategy_type VARCHAR(20) NULL CHECK (
        strategy_type IS NULL
        OR strategy_type IN ('SHORT_TERM', 'LONG_TERM', 'TECHNICAL', 'MAIL_SIGNAL')
    ),
    source_type VARCHAR(64) NULL,
    source_id TEXT NULL,
    session_id VARCHAR(128) NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    model TEXT NULL,
    schema_version VARCHAR(32) NOT NULL DEFAULT '1.0',
    prompt_hash TEXT NOT NULL,
    confidence NUMERIC(5,2) NULL CHECK (confidence IS NULL OR confidence >= 0 AND confidence <= 100),
    reuse_status VARCHAR(16) NOT NULL DEFAULT 'NEW' CHECK (reuse_status IN ('NEW', 'APPROVED', 'REJECTED', 'EXPIRED')),
    input_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    llm_recommendation JSONB NOT NULL DEFAULT '{}'::jsonb,
    final_system_decision JSONB NOT NULL DEFAULT '{}'::jsonb,
    guardrail_result JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_ai_decision_events_workflow_created
ON ai_decision_events(workflow_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_decision_events_symbol_strategy_created
ON ai_decision_events(symbol, strategy_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_decision_events_mode_workflow_created
ON ai_decision_events(account_mode, workflow_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_decision_events_source
ON ai_decision_events(source_type, source_id);

