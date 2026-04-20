-- Bot health time series and alert audit trail
CREATE TABLE IF NOT EXISTS bot_health_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_mode VARCHAR(10) NOT NULL CHECK (account_mode IN ('REAL', 'DEMO')),
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    bot_status VARCHAR(32) NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_bot_health_snapshots_mode_captured
    ON bot_health_snapshots (account_mode, captured_at DESC);

-- Stored monitoring / risk alerts (plain-text message kept for operators)
CREATE TABLE IF NOT EXISTS alert_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_mode VARCHAR(10),
    rule_id VARCHAR(96) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    message TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_logs_created ON alert_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_logs_mode_rule_created
    ON alert_logs (account_mode, rule_id, created_at DESC);

-- Per-account execution kill switch (checked before new orders)
CREATE TABLE IF NOT EXISTS trading_kill_switch (
    account_mode VARCHAR(10) PRIMARY KEY CHECK (account_mode IN ('REAL', 'DEMO')),
    active BOOLEAN NOT NULL DEFAULT FALSE,
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO trading_kill_switch (account_mode, active, reason)
VALUES ('REAL', FALSE, NULL)
ON CONFLICT (account_mode) DO NOTHING;

INSERT INTO trading_kill_switch (account_mode, active, reason)
VALUES ('DEMO', FALSE, NULL)
ON CONFLICT (account_mode) DO NOTHING;
