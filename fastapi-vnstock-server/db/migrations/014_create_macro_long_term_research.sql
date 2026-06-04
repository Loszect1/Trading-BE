CREATE TABLE IF NOT EXISTS macro_observations (
    id UUID PRIMARY KEY,
    metric_key VARCHAR(96) NOT NULL,
    period VARCHAR(32) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit VARCHAR(32) NOT NULL DEFAULT '',
    source_name TEXT NOT NULL,
    source_url TEXT NULL,
    published_at TIMESTAMPTZ NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 75 CHECK (confidence >= 0 AND confidence <= 100),
    data_quality VARCHAR(32) NOT NULL DEFAULT 'manual',
    raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (metric_key, period, source_name)
);

CREATE INDEX IF NOT EXISTS idx_macro_observations_metric_period
ON macro_observations(metric_key, period DESC);

CREATE INDEX IF NOT EXISTS idx_macro_observations_published
ON macro_observations(published_at DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS macro_regime_snapshots (
    id UUID PRIMARY KEY,
    as_of DATE NOT NULL,
    regime VARCHAR(32) NOT NULL,
    regime_score DOUBLE PRECISION NOT NULL CHECK (regime_score >= 0 AND regime_score <= 100),
    components JSONB NOT NULL DEFAULT '{}'::jsonb,
    drivers JSONB NOT NULL DEFAULT '[]'::jsonb,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_macro_regime_snapshots_as_of
ON macro_regime_snapshots(as_of DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS long_term_research_runs (
    id UUID PRIMARY KEY,
    mode VARCHAR(16) NOT NULL CHECK (mode IN ('AUTO', 'MANUAL')),
    run_status VARCHAR(32) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    universe_exchange VARCHAR(16) NOT NULL DEFAULT 'HOSE',
    universe_size INTEGER NOT NULL DEFAULT 0 CHECK (universe_size >= 0),
    scored_count INTEGER NOT NULL DEFAULT 0 CHECK (scored_count >= 0),
    error TEXT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    macro_context JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_long_term_research_runs_started
ON long_term_research_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS stock_universe_snapshots (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES long_term_research_runs(id) ON DELETE CASCADE,
    exchange VARCHAR(16) NOT NULL DEFAULT 'HOSE',
    symbol VARCHAR(20) NOT NULL,
    sector TEXT NULL,
    market_cap DOUBLE PRECISION NULL,
    market_cap_source VARCHAR(32) NOT NULL DEFAULT 'unknown',
    latest_close DOUBLE PRECISION NULL,
    issue_share DOUBLE PRECISION NULL,
    data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_overview JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_stock_universe_snapshots_run_cap
ON stock_universe_snapshots(run_id, market_cap DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS long_term_stock_scores (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES long_term_research_runs(id) ON DELETE CASCADE,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(16) NULL,
    sector TEXT NULL,
    market_cap DOUBLE PRECISION NULL,
    rank INTEGER NULL CHECK (rank IS NULL OR rank > 0),
    final_score DOUBLE PRECISION NOT NULL CHECK (final_score >= 0 AND final_score <= 100),
    rating TEXT NOT NULL,
    score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_penalty DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (risk_penalty >= 0 AND risk_penalty <= 20),
    macro_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    sector_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    news_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
    disclaimer TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_long_term_stock_scores_run_rank
ON long_term_stock_scores(run_id, rank ASC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_long_term_stock_scores_symbol_created
ON long_term_stock_scores(symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS long_term_symbol_analyses (
    id UUID PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    mode VARCHAR(16) NOT NULL CHECK (mode IN ('AUTO', 'MANUAL')),
    run_id UUID NULL REFERENCES long_term_research_runs(id) ON DELETE SET NULL,
    final_score DOUBLE PRECISION NOT NULL CHECK (final_score >= 0 AND final_score <= 100),
    rating TEXT NOT NULL,
    score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_penalty DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (risk_penalty >= 0 AND risk_penalty <= 20),
    macro_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    sector_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    news_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    ai_thesis TEXT NOT NULL DEFAULT '',
    catalysts JSONB NOT NULL DEFAULT '[]'::jsonb,
    risks JSONB NOT NULL DEFAULT '[]'::jsonb,
    data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
    disclaimer TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_long_term_symbol_analyses_symbol_created
ON long_term_symbol_analyses(symbol, created_at DESC);
