CREATE TABLE IF NOT EXISTS news_mail_runs (
    id UUID PRIMARY KEY,
    run_date DATE NOT NULL,
    source_query TEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    error TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_date, source_query)
);

CREATE INDEX IF NOT EXISTS idx_news_mail_runs_date
ON news_mail_runs(run_date DESC);

CREATE TABLE IF NOT EXISTS news_mail_messages (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
    gmail_message_id TEXT NOT NULL,
    subject TEXT NULL,
    internal_date TIMESTAMPTZ NULL,
    snippet TEXT NULL,
    raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, gmail_message_id)
);

CREATE INDEX IF NOT EXISTS idx_news_mail_messages_run
ON news_mail_messages(run_id);

CREATE TABLE IF NOT EXISTS news_mail_articles (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
    message_id UUID NULL REFERENCES news_mail_messages(id) ON DELETE SET NULL,
    section_index INTEGER NOT NULL DEFAULT 0 CHECK (section_index >= 0),
    section_title TEXT NULL,
    url TEXT NOT NULL,
    url_hash VARCHAR(64) NOT NULL,
    source_host TEXT NULL,
    category TEXT NOT NULL DEFAULT 'General',
    category_slug VARCHAR(64) NOT NULL DEFAULT 'general',
    fetch_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    http_status INTEGER NULL,
    fetch_error TEXT NULL,
    title TEXT NULL,
    article_text TEXT NULL,
    article_excerpt TEXT NULL,
    codex_summary TEXT NULL,
    key_points JSONB NOT NULL DEFAULT '[]'::jsonb,
    sector_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    market_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, url_hash)
);

CREATE INDEX IF NOT EXISTS idx_news_mail_articles_run_status
ON news_mail_articles(run_id, fetch_status);

CREATE INDEX IF NOT EXISTS idx_news_mail_articles_updated
ON news_mail_articles(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_mail_articles_category_slug
ON news_mail_articles(category_slug);

CREATE TABLE IF NOT EXISTS news_mail_symbol_impacts (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
    article_id UUID NOT NULL REFERENCES news_mail_articles(id) ON DELETE CASCADE,
    symbol VARCHAR(20) NOT NULL,
    company_name TEXT NULL,
    known_symbol BOOLEAN NOT NULL DEFAULT FALSE,
    relevance_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (relevance_score >= 0 AND relevance_score <= 100),
    sentiment_label VARCHAR(16) NOT NULL DEFAULT 'neutral',
    sentiment_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (sentiment_score >= -100 AND sentiment_score <= 100),
    impact_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (impact_score >= 0 AND impact_score <= 100),
    impact_horizon VARCHAR(16) NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (confidence >= 0 AND confidence <= 100),
    rationale TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (article_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_news_mail_impacts_symbol_created
ON news_mail_symbol_impacts(symbol, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_mail_impacts_run_impact
ON news_mail_symbol_impacts(run_id, impact_score DESC);

CREATE TABLE IF NOT EXISTS news_mail_return_research (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
    article_id UUID NOT NULL REFERENCES news_mail_articles(id) ON DELETE CASCADE,
    symbol_impact_id UUID NOT NULL REFERENCES news_mail_symbol_impacts(id) ON DELETE CASCADE,
    symbol VARCHAR(20) NOT NULL,
    event_date DATE NOT NULL,
    horizon_days INTEGER NOT NULL CHECK (horizon_days IN (1, 3, 5)),
    base_trading_date DATE NULL,
    base_close DOUBLE PRECISION NULL,
    future_trading_date DATE NULL,
    future_close DOUBLE PRECISION NULL,
    return_pct DOUBLE PRECISION NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol_impact_id, horizon_days)
);

CREATE INDEX IF NOT EXISTS idx_news_mail_return_research_symbol_event
ON news_mail_return_research(symbol, event_date DESC);
