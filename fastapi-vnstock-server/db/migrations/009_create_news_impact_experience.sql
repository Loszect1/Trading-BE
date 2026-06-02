ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS benchmark_symbol VARCHAR(20) NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS benchmark_base_trading_date DATE NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS benchmark_base_close DOUBLE PRECISION NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS benchmark_future_trading_date DATE NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS benchmark_future_close DOUBLE PRECISION NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS benchmark_return_pct DOUBLE PRECISION NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS abnormal_return_pct DOUBLE PRECISION NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS base_volume DOUBLE PRECISION NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS future_volume DOUBLE PRECISION NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS volume_change_pct DOUBLE PRECISION NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS outcome_label VARCHAR(32) NULL;

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS outcome_score DOUBLE PRECISION NULL CHECK (
    outcome_score IS NULL OR (outcome_score >= 0 AND outcome_score <= 100)
);

ALTER TABLE news_mail_return_research
ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS news_impact_experience (
    id UUID PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    horizon_days INTEGER NOT NULL CHECK (horizon_days IN (1, 3, 5)),
    sentiment_bucket VARCHAR(16) NOT NULL CHECK (sentiment_bucket IN ('positive', 'negative', 'neutral', 'mixed')),
    impact_bucket VARCHAR(16) NOT NULL CHECK (impact_bucket IN ('low', 'medium', 'high')),
    category_slug VARCHAR(64) NOT NULL DEFAULT 'general',
    sample_count INTEGER NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
    win_count INTEGER NOT NULL DEFAULT 0 CHECK (win_count >= 0),
    loss_count INTEGER NOT NULL DEFAULT 0 CHECK (loss_count >= 0),
    neutral_count INTEGER NOT NULL DEFAULT 0 CHECK (neutral_count >= 0),
    avg_abnormal_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
    median_abnormal_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_volume_change_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
    hit_rate_pct DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (hit_rate_pct >= 0 AND hit_rate_pct <= 100),
    confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (confidence_score >= 0 AND confidence_score <= 100),
    score_adjustment DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (score_adjustment >= -8 AND score_adjustment <= 8),
    last_event_at DATE NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, horizon_days, sentiment_bucket, impact_bucket, category_slug)
);

CREATE INDEX IF NOT EXISTS idx_news_impact_experience_symbol_horizon
ON news_impact_experience(symbol, horizon_days, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_impact_experience_adjustment
ON news_impact_experience(score_adjustment DESC, confidence_score DESC);
