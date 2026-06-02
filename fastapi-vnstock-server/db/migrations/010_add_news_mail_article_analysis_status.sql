ALTER TABLE news_mail_articles
ADD COLUMN IF NOT EXISTS codex_analysis_status VARCHAR(32) NOT NULL DEFAULT 'pending';

ALTER TABLE news_mail_articles
ADD COLUMN IF NOT EXISTS codex_analysis_started_at TIMESTAMPTZ NULL;

ALTER TABLE news_mail_articles
ADD COLUMN IF NOT EXISTS codex_analysis_finished_at TIMESTAMPTZ NULL;

ALTER TABLE news_mail_articles
ADD COLUMN IF NOT EXISTS codex_analysis_error TEXT NULL;

UPDATE news_mail_articles
SET codex_analysis_status = 'completed',
    codex_analysis_finished_at = COALESCE(codex_analysis_finished_at, updated_at)
WHERE codex_analysis_status <> 'completed'
  AND codex_summary IS NOT NULL
  AND BTRIM(codex_summary) <> '';

CREATE INDEX IF NOT EXISTS idx_news_mail_articles_analysis_status
ON news_mail_articles(run_id, codex_analysis_status, section_index, created_at);
