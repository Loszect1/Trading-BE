ALTER TABLE news_mail_articles
ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'General';

ALTER TABLE news_mail_articles
ADD COLUMN IF NOT EXISTS category_slug VARCHAR(64) NOT NULL DEFAULT 'general';

CREATE INDEX IF NOT EXISTS idx_news_mail_articles_category_slug
ON news_mail_articles(category_slug);
