CREATE TABLE IF NOT EXISTS ai_memory_evidence_sources (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES ai_decision_events(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    filename TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    file_sha256 TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL CHECK (file_size_bytes >= 0),
    extraction_method TEXT NOT NULL,
    extracted_text TEXT NOT NULL DEFAULT '',
    excerpt TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE(event_id, file_sha256, filename)
);

CREATE INDEX IF NOT EXISTS idx_ai_memory_evidence_event_created
ON ai_memory_evidence_sources(event_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_memory_evidence_hash
ON ai_memory_evidence_sources(file_sha256);
