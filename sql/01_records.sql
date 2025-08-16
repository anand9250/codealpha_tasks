CREATE TABLE IF NOT EXISTS records (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  content_hash       TEXT NOT NULL UNIQUE,
  natural_key_hash   TEXT UNIQUE,
  canonical_text     TEXT NOT NULL,
  payload            JSONB NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_records_canonical_text_trgm
  ON records USING gin (canonical_text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_records_content_hash
  ON records (content_hash);
