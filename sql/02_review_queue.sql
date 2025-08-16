CREATE TABLE IF NOT EXISTS review_queue (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  new_payload      JSONB NOT NULL,
  candidate_id     UUID,
  similarity       REAL,
  reason           TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  status           TEXT NOT NULL DEFAULT 'OPEN'
);
