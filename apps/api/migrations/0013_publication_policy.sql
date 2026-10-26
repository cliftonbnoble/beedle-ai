-- Existing records are deliberately held as legacy until their source and
-- derived artifacts are verified/reprocessed. Do not infer completeness from
-- historical approval or vector activation. See docs/publication-policy.md.
ALTER TABLE documents ADD COLUMN ingestion_state TEXT NOT NULL DEFAULT 'legacy'
  CHECK (ingestion_state IN ('legacy', 'preparing', 'needs_review', 'ready', 'failed'));
ALTER TABLE documents ADD COLUMN ingestion_completed_at TEXT;
ALTER TABLE documents ADD COLUMN expected_chunk_count INTEGER;
ALTER TABLE documents ADD COLUMN document_revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE documents ADD COLUMN published_revision INTEGER;
ALTER TABLE documents ADD COLUMN approved_revision INTEGER;

CREATE INDEX IF NOT EXISTS idx_documents_publication
  ON documents (ingestion_state, rejected_at, searchable_at, approved_at);
