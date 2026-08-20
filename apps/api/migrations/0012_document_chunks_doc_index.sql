-- PROD-PERF-01: the corpus-scope WHERE used by every search path embeds
--   EXISTS (SELECT 1 FROM document_chunks c_basic WHERE c_basic.document_id = d.id)
-- and document_chunks(document_id) had no index, so each evaluated document row scanned up to
-- ~467k chunk rows. Measured on production D1: 24.5s inside a single vector chunk fetch.
-- The runtime safety net (ensureSearchRuntimeIndexes) creates this same index on first search,
-- so this migration is a durable no-op checkpoint recording it in the schema history.
CREATE INDEX IF NOT EXISTS idx_document_chunks_doc
  ON document_chunks (document_id);
