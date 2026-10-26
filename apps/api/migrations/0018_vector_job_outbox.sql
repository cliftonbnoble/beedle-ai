ALTER TABLE document_vector_jobs ADD COLUMN outbox_ready_at TEXT;
ALTER TABLE document_vector_jobs ADD COLUMN prepared_revision INTEGER;
ALTER TABLE document_vector_jobs ADD COLUMN source_key TEXT;
ALTER TABLE document_vector_jobs ADD COLUMN source_sha256 TEXT;
ALTER TABLE document_vector_jobs ADD COLUMN expected_chunk_count INTEGER;
ALTER TABLE document_vector_jobs ADD COLUMN chunk_manifest_sha256 TEXT;
-- Never invent proof that an old queued job observed a complete import.
-- Keep completed history; hold unfinished legacy jobs for explicit reprocessing.
UPDATE document_vector_jobs SET state='failed',owner_token=NULL,lease_expires_at=NULL,next_attempt_at=NULL,
  last_error='Legacy job has no verified outbox manifest; reprocess the document'
  WHERE state IN ('queued','processing');
CREATE INDEX idx_vector_outbox_ready ON document_vector_jobs(outbox_ready_at,state,next_attempt_at);
