ALTER TABLE document_vector_jobs ADD COLUMN owner_token TEXT;
ALTER TABLE document_vector_jobs ADD COLUMN lease_expires_at TEXT;
ALTER TABLE document_vector_jobs ADD COLUMN next_attempt_at TEXT;
ALTER TABLE document_vector_jobs ADD COLUMN claimed_revision INTEGER;
ALTER TABLE document_vector_jobs ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE document_vector_jobs ADD COLUMN next_chunk_offset INTEGER NOT NULL DEFAULT 0;
-- Legacy delivery attempts were not exclusive. Preserve their consumed budget
-- conservatively; an abandoned processing claim consumes its final failure on recovery.
UPDATE document_vector_jobs SET failure_count = MAX(0, attempts - CASE WHEN state='processing' THEN 1 ELSE 0 END);
UPDATE document_vector_jobs SET state='failed',last_error='Legacy durable retry budget exhausted'
  WHERE state='queued' AND failure_count>=5;
CREATE INDEX idx_vector_jobs_lease_recovery ON document_vector_jobs(state,next_attempt_at,lease_expires_at);
