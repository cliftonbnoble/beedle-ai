-- Preserve original/legacy date evidence. Only the derived projection is usable
-- for calendar filters; '+0 days' forces SQLite to normalize overflow days.
ALTER TABLE documents ADD COLUMN valid_decision_date TEXT GENERATED ALWAYS AS (
 CASE WHEN length(decision_date)=10 AND decision_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
 AND substr(decision_date,1,4)!='0000' AND date(decision_date,'+0 days')=decision_date THEN decision_date ELSE NULL END
) VIRTUAL;
-- Every existing vector filter projection predates this validated date field.
-- Reuse embeddings, but hold semantic readiness until metadata is resynchronized.
ALTER TABLE vector_filter_sync ADD COLUMN projection_version INTEGER NOT NULL DEFAULT 0;
DELETE FROM vector_filter_sync;
UPDATE vector_lifecycle_dirty SET revision=revision+1,dirty=1,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now');
UPDATE reviewer_queue_control SET policy_version='pending-mutation-bounds-1',phase='backfill',after_id='',upper_id=COALESCE((SELECT MAX(id) FROM documents),'') WHERE singleton=1;
CREATE INDEX idx_documents_valid_date ON documents(valid_decision_date,id);
