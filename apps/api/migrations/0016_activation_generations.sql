-- Immutable activation plans and unpublished staged chunks. Existing activation
-- records remain available for inventory; they are never adopted by a new batch.
ALTER TABLE documents ADD COLUMN retrieval_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE documents ADD COLUMN active_retrieval_batch TEXT;
CREATE TABLE retrieval_activation_plans (
  batch_id TEXT PRIMARY KEY,
  digest TEXT NOT NULL,
  rollback_digest TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('staging','retryable','published','rolled_back')),
  owner_token TEXT,
  lease_expires_at TEXT,
  created_at TEXT NOT NULL,
  published_at TEXT,
  rolled_back_at TEXT,
  last_error TEXT
);
CREATE TABLE retrieval_activation_staged_chunks (
  batch_id TEXT NOT NULL REFERENCES retrieval_activation_plans(batch_id) ON DELETE CASCADE,
  chunk_id TEXT NOT NULL,
  document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  payload_json TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  vector_submitted INTEGER NOT NULL DEFAULT 0,
  vector_ready INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (batch_id, chunk_id)
);
CREATE TABLE retrieval_activation_guards (
  token TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL REFERENCES retrieval_activation_plans(batch_id),
  owner_token TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('publish','rollback')),
  expected_documents_json TEXT NOT NULL
);
CREATE TRIGGER retrieval_activation_guard BEFORE INSERT ON retrieval_activation_guards
BEGIN
  SELECT CASE WHEN NOT EXISTS (
    SELECT 1 FROM retrieval_activation_plans p WHERE p.batch_id=NEW.batch_id AND p.owner_token=NEW.owner_token
      AND p.lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now')
      AND ((NEW.mode='publish' AND p.state IN ('staging','retryable')) OR (NEW.mode='rollback' AND p.state='published'))
  ) THEN RAISE(ABORT, 'activation_owner_conflict') END;
  SELECT CASE WHEN EXISTS (
    SELECT 1 FROM json_each(NEW.expected_documents_json) expected LEFT JOIN documents d ON d.id=json_extract(expected.value,'$.documentId')
    WHERE d.id IS NULL OR d.document_revision != json_extract(expected.value,'$.documentRevision')
      OR d.retrieval_revision != json_extract(expected.value,'$.retrievalRevision')
      OR d.active_text_generation != json_extract(expected.value,'$.textGeneration')
      OR d.source_r2_key != json_extract(expected.value,'$.sourceFileRef')
      OR d.active_retrieval_batch IS NOT json_extract(expected.value,'$.activeBatchId')
      OR d.ingestion_state != 'ready' OR d.ingestion_completed_at IS NULL OR d.expected_chunk_count <= 0
      OR d.rejected_at IS NOT NULL OR d.searchable_at IS NULL OR d.published_revision IS NOT d.document_revision
      OR d.approved_at IS NULL OR d.approved_revision IS NOT d.document_revision OR d.qc_required_confirmed != 1
      OR d.approval_policy_version IS NOT 'reviewer-policy-1'
  ) THEN RAISE(ABORT, 'activation_document_conflict') END;
  SELECT CASE WHEN NEW.mode='publish' AND EXISTS (
    SELECT 1 FROM retrieval_activation_plans p WHERE p.batch_id=NEW.batch_id AND (
      (SELECT COUNT(*) FROM retrieval_activation_staged_chunks c WHERE c.batch_id=p.batch_id) != json_array_length(p.plan_json,'$.chunks')
      OR (json_extract(p.plan_json,'$.performVectorUpsert')=1 AND EXISTS (
        SELECT 1 FROM retrieval_activation_staged_chunks c WHERE c.batch_id=p.batch_id AND c.vector_ready!=1
      ))
    )
  ) THEN RAISE(ABORT, 'activation_staging_incomplete') END;
  -- Validate retained rollback rows inside the same transaction as the switch.
  -- A missing/corrupted historical projection must never become active.
  SELECT CASE WHEN NEW.mode='rollback' AND EXISTS (
    SELECT 1 FROM retrieval_activation_plans p, json_each(p.plan_json,'$.rollback.documents') target
    WHERE p.batch_id=NEW.batch_id AND (
      (SELECT COUNT(*) FROM retrieval_search_chunks c WHERE c.document_id=json_extract(target.value,'$.documentId')
        AND c.batch_id=json_extract(target.value,'$.previousBatchId')) != json_array_length(target.value,'$.previousChunkIds')
      OR EXISTS (
        SELECT 1 FROM json_each(target.value,'$.previousChunkIds') item
        LEFT JOIN retrieval_search_chunks c ON c.chunk_id=item.value AND c.batch_id=json_extract(target.value,'$.previousBatchId')
          AND c.document_id=json_extract(target.value,'$.documentId')
        LEFT JOIN retrieval_activation_staged_chunks staged ON staged.batch_id=c.batch_id AND staged.chunk_id=c.chunk_id
        LEFT JOIN retrieval_activation_plans previous_plan ON previous_plan.batch_id=c.batch_id
        LEFT JOIN json_each(previous_plan.plan_json,'$.documents') previous_document
          ON json_extract(previous_document.value,'$.documentId')=c.document_id
        WHERE c.chunk_id IS NULL OR staged.chunk_id IS NULL
          OR previous_plan.state IS NOT 'published' OR previous_document.value IS NULL
          OR c.title IS NOT json_extract(previous_document.value,'$.title')
          OR c.citation IS NOT json_extract(previous_document.value,'$.citation')
          OR c.source_file_ref IS NOT json_extract(previous_document.value,'$.sourceFileRef')
          OR c.source_link IS NOT json_extract(previous_document.value,'$.sourceLink')
          OR c.chunk_text IS NOT json_extract(staged.payload_json,'$.sourceText')
          OR c.section_label IS NOT json_extract(staged.payload_json,'$.sectionLabel')
          OR c.paragraph_anchor IS NOT json_extract(staged.payload_json,'$.paragraphAnchorStart')
          OR c.citation_anchor IS NOT json_extract(staged.payload_json,'$.citationAnchorStart')
          OR c.chunk_type IS NOT json_extract(staged.payload_json,'$.chunkType')
          OR c.retrieval_priority IS NOT json_extract(staged.payload_json,'$.retrievalPriority')
          OR c.has_canonical_reference_alignment IS NOT json_extract(staged.payload_json,'$.hasCanonicalReferenceAlignment')
      )
    )
  ) THEN RAISE(ABORT, 'activation_projection_incomplete') END;
END;

-- A changed evidence/approval revision invalidates any activation derived from
-- it. Activation switches themselves advance retrieval_revision only.
CREATE TRIGGER document_activation_revision_changed AFTER UPDATE OF document_revision ON documents
WHEN OLD.document_revision != NEW.document_revision AND (
  EXISTS (SELECT 1 FROM documents d WHERE d.id=NEW.id AND d.active_retrieval_batch IS NOT NULL)
  OR EXISTS (SELECT 1 FROM retrieval_search_chunks c WHERE c.document_id=NEW.id AND c.active=1)
)
BEGIN
  UPDATE retrieval_search_chunks SET active=0 WHERE document_id=NEW.id AND active=1;
  UPDATE documents SET active_retrieval_batch=NULL,retrieval_revision=retrieval_revision+1 WHERE id=NEW.id;
END;
