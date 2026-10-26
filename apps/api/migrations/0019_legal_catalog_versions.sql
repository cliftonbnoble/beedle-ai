-- Durable catalog versions retain the full prior catalog before any replacement.
CREATE TABLE legal_catalog_versions (
 id TEXT PRIMARY KEY, parent_id TEXT, base_epoch INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('staging','ready','published','retained','failed')),
 payload_sha256 TEXT, manifest_json TEXT, created_at TEXT NOT NULL, published_at TEXT, last_error TEXT
);
CREATE TABLE legal_catalog_rows (
 catalog_id TEXT NOT NULL REFERENCES legal_catalog_versions(id), table_name TEXT NOT NULL,
 row_id TEXT NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY(catalog_id,table_name,row_id)
);
CREATE TABLE legal_catalog_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), active_version TEXT NOT NULL REFERENCES legal_catalog_versions(id), epoch INTEGER NOT NULL);
CREATE TABLE legal_catalog_events (id TEXT PRIMARY KEY,version_id TEXT NOT NULL REFERENCES legal_catalog_versions(id),previous_version TEXT NOT NULL,epoch INTEGER NOT NULL UNIQUE,created_at TEXT NOT NULL);
INSERT INTO legal_catalog_versions(id,base_epoch,state,created_at,published_at) VALUES('legacy',0,'published',strftime('%Y-%m-%dT%H:%M:%fZ','now'),strftime('%Y-%m-%dT%H:%M:%fZ','now'));
INSERT INTO legal_catalog_state VALUES(1,'legacy',0);
INSERT INTO legal_catalog_rows SELECT 'legacy','legal_reference_sources',source_key,json_object('source_key',source_key, 'source_path',source_path, 'updated_at',updated_at) FROM legal_reference_sources;
INSERT INTO legal_catalog_rows SELECT 'legacy','legal_index_codes',id,json_object('id',id, 'code_identifier',code_identifier, 'normalized_code',normalized_code, 'family',family, 'label',label, 'description',description, 'is_reserved',is_reserved, 'is_legacy_pre_1002',is_legacy_pre_1002, 'linked_ordinance_sections_json',linked_ordinance_sections_json, 'linked_rules_sections_json',linked_rules_sections_json, 'source_page_anchor',source_page_anchor, 'active',active, 'created_at',created_at, 'updated_at',updated_at) FROM legal_index_codes;
INSERT INTO legal_catalog_rows SELECT 'legacy','legal_ordinance_sections',id,json_object('id',id, 'section_number',section_number, 'subsection_path',subsection_path, 'citation',citation, 'normalized_citation',normalized_citation, 'heading',heading, 'body_text',body_text, 'source_page_anchor',source_page_anchor, 'active',active, 'created_at',created_at, 'updated_at',updated_at) FROM legal_ordinance_sections;
INSERT INTO legal_catalog_rows SELECT 'legacy','legal_rules_sections',id,json_object('id',id, 'part',part, 'section_number',section_number, 'citation',citation, 'normalized_citation',normalized_citation, 'canonical_bare_citation',canonical_bare_citation, 'normalized_bare_citation',normalized_bare_citation, 'heading',heading, 'body_text',body_text, 'source_page_anchor',source_page_anchor, 'active',active, 'created_at',created_at, 'updated_at',updated_at) FROM legal_rules_sections;
INSERT INTO legal_catalog_rows SELECT 'legacy','legal_reference_crosswalk',id,json_object('id',id, 'index_code_id',index_code_id, 'ordinance_citation',ordinance_citation, 'rules_citation',rules_citation, 'source',source, 'created_at',created_at) FROM legal_reference_crosswalk;
CREATE TRIGGER legal_catalog_rows_immutable_update BEFORE UPDATE ON legal_catalog_rows
BEGIN SELECT RAISE(ABORT,'catalog_rows_immutable'); END;
CREATE TRIGGER legal_catalog_rows_immutable_delete BEFORE DELETE ON legal_catalog_rows
BEGIN SELECT RAISE(ABORT,'catalog_rows_immutable'); END;
CREATE TRIGGER legal_catalog_rows_staging_only BEFORE INSERT ON legal_catalog_rows
WHEN (SELECT state FROM legal_catalog_versions WHERE id=NEW.catalog_id) IS NOT 'staging'
BEGIN SELECT RAISE(ABORT,'catalog_not_staging'); END;
CREATE TABLE legal_catalog_guards(token TEXT PRIMARY KEY,version_id TEXT NOT NULL,expected_epoch INTEGER NOT NULL);
CREATE TRIGGER legal_catalog_publication_guard BEFORE INSERT ON legal_catalog_guards
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_state WHERE singleton=1 AND epoch=NEW.expected_epoch)
   THEN RAISE(ABORT,'catalog_revision_conflict') END;
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_versions WHERE id=NEW.version_id AND state IN ('ready','retained','published') AND manifest_json IS NOT NULL)
   THEN RAISE(ABORT,'catalog_incomplete') END;
 SELECT CASE WHEN EXISTS(
   SELECT 1 FROM legal_catalog_versions v,json_each(v.manifest_json) m WHERE v.id=NEW.version_id
     AND (SELECT COUNT(*) FROM legal_catalog_rows r WHERE r.catalog_id=v.id AND r.table_name=m.key)!=json_extract(m.value,'$.count')
 ) THEN RAISE(ABORT,'catalog_incomplete') END;
END;
ALTER TABLE documents ADD COLUMN reference_catalog_version TEXT NOT NULL DEFAULT 'unverified';
ALTER TABLE document_reference_links ADD COLUMN catalog_version TEXT NOT NULL DEFAULT 'unverified';
ALTER TABLE document_reference_issues ADD COLUMN catalog_version TEXT NOT NULL DEFAULT 'unverified';
ALTER TABLE staged_document_reference_links ADD COLUMN catalog_version TEXT NOT NULL DEFAULT 'unverified';
ALTER TABLE staged_document_reference_issues ADD COLUMN catalog_version TEXT NOT NULL DEFAULT 'unverified';
CREATE TABLE document_reference_validation_history (
 id TEXT PRIMARY KEY,document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
 document_revision INTEGER NOT NULL,catalog_version TEXT NOT NULL,links_json TEXT NOT NULL,issues_json TEXT NOT NULL,created_at TEXT NOT NULL
);
CREATE INDEX idx_reference_validation_history_document ON document_reference_validation_history(document_id,created_at);
CREATE TRIGGER document_reference_catalog_changed AFTER UPDATE OF reference_catalog_version ON documents
WHEN OLD.reference_catalog_version IS NOT NEW.reference_catalog_version AND NEW.ingestion_state='ready'
BEGIN
 UPDATE documents SET document_revision=document_revision+1,approved_at=NULL,approved_revision=NULL,approval_policy_version=NULL,
   qc_required_confirmed=0,qc_confirmed_at=NULL WHERE id=NEW.id;
END;
CREATE TABLE reference_validation_guards(token TEXT PRIMARY KEY,document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,expected_catalog TEXT NOT NULL);
CREATE TRIGGER reference_validation_catalog_guard BEFORE INSERT ON reference_validation_guards
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_state WHERE singleton=1 AND active_version=NEW.expected_catalog)
 THEN RAISE(ABORT,'catalog_revision_conflict') END;
END;
CREATE TRIGGER legal_reference_sources_versioned_insert BEFORE INSERT ON legal_reference_sources
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_reference_sources_versioned_update BEFORE UPDATE ON legal_reference_sources
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_reference_sources_versioned_delete BEFORE DELETE ON legal_reference_sources
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_index_codes_versioned_insert BEFORE INSERT ON legal_index_codes
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_index_codes_versioned_update BEFORE UPDATE ON legal_index_codes
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_index_codes_versioned_delete BEFORE DELETE ON legal_index_codes
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_ordinance_sections_versioned_insert BEFORE INSERT ON legal_ordinance_sections
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_ordinance_sections_versioned_update BEFORE UPDATE ON legal_ordinance_sections
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_ordinance_sections_versioned_delete BEFORE DELETE ON legal_ordinance_sections
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_rules_sections_versioned_insert BEFORE INSERT ON legal_rules_sections
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_rules_sections_versioned_update BEFORE UPDATE ON legal_rules_sections
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_rules_sections_versioned_delete BEFORE DELETE ON legal_rules_sections
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_reference_crosswalk_versioned_insert BEFORE INSERT ON legal_reference_crosswalk
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_reference_crosswalk_versioned_update BEFORE UPDATE ON legal_reference_crosswalk
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;
CREATE TRIGGER legal_reference_crosswalk_versioned_delete BEFORE DELETE ON legal_reference_crosswalk
WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_guards)
BEGIN SELECT RAISE(ABORT,'catalog_projection_read_only'); END;

DROP TRIGGER retrieval_activation_guard;
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
      OR d.reference_catalog_version IS NOT (SELECT active_version FROM legal_catalog_state WHERE singleton=1)
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

-- Retained v1 text manifests did not include catalog provenance columns.
ALTER TABLE document_generations ADD COLUMN manifest_version INTEGER NOT NULL DEFAULT 1;
