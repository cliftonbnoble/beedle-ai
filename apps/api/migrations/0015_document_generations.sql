-- Staging and retained generations are never read by public retrieval.
ALTER TABLE documents ADD COLUMN active_text_generation TEXT NOT NULL DEFAULT 'initial';
ALTER TABLE documents ADD COLUMN reviewer_overrides_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE document_vector_jobs ADD COLUMN generation_id TEXT NOT NULL DEFAULT 'initial';
CREATE TABLE document_generations (
  id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  base_revision INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('staging','ready','published','retained','failed')),
  source_key TEXT NOT NULL,
  source_sha256 TEXT NOT NULL,
  snapshot_json TEXT NOT NULL,
  manifest_json TEXT NOT NULL DEFAULT '{}',
  previous_generation TEXT,
  created_at TEXT NOT NULL,
  published_at TEXT,
  error TEXT
);
CREATE INDEX idx_document_generations_doc ON document_generations(document_id, created_at);
CREATE TABLE staged_document_sections (
  generation_id TEXT NOT NULL REFERENCES document_generations(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  canonical_key TEXT NOT NULL,
  heading TEXT NOT NULL,
  section_order INTEGER NOT NULL,
  section_text TEXT,
  PRIMARY KEY (generation_id, id)
);
CREATE TABLE staged_section_paragraphs (
  generation_id TEXT NOT NULL REFERENCES document_generations(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  section_id TEXT NOT NULL,
  anchor TEXT NOT NULL,
  paragraph_order INTEGER NOT NULL,
  text TEXT NOT NULL,
  PRIMARY KEY (generation_id, id),
  FOREIGN KEY (generation_id, section_id) REFERENCES staged_document_sections(generation_id, id)
);
CREATE TABLE staged_document_chunks (
  generation_id TEXT NOT NULL REFERENCES document_generations(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  section_id TEXT NOT NULL,
  paragraph_id TEXT NOT NULL,
  paragraph_anchor TEXT NOT NULL,
  citation_anchor TEXT NOT NULL,
  section_label TEXT NOT NULL,
  chunk_order INTEGER NOT NULL,
  chunk_text TEXT NOT NULL,
  token_estimate INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  paragraph_anchor_end TEXT,
  chunk_warnings_json TEXT NOT NULL,
  PRIMARY KEY (generation_id, id),
  FOREIGN KEY (generation_id, section_id) REFERENCES staged_document_sections(generation_id, id),
  FOREIGN KEY (generation_id, paragraph_id) REFERENCES staged_section_paragraphs(generation_id, id)
);
CREATE TABLE staged_document_reference_links (
  generation_id TEXT NOT NULL REFERENCES document_generations(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  reference_type TEXT NOT NULL,
  raw_value TEXT NOT NULL,
  normalized_value TEXT NOT NULL,
  canonical_value TEXT,
  is_valid INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (generation_id, id)
);
CREATE TABLE staged_document_reference_issues (
  generation_id TEXT NOT NULL REFERENCES document_generations(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  reference_type TEXT NOT NULL,
  raw_value TEXT NOT NULL,
  normalized_value TEXT NOT NULL,
  message TEXT NOT NULL,
  severity TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (generation_id, id)
);
CREATE TRIGGER document_overrides_changed AFTER UPDATE OF reviewer_overrides_json ON documents
WHEN OLD.reviewer_overrides_json IS NOT NEW.reviewer_overrides_json
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = NEW.id;
END;
CREATE TRIGGER document_sections_text_revision_insert AFTER INSERT ON document_sections
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_sections_text_revision_update AFTER UPDATE ON document_sections
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_sections_text_revision_delete AFTER DELETE ON document_sections
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER section_paragraphs_text_revision_insert AFTER INSERT ON section_paragraphs
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = (SELECT document_id FROM document_sections WHERE id = NEW.section_id) AND ingestion_state = 'ready';
END;
CREATE TRIGGER section_paragraphs_text_revision_update AFTER UPDATE ON section_paragraphs
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = (SELECT document_id FROM document_sections WHERE id = NEW.section_id) AND ingestion_state = 'ready';
END;
CREATE TRIGGER section_paragraphs_text_revision_delete AFTER DELETE ON section_paragraphs
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = (SELECT document_id FROM document_sections WHERE id = OLD.section_id) AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_chunks_text_revision_insert AFTER INSERT ON document_chunks
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_chunks_text_revision_update AFTER UPDATE ON document_chunks
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_chunks_text_revision_delete AFTER DELETE ON document_chunks
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL, document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;
-- A move changes the source document as well as the destination. Keep the old
-- document held even when its remaining rows still satisfy a count-only check.
CREATE TRIGGER document_sections_text_revision_move AFTER UPDATE OF document_id ON document_sections
WHEN OLD.document_id IS NOT NEW.document_id
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL,
    document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER section_paragraphs_text_revision_move AFTER UPDATE OF section_id ON section_paragraphs
WHEN OLD.section_id IS NOT NEW.section_id
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL,
    document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = (SELECT document_id FROM document_sections WHERE id = OLD.section_id) AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_chunks_text_revision_move AFTER UPDATE OF document_id ON document_chunks
WHEN OLD.document_id IS NOT NEW.document_id
BEGIN
  UPDATE documents SET ingestion_state = 'legacy', ingestion_completed_at = NULL, expected_chunk_count = NULL,
    document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;
