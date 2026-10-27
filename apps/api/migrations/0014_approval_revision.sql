-- Material evidence and validation changes revoke approval, even through older tooling.
ALTER TABLE documents ADD COLUMN approval_policy_version TEXT;
CREATE TABLE document_revision_guards (
  token TEXT PRIMARY KEY,
  document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  expected_revision INTEGER NOT NULL
);
CREATE TRIGGER document_revision_guard BEFORE INSERT ON document_revision_guards
BEGIN
  SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM documents WHERE id = NEW.document_id AND document_revision = NEW.expected_revision)
    THEN RAISE(ABORT, 'document_revision_conflict') END;
END;
CREATE TRIGGER document_evidence_changed AFTER UPDATE OF file_type, jurisdiction, title, citation, source_r2_key, index_codes_json, rules_sections_json, ordinance_sections_json, case_number, decision_date, author_name, outcome_label, extraction_confidence, extraction_warnings_json, metadata_json, qc_has_index_codes, qc_has_rules_section, qc_has_ordinance_section, qc_passed, rejected_at, rejected_reason ON documents
WHEN OLD.file_type IS NOT NEW.file_type
  OR OLD.jurisdiction IS NOT NEW.jurisdiction
  OR OLD.title IS NOT NEW.title
  OR OLD.citation IS NOT NEW.citation
  OR OLD.source_r2_key IS NOT NEW.source_r2_key
  OR OLD.index_codes_json IS NOT NEW.index_codes_json
  OR OLD.rules_sections_json IS NOT NEW.rules_sections_json
  OR OLD.ordinance_sections_json IS NOT NEW.ordinance_sections_json
  OR OLD.case_number IS NOT NEW.case_number
  OR OLD.decision_date IS NOT NEW.decision_date
  OR OLD.author_name IS NOT NEW.author_name
  OR OLD.outcome_label IS NOT NEW.outcome_label
  OR OLD.extraction_confidence IS NOT NEW.extraction_confidence
  OR OLD.extraction_warnings_json IS NOT NEW.extraction_warnings_json
  OR OLD.metadata_json IS NOT NEW.metadata_json
  OR OLD.qc_has_index_codes IS NOT NEW.qc_has_index_codes
  OR OLD.qc_has_rules_section IS NOT NEW.qc_has_rules_section
  OR OLD.qc_has_ordinance_section IS NOT NEW.qc_has_ordinance_section
  OR OLD.qc_passed IS NOT NEW.qc_passed
  OR OLD.rejected_at IS NOT NEW.rejected_at
  OR OLD.rejected_reason IS NOT NEW.rejected_reason
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = NEW.id;
END;
CREATE TRIGGER document_confirmation_changed AFTER UPDATE OF qc_required_confirmed ON documents
WHEN OLD.qc_required_confirmed IS NOT NEW.qc_required_confirmed
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL,
    approved_revision = NULL, approval_policy_version = NULL WHERE id = NEW.id;
END;
CREATE TRIGGER document_reference_links_revision_insert AFTER INSERT ON document_reference_links
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_reference_links_revision_update AFTER UPDATE ON document_reference_links
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_reference_links_revision_delete AFTER DELETE ON document_reference_links
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_reference_issues_revision_insert AFTER INSERT ON document_reference_issues
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_reference_issues_revision_update AFTER UPDATE ON document_reference_issues
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = NEW.document_id AND ingestion_state = 'ready';
END;
CREATE TRIGGER document_reference_issues_revision_delete AFTER DELETE ON document_reference_issues
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;

CREATE TRIGGER document_readiness_changed AFTER UPDATE OF ingestion_state, ingestion_completed_at, expected_chunk_count ON documents
WHEN OLD.ingestion_state = 'ready' AND (OLD.ingestion_state IS NOT NEW.ingestion_state
 
  OR OLD.ingestion_completed_at IS NOT NEW.ingestion_completed_at
  OR OLD.expected_chunk_count IS NOT NEW.expected_chunk_count)
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL WHERE id = NEW.id;
END;

CREATE TRIGGER document_reference_links_revision_move AFTER UPDATE OF document_id ON document_reference_links
WHEN OLD.document_id IS NOT NEW.document_id
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;

CREATE TRIGGER document_reference_issues_revision_move AFTER UPDATE OF document_id ON document_reference_issues
WHEN OLD.document_id IS NOT NEW.document_id
BEGIN
  UPDATE documents SET document_revision = document_revision + 1, approved_at = NULL, approved_revision = NULL,
    approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL
    WHERE id = OLD.document_id AND ingestion_state = 'ready';
END;
