PRAGMA foreign_keys = ON;

ALTER TABLE documents ADD COLUMN case_number TEXT;
ALTER TABLE documents ADD COLUMN author_name TEXT;
ALTER TABLE documents ADD COLUMN outcome_label TEXT;
ALTER TABLE documents ADD COLUMN index_codes_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE documents ADD COLUMN rules_sections_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE documents ADD COLUMN ordinance_sections_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE documents ADD COLUMN extraction_confidence REAL NOT NULL DEFAULT 0;
ALTER TABLE documents ADD COLUMN extraction_warnings_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE documents ADD COLUMN qc_required_confirmed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE documents ADD COLUMN qc_confirmed_at TEXT;
ALTER TABLE documents ADD COLUMN rejected_at TEXT;
ALTER TABLE documents ADD COLUMN rejected_reason TEXT;

ALTER TABLE document_chunks ADD COLUMN paragraph_anchor_end TEXT;
ALTER TABLE document_chunks ADD COLUMN chunk_warnings_json TEXT NOT NULL DEFAULT '[]';

UPDATE document_chunks
SET paragraph_anchor_end = paragraph_anchor
WHERE paragraph_anchor_end IS NULL;

CREATE INDEX IF NOT EXISTS idx_documents_case_number
  ON documents (case_number);

CREATE INDEX IF NOT EXISTS idx_documents_qc_review
  ON documents (qc_passed, qc_required_confirmed, approved_at, rejected_at);
