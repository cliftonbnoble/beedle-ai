PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS documents (
  id TEXT PRIMARY KEY,
  file_type TEXT NOT NULL CHECK (file_type IN ('decision_docx', 'law_pdf')),
  jurisdiction TEXT NOT NULL,
  title TEXT NOT NULL,
  citation TEXT NOT NULL,
  decision_date TEXT,
  source_r2_key TEXT NOT NULL,
  source_link TEXT NOT NULL,
  qc_has_index_codes INTEGER NOT NULL DEFAULT 0,
  qc_has_rules_section INTEGER NOT NULL DEFAULT 0,
  qc_has_ordinance_section INTEGER NOT NULL DEFAULT 0,
  qc_passed INTEGER NOT NULL DEFAULT 0,
  approved_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_filters
  ON documents (jurisdiction, file_type, decision_date, approved_at);

CREATE TABLE IF NOT EXISTS document_sections (
  id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL,
  canonical_key TEXT NOT NULL,
  heading TEXT NOT NULL,
  section_order INTEGER NOT NULL,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_sections_document
  ON document_sections (document_id, section_order);

CREATE TABLE IF NOT EXISTS section_paragraphs (
  id TEXT PRIMARY KEY,
  section_id TEXT NOT NULL,
  anchor TEXT NOT NULL,
  paragraph_order INTEGER NOT NULL,
  text TEXT NOT NULL,
  FOREIGN KEY (section_id) REFERENCES document_sections(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_paragraphs_section
  ON section_paragraphs (section_id, paragraph_order);

CREATE INDEX IF NOT EXISTS idx_paragraphs_anchor
  ON section_paragraphs (anchor);
