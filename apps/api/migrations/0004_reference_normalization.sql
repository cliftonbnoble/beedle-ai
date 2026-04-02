PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS legal_index_codes (
  id TEXT PRIMARY KEY,
  code_identifier TEXT NOT NULL UNIQUE,
  normalized_code TEXT NOT NULL UNIQUE,
  family TEXT,
  label TEXT,
  description TEXT,
  is_reserved INTEGER NOT NULL DEFAULT 0,
  is_legacy_pre_1002 INTEGER NOT NULL DEFAULT 0,
  linked_ordinance_sections_json TEXT NOT NULL DEFAULT '[]',
  linked_rules_sections_json TEXT NOT NULL DEFAULT '[]',
  source_page_anchor TEXT,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legal_ordinance_sections (
  id TEXT PRIMARY KEY,
  section_number TEXT NOT NULL,
  subsection_path TEXT,
  citation TEXT NOT NULL UNIQUE,
  normalized_citation TEXT NOT NULL UNIQUE,
  heading TEXT,
  body_text TEXT NOT NULL,
  source_page_anchor TEXT,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legal_rules_sections (
  id TEXT PRIMARY KEY,
  part TEXT,
  section_number TEXT NOT NULL,
  citation TEXT NOT NULL UNIQUE,
  normalized_citation TEXT NOT NULL UNIQUE,
  heading TEXT,
  body_text TEXT NOT NULL,
  source_page_anchor TEXT,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legal_reference_crosswalk (
  id TEXT PRIMARY KEY,
  index_code_id TEXT,
  ordinance_citation TEXT,
  rules_citation TEXT,
  source TEXT NOT NULL DEFAULT 'normalized_import',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legal_reference_sources (
  source_key TEXT PRIMARY KEY,
  source_path TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS document_reference_links (
  id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL,
  reference_type TEXT NOT NULL CHECK (reference_type IN ('index_code', 'rules_section', 'ordinance_section')),
  raw_value TEXT NOT NULL,
  normalized_value TEXT NOT NULL,
  canonical_value TEXT,
  is_valid INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS document_reference_issues (
  id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL,
  reference_type TEXT NOT NULL CHECK (reference_type IN ('index_code', 'rules_section', 'ordinance_section')),
  raw_value TEXT NOT NULL,
  normalized_value TEXT NOT NULL,
  message TEXT NOT NULL,
  severity TEXT NOT NULL DEFAULT 'warning',
  created_at TEXT NOT NULL,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_index_codes_norm
  ON legal_index_codes (normalized_code, active);

CREATE INDEX IF NOT EXISTS idx_ordinance_norm
  ON legal_ordinance_sections (normalized_citation, active);

CREATE INDEX IF NOT EXISTS idx_rules_norm
  ON legal_rules_sections (normalized_citation, active);

CREATE INDEX IF NOT EXISTS idx_doc_ref_links_doc
  ON document_reference_links (document_id, reference_type, normalized_value, is_valid);

CREATE INDEX IF NOT EXISTS idx_doc_ref_issues_doc
  ON document_reference_issues (document_id, reference_type, normalized_value);
