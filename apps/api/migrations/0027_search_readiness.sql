-- Retire request-owned schema/bootstrap. Reassert additive objects here;
-- populated facet reconciliation belongs to the bounded maintenance builder.
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS document_index_codes (
  document_id TEXT NOT NULL,
  code TEXT NOT NULL,
  normalized_code TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (document_id, normalized_code),
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_index_codes_normalized
  ON document_index_codes (normalized_code, document_id);

CREATE TABLE IF NOT EXISTS document_rules_sections (
  document_id TEXT NOT NULL,
  section TEXT NOT NULL,
  normalized_section TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (document_id, normalized_section),
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_rules_sections_normalized
  ON document_rules_sections (normalized_section, document_id);

CREATE TABLE IF NOT EXISTS document_ordinance_sections (
  document_id TEXT NOT NULL,
  section TEXT NOT NULL,
  normalized_section TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (document_id, normalized_section),
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_ordinance_sections_normalized
  ON document_ordinance_sections (normalized_section, document_id);

CREATE TRIGGER IF NOT EXISTS documents_ai_document_facets
AFTER INSERT ON documents
BEGIN
  INSERT OR IGNORE INTO document_index_codes (document_id, code, normalized_code)
  SELECT new.id, code, normalized_code
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS code,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ic-%'
          OR lower(trim(CAST(je.value AS TEXT))) LIKE 'ic %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 4)
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_code
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.index_codes_json, '[]')) THEN COALESCE(new.index_codes_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE code != '' AND normalized_code != '';

  INSERT OR IGNORE INTO document_rules_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 6)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 7))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.rules_sections_json, '[]')) THEN COALESCE(new.rules_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';

  INSERT OR IGNORE INTO document_ordinance_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 11)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 12))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.ordinance_sections_json, '[]')) THEN COALESCE(new.ordinance_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';
END;

CREATE TRIGGER IF NOT EXISTS documents_au_document_facets
AFTER UPDATE OF index_codes_json, rules_sections_json, ordinance_sections_json ON documents
BEGIN
  DELETE FROM document_index_codes WHERE document_id = new.id;
  DELETE FROM document_rules_sections WHERE document_id = new.id;
  DELETE FROM document_ordinance_sections WHERE document_id = new.id;

  INSERT OR IGNORE INTO document_index_codes (document_id, code, normalized_code)
  SELECT new.id, code, normalized_code
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS code,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ic-%'
          OR lower(trim(CAST(je.value AS TEXT))) LIKE 'ic %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 4)
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_code
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.index_codes_json, '[]')) THEN COALESCE(new.index_codes_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE code != '' AND normalized_code != '';

  INSERT OR IGNORE INTO document_rules_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 6)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 7))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.rules_sections_json, '[]')) THEN COALESCE(new.rules_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';

  INSERT OR IGNORE INTO document_ordinance_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 11)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 12))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.ordinance_sections_json, '[]')) THEN COALESCE(new.ordinance_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';
END;

CREATE TRIGGER IF NOT EXISTS documents_ad_document_facets
AFTER DELETE ON documents
BEGIN
  DELETE FROM document_index_codes WHERE document_id = old.id;
  DELETE FROM document_rules_sections WHERE document_id = old.id;
  DELETE FROM document_ordinance_sections WHERE document_id = old.id;
END;

CREATE INDEX IF NOT EXISTS idx_documents_author_name_lookup
  ON documents (lower(coalesce(author_name, '')), file_type, rejected_at, approved_at, decision_date, searchable_at);

CREATE INDEX IF NOT EXISTS idx_documents_search_runtime
  ON documents (file_type, rejected_at, approved_at, searchable_at, decision_date);

CREATE INDEX IF NOT EXISTS idx_retrieval_search_chunks_doc
  ON retrieval_search_chunks (document_id, active, batch_id);
-- PROD-PERF-01: the corpus-scope WHERE used by every search path embeds
--   EXISTS (SELECT 1 FROM document_chunks c_basic WHERE c_basic.document_id = d.id)
-- and document_chunks(document_id) had no index, so each evaluated document row scanned up to
-- ~467k chunk rows. Measured on production D1: 24.5s inside a single vector chunk fetch.
-- The runtime safety net (ensureSearchRuntimeIndexes) creates this same index on first search,
-- so this migration is a durable no-op checkpoint recording it in the schema history.
CREATE INDEX IF NOT EXISTS idx_document_chunks_doc
  ON document_chunks (document_id);

CREATE VIRTUAL TABLE IF NOT EXISTS search_chunks_fts_vocab USING fts5vocab(search_chunks_fts, 'row');
CREATE TABLE search_schema_readiness(singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_version INTEGER NOT NULL);
INSERT INTO search_schema_readiness VALUES(1,27);
CREATE TABLE search_facet_build(singleton INTEGER PRIMARY KEY CHECK(singleton=1),phase TEXT NOT NULL CHECK(phase IN ('pending','ready')),after_id TEXT NOT NULL DEFAULT '',visited INTEGER NOT NULL DEFAULT 0,owner_token TEXT,lease_expires_at TEXT,updated_at TEXT NOT NULL);
INSERT INTO search_facet_build(singleton,phase,updated_at) SELECT 1,CASE WHEN EXISTS(SELECT 1 FROM documents) THEN 'pending' ELSE 'ready' END,strftime('%Y-%m-%dT%H:%M:%fZ','now');
CREATE TABLE search_facet_build_guards(token TEXT PRIMARY KEY,owner_token TEXT NOT NULL);
CREATE TRIGGER search_facet_build_guard_check BEFORE INSERT ON search_facet_build_guards
 WHEN NOT EXISTS(SELECT 1 FROM search_facet_build WHERE singleton=1 AND owner_token=NEW.owner_token AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 BEGIN SELECT RAISE(ABORT,'search_facet_build_lease_lost'); END;
