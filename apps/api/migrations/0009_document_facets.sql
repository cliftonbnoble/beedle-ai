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

INSERT OR IGNORE INTO document_index_codes (document_id, code, normalized_code)
SELECT document_id, code, normalized_code
FROM (
  SELECT
    d.id AS document_id,
    trim(CAST(je.value AS TEXT)) AS code,
    CASE
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ic-%'
        OR lower(trim(CAST(je.value AS TEXT))) LIKE 'ic %'
        THEN substr(lower(trim(CAST(je.value AS TEXT))), 4)
      ELSE lower(trim(CAST(je.value AS TEXT)))
    END AS normalized_code
  FROM documents d
  JOIN json_each(
    CASE
      WHEN json_valid(COALESCE(d.index_codes_json, '[]')) THEN COALESCE(d.index_codes_json, '[]')
      ELSE '[]'
    END
  ) je
)
WHERE code != '' AND normalized_code != '';

INSERT OR IGNORE INTO document_rules_sections (document_id, section, normalized_section)
SELECT document_id, section, normalized_section
FROM (
  SELECT
    d.id AS document_id,
    trim(CAST(je.value AS TEXT)) AS section,
    CASE
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule %'
        THEN substr(lower(trim(CAST(je.value AS TEXT))), 6)
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule.%'
        THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 7))
      ELSE lower(trim(CAST(je.value AS TEXT)))
    END AS normalized_section
  FROM documents d
  JOIN json_each(
    CASE
      WHEN json_valid(COALESCE(d.rules_sections_json, '[]')) THEN COALESCE(d.rules_sections_json, '[]')
      ELSE '[]'
    END
  ) je
)
WHERE section != '' AND normalized_section != '';

INSERT OR IGNORE INTO document_ordinance_sections (document_id, section, normalized_section)
SELECT document_id, section, normalized_section
FROM (
  SELECT
    d.id AS document_id,
    trim(CAST(je.value AS TEXT)) AS section,
    CASE
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance %'
        THEN substr(lower(trim(CAST(je.value AS TEXT))), 11)
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance.%'
        THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 12))
      ELSE lower(trim(CAST(je.value AS TEXT)))
    END AS normalized_section
  FROM documents d
  JOIN json_each(
    CASE
      WHEN json_valid(COALESCE(d.ordinance_sections_json, '[]')) THEN COALESCE(d.ordinance_sections_json, '[]')
      ELSE '[]'
    END
  ) je
)
WHERE section != '' AND normalized_section != '';

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
