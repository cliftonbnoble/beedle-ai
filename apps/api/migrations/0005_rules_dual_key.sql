PRAGMA foreign_keys = ON;

ALTER TABLE legal_rules_sections ADD COLUMN canonical_bare_citation TEXT;
ALTER TABLE legal_rules_sections ADD COLUMN normalized_bare_citation TEXT;

UPDATE legal_rules_sections
SET canonical_bare_citation = TRIM(section_number),
    normalized_bare_citation = LOWER(TRIM(section_number))
WHERE canonical_bare_citation IS NULL OR normalized_bare_citation IS NULL;

CREATE INDEX IF NOT EXISTS idx_rules_norm_bare
  ON legal_rules_sections (normalized_bare_citation, active);
