ALTER TABLE vector_entries ADD COLUMN section_label TEXT NOT NULL DEFAULT '';
ALTER TABLE vector_entries ADD COLUMN filter_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(filter_json));
UPDATE vector_entries SET section_label=COALESCE(
  CASE WHEN source_kind='document' THEN (SELECT c.section_label FROM document_chunks c WHERE c.id=vector_entries.chunk_id)
  ELSE (SELECT c.section_label FROM retrieval_search_chunks c WHERE c.chunk_id=vector_entries.chunk_id) END,
  (SELECT c.section_label FROM staged_document_chunks c WHERE c.id=vector_entries.chunk_id ORDER BY generation_id LIMIT 1),'');
CREATE TRIGGER vector_section_identity_immutable BEFORE UPDATE OF section_label ON vector_entries
BEGIN SELECT RAISE(ABORT,'vector_section_identity_immutable'); END;
CREATE TABLE vector_document_metadata (
  document_id TEXT PRIMARY KEY,
  root_revision INTEGER NOT NULL,
  cohort_key TEXT NOT NULL,
  cohort_exact INTEGER NOT NULL,
  metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json))
);
CREATE INDEX vector_metadata_cohorts ON vector_document_metadata(cohort_key,document_id);
UPDATE vector_lifecycle_dirty SET revision=revision+1,dirty=1;
CREATE TRIGGER vector_catalog_scope_change AFTER UPDATE OF active_version,epoch ON legal_catalog_state
WHEN OLD.active_version IS NOT NEW.active_version OR OLD.epoch IS NOT NEW.epoch
BEGIN UPDATE vector_lifecycle_dirty SET revision=revision+1,dirty=1; END;
CREATE TRIGGER vector_scope_document_index_codes_insert AFTER INSERT ON document_index_codes
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_index_codes_update AFTER UPDATE ON document_index_codes
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_index_codes_delete AFTER DELETE ON document_index_codes
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(OLD.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_rules_sections_insert AFTER INSERT ON document_rules_sections
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_rules_sections_update AFTER UPDATE ON document_rules_sections
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_rules_sections_delete AFTER DELETE ON document_rules_sections
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(OLD.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_ordinance_sections_insert AFTER INSERT ON document_ordinance_sections
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_ordinance_sections_update AFTER UPDATE ON document_ordinance_sections
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_scope_document_ordinance_sections_delete AFTER DELETE ON document_ordinance_sections
BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(OLD.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
-- Kept separate from v1 checkpoints so an older Worker cannot accidentally
-- acknowledge a newer filter projection during a gradual deployment.
CREATE TABLE vector_filter_sync (
  profile_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  PRIMARY KEY(profile_id,document_id)
);
