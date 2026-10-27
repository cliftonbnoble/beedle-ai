-- Generation stamps apply after an explicit cache repair. Existing healthy
-- bootstrap/cutover jobs remain compatible until that maintenance action.
ALTER TABLE search_body_identity ADD COLUMN index_generation TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE search_body_identity ADD COLUMN content_revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE search_body_identity ADD COLUMN indexed_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE search_document_identity ADD COLUMN index_generation TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE search_document_identity ADD COLUMN content_revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE search_document_identity ADD COLUMN indexed_revision INTEGER NOT NULL DEFAULT 0;
CREATE TABLE search_index_rebuild(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 generation TEXT NOT NULL DEFAULT 'legacy',status TEXT NOT NULL DEFAULT 'idle' CHECK(status IN ('idle','running','completed')),
 phase TEXT NOT NULL DEFAULT 'idle',after_id TEXT NOT NULL DEFAULT '',visited INTEGER NOT NULL DEFAULT 0,
 upper_metadata TEXT NOT NULL DEFAULT '',upper_document TEXT NOT NULL DEFAULT '',upper_retrieval TEXT NOT NULL DEFAULT '',
 upper_body_mapping TEXT NOT NULL DEFAULT '0',upper_metadata_mapping TEXT NOT NULL DEFAULT '0',
 owner_token TEXT,lease_expires_at TEXT,last_error TEXT,started_at TEXT,completed_at TEXT,archived_generation TEXT);
INSERT INTO search_index_rebuild(singleton) VALUES(1);
CREATE TABLE search_index_rebuild_guards(token TEXT PRIMARY KEY,owner_token TEXT NOT NULL,generation TEXT NOT NULL);
CREATE TRIGGER search_index_rebuild_guard_check BEFORE INSERT ON search_index_rebuild_guards
 WHEN NOT EXISTS(SELECT 1 FROM search_index_rebuild WHERE singleton=1 AND generation=NEW.generation AND owner_token=NEW.owner_token AND status='running' AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 BEGIN SELECT RAISE(ABORT,'search_index_rebuild_lease_lost'); END;
CREATE TABLE search_index_generations(generation TEXT PRIMARY KEY,previous_generation TEXT NOT NULL,status TEXT NOT NULL,started_at TEXT NOT NULL,completed_at TEXT);
CREATE TABLE search_index_start_guards(token TEXT PRIMARY KEY,expected_generation TEXT NOT NULL,new_generation TEXT NOT NULL,restart INTEGER NOT NULL);
CREATE TRIGGER search_index_start_guard_check BEFORE INSERT ON search_index_start_guards
 WHEN NOT EXISTS(SELECT 1 FROM search_index_rebuild WHERE singleton=1 AND generation=NEW.expected_generation AND (status<>'running' OR NEW.restart=1) AND (owner_token IS NULL OR lease_expires_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now')))
 OR EXISTS(SELECT 1 FROM search_index_generations WHERE generation=NEW.new_generation)
 OR EXISTS(SELECT 1 FROM search_index_build WHERE owner_token IS NOT NULL AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 OR EXISTS(SELECT 1 FROM search_phrase_build WHERE owner_token IS NOT NULL AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 OR EXISTS(SELECT 1 FROM search_facet_build WHERE owner_token IS NOT NULL AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 BEGIN SELECT RAISE(ABORT,'search_index_rebuild_start_conflict'); END;
UPDATE search_schema_readiness SET schema_version=28 WHERE singleton=1;

-- Fence older bootstrap deliveries that claimed a lease just before a repair.
CREATE TRIGGER search_classic_body_rebuild_fence BEFORE INSERT ON search_index_build_guards
 WHEN EXISTS(SELECT 1 FROM search_index_rebuild WHERE status='running') BEGIN SELECT RAISE(ABORT,'search_index_rebuild_in_progress'); END;
CREATE TRIGGER search_classic_phrase_rebuild_fence BEFORE INSERT ON search_phrase_build_guards
 WHEN EXISTS(SELECT 1 FROM search_index_rebuild WHERE status='running') BEGIN SELECT RAISE(ABORT,'search_index_rebuild_in_progress'); END;
CREATE TRIGGER search_classic_facet_rebuild_fence BEFORE INSERT ON search_facet_build_guards
 WHEN EXISTS(SELECT 1 FROM search_index_rebuild WHERE status='running') BEGIN SELECT RAISE(ABORT,'search_index_rebuild_in_progress'); END;
