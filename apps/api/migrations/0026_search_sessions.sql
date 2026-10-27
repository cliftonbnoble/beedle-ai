-- Per-request public-corpus result snapshots; no shared query cache.
CREATE TABLE search_session_capacity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),reserved_bytes INTEGER NOT NULL DEFAULT 0 CHECK(reserved_bytes>=0),reserved_count INTEGER NOT NULL DEFAULT 0 CHECK(reserved_count>=0));
INSERT INTO search_session_capacity(singleton) VALUES(1);
CREATE TABLE search_sessions(
 id TEXT PRIMARY KEY,secret TEXT NOT NULL,request_hash TEXT NOT NULL,
 scope TEXT NOT NULL,ranking_version TEXT NOT NULL,corpus_revision INTEGER NOT NULL,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 payload_bytes INTEGER NOT NULL CHECK(payload_bytes=length(CAST(payload_json AS BLOB)) AND payload_bytes<=524288),
 created_at INTEGER NOT NULL,expires_at INTEGER NOT NULL CHECK(expires_at>created_at));
CREATE INDEX search_sessions_expiry ON search_sessions(expires_at,id);
CREATE TRIGGER search_session_reserved_insert AFTER INSERT ON search_sessions BEGIN UPDATE search_session_capacity SET reserved_bytes=reserved_bytes+NEW.payload_bytes,reserved_count=reserved_count+1 WHERE singleton=1; END;
CREATE TRIGGER search_session_reserved_delete AFTER DELETE ON search_sessions BEGIN UPDATE search_session_capacity SET reserved_bytes=reserved_bytes-OLD.payload_bytes,reserved_count=reserved_count-1 WHERE singleton=1; END;
CREATE TRIGGER search_session_immutable BEFORE UPDATE ON search_sessions BEGIN SELECT RAISE(ABORT,'search_session_immutable'); END;
-- Existing document/source/facet/catalog changes already advance this epoch.
-- Cover direct valid-reference changes and a delayed M07 index cutover too.
CREATE TRIGGER search_reference_insert AFTER INSERT ON document_reference_links BEGIN UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1; END;
CREATE TRIGGER search_reference_update AFTER UPDATE ON document_reference_links BEGIN UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1; END;
CREATE TRIGGER search_reference_delete AFTER DELETE ON document_reference_links BEGIN UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1; END;
CREATE TRIGGER search_phrase_cutover_epoch AFTER UPDATE OF phase ON search_phrase_build
 WHEN NEW.phase='ready' AND OLD.phase<>'ready'
 BEGIN UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1; END;
