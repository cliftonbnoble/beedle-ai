CREATE TABLE vector_profiles (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL UNIQUE,
  environment TEXT NOT NULL,
  model TEXT NOT NULL,
  dimensions INTEGER NOT NULL CHECK(dimensions BETWEEN 1 AND 4096),
  pipeline_version TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE vector_entries (
  vector_id TEXT PRIMARY KEY,
  profile_id TEXT NOT NULL REFERENCES vector_profiles(id),
  logical_key TEXT NOT NULL,
  document_id TEXT NOT NULL,
  source_kind TEXT NOT NULL CHECK(source_kind IN ('document','retrieval')),
  source_generation TEXT NOT NULL,
  chunk_id TEXT NOT NULL,
  chunk_sha256 TEXT NOT NULL,
  chunk_text TEXT NOT NULL,
  source_sha256 TEXT NOT NULL,
  parser_version TEXT NOT NULL,
  chunker_version TEXT NOT NULL,
  prepared_revision INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('prepared','submitting','submitted','verified','uncertain','deleting','deleted')),
  values_json TEXT CHECK(values_json IS NULL OR json_valid(values_json)),
  metadata_live INTEGER NOT NULL DEFAULT 0 CHECK(metadata_live IN (0,1)),
  created_at TEXT NOT NULL,
  submitted_at TEXT,
  verified_at TEXT,
  obsolete_at TEXT,
  deleted_at TEXT
);
CREATE UNIQUE INDEX vector_entry_current_identity ON vector_entries(logical_key) WHERE state NOT IN ('uncertain','deleting','deleted');
CREATE INDEX vector_entries_document ON vector_entries(document_id,profile_id,source_kind,source_generation);
CREATE INDEX vector_entries_cleanup ON vector_entries(obsolete_at,state,vector_id);
CREATE TABLE vector_lifecycle_dirty (
  document_id TEXT PRIMARY KEY,
  revision INTEGER NOT NULL DEFAULT 1,
  dirty INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL
);
INSERT INTO vector_lifecycle_dirty(document_id,updated_at) SELECT id,strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM documents;
CREATE TABLE vector_profile_sync (
  profile_id TEXT NOT NULL REFERENCES vector_profiles(id),
  document_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(profile_id,document_id)
);
CREATE TRIGGER vector_lifecycle_document_insert AFTER INSERT ON documents BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_lifecycle_document_update AFTER UPDATE ON documents BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_lifecycle_document_delete AFTER DELETE ON documents BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(OLD.id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_lifecycle_retrieval_update AFTER UPDATE OF active,chunk_text ON retrieval_search_chunks BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(NEW.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_lifecycle_retrieval_delete AFTER DELETE ON retrieval_search_chunks BEGIN
  INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(OLD.document_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  ON CONFLICT(document_id) DO UPDATE SET revision=revision+1,dirty=1,updated_at=excluded.updated_at;
END;
CREATE TRIGGER vector_profile_immutable BEFORE UPDATE ON vector_profiles BEGIN SELECT RAISE(ABORT,'vector_profile_immutable'); END;
CREATE TRIGGER vector_entry_identity_immutable BEFORE UPDATE OF vector_id,profile_id,logical_key,chunk_text,document_id,source_kind,source_generation,chunk_id,chunk_sha256,source_sha256,parser_version,chunker_version,prepared_revision ON vector_entries
BEGIN SELECT RAISE(ABORT,'vector_identity_immutable'); END;
ALTER TABLE document_vector_jobs ADD COLUMN embedding_profile_id TEXT;

CREATE TABLE vector_lifecycle_epoch(singleton INTEGER PRIMARY KEY CHECK(singleton=1),revision INTEGER NOT NULL);
INSERT INTO vector_lifecycle_epoch VALUES(1,1);
CREATE TRIGGER vector_epoch_insert AFTER INSERT ON vector_lifecycle_dirty BEGIN UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1; END;
CREATE TRIGGER vector_epoch_update AFTER UPDATE OF revision ON vector_lifecycle_dirty BEGIN UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1; END;
CREATE TABLE vector_sync_locks (
  profile_id TEXT NOT NULL REFERENCES vector_profiles(id),
  document_id TEXT NOT NULL,
  owner_token TEXT,
  lease_expires_at TEXT,
  pending_json TEXT CHECK(pending_json IS NULL OR json_valid(pending_json)),
  uncertain INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(profile_id,document_id)
);
ALTER TABLE vector_entries ADD COLUMN embedding_owner TEXT;
ALTER TABLE vector_entries ADD COLUMN embedding_lease_until TEXT;
CREATE TABLE vector_submission_guards(token TEXT PRIMARY KEY,expected_json TEXT NOT NULL);
CREATE TRIGGER vector_submission_guard_check BEFORE INSERT ON vector_submission_guards
WHEN (SELECT COUNT(*) FROM vector_entries WHERE vector_id IN(SELECT value FROM json_each(NEW.expected_json)) AND state='prepared')!=json_array_length(NEW.expected_json)
BEGIN SELECT RAISE(ABORT,'vector_submission_conflict'); END;
