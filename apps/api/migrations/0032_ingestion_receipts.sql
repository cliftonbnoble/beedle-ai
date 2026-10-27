-- Receipts are deployment scoped until authenticated caller namespaces are introduced.
CREATE TABLE ingestion_requests (
 request_id TEXT PRIMARY KEY, operation TEXT NOT NULL, fingerprint TEXT NOT NULL,
 source_sha256 TEXT NOT NULL, document_id TEXT NOT NULL UNIQUE, source_key TEXT NOT NULL UNIQUE,
 record_id TEXT NOT NULL, source_revision INTEGER NOT NULL CHECK(source_revision>0),
 predecessor_document_id TEXT, predecessor_revision INTEGER,
 state TEXT NOT NULL CHECK(state IN ('processing','retryable','complete','conflict')),
 owner_token TEXT, lease_expires_at TEXT, result_json TEXT,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 CHECK((state='complete')=(result_json IS NOT NULL))
);
CREATE INDEX ingestion_requests_state ON ingestion_requests(state,lease_expires_at);
CREATE TABLE ingestion_write_guard(singleton INTEGER PRIMARY KEY CHECK(singleton=1));
CREATE TABLE source_records(record_id TEXT PRIMARY KEY,published_document_id TEXT,published_source_revision INTEGER NOT NULL DEFAULT 0);
CREATE TABLE document_source_revisions(
 document_id TEXT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
 record_id TEXT NOT NULL REFERENCES source_records(record_id), source_revision INTEGER NOT NULL CHECK(source_revision>0),
 predecessor_document_id TEXT, source_sha256 TEXT, identity_origin TEXT NOT NULL CHECK(identity_origin IN ('legacy','verified_upload')),
 UNIQUE(record_id,source_revision), UNIQUE(predecessor_document_id)
);
CREATE INDEX document_source_revisions_hash ON document_source_revisions(source_sha256,document_id);
-- Existing records remain distinct. Old declared hashes are not treated as verified uploads.
INSERT INTO source_records SELECT id,CASE WHEN searchable_at IS NOT NULL AND published_revision=document_revision THEN id ELSE NULL END,
 CASE WHEN searchable_at IS NOT NULL AND published_revision=document_revision THEN 1 ELSE 0 END FROM documents;
INSERT INTO document_source_revisions(document_id,record_id,source_revision,identity_origin) SELECT id,id,1,'legacy' FROM documents;
CREATE TRIGGER source_revision_published AFTER UPDATE OF searchable_at,published_revision ON documents
 WHEN NEW.searchable_at IS NOT NULL AND NEW.published_revision=NEW.document_revision AND NEW.ingestion_state='ready'
 BEGIN
 UPDATE source_records SET published_document_id=NEW.id,
 published_source_revision=(SELECT source_revision FROM document_source_revisions WHERE document_id=NEW.id)
 WHERE record_id=(SELECT record_id FROM document_source_revisions WHERE document_id=NEW.id)
 AND published_source_revision<(SELECT source_revision FROM document_source_revisions WHERE document_id=NEW.id);
 END;
CREATE TRIGGER source_record_publication_changed AFTER UPDATE OF published_document_id ON source_records
 WHEN OLD.published_document_id IS NOT NEW.published_document_id
 BEGIN
 -- Reuse document dirty/epoch triggers so snapshots, reviewer rows and vector
 -- filters cannot continue advertising the superseded logical copy.
 UPDATE documents SET updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=OLD.published_document_id OR id=NEW.published_document_id;
 END;
