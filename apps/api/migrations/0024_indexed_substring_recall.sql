-- Substring recall uses source-backed views: token postings do not duplicate
-- the source text. Explicit integer identities permit indexed maintenance.
CREATE TABLE search_body_identity (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_kind TEXT NOT NULL CHECK(source_kind IN ('document','retrieval')),
  chunk_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  indexed INTEGER NOT NULL DEFAULT 0,
  UNIQUE(source_kind,chunk_id)
);
CREATE INDEX search_body_identity_document ON search_body_identity(document_id,source_kind);
CREATE VIEW search_body_content AS
  SELECT m.row_id,c.section_label,c.chunk_text FROM search_body_identity m JOIN document_chunks c ON m.source_kind='document' AND c.id=m.chunk_id
  UNION ALL SELECT m.row_id,c.section_label,c.chunk_text FROM search_body_identity m JOIN retrieval_search_chunks c ON m.source_kind='retrieval' AND c.chunk_id=m.chunk_id;
CREATE VIRTUAL TABLE search_body_substrings USING fts5(section_label,chunk_text,content='search_body_content',content_rowid='row_id',tokenize='trigram');
CREATE TABLE search_document_identity(row_id INTEGER PRIMARY KEY AUTOINCREMENT,document_id TEXT NOT NULL UNIQUE,indexed INTEGER NOT NULL DEFAULT 0);
CREATE VIEW search_document_content AS SELECT m.row_id,d.title,d.citation,COALESCE(d.author_name,'') AS author FROM search_document_identity m JOIN documents d ON d.id=m.document_id;
CREATE VIRTUAL TABLE search_metadata_substrings USING fts5(title,citation,author,content='search_document_content',content_rowid='row_id',tokenize='trigram');
CREATE TRIGGER document_chunks_substring_insert AFTER INSERT ON document_chunks BEGIN
  INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('document',NEW.id,NEW.document_id);
  INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=NEW.id AND indexed=0;
  UPDATE search_body_identity SET indexed=1 WHERE source_kind='document' AND chunk_id=NEW.id;
END;
CREATE TRIGGER document_chunks_substring_delete AFTER DELETE ON document_chunks BEGIN
  INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id AND indexed=1;
  DELETE FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id;
END;
CREATE TRIGGER document_chunks_substring_update AFTER UPDATE OF id,document_id,section_label,chunk_text ON document_chunks BEGIN
  INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id AND indexed=1;
  UPDATE search_body_identity SET chunk_id=NEW.id,document_id=NEW.document_id WHERE source_kind='document' AND chunk_id=OLD.id;
  INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('document',NEW.id,NEW.document_id);
  INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=NEW.id;
  UPDATE search_body_identity SET indexed=1 WHERE source_kind='document' AND chunk_id=NEW.id;
END;
CREATE TRIGGER retrieval_search_chunks_substring_insert AFTER INSERT ON retrieval_search_chunks BEGIN
  INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('retrieval',NEW.chunk_id,NEW.document_id);
  INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id AND indexed=0;
  UPDATE search_body_identity SET indexed=1 WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id;
END;
CREATE TRIGGER retrieval_search_chunks_substring_delete AFTER DELETE ON retrieval_search_chunks BEGIN
  INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id AND indexed=1;
  DELETE FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id;
END;
CREATE TRIGGER retrieval_search_chunks_substring_update AFTER UPDATE OF chunk_id,document_id,section_label,chunk_text ON retrieval_search_chunks BEGIN
  INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id AND indexed=1;
  UPDATE search_body_identity SET chunk_id=NEW.chunk_id,document_id=NEW.document_id WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id;
  INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('retrieval',NEW.chunk_id,NEW.document_id);
  INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id;
  UPDATE search_body_identity SET indexed=1 WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id;
END;
CREATE TRIGGER documents_substring_insert AFTER INSERT ON documents BEGIN
  INSERT OR IGNORE INTO search_document_identity(document_id) VALUES(NEW.id);
  INSERT INTO search_metadata_substrings(rowid,title,citation,author) SELECT row_id,NEW.title,NEW.citation,COALESCE(NEW.author_name,'') FROM search_document_identity WHERE document_id=NEW.id AND indexed=0;
  UPDATE search_document_identity SET indexed=1 WHERE document_id=NEW.id;
END;
CREATE TRIGGER documents_substring_delete BEFORE DELETE ON documents BEGIN
  INSERT INTO search_metadata_substrings(search_metadata_substrings,rowid,title,citation,author) SELECT 'delete',row_id,OLD.title,OLD.citation,COALESCE(OLD.author_name,'') FROM search_document_identity WHERE document_id=OLD.id AND indexed=1;
  DELETE FROM search_document_identity WHERE document_id=OLD.id;
END;
CREATE TRIGGER documents_substring_update AFTER UPDATE OF id,title,citation,author_name ON documents BEGIN
  INSERT INTO search_metadata_substrings(search_metadata_substrings,rowid,title,citation,author) SELECT 'delete',row_id,OLD.title,OLD.citation,COALESCE(OLD.author_name,'') FROM search_document_identity WHERE document_id=OLD.id AND indexed=1;
  UPDATE search_document_identity SET document_id=NEW.id WHERE document_id=OLD.id;
  INSERT OR IGNORE INTO search_document_identity(document_id) VALUES(NEW.id);
  INSERT INTO search_metadata_substrings(rowid,title,citation,author) SELECT row_id,NEW.title,NEW.citation,COALESCE(NEW.author_name,'') FROM search_document_identity WHERE document_id=NEW.id;
  UPDATE search_document_identity SET indexed=1 WHERE document_id=NEW.id;
END;
CREATE TABLE search_index_build (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  phase TEXT NOT NULL CHECK(phase IN ('documents','document','retrieval','ready')),
  after_id TEXT NOT NULL DEFAULT '',
  visited INTEGER NOT NULL DEFAULT 0,
  owner_token TEXT,
  lease_expires_at TEXT,
  last_error TEXT,
  coverage_json TEXT,
  updated_at TEXT NOT NULL
);
INSERT INTO search_index_build(singleton,phase,updated_at) SELECT 1,
  CASE WHEN EXISTS(SELECT 1 FROM documents) OR EXISTS(SELECT 1 FROM document_chunks) OR EXISTS(SELECT 1 FROM retrieval_search_chunks) THEN 'documents' ELSE 'ready' END,strftime('%Y-%m-%dT%H:%M:%fZ','now');
CREATE TABLE search_index_build_guards(token TEXT PRIMARY KEY,owner_token TEXT NOT NULL);
CREATE TRIGGER search_index_build_guard_check BEFORE INSERT ON search_index_build_guards
WHEN NOT EXISTS(SELECT 1 FROM search_index_build WHERE singleton=1 AND owner_token=NEW.owner_token AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
BEGIN SELECT RAISE(ABORT,'search_index_build_lease_lost'); END;
