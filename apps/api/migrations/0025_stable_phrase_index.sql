-- A shadow phrase index reuses M06 integer identities and source-backed text.
-- The old canonical index stays live until the bounded builder atomically swaps it.
ALTER TABLE search_body_identity ADD COLUMN phrase_indexed INTEGER NOT NULL DEFAULT 0 CHECK(phrase_indexed IN (0,1));
CREATE VIEW search_phrase_content AS
 SELECT m.row_id,m.source_kind,m.chunk_id,m.document_id,
   CASE WHEN m.source_kind='document' THEN 1 ELSE r.active END AS active,
   COALESCE(c.section_label,r.section_label) AS section_label,
   COALESCE(c.paragraph_anchor,r.paragraph_anchor) AS paragraph_anchor,
   COALESCE(c.citation_anchor,r.citation_anchor) AS citation_anchor,
   COALESCE(c.chunk_text,r.chunk_text) AS chunk_text,
   COALESCE(c.created_at,r.created_at) AS created_at,
   CASE WHEN m.source_kind='document' THEN c.chunk_order ELSE 999999 END AS order_rank
 FROM search_body_identity m
 LEFT JOIN document_chunks c ON m.source_kind='document' AND c.id=m.chunk_id
 LEFT JOIN retrieval_search_chunks r ON m.source_kind='retrieval' AND r.chunk_id=m.chunk_id;
CREATE VIRTUAL TABLE search_phrase_fts USING fts5(
 source_kind UNINDEXED,chunk_id UNINDEXED,document_id UNINDEXED,active UNINDEXED,
 section_label,paragraph_anchor UNINDEXED,citation_anchor UNINDEXED,chunk_text,
 created_at UNINDEXED,order_rank UNINDEXED,
 content='search_phrase_content',content_rowid='row_id',tokenize='unicode61');
CREATE TABLE search_phrase_build(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 phase TEXT NOT NULL CHECK(phase IN ('document','retrieval','ready')),
 after_id TEXT NOT NULL DEFAULT '',visited INTEGER NOT NULL DEFAULT 0,
 owner_token TEXT,lease_expires_at TEXT,legacy_frozen INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL);
INSERT INTO search_phrase_build(singleton,phase,updated_at) VALUES(1,'document',strftime('%Y-%m-%dT%H:%M:%fZ','now'));
CREATE TABLE search_phrase_build_guards(token TEXT PRIMARY KEY,owner_token TEXT NOT NULL);
CREATE TRIGGER search_phrase_build_guard_check BEFORE INSERT ON search_phrase_build_guards
 WHEN NOT EXISTS(SELECT 1 FROM search_phrase_build WHERE singleton=1 AND owner_token=NEW.owner_token AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
 BEGIN SELECT RAISE(ABORT,'search_phrase_build_lease_lost'); END;
DROP TRIGGER document_chunks_ai_search_fts;
CREATE TRIGGER document_chunks_ai_search_fts
AFTER INSERT ON document_chunks
WHEN (SELECT legacy_frozen FROM search_phrase_build WHERE singleton=1)=0
BEGIN
  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'document', new.id, new.document_id, 1, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, new.chunk_order
  );
END;
DROP TRIGGER document_chunks_ad_search_fts;
CREATE TRIGGER document_chunks_ad_search_fts
AFTER DELETE ON document_chunks
WHEN (SELECT legacy_frozen FROM search_phrase_build WHERE singleton=1)=0
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'document' AND chunk_id = old.id;
END;
DROP TRIGGER document_chunks_au_search_fts;
CREATE TRIGGER document_chunks_au_search_fts
AFTER UPDATE ON document_chunks
WHEN (SELECT legacy_frozen FROM search_phrase_build WHERE singleton=1)=0
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'document' AND chunk_id = old.id;

  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'document', new.id, new.document_id, 1, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, new.chunk_order
  );
END;
DROP TRIGGER retrieval_search_chunks_ai_search_fts;
CREATE TRIGGER retrieval_search_chunks_ai_search_fts
AFTER INSERT ON retrieval_search_chunks
WHEN (SELECT legacy_frozen FROM search_phrase_build WHERE singleton=1)=0
BEGIN
  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'retrieval', new.chunk_id, new.document_id, new.active, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, 999999
  );
END;
DROP TRIGGER retrieval_search_chunks_ad_search_fts;
CREATE TRIGGER retrieval_search_chunks_ad_search_fts
AFTER DELETE ON retrieval_search_chunks
WHEN (SELECT legacy_frozen FROM search_phrase_build WHERE singleton=1)=0
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'retrieval' AND chunk_id = old.chunk_id;
END;
DROP TRIGGER retrieval_search_chunks_au_search_fts;
CREATE TRIGGER retrieval_search_chunks_au_search_fts
AFTER UPDATE ON retrieval_search_chunks
WHEN (SELECT legacy_frozen FROM search_phrase_build WHERE singleton=1)=0
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'retrieval' AND chunk_id = old.chunk_id;

  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'retrieval', new.chunk_id, new.document_id, new.active, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, 999999
  );
END;
DROP TRIGGER document_chunks_substring_insert;
DROP TRIGGER document_chunks_substring_delete;
DROP TRIGGER document_chunks_substring_update;
DROP TRIGGER retrieval_search_chunks_substring_insert;
DROP TRIGGER retrieval_search_chunks_substring_delete;
DROP TRIGGER retrieval_search_chunks_substring_update;
CREATE TRIGGER document_chunks_substring_insert AFTER INSERT ON document_chunks BEGIN
 INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('document',NEW.id,NEW.document_id);
 INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=NEW.id AND indexed=0;
 INSERT INTO search_phrase_fts(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=NEW.id AND phrase_indexed=0;
 UPDATE search_body_identity SET indexed=1,phrase_indexed=1 WHERE source_kind='document' AND chunk_id=NEW.id;
END;
CREATE TRIGGER document_chunks_substring_delete AFTER DELETE ON document_chunks BEGIN
 INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id AND indexed=1;
 INSERT INTO search_phrase_fts(search_phrase_fts,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id AND phrase_indexed=1;
 DELETE FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id;
END;
CREATE TRIGGER document_chunks_substring_update AFTER UPDATE OF id,document_id,section_label,chunk_text ON document_chunks BEGIN
 INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id AND indexed=1;
 INSERT INTO search_phrase_fts(search_phrase_fts,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=OLD.id AND phrase_indexed=1;
 UPDATE search_body_identity SET chunk_id=NEW.id,document_id=NEW.document_id WHERE source_kind='document' AND chunk_id=OLD.id;
 INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('document',NEW.id,NEW.document_id);
 INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=NEW.id;
 INSERT INTO search_phrase_fts(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='document' AND chunk_id=NEW.id;
 UPDATE search_body_identity SET indexed=1,phrase_indexed=1 WHERE source_kind='document' AND chunk_id=NEW.id;
END;
CREATE TRIGGER retrieval_search_chunks_substring_insert AFTER INSERT ON retrieval_search_chunks BEGIN
 INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('retrieval',NEW.chunk_id,NEW.document_id);
 INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id AND indexed=0;
 INSERT INTO search_phrase_fts(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id AND phrase_indexed=0;
 UPDATE search_body_identity SET indexed=1,phrase_indexed=1 WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id;
END;
CREATE TRIGGER retrieval_search_chunks_substring_delete AFTER DELETE ON retrieval_search_chunks BEGIN
 INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id AND indexed=1;
 INSERT INTO search_phrase_fts(search_phrase_fts,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id AND phrase_indexed=1;
 DELETE FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id;
END;
CREATE TRIGGER retrieval_search_chunks_substring_update AFTER UPDATE OF chunk_id,document_id,section_label,chunk_text ON retrieval_search_chunks BEGIN
 INSERT INTO search_body_substrings(search_body_substrings,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id AND indexed=1;
 INSERT INTO search_phrase_fts(search_phrase_fts,rowid,section_label,chunk_text) SELECT 'delete',row_id,OLD.section_label,OLD.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id AND phrase_indexed=1;
 UPDATE search_body_identity SET chunk_id=NEW.chunk_id,document_id=NEW.document_id WHERE source_kind='retrieval' AND chunk_id=OLD.chunk_id;
 INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('retrieval',NEW.chunk_id,NEW.document_id);
 INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id;
 INSERT INTO search_phrase_fts(rowid,section_label,chunk_text) SELECT row_id,NEW.section_label,NEW.chunk_text FROM search_body_identity WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id;
 UPDATE search_body_identity SET indexed=1,phrase_indexed=1 WHERE source_kind='retrieval' AND chunk_id=NEW.chunk_id;
END;
-- An identity survives text/metadata edits; maintenance must never retarget its
-- integer rowid or source kind to another physical index record.
CREATE TRIGGER search_body_rowid_immutable BEFORE UPDATE OF row_id,source_kind ON search_body_identity
 WHEN NEW.row_id<>OLD.row_id OR NEW.source_kind<>OLD.source_kind
 BEGIN SELECT RAISE(ABORT,'search_body_identity_immutable'); END;
CREATE TRIGGER search_document_rowid_immutable BEFORE UPDATE OF row_id ON search_document_identity
 WHEN NEW.row_id<>OLD.row_id
 BEGIN SELECT RAISE(ABORT,'search_document_identity_immutable'); END;
