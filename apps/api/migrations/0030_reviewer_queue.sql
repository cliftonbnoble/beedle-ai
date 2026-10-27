-- Reviewer rows are disposable derived state. Source versions and the global epoch
-- fence materialization and cursor traversal independently from evidence revisions.
CREATE TABLE reviewer_queue_control (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), epoch INTEGER NOT NULL DEFAULT 1,
 policy_version TEXT NOT NULL DEFAULT '', catalog_version TEXT NOT NULL DEFAULT '',
 phase TEXT NOT NULL DEFAULT 'backfill' CHECK(phase IN ('backfill','ready')),
 after_id TEXT NOT NULL DEFAULT '', upper_id TEXT NOT NULL DEFAULT '',
 lease_owner TEXT, lease_until INTEGER NOT NULL DEFAULT 0, last_token TEXT
);
INSERT INTO reviewer_queue_control(singleton) VALUES(1);
CREATE TABLE reviewer_queue_sources (
 document_id TEXT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
 version INTEGER NOT NULL, dirty INTEGER NOT NULL CHECK(dirty IN (0,1))
);
CREATE INDEX idx_reviewer_queue_dirty ON reviewer_queue_sources(dirty,document_id);
CREATE TABLE reviewer_queue_groups (group_key TEXT PRIMARY KEY,member_count INTEGER NOT NULL CHECK(member_count>=0));
CREATE TABLE reviewer_queue_state (
 id TEXT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,source_version INTEGER NOT NULL,
 policy_version TEXT NOT NULL,catalog_version TEXT NOT NULL,
 created_at TEXT NOT NULL,title TEXT NOT NULL,confidence REAL NOT NULL,warning_count INTEGER NOT NULL,
 unresolved_count INTEGER NOT NULL,critical_count INTEGER NOT NULL,approval_score INTEGER NOT NULL,
 reviewer_ready INTEGER NOT NULL,risk_rank INTEGER NOT NULL,effort_rank INTEGER NOT NULL,
 group_key TEXT NOT NULL,batch_key TEXT NOT NULL,batch_safe INTEGER NOT NULL,payload_json TEXT NOT NULL CHECK(json_valid(payload_json))
);
CREATE INDEX idx_reviewer_queue_created ON reviewer_queue_state(created_at DESC,id);
CREATE INDEX idx_reviewer_queue_title ON reviewer_queue_state(title,id);
CREATE INDEX idx_reviewer_queue_group ON reviewer_queue_state(group_key,id);
CREATE INDEX idx_reviewer_queue_score ON reviewer_queue_state(approval_score DESC,created_at DESC,id);
CREATE INDEX idx_reviewer_queue_effort ON reviewer_queue_state(effort_rank,approval_score DESC,id);
CREATE INDEX idx_reviewer_queue_type ON reviewer_queue_state(json_extract(payload_json,'$.fileType'),created_at DESC,id);
CREATE INDEX idx_reviewer_queue_risk ON reviewer_queue_state(json_extract(payload_json,'$.reviewerRiskLevel'),created_at DESC,id);
CREATE INDEX idx_reviewer_queue_fixture ON reviewer_queue_state(json_extract(payload_json,'$.isLikelyFixture'),created_at DESC,id);
CREATE TRIGGER reviewer_queue_group_insert AFTER INSERT ON reviewer_queue_state BEGIN
 INSERT INTO reviewer_queue_groups VALUES(NEW.group_key,1) ON CONFLICT(group_key) DO UPDATE SET member_count=member_count+1;
END;
CREATE TRIGGER reviewer_queue_group_delete AFTER DELETE ON reviewer_queue_state BEGIN
 UPDATE reviewer_queue_groups SET member_count=member_count-1 WHERE group_key=OLD.group_key;
 DELETE FROM reviewer_queue_groups WHERE group_key=OLD.group_key AND member_count=0;
END;
CREATE TRIGGER reviewer_queue_group_update AFTER UPDATE OF group_key ON reviewer_queue_state WHEN OLD.group_key IS NOT NEW.group_key BEGIN
 UPDATE reviewer_queue_groups SET member_count=member_count-1 WHERE group_key=OLD.group_key;
 DELETE FROM reviewer_queue_groups WHERE group_key=OLD.group_key AND member_count=0;
 INSERT INTO reviewer_queue_groups VALUES(NEW.group_key,1) ON CONFLICT(group_key) DO UPDATE SET member_count=member_count+1;
END;
CREATE TRIGGER reviewer_queue_catalog AFTER UPDATE OF active_version ON legal_catalog_state WHEN OLD.active_version IS NOT NEW.active_version BEGIN
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TABLE reviewer_queue_guards(token TEXT PRIMARY KEY,owner TEXT NOT NULL,catalog TEXT NOT NULL,sources_json TEXT NOT NULL);
CREATE TRIGGER reviewer_queue_guard BEFORE INSERT ON reviewer_queue_guards BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM reviewer_queue_control WHERE singleton=1 AND lease_owner=NEW.owner AND lease_until>unixepoch() AND catalog_version=NEW.catalog)
  OR NOT EXISTS(SELECT 1 FROM legal_catalog_state WHERE singleton=1 AND active_version=NEW.catalog)
  OR EXISTS(SELECT 1 FROM json_each(NEW.sources_json) j LEFT JOIN documents d ON d.id=json_extract(j.value,'$.id')
    LEFT JOIN reviewer_queue_sources s ON s.document_id=d.id
    WHERE d.id IS NULL OR COALESCE(s.version,0)!=json_extract(j.value,'$.version'))
 THEN RAISE(ABORT,'reviewer_queue_changed') END;
END;
CREATE TRIGGER reviewer_queue_documents_insert AFTER INSERT ON documents BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT NEW.id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=NEW.id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_documents_update AFTER UPDATE ON documents BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT NEW.id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=NEW.id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_documents_delete AFTER DELETE ON documents BEGIN

 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_document_reference_links_insert AFTER INSERT ON document_reference_links BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT NEW.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=NEW.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_document_reference_links_update AFTER UPDATE ON document_reference_links BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT NEW.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=NEW.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT OLD.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=OLD.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_document_reference_links_delete AFTER DELETE ON document_reference_links BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT OLD.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=OLD.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_document_reference_issues_insert AFTER INSERT ON document_reference_issues BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT NEW.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=NEW.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_document_reference_issues_update AFTER UPDATE ON document_reference_issues BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT NEW.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=NEW.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT OLD.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=OLD.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
CREATE TRIGGER reviewer_queue_document_reference_issues_delete AFTER DELETE ON document_reference_issues BEGIN
 INSERT INTO reviewer_queue_sources(document_id,version,dirty) SELECT OLD.document_id,1,1 WHERE EXISTS(SELECT 1 FROM documents WHERE id=OLD.document_id) ON CONFLICT(document_id) DO UPDATE SET version=version+1,dirty=1;
 UPDATE reviewer_queue_control SET epoch=epoch+1 WHERE singleton=1;
END;
