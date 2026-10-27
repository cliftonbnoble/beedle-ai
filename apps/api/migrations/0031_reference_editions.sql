-- Catalog presence is not proof of historical legal applicability. Existing
-- snapshots retain their actual text and explicitly unknown edition provenance.
CREATE TABLE legal_reference_editions (
 catalog_id TEXT PRIMARY KEY REFERENCES legal_catalog_versions(id),
 descriptor_json TEXT NOT NULL CHECK(json_valid(descriptor_json)),
 descriptor_sha256 TEXT,
 jurisdiction_key TEXT GENERATED ALWAYS AS (lower(json_extract(descriptor_json,'$.jurisdiction'))) STORED,
 effective_from TEXT GENERATED ALWAYS AS (json_extract(descriptor_json,'$.effective_from')) STORED,
 effective_to TEXT GENERATED ALWAYS AS (json_extract(descriptor_json,'$.effective_to')) STORED,
 policy TEXT GENERATED ALWAYS AS (json_extract(descriptor_json,'$.policy')) STORED,
 created_at TEXT NOT NULL,
 CHECK(policy IN ('unknown','reviewed_interval')),
 CHECK(effective_to IS NULL OR (effective_from IS NOT NULL AND effective_to>effective_from))
);
INSERT INTO legal_reference_editions(catalog_id,descriptor_json,created_at)
 SELECT id,json_object('policy','unknown','jurisdiction',NULL,'effective_from',NULL,'effective_to',NULL,'sources',NULL,'reviewed_by',NULL,'reviewed_at',NULL,'notes','Legacy catalog snapshot; original source hashes and applicability have not been established'),created_at FROM legal_catalog_versions;
CREATE INDEX idx_reference_editions_context ON legal_reference_editions(jurisdiction_key,policy,effective_from,effective_to,catalog_id);
CREATE TRIGGER reference_editions_immutable_update BEFORE UPDATE ON legal_reference_editions BEGIN SELECT RAISE(ABORT,'reference_edition_immutable'); END;
CREATE TRIGGER reference_editions_immutable_delete BEFORE DELETE ON legal_reference_editions BEGIN SELECT RAISE(ABORT,'reference_edition_immutable'); END;
CREATE TRIGGER reference_editions_staging_insert BEFORE INSERT ON legal_reference_editions
 WHEN (SELECT state FROM legal_catalog_versions WHERE id=NEW.catalog_id) IS NOT 'staging'
 BEGIN SELECT RAISE(ABORT,'reference_edition_not_staging'); END;
ALTER TABLE document_reference_links ADD COLUMN edition_id TEXT REFERENCES legal_reference_editions(catalog_id);
ALTER TABLE document_reference_links ADD COLUMN edition_provision_id TEXT;
ALTER TABLE document_reference_links ADD COLUMN edition_body_sha256 TEXT;
ALTER TABLE document_reference_links ADD COLUMN applicability_status TEXT NOT NULL DEFAULT 'unknown' CHECK(applicability_status IN ('unknown','matched','unavailable','ambiguous','not_applicable'));
ALTER TABLE staged_document_reference_links ADD COLUMN edition_id TEXT REFERENCES legal_reference_editions(catalog_id);
ALTER TABLE staged_document_reference_links ADD COLUMN edition_provision_id TEXT;
ALTER TABLE staged_document_reference_links ADD COLUMN edition_body_sha256 TEXT;
ALTER TABLE staged_document_reference_links ADD COLUMN applicability_status TEXT NOT NULL DEFAULT 'unknown' CHECK(applicability_status IN ('unknown','matched','unavailable','ambiguous','not_applicable'));
ALTER TABLE documents ADD COLUMN reference_edition_context_json TEXT NOT NULL DEFAULT '{"status":"unknown","reason":"legacy_unassessed"}';
CREATE TRIGGER document_reference_edition_context_changed AFTER UPDATE OF reference_edition_context_json ON documents
 WHEN OLD.reference_edition_context_json IS NOT NEW.reference_edition_context_json AND NEW.ingestion_state='ready'
 BEGIN UPDATE documents SET document_revision=document_revision+1,approved_at=NULL,approved_revision=NULL,approval_policy_version=NULL,qc_required_confirmed=0,qc_confirmed_at=NULL WHERE id=NEW.id; END;
-- Registry selections are pinned to a catalog epoch as well as its ID, so an
-- A -> B -> A switch cannot hide a newly registered overlapping edition.
ALTER TABLE reference_validation_guards ADD COLUMN expected_epoch INTEGER;
DROP TRIGGER reference_validation_catalog_guard;
CREATE TRIGGER reference_validation_catalog_guard BEFORE INSERT ON reference_validation_guards
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM legal_catalog_state WHERE singleton=1 AND active_version=NEW.expected_catalog AND (NEW.expected_epoch IS NULL OR epoch=NEW.expected_epoch)) THEN RAISE(ABORT,'catalog_revision_conflict') END;
END;
ALTER TABLE document_reference_validation_history ADD COLUMN edition_context_json TEXT NOT NULL DEFAULT '{"status":"unknown","reason":"legacy_unassessed"}';
-- Corrections withdraw an earlier applicability assertion without changing its
-- immutable descriptor/text. Ordinary amendments use distinct bounded intervals.
CREATE TABLE legal_reference_edition_supersessions(
 newer_catalog_id TEXT NOT NULL REFERENCES legal_reference_editions(catalog_id),
 older_catalog_id TEXT NOT NULL REFERENCES legal_reference_editions(catalog_id),
 older_payload_sha256 TEXT,
 PRIMARY KEY(newer_catalog_id,older_catalog_id),CHECK(newer_catalog_id<>older_catalog_id)
);
CREATE INDEX idx_edition_supersessions_older ON legal_reference_edition_supersessions(older_catalog_id,newer_catalog_id);
CREATE INDEX idx_edition_supersessions_payload ON legal_reference_edition_supersessions(older_payload_sha256,newer_catalog_id);
CREATE TRIGGER edition_supersessions_immutable_update BEFORE UPDATE ON legal_reference_edition_supersessions BEGIN SELECT RAISE(ABORT,'edition_supersession_immutable'); END;
CREATE TRIGGER edition_supersessions_immutable_delete BEFORE DELETE ON legal_reference_edition_supersessions BEGIN SELECT RAISE(ABORT,'edition_supersession_immutable'); END;
CREATE TRIGGER edition_supersessions_staging BEFORE INSERT ON legal_reference_edition_supersessions
BEGIN
 SELECT CASE WHEN (SELECT state FROM legal_catalog_versions WHERE id=NEW.newer_catalog_id) IS NOT 'staging' OR NOT EXISTS(SELECT 1 FROM legal_catalog_versions WHERE id=NEW.older_catalog_id AND published_at IS NOT NULL AND state IN ('published','retained')) OR NEW.older_payload_sha256 IS NOT (SELECT payload_sha256 FROM legal_catalog_versions WHERE id=NEW.older_catalog_id) OR (SELECT jurisdiction_key FROM legal_reference_editions WHERE catalog_id=NEW.newer_catalog_id) IS NOT (SELECT jurisdiction_key FROM legal_reference_editions WHERE catalog_id=NEW.older_catalog_id) OR NOT EXISTS(SELECT 1 FROM legal_reference_editions e,json_each(e.descriptor_json,'$.supersedes') target WHERE e.catalog_id=NEW.newer_catalog_id AND target.value=NEW.older_catalog_id) THEN RAISE(ABORT,'edition_supersession_not_publishable') END;
END;
