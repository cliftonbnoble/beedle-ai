// New isolated targets only. These operational tables do not impersonate
// application migrations or modify an existing recovery receipt/epoch.
export const corpusRestorationSql=[
  `CREATE TABLE beedle_restore_owner(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
    source_manifest_sha256 TEXT NOT NULL CHECK(length(source_manifest_sha256)=64),catalog_sha256 TEXT NOT NULL CHECK(length(catalog_sha256)=64),
    partition_index INTEGER NOT NULL CHECK(partition_index BETWEEN 0 AND 31),partition_count INTEGER NOT NULL CHECK(partition_count=32),
    expected_documents INTEGER NOT NULL CHECK(expected_documents>=0),expected_published_documents INTEGER NOT NULL CHECK(expected_published_documents BETWEEN 0 AND expected_documents),
    phase TEXT NOT NULL CHECK(phase IN ('seeded','building','indexing','ready','verifying','sealed','held')),
    started_at TEXT NOT NULL,updated_at TEXT NOT NULL,last_outcome TEXT)`,
  `CREATE TRIGGER beedle_restore_owner_identity_immutable BEFORE UPDATE OF owner_id,plan_sha256,source_manifest_sha256,catalog_sha256,partition_index,partition_count,expected_documents,expected_published_documents ON beedle_restore_owner
    BEGIN SELECT RAISE(ABORT,'restoration_owner_immutable'); END`,
  `CREATE TRIGGER beedle_restore_owner_delete_guard BEFORE DELETE ON beedle_restore_owner BEGIN SELECT RAISE(ABORT,'restoration_owner_immutable'); END`,
  `CREATE TABLE beedle_restore_jobs(document_id TEXT PRIMARY KEY,source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64),
    source_bytes INTEGER NOT NULL CHECK(source_bytes BETWEEN 1 AND 1000000),original_key TEXT NOT NULL,plan_object_key TEXT NOT NULL,
    extraction_policy TEXT NOT NULL CHECK(extraction_policy IN ('verified-source-restoration-v1','recovered-markdown-v1','held-unreadable')),
    phase TEXT NOT NULL CHECK(phase IN ('planned','claimed','source_copied','seeded','processed','held')),
    claim_token TEXT UNIQUE,claimed_at TEXT,source_copy_key TEXT,generation_id TEXT,completed_at TEXT,
    failure_stage TEXT,error_kind TEXT,error_sha256 TEXT,source_verified INTEGER NOT NULL DEFAULT 0 CHECK(source_verified IN (0,1)))`,
  `CREATE INDEX beedle_restore_job_phase ON beedle_restore_jobs(phase,document_id)`,
  `CREATE TRIGGER beedle_restore_job_identity_immutable BEFORE UPDATE OF document_id,source_sha256,source_bytes,original_key,plan_object_key,extraction_policy ON beedle_restore_jobs
    BEGIN SELECT RAISE(ABORT,'restoration_job_immutable'); END`,
  `CREATE TRIGGER beedle_restore_job_claim_immutable BEFORE UPDATE OF claim_token ON beedle_restore_jobs WHEN OLD.claim_token IS NOT NULL
    BEGIN SELECT RAISE(ABORT,'restoration_claim_immutable'); END`,
  `CREATE TRIGGER beedle_restore_job_phase_no_replay BEFORE UPDATE OF phase ON beedle_restore_jobs
    WHEN NEW.phase='planned' OR OLD.phase IN ('processed','held') BEGIN SELECT RAISE(ABORT,'restoration_job_cannot_replay'); END`,
  `CREATE TRIGGER beedle_restore_job_delete_guard BEFORE DELETE ON beedle_restore_jobs BEGIN SELECT RAISE(ABORT,'restoration_job_immutable'); END`,
  `CREATE TABLE beedle_restore_integrity(kind TEXT PRIMARY KEY CHECK(kind IN ('metadata','body','phrase','physical','foreign_keys','source_sha')),
    owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,corpus_revision INTEGER NOT NULL CHECK(corpus_revision>0),
    claim_token TEXT NOT NULL UNIQUE,command_sha256 TEXT NOT NULL CHECK(length(command_sha256)=64),
    status TEXT NOT NULL CHECK(status IN ('claimed','passed','held','uncertain')),engine_acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(engine_acknowledged IN (0,1)),
    result_sha256 TEXT,claimed_at TEXT NOT NULL,completed_at TEXT,
    CHECK(status!='passed' OR (engine_acknowledged=1 AND length(result_sha256)=64 AND completed_at IS NOT NULL)))`,
  `CREATE TRIGGER beedle_restore_integrity_claim_immutable BEFORE UPDATE OF kind,owner_id,plan_sha256,corpus_revision,claim_token,command_sha256,claimed_at ON beedle_restore_integrity
    BEGIN SELECT RAISE(ABORT,'restoration_integrity_claim_immutable'); END`,
  `CREATE TRIGGER beedle_restore_integrity_result_immutable BEFORE UPDATE ON beedle_restore_integrity WHEN OLD.status!='claimed'
    BEGIN SELECT RAISE(ABORT,'restoration_integrity_result_immutable'); END`,
  `CREATE TRIGGER beedle_restore_integrity_delete_guard BEFORE DELETE ON beedle_restore_integrity BEGIN SELECT RAISE(ABORT,'restoration_integrity_claim_immutable'); END`,
  `CREATE TABLE beedle_restore_capacity(id INTEGER PRIMARY KEY,stage TEXT NOT NULL,document_id TEXT,
    allocated_bytes INTEGER NOT NULL CHECK(allocated_bytes>=0),source_bytes INTEGER NOT NULL CHECK(source_bytes>=0),
    measured_at TEXT NOT NULL,owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL)`,
  `CREATE TRIGGER beedle_restore_capacity_update_guard BEFORE UPDATE ON beedle_restore_capacity BEGIN SELECT RAISE(ABORT,'restoration_capacity_immutable'); END`,
  `CREATE TRIGGER beedle_restore_capacity_delete_guard BEFORE DELETE ON beedle_restore_capacity BEGIN SELECT RAISE(ABORT,'restoration_capacity_immutable'); END`
] as const;
