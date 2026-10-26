CREATE TABLE maintenance_jobs (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK(kind IN ('vector_backfill','reference_backfill','activation')),
  state TEXT NOT NULL DEFAULT 'queued' CHECK(state IN ('queued','running','retryable','failed','completed','cancelled')),
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  dry_run INTEGER NOT NULL DEFAULT 0 CHECK(dry_run IN (0,1)),
  owner_token TEXT,
  lease_expires_at TEXT,
  cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)),
  failures INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  result_json TEXT CHECK(result_json IS NULL OR json_valid(result_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX maintenance_jobs_work ON maintenance_jobs(state,next_attempt_at,created_at);
CREATE TABLE maintenance_job_items (
  job_id TEXT NOT NULL REFERENCES maintenance_jobs(id),
  item_key TEXT NOT NULL,
  document_id TEXT NOT NULL,
  document_revision INTEGER NOT NULL,
  source_kind TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','submitted','completed','skipped')),
  last_error TEXT,
  visibility_checks INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(job_id,item_key)
);
CREATE INDEX maintenance_items_work ON maintenance_job_items(job_id,state,item_key);
CREATE TRIGGER maintenance_job_plan_immutable BEFORE UPDATE OF kind,payload_json,dry_run ON maintenance_jobs
BEGIN SELECT RAISE(ABORT,'maintenance_plan_immutable'); END;
CREATE TRIGGER maintenance_item_manifest_immutable BEFORE UPDATE OF job_id,item_key,document_id,document_revision,source_kind,payload_json ON maintenance_job_items
BEGIN SELECT RAISE(ABORT,'maintenance_manifest_immutable'); END;
CREATE TABLE maintenance_revision_guards(token TEXT PRIMARY KEY, job_id TEXT NOT NULL, owner_token TEXT NOT NULL);
CREATE TRIGGER maintenance_revision_guard_check BEFORE INSERT ON maintenance_revision_guards
WHEN NOT EXISTS (SELECT 1 FROM maintenance_jobs WHERE id=NEW.job_id AND owner_token=NEW.owner_token AND state='running' AND lease_expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))
BEGIN SELECT RAISE(ABORT,'maintenance_owner_conflict'); END;
