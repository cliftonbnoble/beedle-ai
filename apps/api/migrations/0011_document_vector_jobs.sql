CREATE TABLE IF NOT EXISTS document_vector_jobs (
  document_id TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN ('queued', 'processing', 'completed', 'failed')) DEFAULT 'queued',
  attempts INTEGER NOT NULL DEFAULT 0,
  vector_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  enqueued_at TEXT,
  started_at TEXT,
  completed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_vector_jobs_requeue
  ON document_vector_jobs (state, enqueued_at, created_at);
