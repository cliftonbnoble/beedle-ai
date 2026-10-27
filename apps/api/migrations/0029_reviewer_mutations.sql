-- A retry receipt and change record share the guarded review transaction.
-- actor_id deliberately remains NULL until authenticated principals exist (H01).
CREATE TABLE reviewer_mutations (
  document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  mutation_id TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('metadata', 'reject')),
  request_digest TEXT NOT NULL,
  expected_revision INTEGER NOT NULL,
  applied_revision INTEGER NOT NULL,
  before_json TEXT NOT NULL,
  after_json TEXT NOT NULL,
  actor_id TEXT CHECK (actor_id IS NULL),
  created_at TEXT NOT NULL,
  PRIMARY KEY (document_id, mutation_id)
);
CREATE INDEX idx_reviewer_mutations_history ON reviewer_mutations(document_id, created_at);
