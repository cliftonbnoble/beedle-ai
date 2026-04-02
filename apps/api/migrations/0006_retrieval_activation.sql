PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS retrieval_activation_batches (
  batch_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  trusted_document_count INTEGER NOT NULL,
  trusted_chunk_count INTEGER NOT NULL,
  activation_checksum TEXT NOT NULL,
  rollback_checksum TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS retrieval_activation_documents (
  batch_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  trust_source TEXT NOT NULL,
  write_status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (batch_id, document_id),
  FOREIGN KEY (batch_id) REFERENCES retrieval_activation_batches(batch_id) ON DELETE CASCADE,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS retrieval_activation_chunks (
  batch_id TEXT NOT NULL,
  chunk_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  chunk_type TEXT NOT NULL,
  embedding_write_status TEXT NOT NULL,
  search_write_status TEXT NOT NULL,
  provenance_complete INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  PRIMARY KEY (batch_id, chunk_id),
  FOREIGN KEY (batch_id) REFERENCES retrieval_activation_batches(batch_id) ON DELETE CASCADE,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS retrieval_embedding_rows (
  embedding_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  chunk_id TEXT NOT NULL,
  chunk_type TEXT NOT NULL,
  retrieval_priority TEXT NOT NULL,
  citation_anchor_start TEXT NOT NULL,
  citation_anchor_end TEXT NOT NULL,
  has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
  source_link TEXT NOT NULL,
  source_text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES retrieval_activation_batches(batch_id) ON DELETE CASCADE,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS retrieval_search_rows (
  search_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  chunk_id TEXT NOT NULL,
  title TEXT NOT NULL,
  chunk_type TEXT NOT NULL,
  retrieval_priority TEXT NOT NULL,
  citation_anchor_start TEXT NOT NULL,
  citation_anchor_end TEXT NOT NULL,
  has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
  source_link TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES retrieval_activation_batches(batch_id) ON DELETE CASCADE,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS retrieval_search_chunks (
  chunk_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  title TEXT NOT NULL,
  citation TEXT NOT NULL,
  source_file_ref TEXT NOT NULL,
  source_link TEXT NOT NULL,
  section_label TEXT NOT NULL,
  paragraph_anchor TEXT NOT NULL,
  citation_anchor TEXT NOT NULL,
  chunk_text TEXT NOT NULL,
  chunk_type TEXT NOT NULL,
  retrieval_priority TEXT NOT NULL,
  has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES retrieval_activation_batches(batch_id) ON DELETE CASCADE,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_retrieval_activation_documents_doc
  ON retrieval_activation_documents (document_id, batch_id);

CREATE INDEX IF NOT EXISTS idx_retrieval_activation_chunks_doc
  ON retrieval_activation_chunks (document_id, batch_id);

CREATE INDEX IF NOT EXISTS idx_retrieval_embedding_rows_doc
  ON retrieval_embedding_rows (document_id, batch_id);

CREATE INDEX IF NOT EXISTS idx_retrieval_search_rows_doc
  ON retrieval_search_rows (document_id, batch_id);

CREATE INDEX IF NOT EXISTS idx_retrieval_search_chunks_doc
  ON retrieval_search_chunks (document_id, active, batch_id);
