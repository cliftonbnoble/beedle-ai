PRAGMA foreign_keys = ON;

ALTER TABLE documents ADD COLUMN searchable_at TEXT;
ALTER TABLE documents ADD COLUMN metadata_json TEXT;

ALTER TABLE document_sections ADD COLUMN section_text TEXT;

CREATE TABLE IF NOT EXISTS document_chunks (
  id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL,
  section_id TEXT NOT NULL,
  paragraph_id TEXT NOT NULL,
  paragraph_anchor TEXT NOT NULL,
  citation_anchor TEXT NOT NULL,
  section_label TEXT NOT NULL,
  chunk_order INTEGER NOT NULL,
  chunk_text TEXT NOT NULL,
  token_estimate INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
  FOREIGN KEY (section_id) REFERENCES document_sections(id) ON DELETE CASCADE,
  FOREIGN KEY (paragraph_id) REFERENCES section_paragraphs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_chunks_doc_order
  ON document_chunks (document_id, chunk_order);

CREATE INDEX IF NOT EXISTS idx_chunks_section
  ON document_chunks (section_id, paragraph_anchor);

CREATE INDEX IF NOT EXISTS idx_chunks_anchor
  ON document_chunks (citation_anchor);
