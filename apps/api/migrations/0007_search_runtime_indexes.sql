CREATE INDEX IF NOT EXISTS idx_documents_author_name_lookup
  ON documents (lower(coalesce(author_name, '')), file_type, rejected_at, approved_at, decision_date, searchable_at);

CREATE INDEX IF NOT EXISTS idx_documents_search_runtime
  ON documents (file_type, rejected_at, approved_at, searchable_at, decision_date);

CREATE INDEX IF NOT EXISTS idx_retrieval_search_chunks_doc
  ON retrieval_search_chunks (document_id, active, batch_id);
