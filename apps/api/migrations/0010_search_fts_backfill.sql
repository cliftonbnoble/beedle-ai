-- FTS is maintained by the triggers introduced in 0008. Backfill once at migration time rather
-- than from a search request: cold Workers can otherwise race and insert duplicate FTS rows.
DELETE FROM search_chunks_fts
WHERE rowid NOT IN (
  SELECT MIN(rowid)
  FROM search_chunks_fts
  GROUP BY source_kind, chunk_id
);

INSERT INTO search_chunks_fts (
  source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
  citation_anchor, chunk_text, created_at, order_rank
)
SELECT
  'document', c.id, c.document_id, 1, c.section_label, c.paragraph_anchor,
  c.citation_anchor, c.chunk_text, c.created_at, c.chunk_order
FROM document_chunks c
WHERE NOT EXISTS (
  SELECT 1
  FROM search_chunks_fts f
  WHERE f.source_kind = 'document' AND f.chunk_id = c.id
);

INSERT INTO search_chunks_fts (
  source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
  citation_anchor, chunk_text, created_at, order_rank
)
SELECT
  'retrieval', rs.chunk_id, rs.document_id, rs.active, rs.section_label, rs.paragraph_anchor,
  rs.citation_anchor, rs.chunk_text, rs.created_at, 999999
FROM retrieval_search_chunks rs
WHERE NOT EXISTS (
  SELECT 1
  FROM search_chunks_fts f
  WHERE f.source_kind = 'retrieval' AND f.chunk_id = rs.chunk_id
);
