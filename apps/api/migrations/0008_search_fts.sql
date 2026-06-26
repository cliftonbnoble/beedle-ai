CREATE VIRTUAL TABLE IF NOT EXISTS search_chunks_fts USING fts5(
  source_kind UNINDEXED,
  chunk_id UNINDEXED,
  document_id UNINDEXED,
  active UNINDEXED,
  section_label,
  paragraph_anchor UNINDEXED,
  citation_anchor UNINDEXED,
  chunk_text,
  created_at UNINDEXED,
  order_rank UNINDEXED,
  tokenize = 'unicode61'
);

INSERT INTO search_chunks_fts (
  source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
  citation_anchor, chunk_text, created_at, order_rank
)
SELECT
  'document',
  c.id,
  c.document_id,
  1,
  c.section_label,
  c.paragraph_anchor,
  c.citation_anchor,
  c.chunk_text,
  c.created_at,
  c.chunk_order
FROM document_chunks c
WHERE NOT EXISTS (
  SELECT 1 FROM search_chunks_fts f
  WHERE f.source_kind = 'document' AND f.chunk_id = c.id
);

INSERT INTO search_chunks_fts (
  source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
  citation_anchor, chunk_text, created_at, order_rank
)
SELECT
  'retrieval',
  rs.chunk_id,
  rs.document_id,
  rs.active,
  rs.section_label,
  rs.paragraph_anchor,
  rs.citation_anchor,
  rs.chunk_text,
  rs.created_at,
  999999
FROM retrieval_search_chunks rs
WHERE NOT EXISTS (
  SELECT 1 FROM search_chunks_fts f
  WHERE f.source_kind = 'retrieval' AND f.chunk_id = rs.chunk_id
);

CREATE TRIGGER IF NOT EXISTS document_chunks_ai_search_fts
AFTER INSERT ON document_chunks
BEGIN
  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'document', new.id, new.document_id, 1, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, new.chunk_order
  );
END;

CREATE TRIGGER IF NOT EXISTS document_chunks_ad_search_fts
AFTER DELETE ON document_chunks
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'document' AND chunk_id = old.id;
END;

CREATE TRIGGER IF NOT EXISTS document_chunks_au_search_fts
AFTER UPDATE ON document_chunks
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'document' AND chunk_id = old.id;

  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'document', new.id, new.document_id, 1, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, new.chunk_order
  );
END;

CREATE TRIGGER IF NOT EXISTS retrieval_search_chunks_ai_search_fts
AFTER INSERT ON retrieval_search_chunks
BEGIN
  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'retrieval', new.chunk_id, new.document_id, new.active, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, 999999
  );
END;

CREATE TRIGGER IF NOT EXISTS retrieval_search_chunks_ad_search_fts
AFTER DELETE ON retrieval_search_chunks
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'retrieval' AND chunk_id = old.chunk_id;
END;

CREATE TRIGGER IF NOT EXISTS retrieval_search_chunks_au_search_fts
AFTER UPDATE ON retrieval_search_chunks
BEGIN
  DELETE FROM search_chunks_fts
  WHERE source_kind = 'retrieval' AND chunk_id = old.chunk_id;

  INSERT INTO search_chunks_fts (
    source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
    citation_anchor, chunk_text, created_at, order_rank
  )
  VALUES (
    'retrieval', new.chunk_id, new.document_id, new.active, new.section_label, new.paragraph_anchor,
    new.citation_anchor, new.chunk_text, new.created_at, 999999
  );
END;
