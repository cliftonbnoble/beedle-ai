import type { Env } from "../lib/types";
import { sha256Bytes } from "../lib/digests";
export class VectorJobIntegrityError extends Error {
}
export type VectorChunkRow = {
  chunkId: string;
  chunkText: string;
  sectionLabel: string;
  paragraphAnchor: string;
  paragraphAnchorEnd: string;
  citationAnchor: string;
  chunkOrder: number;
};
export async function readVectorManifest(env: Env, documentId: string, generationId?: string) {
  const rows = (await env.DB.prepare(`SELECT id AS chunkId,chunk_text AS chunkText,section_label AS sectionLabel,
    paragraph_anchor AS paragraphAnchor,paragraph_anchor_end AS paragraphAnchorEnd,citation_anchor AS citationAnchor,chunk_order AS chunkOrder
    FROM ${generationId ? 'staged_document_chunks' : 'document_chunks'} WHERE document_id=? ${generationId ? 'AND generation_id=?' : ''} ORDER BY chunk_order,id`)
    .bind(...(generationId ? [documentId, generationId] : [documentId])).all<VectorChunkRow>()).results || [];
  const digest = await sha256Bytes(new TextEncoder().encode(JSON.stringify(rows.map(row => [row.chunkId, row.chunkText, row.sectionLabel, row.paragraphAnchor, row.paragraphAnchorEnd, row.citationAnchor, row.chunkOrder]))));
  return { chunks: rows, count: rows.length, digest };
}
export async function verifyVectorSource(env: Env, key: string, digest: string) {
  const source = await env.SOURCE_BUCKET.get(key);
  if (!source || source.size > 15 * 1024 * 1024)
    throw new VectorJobIntegrityError("Vector source is missing or oversized");
  const bytes = new Uint8Array(await source.arrayBuffer());
  if (bytes.length > 15 * 1024 * 1024 || await sha256Bytes(bytes) !== digest)
    throw new VectorJobIntegrityError("Vector source digest mismatch");
}
export function vectorOutboxStatement(env: Env, input: {
  documentId: string;
  generationId: string;
  sourceKey: string;
  sourceSha256: string;
  count: number;
  digest: string;
  timestamp: string;
}) {
  return env.DB.prepare(`INSERT INTO document_vector_jobs (document_id,generation_id,state,created_at,updated_at,outbox_ready_at,
    prepared_revision,source_key,source_sha256,expected_chunk_count,chunk_manifest_sha256)
    SELECT id,active_text_generation,'queued',?,?,?,document_revision,source_r2_key,?,?,? FROM documents
    WHERE id=? AND active_text_generation=? AND source_r2_key=? AND ingestion_state='ready' AND ingestion_completed_at IS NOT NULL
      AND expected_chunk_count=? AND (SELECT COUNT(*) FROM document_chunks WHERE document_id=documents.id)=?
    ON CONFLICT(document_id) DO UPDATE SET generation_id=excluded.generation_id,state='queued',attempts=0,vector_count=0,
      enqueued_at=NULL,started_at=NULL,completed_at=NULL,last_error=NULL,updated_at=excluded.updated_at,
      embedding_profile_id=NULL,owner_token=NULL,lease_expires_at=NULL,next_attempt_at=NULL,claimed_revision=NULL,failure_count=0,next_chunk_offset=0,
      outbox_ready_at=excluded.outbox_ready_at,prepared_revision=excluded.prepared_revision,source_key=excluded.source_key,
      source_sha256=excluded.source_sha256,expected_chunk_count=excluded.expected_chunk_count,chunk_manifest_sha256=excluded.chunk_manifest_sha256 RETURNING document_id`)
    .bind(input.timestamp, input.timestamp, input.timestamp, input.sourceSha256, input.count, input.digest, input.documentId, input.generationId, input.sourceKey, input.count, input.count);
}
export async function validateVectorJobManifest(env: Env, documentId: string, generationId: string) {
  const job = await env.DB.prepare(`SELECT j.source_key,j.source_sha256,j.expected_chunk_count,j.chunk_manifest_sha256
    FROM document_vector_jobs j JOIN documents d ON d.id=j.document_id
    WHERE j.document_id=? AND j.generation_id=? AND j.outbox_ready_at IS NOT NULL AND j.prepared_revision>0
      AND j.prepared_revision<=d.document_revision AND d.ingestion_state='ready' AND d.ingestion_completed_at IS NOT NULL
      AND d.active_text_generation=j.generation_id AND d.source_r2_key=j.source_key AND d.expected_chunk_count=j.expected_chunk_count`)
    .bind(documentId, generationId).first<{
      source_key: string;
      source_sha256: string;
      expected_chunk_count: number;
      chunk_manifest_sha256: string;
    }>();
  if (!job)
    throw new VectorJobIntegrityError("Job no longer identifies a complete current generation");
  await verifyVectorSource(env, job.source_key, job.source_sha256);
  const manifest = await readVectorManifest(env, documentId);
  if (!manifest.count || manifest.count !== job.expected_chunk_count || manifest.digest !== job.chunk_manifest_sha256)
    throw new VectorJobIntegrityError("Vector chunk manifest mismatch");
  return manifest.chunks;
}
