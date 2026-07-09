import type { Env } from "../lib/types";
import { embed } from "./embeddings";

export type VectorJobMessage = { documentId: string };

type VectorChunkRow = {
  chunkId: string;
  chunkText: string;
  sectionLabel: string;
  paragraphAnchor: string;
  paragraphAnchorEnd: string;
  citationAnchor: string;
};

const embeddingConcurrency = 4;
const vectorUpsertBatchSize = 100;
const requeueDelayMs = 10 * 60 * 1000;

function now() {
  return new Date().toISOString();
}

async function loadDocumentChunks(env: Env, documentId: string): Promise<VectorChunkRow[]> {
  const rows = await env.DB.prepare(
    `SELECT
      id as chunkId,
      chunk_text as chunkText,
      section_label as sectionLabel,
      paragraph_anchor as paragraphAnchor,
      paragraph_anchor_end as paragraphAnchorEnd,
      citation_anchor as citationAnchor
     FROM document_chunks
     WHERE document_id = ?
     ORDER BY chunk_order ASC`
  )
    .bind(documentId)
    .all<VectorChunkRow>();
  return rows.results || [];
}

async function mapWithConcurrency<T, R>(items: T[], concurrency: number, work: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let cursor = 0;
  const worker = async () => {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) return;
      const item = items[index];
      if (item === undefined) return;
      out[index] = await work(item);
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
  return out;
}

export async function markVectorJobEnqueued(env: Env, documentId: string) {
  await env.DB.prepare(
    `UPDATE document_vector_jobs
     SET enqueued_at = ?, updated_at = ?
     WHERE document_id = ? AND state = 'queued'`
  )
    .bind(now(), now(), documentId)
    .run();
}

export async function enqueueVectorJob(env: Env, documentId: string) {
  await env.VECTOR_JOBS_QUEUE.send({ documentId }, { contentType: "json" });
  await markVectorJobEnqueued(env, documentId);
}

export async function requeueStaleVectorJobs(env: Env, limit = 50) {
  const cutoff = new Date(Date.now() - requeueDelayMs).toISOString();
  const rows = await env.DB.prepare(
    `SELECT document_id as documentId
     FROM document_vector_jobs
     WHERE state = 'queued' AND (enqueued_at IS NULL OR enqueued_at < ?)
     ORDER BY created_at ASC
     LIMIT ?`
  )
    .bind(cutoff, limit)
    .all<{ documentId: string }>();

  for (const row of rows.results || []) {
    await enqueueVectorJob(env, row.documentId);
  }
  return (rows.results || []).length;
}

export async function recordVectorJobRetry(env: Env, documentId: string, error: unknown) {
  const message = error instanceof Error ? error.message : String(error || "unknown vector job failure");
  await env.DB.prepare(
    `UPDATE document_vector_jobs
     SET state = 'queued', last_error = ?, updated_at = ?
     WHERE document_id = ? AND state != 'completed'`
  )
    .bind(message.slice(0, 500), now(), documentId)
    .run();
}

export async function markVectorJobFailed(env: Env, documentId: string, error: unknown) {
  const message = error instanceof Error ? error.message : String(error || "unknown vector job failure");
  await env.DB.prepare(
    `UPDATE document_vector_jobs
     SET state = 'failed', last_error = ?, updated_at = ?
     WHERE document_id = ? AND state != 'completed'`
  )
    .bind(message.slice(0, 500), now(), documentId)
    .run();
}

export async function processDocumentVectorJob(env: Env, documentId: string) {
  const claim = await env.DB.prepare(
    `UPDATE document_vector_jobs
     SET state = 'processing', attempts = attempts + 1, started_at = ?, updated_at = ?
     WHERE document_id = ? AND state != 'completed'`
  )
    .bind(now(), now(), documentId)
    .run();
  if (!claim.meta.changes) return { skipped: true, vectorCount: 0 };

  const chunks = await loadDocumentChunks(env, documentId);
  const embedded = await mapWithConcurrency(chunks, embeddingConcurrency, async (chunk) => {
    const values = await embed(env, chunk.chunkText);
    if (!values) throw new Error("embedding unavailable");
    return {
      id: chunk.chunkId,
      values,
      namespace: env.VECTOR_NAMESPACE,
      metadata: {
        documentId,
        paragraphAnchor: chunk.paragraphAnchor,
        paragraphAnchorEnd: chunk.paragraphAnchorEnd,
        citationAnchor: chunk.citationAnchor,
        sectionLabel: chunk.sectionLabel
      } as Record<string, VectorizeVectorMetadata>
    } satisfies VectorizeVector;
  });

  for (let start = 0; start < embedded.length; start += vectorUpsertBatchSize) {
    await env.VECTOR_INDEX.upsert(embedded.slice(start, start + vectorUpsertBatchSize));
  }

  await env.DB.prepare(
    `UPDATE document_vector_jobs
     SET state = 'completed', vector_count = ?, last_error = NULL, completed_at = ?, updated_at = ?
     WHERE document_id = ?`
  )
    .bind(embedded.length, now(), now(), documentId)
    .run();
  return { skipped: false, vectorCount: embedded.length };
}
