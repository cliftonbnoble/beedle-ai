import { z } from "zod";
import type { Env } from "../lib/types";
import { embed } from "./embeddings";
export type VectorJobMessage = {
  documentId: string;
  generationId?: string;
};
const messageSchema = z.object({ documentId: z.string().min(1).max(128), generationId: z.string().min(1).max(128).optional() }).strict();
type VectorChunkRow = {
  chunkId: string;
  chunkText: string;
  sectionLabel: string;
  paragraphAnchor: string;
  paragraphAnchorEnd: string;
  citationAnchor: string;
};
type Claim = {
  documentId: string;
  generationId: string;
  token: string;
  revision: number;
  failures: number;
  offset: number;
};
export const vectorJobPolicy = { maxFailures: 5, leaseMs: 120000, maxChunksPerDelivery: 64 } as const;
const embeddingConcurrency = 4;
const vectorUpsertBatchSize = 16;
const requeueDelayMs = 10 * 60 * 1000;
const now = () => new Date(Date.now()).toISOString();
const leaseExpiry = () => new Date(Date.now() + vectorJobPolicy.leaseMs).toISOString();
const ownerWhere = "document_id=? AND generation_id=? AND owner_token=? AND state='processing' AND lease_expires_at>?";
const currentRevisionWhere = `EXISTS (SELECT 1 FROM documents d WHERE d.id=document_id AND d.active_text_generation=generation_id
  AND d.document_revision=claimed_revision AND d.ingestion_state='ready')`;
class LeaseLost extends Error {
}
async function loadDocumentChunks(env: Env, documentId: string, generationId: string): Promise<VectorChunkRow[]> {
  const rows = await env.DB.prepare(`SELECT id as chunkId,chunk_text as chunkText,section_label as sectionLabel,
    paragraph_anchor as paragraphAnchor,paragraph_anchor_end as paragraphAnchorEnd,citation_anchor as citationAnchor
    FROM document_chunks WHERE document_id=? AND EXISTS (SELECT 1 FROM documents d WHERE d.id=document_id
      AND d.active_text_generation=? AND d.ingestion_state='ready') ORDER BY chunk_order ASC,id ASC`)
    .bind(documentId, generationId).all<VectorChunkRow>();
  return rows.results || [];
}
async function renew(env: Env, claim: Claim) {
  const result = await env.DB.prepare(`UPDATE document_vector_jobs SET lease_expires_at=?,updated_at=? WHERE ${ownerWhere} AND ${currentRevisionWhere}`)
    .bind(leaseExpiry(), now(), claim.documentId, claim.generationId, claim.token, now()).run();
  if (Number(result.meta.changes) !== 1)
    throw new LeaseLost("Job ownership or document revision changed");
}
async function claimJob(env: Env, documentId: string, generationId: string): Promise<Claim | null> {
  await recoverExpiredJobs(env);
  const token = crypto.randomUUID();
  const result = await env.DB.prepare(`UPDATE document_vector_jobs SET state='processing',owner_token=?,lease_expires_at=?,
    attempts=attempts+1,
    next_chunk_offset=CASE WHEN claimed_revision IS (SELECT document_revision FROM documents WHERE id=?) THEN next_chunk_offset ELSE 0 END,
    claimed_revision=(SELECT document_revision FROM documents WHERE id=?),started_at=?,updated_at=?,next_attempt_at=NULL
    WHERE document_id=? AND generation_id=? AND failure_count<?
      AND state='queued' AND (next_attempt_at IS NULL OR next_attempt_at<=?)
      AND EXISTS (SELECT 1 FROM documents d WHERE d.id=document_id AND d.active_text_generation=generation_id AND d.ingestion_state='ready')`)
    .bind(token, leaseExpiry(), documentId, documentId, now(), now(), documentId, generationId, vectorJobPolicy.maxFailures, now()).run();
  if (Number(result.meta.changes) !== 1)
    return null;
  const row = await env.DB.prepare(`SELECT claimed_revision AS revision,failure_count AS failures,next_chunk_offset AS offset
    FROM document_vector_jobs WHERE ${ownerWhere}`).bind(documentId, generationId, token, now()).first<{
    revision: number;
    failures: number;
    offset: number;
  }>();
  return row ? { documentId, generationId, token, ...row } : null;
}
async function recoverExpiredJobs(env: Env) {
  await env.DB.prepare(`UPDATE document_vector_jobs SET state='failed',failure_count=?,owner_token=NULL,lease_expires_at=NULL,
    next_attempt_at=NULL,last_error='Durable retry budget exhausted',updated_at=?
    WHERE (state='queued' AND failure_count>=?) OR (state='processing' AND (lease_expires_at IS NULL OR lease_expires_at<=?) AND failure_count+1>=?)`)
    .bind(vectorJobPolicy.maxFailures, now(), vectorJobPolicy.maxFailures, now(), vectorJobPolicy.maxFailures).run();
  // Recovery itself consumes one durable failure. Its backoff starts at expiry,
  // so long-abandoned work is eligible promptly and fresh expirations wait.
  await env.DB.prepare(`UPDATE document_vector_jobs SET state='queued',owner_token=NULL,
    next_attempt_at=strftime('%Y-%m-%dT%H:%M:%fZ',COALESCE(lease_expires_at,updated_at),
      '+' || (MIN(3600,60*(1<<MIN(failure_count,6)))+ABS(RANDOM()%30)) || ' seconds'),
    failure_count=failure_count+1,lease_expires_at=NULL,enqueued_at=NULL,last_error='Previous owner lease expired',updated_at=?
    WHERE state='processing' AND (lease_expires_at IS NULL OR lease_expires_at<=?) AND failure_count+1<?`)
    .bind(now(), now(), vectorJobPolicy.maxFailures).run();
}
async function recordFailure(env: Env, claim: Claim, error: unknown) {
  const failures = claim.failures + 1;
  const delay = Math.min(60 * 2 ** (failures - 1), 3600) + Math.floor(Math.random() * 30);
  const next = new Date(Date.now() + delay * 1000).toISOString();
  // Provider responses can contain credentials or source text. Persist a bounded
  // operational category, not arbitrary upstream error bodies.
  const message = error instanceof LeaseLost ? "Document changed during processing" : "Embedding or vector submission failed";
  const result = await env.DB.prepare(`UPDATE document_vector_jobs SET state=?,failure_count=failure_count+1,owner_token=NULL,
    lease_expires_at=NULL,next_attempt_at=?,enqueued_at=NULL,last_error=?,updated_at=? WHERE ${ownerWhere}`)
    .bind(failures >= vectorJobPolicy.maxFailures ? 'failed' : 'queued', failures >= vectorJobPolicy.maxFailures ? null : next, message, now(), claim.documentId, claim.generationId, claim.token, now()).run();
  return Number(result.meta.changes) === 1;
}
async function boundedUpsert(env: Env, vectors: VectorizeVector[]) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([env.VECTOR_INDEX.upsert(vectors), new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Vector upsert timed out")), 15000); })]);
  }
  finally {
    if (timer)
      clearTimeout(timer);
  }
}
export async function processDocumentVectorJob(env: Env, documentId: string, generationId = "initial") {
  const claim = await claimJob(env, documentId, generationId);
  if (!claim)
    return { skipped: true, vectorCount: 0 };
  try {
    const chunks = await loadDocumentChunks(env, documentId, generationId);
    if (!chunks.length || claim.offset > chunks.length)
      throw new Error("Incomplete chunk set");
    let offset = claim.offset;
    const end = Math.min(chunks.length, offset + vectorJobPolicy.maxChunksPerDelivery);
    while (offset < end) {
      const group = chunks.slice(offset, Math.min(offset + vectorUpsertBatchSize, end));
      const vectors: VectorizeVector[] = [];
      for (let at = 0;at < group.length;at += embeddingConcurrency) {
        await renew(env, claim);
        // Wait for every started provider request before releasing ownership.
        const results = await Promise.allSettled(group.slice(at, at + embeddingConcurrency).map(async (chunk) => {
          const values = await embed(env, chunk.chunkText);
          if (!values?.length || values.some(value => !Number.isFinite(value)))
            throw new Error("Embedding unavailable or invalid");
          return {
            id: chunk.chunkId, values, namespace: env.VECTOR_NAMESPACE, metadata: {
              documentId, generationId,
              paragraphAnchor: chunk.paragraphAnchor, paragraphAnchorEnd: chunk.paragraphAnchorEnd, citationAnchor: chunk.citationAnchor, sectionLabel: chunk.sectionLabel
            }
          } satisfies VectorizeVector;
        }));
        for (const result of results) {
          if (result.status === 'rejected')
            throw result.reason;
          vectors.push(result.value);
        }
      }
      await renew(env, claim);
      await boundedUpsert(env, vectors);
      offset += group.length;
      const progress = await env.DB.prepare(`UPDATE document_vector_jobs SET next_chunk_offset=?,vector_count=?,updated_at=?
        WHERE ${ownerWhere} AND ${currentRevisionWhere}`).bind(offset, offset, now(), documentId, generationId, claim.token, now()).run();
      if (Number(progress.meta.changes) !== 1)
        throw new LeaseLost("Ownership changed after vector submission");
    }
    const completed = offset === chunks.length;
    const result = await env.DB.prepare(`UPDATE document_vector_jobs SET state=?,completed_at=?,owner_token=NULL,lease_expires_at=NULL,
      next_attempt_at=NULL,enqueued_at=NULL,last_error=NULL,updated_at=? WHERE ${ownerWhere} AND ${currentRevisionWhere}`)
      .bind(completed ? 'completed' : 'queued', completed ? now() : null, now(), documentId, generationId, claim.token, now()).run();
    if (Number(result.meta.changes) !== 1)
      throw new LeaseLost("Ownership changed before completion");
    return { skipped: false, vectorCount: offset, continued: !completed };
  }
  catch (error) {
    await recordFailure(env, claim, error);
    return { skipped: true, vectorCount: 0 };
  }
}
export async function markVectorJobEnqueued(env: Env, documentId: string, generationId: string) {
  await env.DB.prepare(`UPDATE document_vector_jobs SET enqueued_at=?,updated_at=? WHERE document_id=? AND generation_id=?
    AND (state='queued' OR (state='processing' AND (lease_expires_at IS NULL OR lease_expires_at<=?)))`)
    .bind(now(), now(), documentId, generationId, now()).run();
}
export async function enqueueVectorJob(env: Env, documentId: string, generationId?: string) {
  const generation = generationId ?? (await env.DB.prepare("SELECT generation_id FROM document_vector_jobs WHERE document_id=?").bind(documentId).first<{
    generation_id: string;
  }>())?.generation_id;
  if (!generation)
    return;
  await env.VECTOR_JOBS_QUEUE.send({ documentId, generationId: generation }, { contentType: "json" });
  await markVectorJobEnqueued(env, documentId, generation);
}
export async function requeueStaleVectorJobs(env: Env, limit = 50) {
  await recoverExpiredJobs(env);
  const cutoff = new Date(Date.now() - requeueDelayMs).toISOString();
  const rows = await env.DB.prepare(`SELECT document_id AS documentId,generation_id AS generationId FROM document_vector_jobs
    WHERE failure_count<? AND ((state='queued' AND (next_attempt_at IS NULL OR next_attempt_at<=?) AND (enqueued_at IS NULL OR enqueued_at<?))
      OR (state='processing' AND (lease_expires_at IS NULL OR lease_expires_at<=?)))
      AND EXISTS (SELECT 1 FROM documents d WHERE d.id=document_id AND d.active_text_generation=generation_id AND d.ingestion_state='ready')
    ORDER BY created_at,document_id LIMIT ?`).bind(vectorJobPolicy.maxFailures, now(), cutoff, now(), Math.max(1, Math.min(100, limit)))
    .all<{
      documentId: string;
      generationId: string;
    }>();
  let sent = 0;
  for (const row of rows.results || []) {
    try {
      await enqueueVectorJob(env, row.documentId, row.generationId);
      sent++;
    }
    catch {
      console.warn("Vector job dispatch failed; durable row retained", { documentId: row.documentId, generationId: row.generationId });
    }
  }
  return sent;
}
export async function consumeVectorJobBatch(batch: MessageBatch<VectorJobMessage>, env: Env) {
  for (const message of batch.messages) {
    const parsed = messageSchema.safeParse(message.body);
    if (!parsed.success) {
      console.warn("Invalid vector job message discarded");
      message.ack();
      continue;
    }
    try {
      await processDocumentVectorJob(env, parsed.data.documentId, parsed.data.generationId);
      message.ack();
    }
    catch {
      message.retry({ delaySeconds: 60 });
    }
  }
}
export async function listVectorJobs(env: Env, options: {
  state?: string;
  after?: string;
  limit?: number;
}) {
  const input = z.object({ state: z.enum(['queued', 'processing', 'completed', 'failed']).optional(), after: z.string().max(128).default(''), limit: z.number().int().min(1).max(100).default(50) }).parse(options);
  const rows = (await env.DB.prepare(`SELECT document_id AS documentId,generation_id AS generationId,state,attempts,failure_count AS failureCount,
    claimed_revision AS claimedRevision,next_chunk_offset AS completedChunkCount,vector_count AS vectorCount,next_attempt_at AS nextAttemptAt,
    lease_expires_at AS leaseExpiresAt,last_error AS lastError,updated_at AS updatedAt FROM document_vector_jobs
    WHERE document_id>? AND (? IS NULL OR state=?) ORDER BY document_id LIMIT ?`).bind(input.after, input.state ?? null, input.state ?? null, input.limit + 1).all()).results || [];
  return { jobs: rows.slice(0, input.limit), nextCursor: rows.length > input.limit ? rows[input.limit - 1]!.documentId : null, policy: vectorJobPolicy };
}
