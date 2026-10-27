import { validateVectorJobManifest, VectorJobIntegrityError } from "./vector-job-manifest";
import { ensureVersionedVectorBatch, VectorVisibilityPending } from "./vector-generations";
import { vectorProfile } from "./vector-profile";
import { RequestQuotaError } from "../lib/http";
import { z } from "zod";
import type { Env } from "../lib/types";
export type VectorJobMessage = {
  documentId: string;
  generationId?: string;
};
const messageSchema = z.object({ documentId: z.string().min(1).max(128), generationId: z.string().min(1).max(128).optional() }).strict();
type Claim = {
  documentId: string;
  generationId: string;
  token: string;
  revision: number;
  failures: number;
  offset: number;
  attempts: number;
};
export const vectorJobPolicy = { maxFailures: 5, leaseMs: 120000, maxChunksPerDelivery: 64 } as const;
const vectorUpsertBatchSize = 16;
const requeueDelayMs = 10 * 60 * 1000;
const now = () => new Date(Date.now()).toISOString();
const leaseExpiry = () => new Date(Date.now() + vectorJobPolicy.leaseMs).toISOString();
const ownerWhere = "document_id=? AND generation_id=? AND owner_token=? AND state='processing' AND lease_expires_at>?";
const currentRevisionWhere = `EXISTS (SELECT 1 FROM documents d WHERE d.id=document_id AND d.active_text_generation=generation_id
  AND d.document_revision=claimed_revision AND d.ingestion_state='ready')`;
class LeaseLost extends Error {
}
async function renew(env: Env, claim: Claim) {
  const result = await env.DB.prepare(`UPDATE document_vector_jobs SET lease_expires_at=?,updated_at=? WHERE ${ownerWhere} AND ${currentRevisionWhere}`)
    .bind(leaseExpiry(), now(), claim.documentId, claim.generationId, claim.token, now()).run();
  if (Number(result.meta.changes) !== 1)
    throw new LeaseLost("Job ownership or document revision changed");
}
async function claimJob(env: Env, documentId: string, generationId: string): Promise<Claim | null> {
  await recoverExpiredJobs(env);
  const profile=await vectorProfile(env);
  await env.DB.prepare(`UPDATE document_vector_jobs SET state='failed',last_error='Embedding profile changed; prepare a new backfill',updated_at=?
    WHERE document_id=? AND generation_id=? AND embedding_profile_id IS NOT NULL AND embedding_profile_id!=? AND state='queued'`).bind(now(),documentId,generationId,profile.id).run();
  const token = crypto.randomUUID();
  const result = await env.DB.prepare(`UPDATE document_vector_jobs SET state='processing',owner_token=?,lease_expires_at=?,embedding_profile_id=?,
    attempts=attempts+1,
    next_chunk_offset=CASE WHEN claimed_revision IS (SELECT document_revision FROM documents WHERE id=?) THEN next_chunk_offset ELSE 0 END,
    claimed_revision=(SELECT document_revision FROM documents WHERE id=?),started_at=?,updated_at=?,next_attempt_at=NULL
    WHERE document_id=? AND generation_id=? AND failure_count<?
      AND outbox_ready_at IS NOT NULL AND state='queued' AND (next_attempt_at IS NULL OR next_attempt_at<=?)
      AND EXISTS (SELECT 1 FROM documents d WHERE d.id=document_id AND d.active_text_generation=generation_id AND d.ingestion_state='ready')`)
    .bind(token, leaseExpiry(), profile.id, documentId, documentId, now(), now(), documentId, generationId, vectorJobPolicy.maxFailures, now()).run();
  if (Number(result.meta.changes) !== 1)
    return null;
  const row = await env.DB.prepare(`SELECT claimed_revision AS revision,failure_count AS failures,next_chunk_offset AS offset,attempts
    FROM document_vector_jobs WHERE ${ownerWhere}`).bind(documentId, generationId, token, now()).first<{
    revision: number;
    failures: number;
    offset: number;
  attempts: number;
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
  if(error instanceof RequestQuotaError || error instanceof VectorVisibilityPending) {
    const next=new Date(Date.now()+(error instanceof RequestQuotaError?error.retryAfterSeconds:10)*1000).toISOString();
    const result=await env.DB.prepare(`UPDATE document_vector_jobs SET state='queued',owner_token=NULL,lease_expires_at=NULL,
      next_attempt_at=?,enqueued_at=NULL,last_error=?,updated_at=? WHERE ${ownerWhere}`)
      .bind(next,error instanceof RequestQuotaError?'Application compute budget reached; waiting for reset':'Vector visibility pending',now(),claim.documentId,claim.generationId,claim.token,now()).run();
    return Number(result.meta.changes)===1;
  }
  const failures = claim.failures + 1;
  const delay = Math.min(60 * 2 ** (failures - 1), 3600) + Math.floor(Math.random() * 30);
  const next = new Date(Date.now() + delay * 1000).toISOString();
  // Provider responses can contain credentials or source text. Persist a bounded
  // operational category, not arbitrary upstream error bodies.
  const terminal = error instanceof VectorJobIntegrityError || failures >= vectorJobPolicy.maxFailures;
  const message = error instanceof VectorJobIntegrityError ? "Source or chunk manifest invalid; reprocess the document" : error instanceof LeaseLost ? "Document changed during processing" : "Embedding or vector submission failed";
  const result = await env.DB.prepare(`UPDATE document_vector_jobs SET state=?,failure_count=failure_count+1,owner_token=NULL,
    lease_expires_at=NULL,next_attempt_at=?,enqueued_at=NULL,last_error=?,updated_at=? WHERE ${ownerWhere}`)
    .bind(terminal ? 'failed' : 'queued', terminal ? null : next, message, now(), claim.documentId, claim.generationId, claim.token, now()).run();
  return Number(result.meta.changes) === 1;
}
export async function processDocumentVectorJob(env: Env, documentId: string, generationId = "initial") {
  const claim = await claimJob(env, documentId, generationId);
  if (!claim){await env.TELEMETRY?.job(documentId,generationId,"skipped",0,0);return { skipped: true, vectorCount: 0 };}
  try {
    const chunks = await validateVectorJobManifest(env, documentId, generationId);
    if (!chunks.length || claim.offset > chunks.length)
      throw new Error("Incomplete chunk set");
    let offset = claim.offset;
    const end = Math.min(chunks.length, offset + vectorJobPolicy.maxChunksPerDelivery);
    while (offset < end) {
      const group = chunks.slice(offset, Math.min(offset + vectorUpsertBatchSize, end));
      await ensureVersionedVectorBatch(env,group.map(chunk=>({documentId,chunkId:chunk.chunkId,sourceKind:"document",generation:generationId,text:chunk.chunkText,
        metadata:{paragraphAnchor:chunk.paragraphAnchor,paragraphAnchorEnd:chunk.paragraphAnchorEnd,citationAnchor:chunk.citationAnchor,sectionLabel:chunk.sectionLabel}})),()=>renew(env,claim));
      offset += group.length;
      const progress = await env.DB.prepare(`UPDATE document_vector_jobs SET next_chunk_offset=?,vector_count=?,updated_at=?
        WHERE ${ownerWhere} AND ${currentRevisionWhere}`).bind(offset, offset, now(), documentId, generationId, claim.token, now()).run();
      if (Number(progress.meta.changes) !== 1)
        throw new LeaseLost("Ownership changed after vector submission");
    }
    await validateVectorJobManifest(env, documentId, generationId);
    const completed = offset === chunks.length;
    const result = await env.DB.prepare(`UPDATE document_vector_jobs SET state=?,completed_at=?,owner_token=NULL,lease_expires_at=NULL,
      next_attempt_at=NULL,enqueued_at=NULL,last_error=NULL,updated_at=? WHERE ${ownerWhere} AND ${currentRevisionWhere}`)
      .bind(completed ? 'completed' : 'queued', completed ? now() : null, now(), documentId, generationId, claim.token, now()).run();
    if (Number(result.meta.changes) !== 1)
      throw new LeaseLost("Ownership changed before completion");
    await env.TELEMETRY?.job(documentId,generationId,completed?"completed":"continued",claim.attempts,offset);
    return { skipped: false, vectorCount: offset, continued: !completed };
  }
  catch (error) {
    const retained=await recordFailure(env, claim, error);
    await env.TELEMETRY?.job(documentId,generationId,!retained?"lease_lost":error instanceof VectorJobIntegrityError||(!(error instanceof RequestQuotaError)&&!(error instanceof VectorVisibilityPending)&&claim.failures+1>=vectorJobPolicy.maxFailures)?"failed":"retryable",claim.attempts,claim.offset);
    return { skipped: true, vectorCount: 0 };
  }
}
export async function markVectorJobEnqueued(env: Env, documentId: string, generationId: string) {
  await env.DB.prepare(`UPDATE document_vector_jobs SET enqueued_at=?,updated_at=? WHERE document_id=? AND generation_id=?
    AND (state='queued' OR (state='processing' AND (lease_expires_at IS NULL OR lease_expires_at<=?)))`)
    .bind(now(), now(), documentId, generationId, now()).run();
}
export async function enqueueVectorJob(env: Env, documentId: string, generationId?: string) {
  const row = await env.DB.prepare(`SELECT generation_id FROM document_vector_jobs WHERE document_id=? AND outbox_ready_at IS NOT NULL
    AND state='queued' AND (? IS NULL OR generation_id=?)`).bind(documentId,generationId??null,generationId??null).first<{generation_id:string}>();
  const generation = row?.generation_id;
  if (!generation) return;
  try{
    await env.VECTOR_JOBS_QUEUE.send({ documentId, generationId: generation }, { contentType: "json" });
    await markVectorJobEnqueued(env, documentId, generation);
    await env.TELEMETRY?.job(documentId,generation,"dispatched",0,0);
  }catch(error){await env.TELEMETRY?.job(documentId,generation,"dispatch_deferred",0,0);throw error;}
}
export async function requeueStaleVectorJobs(env: Env, limit = 50) {
  await recoverExpiredJobs(env);
  const cutoff = new Date(Date.now() - requeueDelayMs).toISOString();
  const rows = await env.DB.prepare(`SELECT document_id AS documentId,generation_id AS generationId FROM document_vector_jobs
    WHERE outbox_ready_at IS NOT NULL AND failure_count<? AND ((state='queued' AND (next_attempt_at IS NULL OR next_attempt_at<=?) AND (enqueued_at IS NULL OR enqueued_at<?))
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
      env.TELEMETRY?.tag("job_state","dispatch_deferred");
    }
  }
  return sent;
}
export async function consumeVectorJobBatch(batch: MessageBatch<VectorJobMessage>, env: Env) {
  for (const message of batch.messages) {
    const parsed = messageSchema.safeParse(message.body);
    if (!parsed.success) {
      env.TELEMETRY?.tag("job_state","invalid_message");
      message.ack();
      continue;
    }
    try {
      await processDocumentVectorJob(env, parsed.data.documentId, parsed.data.generationId);
      message.ack();
    }
    catch {
      env.TELEMETRY?.tag("job_state","retryable");
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
    claimed_revision AS claimedRevision,outbox_ready_at AS outboxReadyAt,prepared_revision AS preparedRevision,expected_chunk_count AS expectedChunkCount,next_chunk_offset AS completedChunkCount,vector_count AS vectorCount,next_attempt_at AS nextAttemptAt,
    lease_expires_at AS leaseExpiresAt,last_error AS lastError,updated_at AS updatedAt FROM document_vector_jobs
    WHERE document_id>? AND (? IS NULL OR state=?) ORDER BY document_id LIMIT ?`).bind(input.after, input.state ?? null, input.state ?? null, input.limit + 1).all()).results || [];
  return { jobs: rows.slice(0, input.limit), nextCursor: rows.length > input.limit ? rows[input.limit - 1]!.documentId : null, policy: vectorJobPolicy };
}
