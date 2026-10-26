import type { Env } from "../lib/types";
import { ensureVersionedVector, VectorVisibilityPending } from "./vector-generations";
import { RequestConflictError, RequestValidationError } from "../lib/http";
import { activationWriteSchema, activationRollbackSchema, canonicalJson, sha256, type ActivationPlan } from "./activation-contract";
import { verifyCurrentActivationPlan, verifyPlanDigest, sourceDigest } from "./activation-plan";
import { trustedDocumentSql } from "./publication-policy";
type PlanRow = {
  batch_id: string;
  digest: string;
  rollback_digest: string;
  plan_json: string;
  state: "staging" | "retryable" | "published" | "rolled_back";
  owner_token: string | null;
  lease_expires_at: string | null;
};
type StagedRow = {
  chunk_id: string;
  document_id: string;
  payload_json: string;
  payload_digest: string;
  vector_submitted: number;
  vector_ready: number;
};
const leaseMilliseconds = 15 * 60 * 1000;
const now = () => new Date().toISOString();
const leaseExpiry = () => new Date(Date.now() + leaseMilliseconds).toISOString();
const ownerClause = "EXISTS (SELECT 1 FROM retrieval_activation_plans p WHERE p.batch_id=? AND p.owner_token=? AND p.lease_expires_at>?)";
async function readPlan(env: Env, batchId: string) {
  return env.DB.prepare("SELECT * FROM retrieval_activation_plans WHERE batch_id=?").bind(batchId).first<PlanRow>();
}
async function report(env: Env, plan: ActivationPlan, options: {
  dryRun?: boolean;
  replayed?: boolean;
} = {}) {
  const row = await readPlan(env, plan.batchId);
  const active = await env.DB.prepare(`SELECT COUNT(*) AS count FROM retrieval_search_chunks c JOIN documents d ON d.id=c.document_id
    WHERE c.batch_id=? AND c.active=1 AND ${trustedDocumentSql()}`).bind(plan.batchId).first<{
    count: number;
  }>();
  const pending = await env.DB.prepare("SELECT COUNT(*) AS count FROM retrieval_activation_staged_chunks WHERE batch_id=? AND vector_ready=0")
    .bind(plan.batchId).first<{
      count: number;
    }>();
  return {
    schemaVersion: 2, batchId: plan.batchId, digest: plan.digest, rollbackDigest: plan.rollbackDigest,
    state: options.dryRun ? "validated" : row?.state || "staging", readOnly: Boolean(options.dryRun), replayed: Boolean(options.replayed),
    expectedDocumentCount: plan.documents.length, expectedChunkCount: plan.chunks.length, currentlyQueryableChunkCount: active?.count || 0,
    pendingVectorCount: plan.performVectorUpsert ? pending?.count || 0 : 0,
    rollback: { schemaVersion: 2, batchId: plan.batchId, digest: plan.digest, rollbackDigest: plan.rollbackDigest }
  };
}
async function claim(env: Env, plan: ActivationPlan, rollback = false) {
  const token = crypto.randomUUID();
  const result = await env.DB.prepare(`UPDATE retrieval_activation_plans SET owner_token=?, lease_expires_at=?, last_error=NULL
    WHERE batch_id=? AND digest=? AND state ${rollback ? "= 'published'" : "IN ('staging','retryable')"}
      AND (owner_token IS NULL OR lease_expires_at<=?)`).bind(token, leaseExpiry(), plan.batchId, plan.digest, now()).run();
  if (Number(result.meta.changes) !== 1)
    throw new RequestConflictError("Activation is already running or changed. Retry the identical plan after its lease expires.");
  return token;
}
async function release(env: Env, plan: ActivationPlan, token: string, rollback: boolean, message: string) {
  await env.DB.prepare(`UPDATE retrieval_activation_plans SET owner_token=NULL,lease_expires_at=NULL,last_error=?,
    state=CASE WHEN state IN ('staging','retryable') THEN 'retryable' ELSE state END
    WHERE batch_id=? AND owner_token=? AND state ${rollback ? "= 'published'" : "IN ('staging','retryable')"}`)
    .bind(message, plan.batchId, token).run();
}
async function stagedRows(env: Env, plan: ActivationPlan) {
  const rows = (await env.DB.prepare("SELECT * FROM retrieval_activation_staged_chunks WHERE batch_id=? ORDER BY chunk_id").bind(plan.batchId).all<StagedRow>()).results || [];
  const expected = new Map(plan.chunks.map(chunk => [chunk.chunkId, chunk]));
  if (rows.length !== expected.size)
    throw new RequestValidationError("Staged activation row count differs from its immutable plan.");
  for (const row of rows) {
    const chunk = expected.get(row.chunk_id);
    if (!chunk || row.document_id !== chunk.documentId || row.payload_digest !== await sha256(chunk) || canonicalJson(JSON.parse(row.payload_json)) !== canonicalJson(chunk)) {
      throw new RequestValidationError("Staged activation content differs from its immutable plan.");
    }
  }
  return rows;
}
async function stage(env: Env, plan: ActivationPlan, token: string) {
  const statements = await Promise.all(plan.chunks.map(async (chunk) => env.DB.prepare(`INSERT INTO retrieval_activation_staged_chunks
    (batch_id,chunk_id,document_id,payload_json,payload_digest) SELECT ?,?,?,?,? WHERE ${ownerClause}
    ON CONFLICT(batch_id,chunk_id) DO NOTHING`).bind(plan.batchId, chunk.chunkId, chunk.documentId, canonicalJson(chunk), await sha256(chunk), plan.batchId, token, now())));
  for (let at = 0;at < statements.length;at += 50)
    await env.DB.batch(statements.slice(at, at + 50));
  return stagedRows(env, plan);
}
async function vectors(env: Env, plan: ActivationPlan, token: string, rows: StagedRow[]) {
  if (!plan.performVectorUpsert) return true;
  const pending=rows.filter(row=>!row.vector_ready).slice(0,4);
  const outcomes=await Promise.allSettled(pending.map(async row=>{
    const chunk=plan.chunks.find(chunk=>chunk.chunkId===row.chunk_id)!;
    const source=plan.documents.find(document=>document.documentId===chunk.documentId)!;
    try {
      await ensureVersionedVector(env,{documentId:chunk.documentId,chunkId:chunk.chunkId,sourceKind:"retrieval",generation:plan.batchId,text:chunk.sourceText,sourceSha256:source.sourceSha256,
        metadata:{activationDigest:plan.digest,chunkDigest:row.payload_digest,citationAnchor:chunk.citationAnchorStart,sectionLabel:chunk.sectionLabel}},async()=>{
          const owner=await env.DB.prepare(`SELECT 1 FROM retrieval_activation_plans WHERE batch_id=? AND owner_token=? AND lease_expires_at>?`).bind(plan.batchId,token,now()).first();
          if(!owner) throw new RequestConflictError("Activation ownership changed during vector work");
        });
      const result=await env.DB.prepare(`UPDATE retrieval_activation_staged_chunks SET vector_submitted=1,vector_ready=1 WHERE batch_id=? AND chunk_id=? AND ${ownerClause}`)
        .bind(plan.batchId,row.chunk_id,plan.batchId,token,now()).run();
      if(Number(result.meta.changes)!==1) throw new RequestConflictError("Activation ownership changed during verification");
    }catch(error) {
      if(error instanceof VectorVisibilityPending) {
        await env.DB.prepare(`UPDATE retrieval_activation_staged_chunks SET vector_submitted=1 WHERE batch_id=? AND chunk_id=? AND ${ownerClause}`).bind(plan.batchId,row.chunk_id,plan.batchId,token,now()).run();
        return;
      }
      throw error;
    }
  }));
  for(const outcome of outcomes) if(outcome.status==='rejected') throw outcome.reason;
  return (await stagedRows(env,plan)).every(row=>row.vector_ready===1);
}
function guardStatement(env: Env, plan: ActivationPlan, token: string, mode: "publish" | "rollback", expected: unknown[]) {
  const guard = crypto.randomUUID();
  return {
    guard, statement: env.DB.prepare("INSERT INTO retrieval_activation_guards (token,batch_id,owner_token,mode,expected_documents_json) VALUES (?,?,?,?,?)")
      .bind(guard, plan.batchId, token, mode, JSON.stringify(expected))
  };
}
async function transactional(env: Env, statements: D1PreparedStatement[]) {
  try {
    await env.DB.batch(statements);
  }
  catch (error) {
    if (/activation_(owner|document|staging|projection)/.test(String(error)))
      throw new RequestConflictError("Activation inputs, ownership, or document revisions changed before publication.");
    throw error;
  }
}
async function publish(env: Env, plan: ActivationPlan, token: string) {
  await stagedRows(env, plan);
  for (const document of plan.documents) {
    if (await sourceDigest(env, document.sourceFileRef) !== document.sourceSha256)
      throw new RequestConflictError("Source changed during activation; publication was stopped.");
  }
  const expected = plan.documents.map(doc => ({ ...doc, activeBatchId: plan.rollback.documents.find(previous => previous.documentId === doc.documentId)!.previousBatchId }));
  const { guard, statement } = guardStatement(env, plan, token, "publish", expected);
  const timestamp = now();
  const planJson = canonicalJson(plan);
  const json = (field: string) => `json_extract(c.payload_json,'$.${field}')`;
  await transactional(env, [statement,
    env.DB.prepare(`INSERT INTO retrieval_activation_batches (batch_id,created_at,trusted_document_count,trusted_chunk_count,activation_checksum,rollback_checksum)
      VALUES (?,?,?,?,?,?)`).bind(plan.batchId, timestamp, plan.documents.length, plan.chunks.length, plan.digest, plan.rollbackDigest),
    env.DB.prepare(`INSERT INTO retrieval_activation_documents (batch_id,document_id,trust_source,write_status,created_at)
      SELECT ?,json_extract(value,'$.documentId'),'current_policy','written',? FROM json_each(?,'$.documents')`).bind(plan.batchId, timestamp, planJson),
    env.DB.prepare("UPDATE retrieval_search_chunks SET active=0 WHERE active=1 AND document_id IN (SELECT json_extract(value,'$.documentId') FROM json_each(?,'$.documents'))").bind(planJson),
    env.DB.prepare(`INSERT INTO retrieval_search_chunks (chunk_id,batch_id,document_id,title,citation,source_file_ref,source_link,section_label,paragraph_anchor,citation_anchor,
      chunk_text,chunk_type,retrieval_priority,has_canonical_reference_alignment,active,created_at)
      SELECT c.chunk_id,c.batch_id,c.document_id,d.title,d.citation,d.source_r2_key,json_extract(doc.value,'$.sourceLink'),${json('sectionLabel')},${json('paragraphAnchorStart')},${json('citationAnchorStart')},
      ${json('sourceText')},${json('chunkType')},${json('retrievalPriority')},${json('hasCanonicalReferenceAlignment')},1,?
      FROM retrieval_activation_staged_chunks c JOIN documents d ON d.id=c.document_id
      JOIN json_each(?,'$.documents') doc ON json_extract(doc.value,'$.documentId')=d.id WHERE c.batch_id=?`).bind(timestamp, planJson, plan.batchId),
    env.DB.prepare(`INSERT INTO retrieval_embedding_rows (embedding_id,batch_id,document_id,chunk_id,chunk_type,retrieval_priority,citation_anchor_start,citation_anchor_end,
      has_canonical_reference_alignment,source_link,source_text,created_at)
      SELECT 'emb_'||c.chunk_id,c.batch_id,c.document_id,c.chunk_id,${json('chunkType')},${json('retrievalPriority')},${json('citationAnchorStart')},${json('citationAnchorEnd')},
      ${json('hasCanonicalReferenceAlignment')},json_extract(doc.value,'$.sourceLink'),${json('sourceText')},?
      FROM retrieval_activation_staged_chunks c JOIN json_each(?,'$.documents') doc ON json_extract(doc.value,'$.documentId')=c.document_id WHERE c.batch_id=?`).bind(timestamp, planJson, plan.batchId),
    env.DB.prepare(`INSERT INTO retrieval_search_rows (search_id,batch_id,document_id,chunk_id,title,chunk_type,retrieval_priority,citation_anchor_start,citation_anchor_end,
      has_canonical_reference_alignment,source_link,created_at)
      SELECT 'srch_'||c.chunk_id,c.batch_id,c.document_id,c.chunk_id,json_extract(doc.value,'$.title'),${json('chunkType')},${json('retrievalPriority')},${json('citationAnchorStart')},${json('citationAnchorEnd')},
      ${json('hasCanonicalReferenceAlignment')},json_extract(doc.value,'$.sourceLink'),?
      FROM retrieval_activation_staged_chunks c JOIN json_each(?,'$.documents') doc ON json_extract(doc.value,'$.documentId')=c.document_id WHERE c.batch_id=?`).bind(timestamp, planJson, plan.batchId),
    env.DB.prepare(`INSERT INTO retrieval_activation_chunks (batch_id,chunk_id,document_id,chunk_type,embedding_write_status,search_write_status,provenance_complete,created_at)
      SELECT c.batch_id,c.chunk_id,c.document_id,${json('chunkType')},?,'written',1,? FROM retrieval_activation_staged_chunks c WHERE c.batch_id=?`)
      .bind(plan.performVectorUpsert ? 'stored_vector_verified' : 'db_only', timestamp, plan.batchId),
    env.DB.prepare("UPDATE documents SET active_retrieval_batch=?,retrieval_revision=retrieval_revision+1 WHERE id IN (SELECT json_extract(value,'$.documentId') FROM json_each(?,'$.documents'))").bind(plan.batchId, planJson),
    env.DB.prepare("UPDATE retrieval_activation_plans SET state='published',published_at=?,owner_token=NULL,lease_expires_at=NULL,last_error=NULL WHERE batch_id=? AND owner_token=?").bind(timestamp, plan.batchId, token),
    env.DB.prepare("DELETE FROM retrieval_activation_guards WHERE token=?").bind(guard)
  ]);
}
export async function writeTrustedRetrievalActivation(env: Env, rawInput: unknown) {
  const { plan, dryRun } = activationWriteSchema.parse(rawInput);
  await verifyPlanDigest(plan);
  const existing = await readPlan(env, plan.batchId);
  if (existing && existing.digest !== plan.digest)
    throw new RequestConflictError("Batch ID is immutable and already belongs to a different activation plan.");
  if (existing && (existing.state === 'published' || existing.state === 'rolled_back'))
    return report(env, plan, { dryRun, replayed: true });
  const legacy = await env.DB.prepare("SELECT batch_id FROM retrieval_activation_batches WHERE batch_id=?").bind(plan.batchId).first();
  if (legacy)
    throw new RequestConflictError("Batch ID belongs to a historical activation; use a fresh ID and an explicitly prepared v2 plan.");
  await verifyCurrentActivationPlan(env, plan);
  if (dryRun)
    return report(env, plan, { dryRun: true });
  await env.DB.prepare(`INSERT INTO retrieval_activation_plans (batch_id,digest,rollback_digest,plan_json,state,created_at)
    VALUES (?,?,?,?,'staging',?) ON CONFLICT(batch_id) DO NOTHING`).bind(plan.batchId, plan.digest, plan.rollbackDigest, canonicalJson(plan), now()).run();
  const token = await claim(env, plan);
  try {
    const rows = await stage(env, plan, token);
    const ready = await vectors(env, plan, token, rows);
    await verifyCurrentActivationPlan(env, plan);
    if (!ready) {
      await release(env, plan, token, false, "Submitted vectors are not yet visible; retry this identical plan.");
      return report(env, plan);
    }
    await publish(env, plan, token);
    return report(env, plan);
  }
  catch (error) {
    await release(env, plan, token, false, "Activation incomplete; inspect and retry the identical plan.").catch(() => { });
    throw error;
  }
}
async function validateRollbackTargets(env: Env, plan: ActivationPlan) {
  for (const target of plan.rollback.documents) {
    if (!target.previousBatchId) {
      if (target.previousChunkIds.length)
        throw new RequestValidationError("Empty rollback target cannot contain chunks.");
      continue;
    }
    const previous = await readPlan(env, target.previousBatchId);
    if (previous?.state !== "published")
      throw new RequestConflictError("Retained rollback activation is unavailable.");
    const previousPlan = activationWriteSchema.shape.plan.parse(JSON.parse(previous.plan_json));
    await verifyPlanDigest(previousPlan);
    await stagedRows(env, previousPlan);
    const previousDocument = previousPlan.documents.find(doc => doc.documentId === target.documentId);
    const document = plan.documents.find(doc => doc.documentId === target.documentId)!;
    if (!previousDocument || previousDocument.documentRevision !== document.documentRevision ||
      previousDocument.sourceSha256 !== document.sourceSha256 || previousDocument.sourceFileRef !== document.sourceFileRef) {
      throw new RequestConflictError("Retained rollback source revision differs from the current activation.");
    }
    const chunks = previousPlan.chunks.filter(chunk => chunk.documentId === target.documentId);
    if (canonicalJson(chunks.map(chunk => chunk.chunkId).sort()) !== canonicalJson([...target.previousChunkIds].sort())) {
      throw new RequestConflictError("Retained rollback selection differs from its immutable plan.");
    }
    const rows = (await env.DB.prepare(`SELECT chunk_id,chunk_text,section_label,paragraph_anchor,citation_anchor,chunk_type,retrieval_priority,
      has_canonical_reference_alignment,title,citation,source_file_ref,source_link FROM retrieval_search_chunks WHERE batch_id=? AND document_id=?`)
      .bind(target.previousBatchId, target.documentId).all<Record<string, unknown>>()).results || [];
    if (rows.length !== chunks.length)
      throw new RequestConflictError("Retained rollback projection is incomplete.");
    for (const chunk of chunks) {
      const row = rows.find(row => row.chunk_id === chunk.chunkId);
      const expected = {
        chunk_id: chunk.chunkId, chunk_text: chunk.sourceText, section_label: chunk.sectionLabel,
        paragraph_anchor: chunk.paragraphAnchorStart, citation_anchor: chunk.citationAnchorStart, chunk_type: chunk.chunkType,
        retrieval_priority: chunk.retrievalPriority, has_canonical_reference_alignment: Number(chunk.hasCanonicalReferenceAlignment),
        title: previousDocument.title, citation: previousDocument.citation, source_file_ref: previousDocument.sourceFileRef, source_link: previousDocument.sourceLink
      };
      if (canonicalJson(row) !== canonicalJson(expected))
        throw new RequestConflictError("Retained rollback projection differs from its immutable plan.");
    }
  }
}
export async function rollbackTrustedRetrievalActivation(env: Env, rawInput: unknown) {
  const input = activationRollbackSchema.parse(rawInput);
  const row = await readPlan(env, input.batchId);
  if (!row || row.digest !== input.digest || row.rollback_digest !== input.rollbackDigest)
    throw new RequestValidationError("Rollback must identify the exact stored activation and rollback digests.");
  const plan = activationWriteSchema.shape.plan.parse(JSON.parse(row.plan_json));
  await verifyPlanDigest(plan);
  if (row.state === 'rolled_back')
    return report(env, plan, { dryRun: input.dryRun, replayed: true });
  if (row.state !== 'published')
    throw new RequestConflictError("Only a published activation can be rolled back.");
  const expected = [];
  for (const document of plan.documents) {
    const current = await env.DB.prepare(`SELECT d.document_revision AS documentRevision,d.retrieval_revision AS retrievalRevision,d.active_retrieval_batch AS activeBatchId,
      d.active_text_generation AS textGeneration,d.source_r2_key AS sourceFileRef FROM documents d WHERE d.id=? AND ${trustedDocumentSql()}`)
      .bind(document.documentId).first<{
        documentRevision: number;
        retrievalRevision: number;
        activeBatchId: string | null;
        textGeneration: string;
        sourceFileRef: string;
      }>();
    if (!current || current.documentRevision !== document.documentRevision || current.activeBatchId !== plan.batchId || current.textGeneration !== document.textGeneration ||
      current.sourceFileRef !== document.sourceFileRef || await sourceDigest(env, current.sourceFileRef) !== document.sourceSha256)
      throw new RequestConflictError("A newer activation or document revision prevents this rollback.");
    expected.push({ ...current, documentId: document.documentId });
  }
  await validateRollbackTargets(env, plan);
  if (input.dryRun)
    return report(env, plan, { dryRun: true });
  const token = await claim(env, plan, true);
  try {
    const { guard, statement } = guardStatement(env, plan, token, "rollback", expected);
    const mutations = [statement,
      env.DB.prepare("UPDATE retrieval_search_chunks SET active=0 WHERE batch_id=? AND active=1").bind(plan.batchId)];
    for (const target of plan.rollback.documents) {
      mutations.push(env.DB.prepare("UPDATE retrieval_search_chunks SET active=1 WHERE document_id=? AND batch_id=? AND chunk_id IN (SELECT value FROM json_each(?))")
        .bind(target.documentId, target.previousBatchId, JSON.stringify(target.previousChunkIds)));
      mutations.push(env.DB.prepare("UPDATE documents SET active_retrieval_batch=?,retrieval_revision=retrieval_revision+1 WHERE id=?")
        .bind(target.previousBatchId, target.documentId));
    }
    mutations.push(env.DB.prepare("UPDATE retrieval_activation_plans SET state='rolled_back',rolled_back_at=?,owner_token=NULL,lease_expires_at=NULL WHERE batch_id=? AND owner_token=?").bind(now(), plan.batchId, token), env.DB.prepare("DELETE FROM retrieval_activation_guards WHERE token=?").bind(guard));
    await transactional(env, mutations);
    return report(env, plan);
  }
  catch (error) {
    await release(env, plan, token, true, "Rollback incomplete; inspect and retry the exact stored target.").catch(() => { });
    throw error;
  }
}
