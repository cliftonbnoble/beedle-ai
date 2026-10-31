import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {inspectRestorationOwner,RestorationControlError,type RestorationIdentity,type RestorationPlannedJob} from './restoration-job-control';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';

// Additive operational controls for a NEW execution. The old held tick and
// terminal history are never updated, deleted, impersonated or made idle.
export const restorationSourceSuccessorSql=[
  "CREATE TABLE beedle_restore_source_successor(singleton INTEGER PRIMARY KEY CHECK(singleton=1),successor_id TEXT NOT NULL UNIQUE,successor_plan_sha256 TEXT NOT NULL,owner_id TEXT NOT NULL,source_plan_sha256 TEXT NOT NULL,predecessor_tick_token TEXT NOT NULL,predecessor_owner_sha256 TEXT NOT NULL,predecessor_tick_sha256 TEXT NOT NULL,predecessor_history_sha256 TEXT NOT NULL,completion_operation_id TEXT NOT NULL,completion_result_sha256 TEXT NOT NULL,preserved_generation_id TEXT NOT NULL,preserved_generation_sha256 TEXT NOT NULL,preserved_generation_status TEXT NOT NULL CHECK(preserved_generation_status IN ('failed','published')),preserved_document_sha256 TEXT NOT NULL,preserved_generations_sha256 TEXT NOT NULL,initial_processed INTEGER NOT NULL CHECK(initial_processed>=1),initial_remaining INTEGER NOT NULL CHECK(initial_remaining>=1),state TEXT NOT NULL CHECK(state IN ('building','completed','held')),created_at TEXT NOT NULL,updated_at TEXT NOT NULL,error_sha256 TEXT)",
  "CREATE TRIGGER beedle_restore_source_successor_identity BEFORE UPDATE OF successor_id,successor_plan_sha256,owner_id,source_plan_sha256,predecessor_tick_token,predecessor_owner_sha256,predecessor_tick_sha256,predecessor_history_sha256,completion_operation_id,completion_result_sha256,preserved_generation_id,preserved_generation_sha256,preserved_generation_status,preserved_document_sha256,preserved_generations_sha256,initial_processed,initial_remaining,created_at ON beedle_restore_source_successor BEGIN SELECT RAISE(ABORT,'source_successor_identity_immutable'); END",
  "CREATE TRIGGER beedle_restore_source_successor_terminal BEFORE UPDATE ON beedle_restore_source_successor WHEN OLD.state!='building' BEGIN SELECT RAISE(ABORT,'source_successor_terminal_immutable'); END",
  "CREATE TRIGGER beedle_restore_source_successor_delete BEFORE DELETE ON beedle_restore_source_successor BEGIN SELECT RAISE(ABORT,'source_successor_identity_immutable'); END",
  "CREATE TABLE beedle_restore_source_successor_tick(singleton INTEGER PRIMARY KEY CHECK(singleton=1),successor_id TEXT NOT NULL,successor_plan_sha256 TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('idle','running','held')),token TEXT,started_at TEXT,updated_at TEXT NOT NULL,CHECK(state='idle' OR token IS NOT NULL))",
  "CREATE TRIGGER beedle_restore_source_successor_tick_identity BEFORE UPDATE OF successor_id,successor_plan_sha256 ON beedle_restore_source_successor_tick BEGIN SELECT RAISE(ABORT,'source_successor_tick_immutable'); END",
  "CREATE TRIGGER beedle_restore_source_successor_tick_held BEFORE UPDATE ON beedle_restore_source_successor_tick WHEN OLD.state='held' BEGIN SELECT RAISE(ABORT,'source_successor_tick_held_immutable'); END",
  "CREATE TRIGGER beedle_restore_source_successor_tick_delete BEFORE DELETE ON beedle_restore_source_successor_tick BEGIN SELECT RAISE(ABORT,'source_successor_tick_immutable'); END",
  "CREATE TABLE beedle_restore_source_successor_history(token TEXT PRIMARY KEY,successor_id TEXT NOT NULL,successor_plan_sha256 TEXT NOT NULL,started_at TEXT NOT NULL,completed_at TEXT,state TEXT NOT NULL CHECK(state IN ('running','completed','held')),deliveries INTEGER NOT NULL DEFAULT 0 CHECK(deliveries BETWEEN 0 AND 4),error_sha256 TEXT)",
  "CREATE TRIGGER beedle_restore_source_successor_history_identity BEFORE UPDATE OF token,successor_id,successor_plan_sha256,started_at ON beedle_restore_source_successor_history BEGIN SELECT RAISE(ABORT,'source_successor_history_immutable'); END",
  "CREATE TRIGGER beedle_restore_source_successor_history_terminal BEFORE UPDATE ON beedle_restore_source_successor_history WHEN OLD.state!='running' BEGIN SELECT RAISE(ABORT,'source_successor_history_terminal'); END",
  "CREATE TRIGGER beedle_restore_source_successor_history_delete BEFORE DELETE ON beedle_restore_source_successor_history BEGIN SELECT RAISE(ABORT,'source_successor_history_immutable'); END"
] as const;
export interface RestorationSourceSuccessorIdentity {
  source:RestorationIdentity;successorId:string;successorPlanSha256:string;
  predecessorOwnerSha256:string;predecessorTickSha256:string;predecessorHistorySha256:string;
  completionOperationId:string;completionResultSha256:string;preservedGenerationId:string;preservedGenerationSha256:string;preservedGenerationStatus:'failed'|'published';preservedDocumentSha256:string;preservedGenerationsSha256:string;initialProcessed:number;initialRemaining:number;
}
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/,hash=/^[a-f0-9]{64}$/;
function validate(x:RestorationSourceSuccessorIdentity){
  if(!uuid.test(x.successorId)||!uuid.test(x.completionOperationId)||!uuid.test(x.preservedGenerationId)||x.successorId===x.completionOperationId||
    ![x.successorPlanSha256,x.predecessorOwnerSha256,x.predecessorTickSha256,x.predecessorHistorySha256,x.completionResultSha256,x.preservedGenerationSha256,x.preservedDocumentSha256,x.preservedGenerationsSha256].every(v=>hash.test(v))||
    !['failed','published'].includes(x.preservedGenerationStatus)||!Number.isSafeInteger(x.source.partitionIndex)||x.source.partitionIndex<0||x.source.partitionIndex>31||!Number.isSafeInteger(x.initialProcessed)||x.initialProcessed<1||
    !Number.isSafeInteger(x.initialRemaining)||x.initialRemaining<1)throw new RestorationControlError('source_successor_identity');
}
// A published predecessor is preserved as published; it is never relabelled as
// a failed generation. The whole document and its generation set remain pinned.
async function inspectPreservedDocument(db:D1Database,x:RestorationSourceSuccessorIdentity){
  const generation=await db.prepare('SELECT * FROM document_generations WHERE id=?').bind(x.preservedGenerationId).first<Record<string,unknown>>();
  if(!generation||generation.status!==x.preservedGenerationStatus||
    (x.preservedGenerationStatus==='failed'?generation.published_at!==null:typeof generation.published_at!=='string')||
    await fingerprint(generation)!==x.preservedGenerationSha256)
    throw new RestorationControlError('source_successor_preserved_generation_changed');
  const document=await db.prepare('SELECT * FROM documents WHERE id=?').bind(generation.document_id).first<Record<string,unknown>>();
  const generations=(await db.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').bind(generation.document_id).all<Record<string,unknown>>()).results;
  if(!document||await fingerprint(document)!==x.preservedDocumentSha256||await fingerprint(generations)!==x.preservedGenerationsSha256||
    (x.preservedGenerationStatus==='published'&&(document.active_text_generation!==generation.id||document.ingestion_state!=='ready')))
    throw new RestorationControlError('source_successor_preserved_document_changed');
  return generation;
}
export async function inspectRestorationSourceSuccessorPredecessor(db:D1Database,x:RestorationSourceSuccessorIdentity){
  validate(x);const owner=await inspectRestorationOwner(db,x.source);
  const tick=await db.prepare('SELECT * FROM beedle_restore_tick WHERE singleton=1').first<Record<string,unknown>>();
  const history=tick?await db.prepare('SELECT * FROM beedle_restore_tick_history WHERE token=?').bind(tick.token).first<Record<string,unknown>>():null;
  const preserved=await inspectPreservedDocument(db,x);
  if(owner.phase!=='held'||await fingerprint(owner)!==x.predecessorOwnerSha256||owner.expected_documents!==x.initialProcessed+x.initialRemaining||
    !tick||tick.state!=='held'||await fingerprint(tick)!==x.predecessorTickSha256||
    !history||history.state!=='held'||await fingerprint(history)!==x.predecessorHistorySha256)
    throw new RestorationControlError('source_successor_initial_predecessor_changed');
  const counts=await db.prepare("SELECT (SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase='planned' AND claim_token IS NULL AND source_verified=0) remaining,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN ('processed','held') AND source_verified=1) processed,(SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')) unresolved,(SELECT owner_token FROM search_index_rebuild WHERE singleton=1) indexOwner,(SELECT owner_token FROM search_facet_build WHERE singleton=1) facetOwner,(SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner")
    .first<{remaining:number;processed:number;receipts:number;unresolved:number;indexOwner:string|null;facetOwner:string|null;reviewerOwner:string|null}>();
  if(!counts||counts.remaining!==x.initialRemaining||counts.processed!==x.initialProcessed||counts.receipts!==x.initialProcessed||counts.unresolved!==0||
    counts.indexOwner!==null||counts.facetOwner!==null||counts.reviewerOwner!==null)
    throw new RestorationControlError('source_successor_initial_coverage_or_lease_changed');
  const invalid=await db.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs j JOIN documents d ON d.id=j.document_id WHERE j.phase IN ('processed','held') AND (NOT EXISTS(SELECT 1 FROM beedle_restore_source_results s WHERE s.document_id=j.document_id AND s.claim_token=j.claim_token AND s.owner_id=? AND s.plan_sha256=? AND s.source_sha256=j.source_sha256 AND s.source_bytes=j.source_bytes AND s.document_revision=d.document_revision AND ((j.phase='processed' AND s.generation_id=j.generation_id) OR (j.phase='held' AND s.generation_id='initial'))) OR (j.phase='processed' AND (d.ingestion_state!='ready' OR d.active_text_generation!=j.generation_id OR NOT EXISTS(SELECT 1 FROM document_generations g WHERE g.id=j.generation_id AND g.document_id=j.document_id AND g.status='published' AND g.source_sha256=j.source_sha256))) OR (j.phase='held' AND (j.extraction_policy!='held-unreadable' OR d.active_text_generation!='initial' OR d.ingestion_state!='legacy'))) ")
    .bind(x.source.ownerId,x.source.planSha256).first<{n:number}>();
  if(!invalid||invalid.n!==0)throw new RestorationControlError('source_successor_initial_receipts_changed');
  return {owner,tick,history,preserved};
}
export async function initializeRestorationSourceSuccessor(db:D1Database,x:RestorationSourceSuccessorIdentity,
  beforeSchemaSha256:string,afterSchemaSha256:string){
  if(!hash.test(beforeSchemaSha256)||!hash.test(afterSchemaSha256)||beforeSchemaSha256===afterSchemaSha256)
    throw new RestorationControlError('source_successor_schema_plan');
  const schema=async()=>restorationSchemaFingerprint((await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results);
  if(await schema()!==beforeSchemaSha256)throw new RestorationControlError('source_successor_initial_schema_changed');
  const predecessor=await inspectRestorationSourceSuccessorPredecessor(db,x),now=new Date().toISOString();
  // Called only behind an immutable one-attempt proof claim. D1 batch is a
  // transaction; no IF NOT EXISTS or automatic migration replay is accepted.
  const statements=[...restorationSourceSuccessorSql.map(sql=>db.prepare(sql)),
    db.prepare("INSERT INTO beedle_restore_source_successor SELECT 1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'building',?,?,NULL WHERE EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='held') AND EXISTS(SELECT 1 FROM beedle_restore_tick WHERE singleton=1 AND token=? AND state='held')")
      .bind(x.successorId,x.successorPlanSha256,x.source.ownerId,x.source.planSha256,predecessor.tick.token,
        x.predecessorOwnerSha256,x.predecessorTickSha256,x.predecessorHistorySha256,x.completionOperationId,x.completionResultSha256,
        x.preservedGenerationId,x.preservedGenerationSha256,x.preservedGenerationStatus,x.preservedDocumentSha256,x.preservedGenerationsSha256,x.initialProcessed,x.initialRemaining,now,now,
        x.source.ownerId,x.source.planSha256,predecessor.tick.token),
    db.prepare("INSERT INTO beedle_restore_source_successor_tick SELECT 1,?,?,'idle',NULL,NULL,? WHERE EXISTS(SELECT 1 FROM beedle_restore_source_successor WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND state='building')")
      .bind(x.successorId,x.successorPlanSha256,now,x.successorId,x.successorPlanSha256)];
  const result=await db.batch(statements);
  if(result.length!==statements.length||result.some(row=>row.success!==true)||
    result.slice(-2).some(row=>Number(row.meta.changes)!==1))throw new RestorationControlError('source_successor_initialization_unacknowledged');
  if(await schema()!==afterSchemaSha256)throw new RestorationControlError('source_successor_result_schema_changed');
  await inspectRestorationSourceSuccessor(db,x);
  await inspectRestorationSourceSuccessorPredecessor(db,x);
  return {state:'initialized' as const,successorId:x.successorId,schemaSha256:afterSchemaSha256,oldExecutionStillHeld:true};
}
export async function inspectRestorationSourceSuccessor(db:D1Database,x:RestorationSourceSuccessorIdentity){
  validate(x);const owner=await inspectRestorationOwner(db,x.source);
  const row=await db.prepare('SELECT * FROM beedle_restore_source_successor WHERE singleton=1').first<Record<string,unknown>>();
  if(!row||row.successor_id!==x.successorId||row.successor_plan_sha256!==x.successorPlanSha256||
    row.owner_id!==x.source.ownerId||row.source_plan_sha256!==x.source.planSha256||
    row.predecessor_owner_sha256!==x.predecessorOwnerSha256||row.predecessor_tick_sha256!==x.predecessorTickSha256||
    row.predecessor_history_sha256!==x.predecessorHistorySha256||row.completion_operation_id!==x.completionOperationId||
    row.completion_result_sha256!==x.completionResultSha256||row.preserved_generation_id!==x.preservedGenerationId||row.preserved_generation_sha256!==x.preservedGenerationSha256||row.preserved_generation_status!==x.preservedGenerationStatus||row.preserved_document_sha256!==x.preservedDocumentSha256||row.preserved_generations_sha256!==x.preservedGenerationsSha256||row.initial_processed!==x.initialProcessed||
    row.initial_remaining!==x.initialRemaining||owner.expected_documents!==x.initialProcessed+x.initialRemaining)
    throw new RestorationControlError('source_successor_claim_changed');
  const oldTick=await db.prepare('SELECT * FROM beedle_restore_tick WHERE singleton=1').first<Record<string,unknown>>();
  const oldHistory=await db.prepare('SELECT * FROM beedle_restore_tick_history WHERE token=?').bind(row.predecessor_tick_token).first<Record<string,unknown>>();
  if(!oldTick||oldTick.state!=='held'||oldTick.token!==row.predecessor_tick_token||await fingerprint(oldTick)!==x.predecessorTickSha256||
    !oldHistory||oldHistory.state!=='held'||await fingerprint(oldHistory)!==x.predecessorHistorySha256)
    throw new RestorationControlError('source_successor_predecessor_changed');
  await inspectPreservedDocument(db,x);
  if(row.state==='completed'){
    if(owner.phase!=='indexing'||(await db.prepare('SELECT last_outcome FROM beedle_restore_owner WHERE singleton=1').first<{last_outcome:string|null}>())?.last_outcome!=='source_successor_completed:'+x.successorId)
      throw new RestorationControlError('source_successor_completion_changed');
  }else if(owner.phase!=='held'||await fingerprint(owner)!==x.predecessorOwnerSha256){
    throw new RestorationControlError('source_successor_old_owner_changed');
  }
  const tick=await db.prepare('SELECT * FROM beedle_restore_source_successor_tick WHERE singleton=1').first<Record<string,unknown>>();
  if(!tick||tick.successor_id!==x.successorId||tick.successor_plan_sha256!==x.successorPlanSha256)
    throw new RestorationControlError('source_successor_tick_owner_changed');
  if(row.state!=='completed'){
    const leases=await db.prepare('SELECT (SELECT owner_token FROM search_index_rebuild WHERE singleton=1) indexOwner,(SELECT owner_token FROM search_facet_build WHERE singleton=1) facetOwner,(SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner').first<Record<string,unknown>>();
    if(!leases||Object.values(leases).some(v=>v!==null))throw new RestorationControlError('source_successor_other_lease_active');
  }
  return {owner,row,tick};
}
export async function claimRestorationSourceSuccessorTick(db:D1Database,x:RestorationSourceSuccessorIdentity){
  const state=await inspectRestorationSourceSuccessor(db,x);
  if(state.row.state!=='building'||state.tick.state!=='idle')return null;
  const pending=await db.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')").first<{n:number}>();
  if(!pending||pending.n!==0)throw new RestorationControlError('source_successor_unresolved_job');
  const token=crypto.randomUUID(),now=new Date().toISOString();
  const results=await db.batch([
    db.prepare("UPDATE beedle_restore_source_successor_tick SET state='running',token=?,started_at=?,updated_at=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND state='idle' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor WHERE singleton=1 AND successor_id=? AND state='building') RETURNING token")
      .bind(token,now,now,x.successorId,x.successorPlanSha256,x.successorId),
    db.prepare("INSERT INTO beedle_restore_source_successor_history(token,successor_id,successor_plan_sha256,started_at,state) SELECT ?,?,?,?,'running' WHERE EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND token=? AND state='running')")
      .bind(token,x.successorId,x.successorPlanSha256,now,token)
  ]);
  if(results.length===2&&results.every(r=>r.success===true&&Number(r.meta.changes)===0))return null;
  if(results.length!==2||results.some(r=>r.success!==true)||Number(results[0]?.meta.changes)!==1||Number(results[1]?.meta.changes)!==1)
    throw new RestorationControlError('source_successor_tick_claim_unacknowledged');
  return {token};
}
export async function claimRestorationSourceSuccessorJob(db:D1Database,x:RestorationSourceSuccessorIdentity,tickToken:string,job:RestorationPlannedJob){
  const state=await inspectRestorationSourceSuccessor(db,x);
  if(state.row.state!=='building'||state.tick.state!=='running'||state.tick.token!==tickToken)
    throw new RestorationControlError('source_successor_job_tick_changed');
  const row=await db.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').bind(job.documentId).first<Record<string,unknown>>();
  if(!row||row.source_sha256!==job.sourceSha256||row.source_bytes!==job.sourceBytes||row.original_key!==job.originalKey||
    row.plan_object_key!==job.planObjectKey||row.extraction_policy!==job.extractionPolicy)
    throw new RestorationControlError('source_successor_job_plan_changed');
  if(row.phase!=='planned'||row.claim_token!==null)return null;
  const token=crypto.randomUUID(),claimedAt=new Date().toISOString();
  const result=await db.prepare("UPDATE beedle_restore_jobs SET phase='claimed',claim_token=?,claimed_at=? WHERE document_id=? AND phase='planned' AND claim_token IS NULL AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND state='running' AND token=?) AND EXISTS(SELECT 1 FROM beedle_restore_source_successor WHERE singleton=1 AND successor_id=? AND state='building') AND EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='held') RETURNING claim_token")
    .bind(token,claimedAt,job.documentId,x.successorId,x.successorPlanSha256,tickToken,x.successorId,x.source.ownerId,x.source.planSha256).all<{claim_token:string}>();
  if(result.success!==true||result.results.length!==1||result.results[0]?.claim_token!==token)
    throw new RestorationControlError('source_successor_job_claim_unacknowledged');
  return {token,claimedAt};
}
export async function completeRestorationSourceSuccessorTick(db:D1Database,x:RestorationSourceSuccessorIdentity,token:string,deliveries:number){
  await inspectRestorationSourceSuccessor(db,x);
  if(!Number.isSafeInteger(deliveries)||deliveries<0||deliveries>4)throw new RestorationControlError('source_successor_delivery_count');
  const now=new Date().toISOString();
  const results=await db.batch([
    db.prepare("UPDATE beedle_restore_source_successor_history SET state='completed',completed_at=?,deliveries=? WHERE token=? AND successor_id=? AND successor_plan_sha256=? AND state='running' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND token=? AND state='running') AND NOT EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded'))")
      .bind(now,deliveries,token,x.successorId,x.successorPlanSha256,token),
    db.prepare("UPDATE beedle_restore_source_successor_tick SET state='idle',token=NULL,updated_at=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND token=? AND state='running' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_history WHERE token=? AND state='completed')")
      .bind(now,x.successorId,x.successorPlanSha256,token,token)
  ]);
  if(results.length!==2||results.some(r=>r.success!==true||Number(r.meta.changes)!==1))
    throw new RestorationControlError('source_successor_tick_completion_unacknowledged');
}
export async function holdRestorationSourceSuccessorTick(db:D1Database,x:RestorationSourceSuccessorIdentity,token:string,errorSha256:string){
  validate(x);if(!hash.test(errorSha256))throw new RestorationControlError('source_successor_error_digest');
  const now=new Date().toISOString();
  const results=await db.batch([
    db.prepare("UPDATE beedle_restore_source_successor SET state='held',updated_at=?,error_sha256=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND state='building' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND token=? AND state='running')")
      .bind(now,errorSha256,x.successorId,x.successorPlanSha256,token),
    db.prepare("UPDATE beedle_restore_source_successor_history SET state='held',completed_at=?,error_sha256=? WHERE token=? AND successor_id=? AND successor_plan_sha256=? AND state='running'")
      .bind(now,errorSha256,token,x.successorId,x.successorPlanSha256),
    db.prepare("UPDATE beedle_restore_source_successor_tick SET state='held',updated_at=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND token=? AND state='running'")
      .bind(now,x.successorId,x.successorPlanSha256,token)
  ]);
  if(results.length!==3||results.some(r=>r.success!==true||Number(r.meta.changes)!==1))
    throw new RestorationControlError('source_successor_hold_unacknowledged');
}
export async function completeRestorationSourceSuccessorSources(db:D1Database,x:RestorationSourceSuccessorIdentity){
  const state=await inspectRestorationSourceSuccessor(db,x);
  if(state.row.state==='completed')return {completed:true,completionReissued:false};
  if(state.row.state!=='building'||state.tick.state!=='idle'||state.tick.token!==null)
    throw new RestorationControlError('source_successor_completion_not_quiescent');
  const coverage=await db.prepare("SELECT (SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1) unfinished,(SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs j WHERE NOT EXISTS(SELECT 1 FROM beedle_restore_source_results s WHERE s.document_id=j.document_id AND s.claim_token=j.claim_token AND s.owner_id=? AND s.plan_sha256=? AND s.source_sha256=j.source_sha256 AND s.source_bytes=j.source_bytes AND ((j.phase='held' AND j.extraction_policy='held-unreadable' AND j.generation_id IS NULL AND s.generation_id='initial') OR (j.phase='processed' AND s.generation_id=j.generation_id)))) missing")
    .bind(x.source.ownerId,x.source.planSha256).first<{unfinished:number;receipts:number;missing:number}>();
  if(!coverage||coverage.unfinished!==0||coverage.missing!==0||coverage.receipts!==state.owner.expected_documents)
    throw new RestorationControlError('source_successor_source_coverage_incomplete');
  const now=new Date().toISOString();
  const results=await db.batch([
    db.prepare("UPDATE beedle_restore_source_successor SET state='completed',updated_at=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND state='building' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND successor_id=? AND state='idle' AND token IS NULL) AND NOT EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1)")
      .bind(now,x.successorId,x.successorPlanSha256,x.successorId),
    db.prepare("UPDATE beedle_restore_owner SET phase='indexing',updated_at=?,last_outcome=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='held' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND state='completed')")
      .bind(now,'source_successor_completed:'+x.successorId,x.source.ownerId,x.source.planSha256,x.successorId,x.successorPlanSha256)
  ]);
  if(results.length!==2||results.some(r=>r.success!==true||Number(r.meta.changes)!==1))
    throw new RestorationControlError('source_successor_final_completion_unacknowledged');
  await inspectRestorationSourceSuccessor(db,x);
  return {completed:true,completionReissued:false};
}
export async function holdRestorationSourceSuccessorPreflight(db:D1Database,x:RestorationSourceSuccessorIdentity,errorSha256:string){
  validate(x);if(!hash.test(errorSha256))throw new RestorationControlError('source_successor_error_digest');
  const result=await db.prepare("UPDATE beedle_restore_source_successor SET state='held',updated_at=?,error_sha256=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND owner_id=? AND source_plan_sha256=? AND state='building' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND successor_id=? AND state='idle' AND token IS NULL) AND EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='held') RETURNING singleton")
    .bind(new Date().toISOString(),errorSha256,x.successorId,x.successorPlanSha256,x.source.ownerId,x.source.planSha256,
      x.successorId,x.source.ownerId,x.source.planSha256).all<{singleton:number}>();
  if(result.success!==true||result.results.length!==1)throw new RestorationControlError('source_successor_preflight_hold_unacknowledged');
}
