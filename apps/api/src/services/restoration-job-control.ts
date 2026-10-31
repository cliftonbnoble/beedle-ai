import releaseContract from '../../release/contract.json';

export interface RestorationIdentity {
  ownerId:string;planSha256:string;resourcePlanSha256:string;resourceId:string;
  partitionIndex:number;sourceManifestSha256:string;catalogSha256:string;
}
interface Owner {
  owner_id:string;plan_sha256:string;source_manifest_sha256:string;catalog_sha256:string;
  partition_index:number;partition_count:number;expected_documents:number;expected_published_documents:number;
  phase:'seeded'|'building'|'indexing'|'ready'|'verifying'|'sealed'|'held';
}
export interface RestorationPlannedJob {
  documentId:string;sourceSha256:string;sourceBytes:number;originalKey:string;planObjectKey:string;
  extractionPolicy:'verified-source-restoration-v1'|'recovered-markdown-v1'|'held-unreadable';
}
export const restorationTickSql=[
  `CREATE TABLE beedle_restore_tick(singleton INTEGER PRIMARY KEY CHECK(singleton=1),owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('idle','running','held')),token TEXT,started_at TEXT,updated_at TEXT NOT NULL,
    CHECK(state='idle' OR token IS NOT NULL))`,
  `CREATE TRIGGER beedle_restore_tick_owner_immutable BEFORE UPDATE OF owner_id,plan_sha256 ON beedle_restore_tick BEGIN SELECT RAISE(ABORT,'restoration_tick_owner_immutable'); END`,
  `CREATE TRIGGER beedle_restore_tick_delete_guard BEFORE DELETE ON beedle_restore_tick BEGIN SELECT RAISE(ABORT,'restoration_tick_owner_immutable'); END`,
  `CREATE TABLE beedle_restore_tick_history(token TEXT PRIMARY KEY,owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,
    started_at TEXT NOT NULL,completed_at TEXT,state TEXT NOT NULL CHECK(state IN ('running','completed','held')),
    deliveries INTEGER NOT NULL DEFAULT 0 CHECK(deliveries BETWEEN 0 AND 20),stage TEXT,error_sha256 TEXT)`,
  `CREATE TRIGGER beedle_restore_tick_history_identity BEFORE UPDATE OF token,owner_id,plan_sha256,started_at ON beedle_restore_tick_history BEGIN SELECT RAISE(ABORT,'restoration_tick_history_immutable'); END`,
  `CREATE TRIGGER beedle_restore_tick_history_result BEFORE UPDATE ON beedle_restore_tick_history WHEN OLD.state!='running' BEGIN SELECT RAISE(ABORT,'restoration_tick_history_immutable'); END`,
  `CREATE TRIGGER beedle_restore_tick_history_delete BEFORE DELETE ON beedle_restore_tick_history BEGIN SELECT RAISE(ABORT,'restoration_tick_history_immutable'); END`
] as const;
export class RestorationControlError extends Error {
  constructor(readonly code:string){super(code);this.name='RestorationControlError';}
}
function validate(identity:RestorationIdentity){
  const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
  if(!uuid.test(identity.ownerId)||!uuid.test(identity.resourceId)||
    ![identity.planSha256,identity.resourcePlanSha256,identity.sourceManifestSha256,identity.catalogSha256].every(x=>/^[a-f0-9]{64}$/.test(x))||
    !Number.isSafeInteger(identity.partitionIndex)||identity.partitionIndex<0||identity.partitionIndex>31)throw new RestorationControlError('invalid_restoration_identity');
}
export async function inspectRestorationOwner(db:D1Database,identity:RestorationIdentity):Promise<Owner>{
  validate(identity);
  const claim=await db.prepare('SELECT owner_id,resource_plan_sha256,resource_id,partition_index FROM beedle_restore_resource_claim WHERE singleton=1')
    .first<{owner_id:string;resource_plan_sha256:string;resource_id:string;partition_index:number}>();
  const owner=await db.prepare('SELECT * FROM beedle_restore_owner WHERE singleton=1').first<Owner>();
  if(!claim||claim.owner_id!==identity.ownerId||claim.resource_plan_sha256!==identity.resourcePlanSha256||claim.resource_id!==identity.resourceId||claim.partition_index!==identity.partitionIndex||
    !owner||owner.owner_id!==identity.ownerId||owner.plan_sha256!==identity.planSha256||owner.source_manifest_sha256!==identity.sourceManifestSha256||owner.catalog_sha256!==identity.catalogSha256||
    owner.partition_index!==identity.partitionIndex||owner.partition_count!==32)throw new RestorationControlError('restoration_owner_changed');
  const ledger=(await db.prepare('SELECT name FROM d1_migrations ORDER BY id').all<{name:string}>()).results.map(x=>x.name);
  if(JSON.stringify(ledger)!==JSON.stringify(releaseContract.requiredMigrations))throw new RestorationControlError('restoration_ledger_changed');
  const counts=await db.prepare(`SELECT (SELECT COUNT(*) FROM documents) AS documents,(SELECT COUNT(*) FROM beedle_restore_jobs) AS jobs,
    (SELECT COUNT(*) FROM documents WHERE approved_at IS NOT NULL OR approved_revision IS NOT NULL OR qc_required_confirmed!=0) AS approvals`).first<{documents:number;jobs:number;approvals:number}>();
  if(!counts||counts.documents!==owner.expected_documents||counts.jobs!==owner.expected_documents||counts.approvals!==0)throw new RestorationControlError('restoration_corpus_or_review_state_changed');
  if(!['seeded','building','held'].includes(owner.phase)){
    const unfinished=await db.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1").first<{n:number}>();
    if(!unfinished||unfinished.n!==0)throw new RestorationControlError('restoration_sources_incomplete');
  }
  return owner;
}
export async function claimRestorationTick(db:D1Database,identity:RestorationIdentity){
  const owner=await inspectRestorationOwner(db,identity);
  if(owner.phase!=='building'&&owner.phase!=='indexing'&&owner.phase!=='verifying')return null;
  const control=await db.prepare('SELECT owner_id,plan_sha256,state,token FROM beedle_restore_tick WHERE singleton=1')
    .first<{owner_id:string;plan_sha256:string;state:string;token:string|null}>();
  if(!control||control.owner_id!==identity.ownerId||control.plan_sha256!==identity.planSha256)throw new RestorationControlError('restoration_tick_owner_changed');
  if(control.state!=='idle')return null;
  const pending=await db.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')").first<{n:number}>();
  if(!pending||pending.n!==0)throw new RestorationControlError('unresolved_prior_job');
  const token=crypto.randomUUID(),now=new Date().toISOString();
  const results=await db.batch<{token:string}>([
    db.prepare(`UPDATE beedle_restore_tick SET state='running',token=?,started_at=?,updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state='idle'
      AND EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase=?) RETURNING token`)
      .bind(token,now,now,identity.ownerId,identity.planSha256,identity.ownerId,identity.planSha256,owner.phase),
    db.prepare(`INSERT INTO beedle_restore_tick_history(token,owner_id,plan_sha256,started_at,state)
      SELECT ?,?,?,?,'running' WHERE EXISTS(SELECT 1 FROM beedle_restore_tick WHERE singleton=1 AND token=? AND state='running')`)
      .bind(token,identity.ownerId,identity.planSha256,now,token)
  ]);
  const claim=results[0],history=results[1];
  if(results.length!==2||!claim||!history||results.some(result=>result.success!==true))throw new RestorationControlError('tick_claim_unacknowledged');
  if(claim.results.length===0&&Number(history.meta.changes)===0)return null;
  if(claim.results.length!==1||claim.results[0]?.token!==token||Number(history.meta.changes)!==1)throw new RestorationControlError('tick_claim_unacknowledged');
  return{token,stage:owner.phase};
}
export async function claimRestorationJob(db:D1Database,identity:RestorationIdentity,tickToken:string,job:RestorationPlannedJob){
  const owner=await inspectRestorationOwner(db,identity);if(owner.phase!=='building')throw new RestorationControlError('source_build_phase_changed');
  const row=await db.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').bind(job.documentId).first<{
    document_id:string;source_sha256:string;source_bytes:number;original_key:string;plan_object_key:string;extraction_policy:string;phase:string;claim_token:string|null;
  }>();
  if(!row||row.source_sha256!==job.sourceSha256||row.source_bytes!==job.sourceBytes||row.original_key!==job.originalKey||
    row.plan_object_key!==job.planObjectKey||row.extraction_policy!==job.extractionPolicy)throw new RestorationControlError('source_job_plan_changed');
  if(row.phase!=='planned'||row.claim_token!==null)return null;
  const token=crypto.randomUUID(),now=new Date().toISOString();
  const result=await db.prepare(`UPDATE beedle_restore_jobs SET phase='claimed',claim_token=?,claimed_at=? WHERE document_id=? AND phase='planned' AND claim_token IS NULL
    AND EXISTS(SELECT 1 FROM beedle_restore_tick WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state='running' AND token=?)
    AND EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='building') RETURNING claim_token`)
    .bind(token,now,job.documentId,identity.ownerId,identity.planSha256,tickToken,identity.ownerId,identity.planSha256).all<{claim_token:string}>();
  return result.results.length===1?{token,claimedAt:now}:null;
}
export async function completeRestorationTick(db:D1Database,identity:RestorationIdentity,token:string,stage:'building'|'indexing'|'verifying',deliveries:number){
  validate(identity);if(!Number.isSafeInteger(deliveries)||deliveries<0||deliveries>20)throw new RestorationControlError('invalid_tick_delivery_count');
  const now=new Date().toISOString();
  const results=await db.batch([
    db.prepare(`UPDATE beedle_restore_tick_history SET state='completed',completed_at=?,deliveries=?,stage=? WHERE token=? AND owner_id=? AND plan_sha256=? AND state='running'
      AND EXISTS(SELECT 1 FROM beedle_restore_tick WHERE singleton=1 AND token=? AND state='running')
      AND NOT EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded'))`).bind(now,deliveries,stage,token,identity.ownerId,identity.planSha256,token),
    db.prepare(`UPDATE beedle_restore_tick SET state='idle',token=NULL,updated_at=? WHERE singleton=1 AND token=? AND owner_id=? AND plan_sha256=? AND state='running'
      AND EXISTS(SELECT 1 FROM beedle_restore_tick_history WHERE token=? AND state='completed')`).bind(now,token,identity.ownerId,identity.planSha256,token)
  ]);
  if(results.length!==2||results.some(result=>result.success!==true||Number(result.meta.changes)!==1))throw new RestorationControlError('tick_completion_unconfirmed');
}
export async function observeRestorationTick(db:D1Database,identity:RestorationIdentity,token:string){
  await inspectRestorationOwner(db,identity);
  const history=await db.prepare('SELECT token,owner_id,plan_sha256,state,deliveries,stage,error_sha256 FROM beedle_restore_tick_history WHERE token=?')
    .bind(token).first<{token:string;owner_id:string;plan_sha256:string;state:string;deliveries:number;stage:string|null;error_sha256:string|null}>();
  if(!history||history.owner_id!==identity.ownerId||history.plan_sha256!==identity.planSha256)throw new RestorationControlError('tick_outcome_not_owned');
  return history;
}
export async function holdRestorationTick(db:D1Database,identity:RestorationIdentity,token:string,errorSha256:string){
  validate(identity);if(!/^[a-f0-9]{64}$/.test(errorSha256))throw new RestorationControlError('invalid_safe_error_digest');
  const now=new Date().toISOString();
  const results=await db.batch([
    db.prepare(`UPDATE beedle_restore_owner SET phase='held',updated_at=?,last_outcome='owned_tick_failed' WHERE singleton=1 AND owner_id=? AND plan_sha256=?
      AND EXISTS(SELECT 1 FROM beedle_restore_tick WHERE singleton=1 AND token=? AND state='running' AND owner_id=? AND plan_sha256=?)`)
      .bind(now,identity.ownerId,identity.planSha256,token,identity.ownerId,identity.planSha256),
    db.prepare(`UPDATE beedle_restore_tick_history SET state='held',completed_at=?,error_sha256=? WHERE token=? AND owner_id=? AND plan_sha256=? AND state='running'
      AND EXISTS(SELECT 1 FROM beedle_restore_tick WHERE singleton=1 AND token=? AND state='running')`).bind(now,errorSha256,token,identity.ownerId,identity.planSha256,token),
    db.prepare("UPDATE beedle_restore_tick SET state='held',updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND token=? AND state='running'")
      .bind(now,identity.ownerId,identity.planSha256,token)
  ]);
  if(results.length!==3||results.some(result=>result.success!==true))throw new RestorationControlError('tick_hold_unacknowledged');
  return{held:results.every(result=>Number(result.meta.changes)===1)};
}
