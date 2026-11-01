import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {inspectRestorationOwner,type RestorationPlannedJob} from './restoration-job-control';
import {generationTables} from './generation-tables';
import {heldSourceSuccessorSchema,verifyHeldSourceLineage} from './restoration-held-source-lineage';
import {corpusPartitionIndex} from './corpus-partitions';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid();
const identitySchema=z.object({source:z.object({ownerId:uuid,resourceId:uuid,partitionIndex:z.number().int().min(0).max(31),
  planSha256:hash,resourcePlanSha256:hash,sourceManifestSha256:hash,catalogSha256:hash}).strict(),
  executionId:uuid,executionPlanSha256:hash,initialProcessed:z.number().int().min(0),initialRemaining:z.number().int().min(0).max(1000),
  parent:z.object({phase:z.enum(['seeded','held']),ownerSha256:hash,tickSha256:hash,historySha256:hash.nullable(),
    heldSuccessor:heldSourceSuccessorSchema.nullable()}).strict(),priorJournalSha256:hash,previousCurrentSha256:hash.nullable(),
  preservedCases:z.array(z.object({documentId:z.string().min(1).max(200),documentSha256:hash,jobSha256:hash,
    generationsSha256:hash,stagesSha256:hash}).strict()).max(8)}).strict();
export type RestorationSourceExecutionIdentity=z.infer<typeof identitySchema>;

// These tables belong to a new execution protocol. Every terminal run, tick and
// history row survives continuation; only the separately guarded current pointer
// may advance to a distinct run. Original root and V2 journals are not reset.
export const restorationSourceExecutionSql=[
  "CREATE TABLE beedle_restore_execution_runs(execution_id TEXT PRIMARY KEY,execution_plan_sha256 TEXT NOT NULL,owner_id TEXT NOT NULL,source_plan_sha256 TEXT NOT NULL,identity_sha256 TEXT NOT NULL,identity_json TEXT NOT NULL,parent_owner_json TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('building','completed','held')),created_at TEXT NOT NULL,updated_at TEXT NOT NULL,error_sha256 TEXT)",
  "CREATE TRIGGER beedle_restore_execution_runs_identity BEFORE UPDATE OF execution_id,execution_plan_sha256,owner_id,source_plan_sha256,identity_sha256,identity_json,parent_owner_json,created_at ON beedle_restore_execution_runs BEGIN SELECT RAISE(ABORT,'source_execution_identity_immutable'); END",
  "CREATE TRIGGER beedle_restore_execution_runs_terminal BEFORE UPDATE ON beedle_restore_execution_runs WHEN OLD.state!='building' BEGIN SELECT RAISE(ABORT,'source_execution_terminal_immutable'); END",
  "CREATE TRIGGER beedle_restore_execution_runs_delete BEFORE DELETE ON beedle_restore_execution_runs BEGIN SELECT RAISE(ABORT,'source_execution_immutable'); END",
  "CREATE TABLE beedle_restore_execution_ticks(execution_id TEXT PRIMARY KEY,execution_plan_sha256 TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('idle','running','held')),token TEXT,start_receipts INTEGER,started_at TEXT,updated_at TEXT NOT NULL,CHECK(state!='running' OR (token IS NOT NULL AND start_receipts IS NOT NULL)))",
  "CREATE TRIGGER beedle_restore_execution_ticks_identity BEFORE UPDATE OF execution_id,execution_plan_sha256 ON beedle_restore_execution_ticks BEGIN SELECT RAISE(ABORT,'source_execution_tick_immutable'); END",
  "CREATE TRIGGER beedle_restore_execution_ticks_terminal BEFORE UPDATE ON beedle_restore_execution_ticks WHEN OLD.state='held' BEGIN SELECT RAISE(ABORT,'source_execution_tick_held'); END",
  "CREATE TRIGGER beedle_restore_execution_ticks_delete BEFORE DELETE ON beedle_restore_execution_ticks BEGIN SELECT RAISE(ABORT,'source_execution_tick_immutable'); END",
  "CREATE TABLE beedle_restore_execution_history(token TEXT PRIMARY KEY,execution_id TEXT NOT NULL,execution_plan_sha256 TEXT NOT NULL,started_at TEXT NOT NULL,completed_at TEXT,state TEXT NOT NULL CHECK(state IN ('running','completed','held')),deliveries INTEGER NOT NULL DEFAULT 0 CHECK(deliveries BETWEEN 0 AND 1),error_sha256 TEXT)",
  "CREATE TRIGGER beedle_restore_execution_history_identity BEFORE UPDATE OF token,execution_id,execution_plan_sha256,started_at ON beedle_restore_execution_history BEGIN SELECT RAISE(ABORT,'source_execution_history_immutable'); END",
  "CREATE TRIGGER beedle_restore_execution_history_terminal BEFORE UPDATE ON beedle_restore_execution_history WHEN OLD.state!='running' BEGIN SELECT RAISE(ABORT,'source_execution_history_terminal'); END",
  "CREATE TRIGGER beedle_restore_execution_history_delete BEFORE DELETE ON beedle_restore_execution_history BEGIN SELECT RAISE(ABORT,'source_execution_history_immutable'); END",
  "CREATE TABLE beedle_restore_execution_current(singleton INTEGER PRIMARY KEY CHECK(singleton=1),owner_id TEXT NOT NULL,source_plan_sha256 TEXT NOT NULL,execution_id TEXT NOT NULL,execution_plan_sha256 TEXT NOT NULL,updated_at TEXT NOT NULL)",
  "CREATE TRIGGER beedle_restore_execution_current_identity BEFORE UPDATE OF owner_id,source_plan_sha256,singleton ON beedle_restore_execution_current BEGIN SELECT RAISE(ABORT,'source_execution_pointer_identity'); END",
  "CREATE TRIGGER beedle_restore_execution_current_transition BEFORE UPDATE ON beedle_restore_execution_current WHEN OLD.execution_id=NEW.execution_id OR NOT EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=OLD.execution_id AND state IN ('held','completed')) OR NOT EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=NEW.execution_id AND execution_plan_sha256=NEW.execution_plan_sha256 AND owner_id=NEW.owner_id AND source_plan_sha256=NEW.source_plan_sha256 AND state='building') BEGIN SELECT RAISE(ABORT,'source_execution_pointer_transition'); END",
  "CREATE TRIGGER beedle_restore_execution_current_delete BEFORE DELETE ON beedle_restore_execution_current BEGIN SELECT RAISE(ABORT,'source_execution_pointer_immutable'); END"
] as const;
const journalNames=['beedle_restore_execution_current','beedle_restore_execution_history','beedle_restore_execution_runs','beedle_restore_execution_ticks'];
const row=async(db:D1Database,sql:string,args:unknown[]=[])=>{
  const rows=(await db.prepare(sql).bind(...args).all<Record<string,unknown>>()).results;
  if(rows.length!==1||!rows[0])throw Error('source_execution_expected_one');return rows[0];
};
function identity(input:unknown){
  const x=identitySchema.parse(input);
  if(x.initialProcessed+x.initialRemaining<1||new Set(x.preservedCases.map(value=>value.documentId)).size!==x.preservedCases.length||
    (x.parent.phase==='seeded'?(x.parent.historySha256!==null||x.parent.heldSuccessor!==null||
      x.previousCurrentSha256===null&&(x.initialProcessed!==0||x.preservedCases.length!==0)):
      x.parent.historySha256===null||x.preservedCases.length===0))throw Error('source_execution_parent_declaration');
  return x;
}
export const parseRestorationSourceExecutionIdentity=identity;
export async function restorationPriorExecutionJournal(db:D1Database,executionId:string){
  const tables=(await db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'beedle_restore_execution_%' ORDER BY name").all<{name:string}>()).results.map(value=>value.name);
  if(tables.length===0)return {runs:[],ticks:[],history:[]};
  if(tables.join('|')!==journalNames.join('|'))throw Error('source_execution_unknown_journal');
  const read=async(table:string,order:string)=>(await db.prepare('SELECT * FROM '+table+' WHERE execution_id!=? ORDER BY '+order).bind(executionId).all<Record<string,unknown>>()).results;
  const value={runs:await read('beedle_restore_execution_runs','execution_id'),ticks:await read('beedle_restore_execution_ticks','execution_id'),history:await read('beedle_restore_execution_history','token')};
  if(value.runs.length>16||value.ticks.length>16||value.history.length>16000||JSON.stringify(value).length>8*1024*1024)throw Error('source_execution_journal_bound');
  if(value.runs.some(run=>!['held','completed'].includes(String(run.state)))||value.ticks.some(tick=>tick.state==='running')||value.history.some(item=>item.state==='running'))throw Error('source_execution_prior_not_terminal');
  return value;
}
async function parent(db:D1Database,x:RestorationSourceExecutionIdentity,completedOwner?:Record<string,unknown>){
  await inspectRestorationOwner(db,x.source);
  const owner=await row(db,'SELECT * FROM beedle_restore_owner WHERE singleton=1'),tick=await row(db,'SELECT * FROM beedle_restore_tick WHERE singleton=1');
  if(completedOwner){
    if(owner.phase!=='indexing'||owner.last_outcome!=='source_execution_completed:'+x.executionId||
      await fingerprint({...owner,phase:completedOwner.phase,updated_at:completedOwner.updated_at,last_outcome:completedOwner.last_outcome})!==x.parent.ownerSha256)
      throw Error('source_execution_completed_owner_changed');
  }else if(owner.phase!==x.parent.phase||await fingerprint(owner)!==x.parent.ownerSha256)throw Error('source_execution_parent_owner_changed');
  if(await fingerprint(tick)!==x.parent.tickSha256)throw Error('source_execution_parent_tick_changed');
  if(x.parent.phase==='seeded'){
    const histories=await row(db,'SELECT COUNT(*) n FROM beedle_restore_tick_history');
    if(tick.state!=='idle'||tick.token!==null||histories.n!==0)throw Error('source_execution_seeded_root_changed');
  }else{
    const history=await row(db,'SELECT * FROM beedle_restore_tick_history WHERE token=?',[tick.token]);
    if(tick.state!=='held'||history.state!=='held'||await fingerprint(history)!==x.parent.historySha256)throw Error('source_execution_held_root_changed');
  }
  await verifyHeldSourceLineage(db,x.source,x.parent.heldSuccessor);
  if(x.parent.heldSuccessor){
    const control=await row(db,'SELECT * FROM beedle_restore_source_successor'),generation=await row(db,'SELECT * FROM document_generations WHERE id=?',[control.preserved_generation_id]);
    if(await fingerprint(generation)!==control.preserved_generation_sha256||
      await fingerprint(await row(db,'SELECT * FROM documents WHERE id=?',[generation.document_id]))!==control.preserved_document_sha256||
      await fingerprint((await db.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').bind(generation.document_id).all()).results)!==control.preserved_generations_sha256)
      throw Error('source_execution_successor_preserved_case_changed');
  }
  for(const item of x.preservedCases){
    if(await corpusPartitionIndex(item.documentId)!==x.source.partitionIndex)throw Error('source_execution_preserved_partition_changed');
    const document=await row(db,'SELECT * FROM documents WHERE id=?',[item.documentId]),job=await row(db,'SELECT * FROM beedle_restore_jobs WHERE document_id=?',[item.documentId]),
      generations=(await db.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').bind(item.documentId).all()).results,stages:Record<string,unknown[]>={};
    for(const table of Object.keys(generationTables))stages[table]=(await db.prepare('SELECT * FROM staged_'+table+' WHERE generation_id IN(SELECT id FROM document_generations WHERE document_id=?) ORDER BY generation_id,id').bind(item.documentId).all()).results;
    if(Object.values(stages).some(rows=>rows.length>10000)||JSON.stringify(stages).length>8*1024*1024||await fingerprint(document)!==item.documentSha256||await fingerprint(job)!==item.jobSha256||
      await fingerprint(generations)!==item.generationsSha256||await fingerprint(stages)!==item.stagesSha256)throw Error('source_execution_preserved_case_changed');
  }
  const leases=await row(db,'SELECT (SELECT owner_token FROM search_index_rebuild WHERE singleton=1) indexOwner,(SELECT owner_token FROM search_facet_build WHERE singleton=1) facetOwner,(SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner');
  if(Object.values(leases).some(value=>value!==null)||(await row(db,'SELECT COUNT(*) n FROM documents WHERE searchable_at IS NOT NULL')).n!==0)throw Error('source_execution_other_writer_or_publication');
  if(await fingerprint(await restorationPriorExecutionJournal(db,x.executionId))!==x.priorJournalSha256)throw Error('source_execution_prior_journal_changed');
  return owner;
}
export async function initializeRestorationSourceExecution(db:D1Database,input:RestorationSourceExecutionIdentity,beforeSchemaSha256:string,afterSchemaSha256:string){
  const x=identity(input),schema=async()=>restorationSchemaFingerprint((await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results);
  if(await schema()!==beforeSchemaSha256)throw Error('source_execution_before_schema_changed');
  const owner=await parent(db,x),counts=await row(db,"SELECT (SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase='planned') planned,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')) unresolved");
  if(counts.receipts!==x.initialProcessed||counts.planned!==x.initialRemaining||counts.unresolved!==0||owner.expected_documents!==x.initialProcessed+x.initialRemaining)throw Error('source_execution_initial_coverage_changed');
  const create=x.previousCurrentSha256===null,current=create?null:await row(db,'SELECT * FROM beedle_restore_execution_current WHERE singleton=1');
  if(create){if(beforeSchemaSha256===afterSchemaSha256||(await db.prepare("SELECT name FROM sqlite_schema WHERE name LIKE 'beedle_restore_execution_%'").all()).results.length)throw Error('source_execution_bootstrap_exists');}
  else if(beforeSchemaSha256!==afterSchemaSha256||await fingerprint(current)!==x.previousCurrentSha256||current?.owner_id!==x.source.ownerId||current.source_plan_sha256!==x.source.planSha256)throw Error('source_execution_prior_pointer_changed');
  const now=new Date().toISOString(),statements=create?restorationSourceExecutionSql.map(sql=>db.prepare(sql)):[];
  const values=[x.executionId,x.executionPlanSha256,x.source.ownerId,x.source.planSha256,await fingerprint(x),canonical(x),canonical(owner),now,now];
  statements.push(create?db.prepare("INSERT INTO beedle_restore_execution_runs VALUES(?,?,?,?,?,?,?,'building',?,?,NULL)").bind(...values):
    db.prepare("INSERT INTO beedle_restore_execution_runs SELECT ?,?,?,?,?,?,?,'building',?,?,NULL WHERE EXISTS(SELECT 1 FROM beedle_restore_execution_current c JOIN beedle_restore_execution_runs r ON r.execution_id=c.execution_id WHERE c.singleton=1 AND c.execution_id=? AND c.execution_plan_sha256=? AND r.state IN ('held','completed'))").bind(...values,current?.execution_id,current?.execution_plan_sha256),
    db.prepare("INSERT INTO beedle_restore_execution_ticks SELECT ?,?,'idle',NULL,NULL,NULL,? WHERE EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND execution_plan_sha256=? AND state='building')").bind(x.executionId,x.executionPlanSha256,now,x.executionId,x.executionPlanSha256));
  if(create)statements.push(db.prepare('INSERT INTO beedle_restore_execution_current VALUES(1,?,?,?,?,?)').bind(x.source.ownerId,x.source.planSha256,x.executionId,x.executionPlanSha256,now));
  else statements.push(db.prepare("UPDATE beedle_restore_execution_current SET execution_id=?,execution_plan_sha256=?,updated_at=? WHERE singleton=1 AND execution_id=? AND execution_plan_sha256=? AND EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND execution_plan_sha256=? AND state='building')").bind(x.executionId,x.executionPlanSha256,now,current?.execution_id,current?.execution_plan_sha256,x.executionId,x.executionPlanSha256));
  const result=await db.batch(statements);
  if(result.length!==statements.length||result.some(value=>value.success!==true)||result.slice(-3).some(value=>Number(value.meta.changes)!==1))throw Error('source_execution_initialize_unacknowledged');
  if(await schema()!==afterSchemaSha256)throw Error('source_execution_after_schema_changed');
  await inspectRestorationSourceExecution(db,x);return {state:'initialized' as const,executionId:x.executionId};
}
export async function inspectRestorationSourceExecution(db:D1Database,input:RestorationSourceExecutionIdentity){
  const x=identity(input),run=await row(db,'SELECT * FROM beedle_restore_execution_runs WHERE execution_id=?',[x.executionId]),tick=await row(db,'SELECT * FROM beedle_restore_execution_ticks WHERE execution_id=?',[x.executionId]),current=await row(db,'SELECT * FROM beedle_restore_execution_current WHERE singleton=1');
  if(run.execution_plan_sha256!==x.executionPlanSha256||run.owner_id!==x.source.ownerId||run.source_plan_sha256!==x.source.planSha256||run.identity_sha256!==await fingerprint(x)||run.identity_json!==canonical(x)||
    tick.execution_plan_sha256!==x.executionPlanSha256||current.owner_id!==x.source.ownerId||current.source_plan_sha256!==x.source.planSha256||current.execution_id!==x.executionId||current.execution_plan_sha256!==x.executionPlanSha256)
    throw Error('source_execution_identity_changed');
  const owner=await parent(db,x,run.state==='completed'?JSON.parse(String(run.parent_owner_json)) as Record<string,unknown>:undefined);
  return {identity:x,owner,run,tick,current};
}
export async function claimRestorationSourceExecutionTick(db:D1Database,input:RestorationSourceExecutionIdentity){
  const state=await inspectRestorationSourceExecution(db,input),x=state.identity;
  if(state.run.state!=='building'||state.tick.state!=='idle'||state.tick.token!==null)return null;
  const counts=await row(db,"SELECT (SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')) unresolved,(SELECT COUNT(*) FROM beedle_restore_source_results) receipts");
  if(counts.unresolved!==0)throw Error('source_execution_unresolved_job');
  const token=crypto.randomUUID(),now=new Date().toISOString(),results=await db.batch([
    db.prepare("UPDATE beedle_restore_execution_ticks SET state='running',token=?,start_receipts=?,started_at=?,updated_at=? WHERE execution_id=? AND execution_plan_sha256=? AND state='idle' AND token IS NULL AND EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND state='building') AND EXISTS(SELECT 1 FROM beedle_restore_execution_current WHERE singleton=1 AND execution_id=? AND execution_plan_sha256=?)")
      .bind(token,counts.receipts,now,now,x.executionId,x.executionPlanSha256,x.executionId,x.executionId,x.executionPlanSha256),
    db.prepare("INSERT INTO beedle_restore_execution_history(token,execution_id,execution_plan_sha256,started_at,state) SELECT ?,?,?,?,'running' WHERE EXISTS(SELECT 1 FROM beedle_restore_execution_ticks WHERE execution_id=? AND token=? AND state='running')")
      .bind(token,x.executionId,x.executionPlanSha256,now,x.executionId,token)
  ]);
  if(results.length===2&&results.every(value=>value.success===true&&Number(value.meta.changes)===0))return null;
  if(results.length!==2||results.some(value=>value.success!==true||Number(value.meta.changes)!==1))throw Error('source_execution_tick_claim_unacknowledged');
  return {token};
}
export async function claimRestorationSourceExecutionJob(db:D1Database,input:RestorationSourceExecutionIdentity,token:string,job:RestorationPlannedJob){
  const state=await inspectRestorationSourceExecution(db,input),x=state.identity;
  if(state.run.state!=='building'||state.tick.state!=='running'||state.tick.token!==token)throw Error('source_execution_job_tick_changed');
  if(await corpusPartitionIndex(job.documentId)!==x.source.partitionIndex)throw Error('source_execution_job_partition_changed');
  const planned=await row(db,'SELECT * FROM beedle_restore_jobs WHERE document_id=?',[job.documentId]);
  if(planned.source_sha256!==job.sourceSha256||planned.source_bytes!==job.sourceBytes||planned.original_key!==job.originalKey||planned.plan_object_key!==job.planObjectKey||planned.extraction_policy!==job.extractionPolicy)
    throw Error('source_execution_job_plan_changed');
  if(planned.phase!=='planned'||planned.claim_token!==null)return null;
  // A step may claim exactly one job, including when its callback is delivered
  // again before completion. The immutable Workflow claim is a second barrier.
  if((await row(db,"SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')")).n!==0||
    (await row(db,'SELECT COUNT(*) n FROM beedle_restore_source_results')).n!==state.tick.start_receipts)throw Error('source_execution_step_already_used');
  const claimToken=crypto.randomUUID(),at=new Date().toISOString(),result=await db.prepare("UPDATE beedle_restore_jobs SET phase='claimed',claim_token=?,claimed_at=? WHERE document_id=? AND phase='planned' AND claim_token IS NULL AND EXISTS(SELECT 1 FROM beedle_restore_execution_ticks WHERE execution_id=? AND execution_plan_sha256=? AND token=? AND state='running') AND EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND state='building') AND EXISTS(SELECT 1 FROM beedle_restore_execution_current WHERE singleton=1 AND execution_id=?) AND EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase=?) AND NOT EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')) AND (SELECT COUNT(*) FROM beedle_restore_source_results)=? RETURNING claim_token")
    .bind(claimToken,at,job.documentId,x.executionId,x.executionPlanSha256,token,x.executionId,x.executionId,x.source.ownerId,x.source.planSha256,x.parent.phase,state.tick.start_receipts).all<{claim_token:string}>();
  if(result.success!==true||result.results.length!==1||result.results[0]?.claim_token!==claimToken)throw Error('source_execution_job_claim_unacknowledged');
  return {token:claimToken,claimedAt:at};
}
export async function completeRestorationSourceExecutionTick(db:D1Database,input:RestorationSourceExecutionIdentity,token:string){
  const state=await inspectRestorationSourceExecution(db,input),x=state.identity;
  if(state.run.state!=='building'||state.tick.state!=='running'||state.tick.token!==token)throw Error('source_execution_completion_tick_changed');
  const counts=await row(db,"SELECT (SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')) unresolved");
  if(typeof state.tick.start_receipts!=='number'||counts.receipts!==state.tick.start_receipts+1||counts.unresolved!==0)throw Error('source_execution_single_delivery_unconfirmed');
  const now=new Date().toISOString(),results=await db.batch([
    db.prepare("UPDATE beedle_restore_execution_history SET state='completed',completed_at=?,deliveries=1 WHERE token=? AND execution_id=? AND execution_plan_sha256=? AND state='running' AND EXISTS(SELECT 1 FROM beedle_restore_execution_ticks WHERE execution_id=? AND state='running' AND token=?) AND (SELECT COUNT(*) FROM beedle_restore_source_results)=? AND NOT EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded'))")
      .bind(now,token,x.executionId,x.executionPlanSha256,x.executionId,token,counts.receipts),
    db.prepare("UPDATE beedle_restore_execution_ticks SET state='idle',token=NULL,start_receipts=NULL,updated_at=? WHERE execution_id=? AND execution_plan_sha256=? AND token=? AND state='running' AND EXISTS(SELECT 1 FROM beedle_restore_execution_history WHERE token=? AND execution_id=? AND state='completed' AND deliveries=1)")
      .bind(now,x.executionId,x.executionPlanSha256,token,token,x.executionId)
  ]);
  if(results.length!==2||results.some(value=>value.success!==true||Number(value.meta.changes)!==1))throw Error('source_execution_tick_completion_unacknowledged');
}
export async function holdRestorationSourceExecution(db:D1Database,input:RestorationSourceExecutionIdentity,errorSha256:string){
  const x=identity(input);hash.parse(errorSha256);
  const current=await row(db,'SELECT * FROM beedle_restore_execution_current WHERE singleton=1'),tick=await row(db,'SELECT * FROM beedle_restore_execution_ticks WHERE execution_id=?',[x.executionId]);
  if(current.execution_id!==x.executionId||current.execution_plan_sha256!==x.executionPlanSha256||current.owner_id!==x.source.ownerId||tick.execution_plan_sha256!==x.executionPlanSha256||!['idle','running'].includes(String(tick.state)))throw Error('source_execution_hold_owner_changed');
  const now=new Date().toISOString(),statements=[db.prepare("UPDATE beedle_restore_execution_runs SET state='held',error_sha256=?,updated_at=? WHERE execution_id=? AND execution_plan_sha256=? AND owner_id=? AND source_plan_sha256=? AND identity_sha256=? AND state='building'").bind(errorSha256,now,x.executionId,x.executionPlanSha256,x.source.ownerId,x.source.planSha256,await fingerprint(x))];
  if(tick.state==='running')statements.push(db.prepare("UPDATE beedle_restore_execution_history SET state='held',completed_at=?,error_sha256=? WHERE token=? AND execution_id=? AND state='running' AND EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND state='held' AND error_sha256=?)").bind(now,errorSha256,tick.token,x.executionId,x.executionId,errorSha256));
  statements.push(db.prepare("UPDATE beedle_restore_execution_ticks SET state='held',updated_at=? WHERE execution_id=? AND execution_plan_sha256=? AND state=? AND token IS ? AND EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND state='held' AND error_sha256=?)").bind(now,x.executionId,x.executionPlanSha256,tick.state,tick.token,x.executionId,errorSha256));
  const results=await db.batch(statements);
  if(results.length!==statements.length||results.some(value=>value.success!==true||Number(value.meta.changes)!==1))throw Error('source_execution_hold_unacknowledged');
}
export async function completeRestorationSourceExecution(db:D1Database,input:RestorationSourceExecutionIdentity){
  const state=await inspectRestorationSourceExecution(db,input),x=state.identity;
  if(state.run.state==='completed')return {completed:true,completionReissued:false};
  if(state.run.state!=='building'||state.tick.state!=='idle'||state.tick.token!==null)throw Error('source_execution_completion_not_quiescent');
  const coverage=await row(db,"SELECT (SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1) unfinished,(SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs j WHERE NOT EXISTS(SELECT 1 FROM beedle_restore_source_results s WHERE s.document_id=j.document_id AND s.claim_token=j.claim_token AND s.owner_id=? AND s.plan_sha256=? AND s.source_sha256=j.source_sha256 AND s.source_bytes=j.source_bytes AND ((j.phase='held' AND j.extraction_policy='held-unreadable' AND j.generation_id IS NULL AND s.generation_id='initial') OR (j.phase='processed' AND s.generation_id=j.generation_id)))) missing",[x.source.ownerId,x.source.planSha256]);
  if(coverage.unfinished!==0||coverage.missing!==0||coverage.receipts!==state.owner.expected_documents)throw Error('source_execution_full_coverage_incomplete');
  const at=new Date().toISOString(),results=await db.batch([
    db.prepare("UPDATE beedle_restore_execution_runs SET state='completed',updated_at=? WHERE execution_id=? AND execution_plan_sha256=? AND state='building' AND EXISTS(SELECT 1 FROM beedle_restore_execution_ticks WHERE execution_id=? AND state='idle' AND token IS NULL) AND NOT EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1)").bind(at,x.executionId,x.executionPlanSha256,x.executionId),
    db.prepare("UPDATE beedle_restore_owner SET phase='indexing',updated_at=?,last_outcome=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase=? AND EXISTS(SELECT 1 FROM beedle_restore_execution_current WHERE singleton=1 AND execution_id=? AND execution_plan_sha256=?) AND EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND state='completed')")
      .bind(at,'source_execution_completed:'+x.executionId,x.source.ownerId,x.source.planSha256,x.parent.phase,x.executionId,x.executionPlanSha256,x.executionId)
  ]);
  if(results.length!==2||results.some(value=>value.success!==true||Number(value.meta.changes)!==1))throw Error('source_execution_final_completion_unacknowledged');
  await inspectRestorationSourceExecution(db,x);return {completed:true,completionReissued:false};
}
