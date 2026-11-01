import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import releaseContract from '../../release/contract.json';
import {inspectRestorationOwner} from './restoration-job-control';
import {inspectSearchReadiness} from './search-readiness';
import {inspectReviewerQueue,advanceReviewerQueue} from './reviewer-queue-index';
import {buildReviewerRows} from './reviewer-read-model';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationMeasuredDatabase} from './restoration-measured-database';
import {restorationCapacityObserver} from './restoration-capacity-observer';
import {restorationErrorEvidence} from './restoration-failure-proof';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid();
const basePlanSchema=z.object({operationId:uuid,
  source:z.object({ownerId:uuid,resourceId:uuid,partitionIndex:z.number().int().min(0).max(31),
    planSha256:hash,resourcePlanSha256:hash,sourceManifestSha256:hash,catalogSha256:hash}).strict(),
  schemaSha256:hash,ownerSha256:hash,journalsSha256:hash,jobsSha256:hash,receiptsSha256:hash,
  indexControlsSha256:hash,corpusRevision:z.number().int().positive(),catalogVersion:z.string().min(1).max(200),
  documents:z.number().int().min(1).max(1000),eligibleDocuments:z.number().int().min(0).max(1000),
  notBefore:z.string().datetime(),notAfter:z.string().datetime(),completionDeadline:z.string().datetime(),
  maximumSteps:z.number().int().min(3).max(43)}).strict();
const planSchema=z.discriminatedUnion('version',[basePlanSchema.extend({version:z.literal(1)}).strict(),
  basePlanSchema.extend({version:z.literal(2),publicationPolicy:z.literal('unreviewed-provisional-v1'),publishedDocuments:z.number().int().min(1).max(1000)}).strict()]);
export interface RestorationReviewerWorkflowBindings {
  DB:D1Database;PROOFS:R2Bucket;OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export interface RestorationReviewerWorkflowParams {operationPlanSha256:string}
export function parseRestorationReviewerPlan(text:string){
  if(new TextEncoder().encode(text).length>5000)throw Error('reviewer_maintenance_plan_size');
  const plan=planSchema.parse(JSON.parse(text)),begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter),deadline=Date.parse(plan.completionDeadline);
  if(end<=begin||end-begin>30*60000||deadline<=end||deadline-begin>4*60*60000||
    plan.eligibleDocuments>plan.documents||plan.version===2&&plan.publishedDocuments!==plan.eligibleDocuments||plan.maximumSteps!==Math.floor(plan.documents/25)+3)
    throw Error('reviewer_maintenance_plan_bounds');
  return plan;
}
const journalTables=[['beedle_restore_tick','singleton'],['beedle_restore_tick_history','token'],
  ['beedle_restore_source_successor','singleton'],['beedle_restore_source_successor_tick','singleton'],
  ['beedle_restore_source_successor_history','token'],['beedle_restore_execution_runs','execution_id'],
  ['beedle_restore_execution_ticks','execution_id'],['beedle_restore_execution_history','token'],
  ['beedle_restore_execution_current','singleton']] as const;
export async function restorationSourceJournalSnapshot(db:D1Database){
  const names=new Set((await db.prepare("SELECT name FROM sqlite_schema WHERE type='table'").all<{name:string}>()).results.map(row=>row.name));
  const result:Record<string,unknown[]>={};
  for(const[table,order]of journalTables){
    const rows=names.has(table)?(await db.prepare('SELECT * FROM '+table+' ORDER BY '+order).all()).results:[];
    if(rows.length>16000)throw Error('reviewer_maintenance_journal_rows');result[table]=rows;
  }
  if(JSON.stringify(result).length>8*1024*1024)throw Error('reviewer_maintenance_journal_bytes');
  return result;
}
export async function restorationIndexControlSnapshot(db:D1Database){
  const result:Record<string,unknown[]>={};
  for(const table of ['search_index_rebuild','search_index_build','search_phrase_build','search_facet_build']){
    const rows=(await db.prepare('SELECT * FROM '+table+' WHERE singleton=1').all()).results;
    if(rows.length!==1||rows[0]?.owner_token!==null)throw Error('reviewer_maintenance_index_owner');result[table]=rows;
  }
  return result;
}
async function inspect(db:D1Database,plan:z.infer<typeof planSchema>){
  await inspectRestorationOwner(db,plan.source);
  const owners=(await db.prepare('SELECT * FROM beedle_restore_owner WHERE singleton=1').all()).results;
  if(owners.length!==1||owners[0]?.phase!=='indexing'||await fingerprint(owners[0])!==plan.ownerSha256)
    throw Error('reviewer_maintenance_source_owner');
  const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
  if(await restorationSchemaFingerprint(objects)!==plan.schemaSha256)throw Error('reviewer_maintenance_schema');
  const ledger=(await db.prepare('SELECT name FROM d1_migrations ORDER BY id').all<{name:string}>()).results.map(row=>row.name);
  if(canonical(ledger)!==canonical(releaseContract.requiredMigrations))throw Error('reviewer_maintenance_ledger');
  if(await fingerprint(await restorationSourceJournalSnapshot(db))!==plan.journalsSha256||
    await fingerprint(await restorationIndexControlSnapshot(db))!==plan.indexControlsSha256)
    throw Error('reviewer_maintenance_source_or_index_journal');
  const jobs=(await db.prepare('SELECT * FROM beedle_restore_jobs ORDER BY document_id').all<Record<string,unknown>>()).results,
    receipts=(await db.prepare('SELECT * FROM beedle_restore_source_results ORDER BY document_id').all()).results;
  if(jobs.length!==plan.documents||receipts.length!==plan.documents||jobs.some(job=>!['processed','held'].includes(String(job.phase))||job.source_verified!==1)||
    jobs.filter(job=>job.phase==='processed'&&job.extraction_policy!=='held-unreadable').length!==plan.eligibleDocuments||
    await fingerprint(jobs)!==plan.jobsSha256||await fingerprint(receipts)!==plan.receiptsSha256)
    throw Error('reviewer_maintenance_source_coverage');
  const state=await db.prepare(`SELECT (SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1) revision,
    (SELECT active_version FROM legal_catalog_state WHERE singleton=1) catalog,
    (SELECT COUNT(*) FROM documents) documents,
    (SELECT COUNT(*) FROM documents WHERE searchable_at IS NOT NULL OR published_revision IS NOT NULL) publications,
    (SELECT COUNT(*) FROM documents d JOIN beedle_restore_jobs j ON j.document_id=d.id WHERE j.phase='processed' AND j.source_verified=1 AND j.extraction_policy!='held-unreadable' AND d.ingestion_state='ready' AND d.active_text_generation!='initial' AND d.searchable_at IS NOT NULL AND d.published_revision=d.document_revision AND d.approved_at IS NULL AND d.approved_revision IS NULL AND d.qc_required_confirmed=0) provisionalPublications,
    (SELECT COUNT(*) FROM documents WHERE approved_at IS NOT NULL OR approved_revision IS NOT NULL OR qc_required_confirmed!=0) approvals,
    (SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner`).first<{
      revision:number;catalog:string;documents:number;publications:number;provisionalPublications:number;approvals:number;reviewerOwner:string|null}>();
  if(!state||state.revision!==plan.corpusRevision||state.catalog!==plan.catalogVersion||state.documents!==plan.documents||
    state.publications!==(plan.version===2?plan.publishedDocuments:0)||state.provisionalPublications!==(plan.version===2?plan.publishedDocuments:0)||state.approvals!==0||state.reviewerOwner!==null||!(await inspectSearchReadiness({DB:db})).ready)
    throw Error('reviewer_maintenance_frozen_source_or_readiness');
  return inspectReviewerQueue({DB:db});
}
export async function runRestorationReviewerWorkflow(env:RestorationReviewerWorkflowBindings,
  event:Pick<WorkflowEvent<RestorationReviewerWorkflowParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true')throw Error('reviewer_maintenance_disabled');
  const plan=parseRestorationReviewerPlan(env.OPERATION_JSON),planSha256=await fingerprint(plan);
  if(event.instanceId!==plan.operationId||!event.payload||Object.keys(event.payload).join('|')!=='operationPlanSha256'||event.payload.operationPlanSha256!==planSha256)
    throw Error('reviewer_maintenance_instance');
  const prefix='reviewer-maintenance/'+plan.operationId+'/',metadata={operationid:plan.operationId,plansha256:planSha256};
  async function put(name:string,value:unknown){
    const text=canonical(value),written=await env.PROOFS.put(prefix+name,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('reviewer_maintenance_proof_conflict');const read=await env.PROOFS.get(prefix+name);
    if(!read||!read.version||!read.etag||read.version!==written.version||read.etag!==written.etag||read.size!==new TextEncoder().encode(text).length||
      read.customMetadata?.operationid!==plan.operationId||read.customMetadata.plansha256!==planSha256||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await read.arrayBuffer())!==text)throw Error('reviewer_maintenance_proof_unacknowledged');
  }
  const options={retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const};
  async function once<T>(name:string,run:(db:D1Database)=>Promise<T>){
    if(await env.PROOFS.get(prefix+name+'-claim.json'))throw Error('reviewer_maintenance_already_claimed');
    await put(name+'-claim.json',{version:1,operationId:plan.operationId,planSha256,step:name});
    const capacity=restorationCapacityObserver(),meter=restorationBindingBudget(restorationMeasuredDatabase(env.DB,capacity,'staging'));
    try{
      const now=Date.now();if(name==='initialize'?(now<Date.parse(plan.notBefore)||now>=Date.parse(plan.notAfter)):now>=Date.parse(plan.completionDeadline))
        throw Error('reviewer_maintenance_window');
      const result=await run(meter.db);
      await put(name+'-outcome.json',{version:1,operationId:plan.operationId,planSha256,step:name,result,capacity:capacity.snapshot(),bindingCalls:meter.calls,at:new Date().toISOString()});return result;
    }catch(error){
      // No source/control reset or retry follows a lost builder acknowledgment.
      // The immutable attempt remains the authority for later reconciliation.
      try{await put(name+'-failure.json',{version:1,operationId:plan.operationId,planSha256,step:name,
        errorChain:await restorationErrorEvidence(error),bindingFailure:await meter.failureEvidence()??null,capacity:capacity.snapshot(),noRetry:true,at:new Date().toISOString()});}catch{/* Preserve the conditional claim even if diagnostic delivery is unknown. */}
      throw Error('reviewer_maintenance_held');
    }
  }
  await step.do('reviewer-maintenance-initialize-once',options,()=>once('initialize',db=>inspect(db,plan)));
  for(let index=0;index<Math.floor(plan.documents/25)+1;index++){
    const name='reviewer-'+String(index).padStart(4,'0');
    await step.do('reviewer-maintenance-'+name,options,()=>once(name,async db=>{
      const current=await inspect(db,plan);if(current.ready)return{state:'already_ready',visited:0};
      const advanced=await advanceReviewerQueue({DB:db},buildReviewerRows);if('busy'in advanced&&advanced.busy)throw Error('reviewer_maintenance_different_builder');
      return{state:'bounded_reviewer_checkpoint',visited:'visited'in advanced?advanced.visited:0,ready:advanced.ready,reconciled:'reconciled'in advanced&&advanced.reconciled===true};
    }));
  }
  await step.do('reviewer-maintenance-complete-once',options,()=>once('complete',async db=>{
    const current=await inspect(db,plan);if(!current.ready||current.sourceCount!==plan.documents||current.indexedCount!==plan.documents)
      throw Error('reviewer_maintenance_incomplete');return{state:'reviewer_ready',rows:current.indexedCount};
  }));
  return{state:'reviewer_ready',operationId:plan.operationId,steps:plan.maximumSteps,rows:plan.documents,
    sourceAndReviewHoldsPreserved:true,...(plan.version===2?{publicationPolicy:plan.publicationPolicy,existingProvisionalPublications:plan.publishedDocuments}:{}),integrityPublicationAndHumanApproval:false};
}
