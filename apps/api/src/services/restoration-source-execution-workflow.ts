import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {parseRestorationSourceWorkerSpec,readRestorationPartitionJobs} from '../restoration-source-worker';
import {initializeRestorationSourceExecution,parseRestorationSourceExecutionIdentity,
  inspectRestorationSourceExecution,claimRestorationSourceExecutionTick,completeRestorationSourceExecutionTick,
  holdRestorationSourceExecution,completeRestorationSourceExecution} from './restoration-source-execution-control';
import {runOwnedRestorationSourceJob} from './restoration-source-job';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationCapacityObserver,RestorationCapacityError,type RestorationCapacityPhase} from './restoration-capacity-observer';
import {restorationMeasuredDatabase} from './restoration-measured-database';
import {restorationErrorEvidence} from './restoration-failure-proof';
import {inspectRestorationOwner} from './restoration-job-control';
import {restorationExecutionJournalSnapshot} from './restoration-held-execution-lineage';

const hash=z.string().regex(/^[a-f0-9]{64}$/),baseSchema=z.object({sourceSpecJson:z.string().min(1).max(16384),
  execution:z.unknown(),beforeSchemaSha256:hash,notBefore:z.string().datetime(),notAfter:z.string().datetime(),
  completionDeadline:z.string().datetime(),maximumSteps:z.number().int().min(2).max(1000)}).strict();
const schema=z.discriminatedUnion('version',[
  baseSchema.extend({version:z.literal(1)}).strict(),
  baseSchema.extend({version:z.literal(2),maximumSources:z.number().int().min(0).max(32)}).strict(),
  baseSchema.extend({version:z.literal(3),maximumSources:z.number().int().min(0).max(64)}).strict(),
  baseSchema.extend({version:z.literal(4),maximumSources:z.number().int().min(0).max(128)}).strict(),
  baseSchema.extend({version:z.literal(5),maximumSources:z.number().int().min(0).max(512)}).strict()
]);
export interface SourceExecutionWorkflowBindings {
  DB:D1Database;ORIGINALS:R2Bucket;TARGET_ORIGINALS:R2Bucket;PROOFS:R2Bucket;OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export interface SourceExecutionWorkflowParams {operationPlanSha256:string}
export function parseSourceExecutionWorkflowPlan(text:string){
  if(new TextEncoder().encode(text).length>20000)throw Error('source_execution_workflow_plan_size');
  const plan=schema.parse(JSON.parse(text)),execution=parseRestorationSourceExecutionIdentity(plan.execution),source=parseRestorationSourceWorkerSpec(plan.sourceSpecJson),
    begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter),deadline=Date.parse(plan.completionDeadline);
  const sources=plan.version===1?execution.initialRemaining:plan.maximumSources;
  if(end<=begin||end-begin>30*60000||deadline<=end||deadline-begin>4*60*60000||plan.maximumSteps!==sources+2||
    sources>execution.initialRemaining||plan.version!==1&&sources===0&&execution.initialRemaining!==0||
    (execution.previousCurrentSha256===null?plan.beforeSchemaSha256===source.schemaSha256:plan.beforeSchemaSha256!==source.schemaSha256))
    throw Error('source_execution_workflow_bounds');
  return {plan,execution,source};
}
export async function runSourceExecutionWorkflow(env:SourceExecutionWorkflowBindings,
  event:Pick<WorkflowEvent<SourceExecutionWorkflowParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true')throw Error('source_execution_workflow_disabled');
  const {plan,execution,source}=parseSourceExecutionWorkflowPlan(env.OPERATION_JSON),planSha256=await fingerprint(plan);
  if(event.instanceId!==execution.executionId||!event.payload||Object.keys(event.payload).join('|')!=='operationPlanSha256'||event.payload.operationPlanSha256!==planSha256||
    await fingerprint(source.identity)!==await fingerprint(execution.source))throw Error('source_execution_workflow_instance_or_identity');
  if(env.ORIGINALS===env.TARGET_ORIGINALS||env.ORIGINALS===env.PROOFS||env.TARGET_ORIGINALS===env.PROOFS)throw Error('source_execution_workflow_binding_alias');
  const prefix='source-execution/'+execution.executionId+'/',metadata={executionid:execution.executionId,plansha256:planSha256},
    options={retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const};
  async function put(name:string,value:unknown){
    const text=canonical(value),written=await env.PROOFS.put(prefix+name,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('source_execution_workflow_conditional_conflict');
    const read=await env.PROOFS.get(prefix+name);
    if(!read||!read.version||!read.etag||read.version!==written.version||read.etag!==written.etag||read.size!==new TextEncoder().encode(text).length||
      read.customMetadata?.executionid!==execution.executionId||read.customMetadata.plansha256!==planSha256||new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await read.arrayBuffer())!==text)
      throw Error('source_execution_workflow_proof_unacknowledged');
  }
  async function once<T>(name:string,phase:RestorationCapacityPhase,run:(db:D1Database)=>Promise<T>){
    // A claimed callback can only be observed after a lost acknowledgment. It
    // cannot acquire another source, repeat DDL or replace a durable outcome.
    if(await env.PROOFS.get(prefix+name+'-claim.json'))throw Error('source_execution_workflow_step_already_claimed');
    await put(name+'-claim.json',{version:1,executionId:execution.executionId,planSha256,step:name});
    const capacity=restorationCapacityObserver(),meter=restorationBindingBudget(restorationMeasuredDatabase(env.DB,capacity,phase));
    try{
      const now=Date.now();
      if(name==='initialize'?(now<Date.parse(plan.notBefore)||now>=Date.parse(plan.notAfter)):now>=Date.parse(plan.completionDeadline))throw Error('source_execution_workflow_window_expired');
      const result=await run(meter.db);
      await put(name+'-outcome.json',{version:1,executionId:execution.executionId,planSha256,step:name,result,capacity:capacity.snapshot(),bindingCalls:meter.calls,at:new Date().toISOString()});
      return result;
    }catch(error){
      // The reserved hold has a separate fixed 30-call budget, below the 200
      // control/storage reserve. This does not extend a SQL or source-job limit.
      let holdAcknowledged=false;const reserve=restorationBindingBudget(env.DB,30);
      try{await holdRestorationSourceExecution(reserve.db,execution,await fingerprint(error instanceof Error?error.message:String(error)));holdAcknowledged=true;}catch{/* Preserve any unknown or not-yet-created control and all original claims. */}
      try{await put(name+'-failure.json',{version:1,executionId:execution.executionId,planSha256,step:name,errorChain:await restorationErrorEvidence(error),
        bindingFailure:await meter.failureEvidence()??null,capacity:capacity.snapshot(),capacityReason:error instanceof RestorationCapacityError?error.reason:null,
        sqlResponseAcknowledged:error instanceof RestorationCapacityError?error.sqlResponseAcknowledged:null,holdAcknowledged,holdBindingCalls:reserve.calls,noRetry:true,at:new Date().toISOString()});}catch{/* A lost diagnostic cannot clear the attempt claim. */}
      throw Error('source_execution_workflow_step_held');
    }
  }
  await step.do('source-execution-initialize-once',options,()=>once('initialize','bootstrap',async db=>{
    const jobs=await readRestorationPartitionJobs(env.PROOFS,source),owner=await inspectRestorationOwner(db,execution.source);
    if(jobs.length!==execution.initialProcessed+execution.initialRemaining||jobs.length!==owner.expected_documents||
      jobs.filter(job=>job.extractionPolicy!=='held-unreadable').length!==owner.expected_published_documents)throw Error('source_execution_workflow_source_plan_count');
    return initializeRestorationSourceExecution(db,execution,plan.beforeSchemaSha256,source.schemaSha256);
  }));
  const maximumSources=plan.version===1?execution.initialRemaining:plan.maximumSources;
  for(let index=0;index<maximumSources;index++){
    const name='source-'+String(index).padStart(4,'0');
    await step.do('source-execution-'+name,options,()=>once(name,'source',async db=>{
      const started=Date.now(),objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
      if(await restorationSchemaFingerprint(objects)!==source.schemaSha256)throw Error('source_execution_workflow_schema_changed');
      const current=await inspectRestorationSourceExecution(db,execution),jobs=await readRestorationPartitionJobs(env.PROOFS,source);
      if(jobs.length!==current.owner.expected_documents||jobs.filter(job=>job.extractionPolicy!=='held-unreadable').length!==current.owner.expected_published_documents)throw Error('source_execution_workflow_plan_count_changed');
      const pending=(await db.prepare("SELECT document_id FROM beedle_restore_jobs WHERE phase='planned' ORDER BY document_id LIMIT 1").all<{document_id:string}>()).results;
      if(pending.length!==1||Date.now()-started>=20000)throw Error('source_execution_workflow_no_bounded_source');
      const job=jobs.find(value=>value.documentId===pending[0]?.document_id);if(!job)throw Error('source_execution_workflow_source_not_planned');
      const tick=await claimRestorationSourceExecutionTick(db,execution);if(!tick)throw Error('source_execution_workflow_tick_not_claimed');
      const result=await runOwnedRestorationSourceJob({db,originals:env.ORIGINALS,target:env.TARGET_ORIGINALS,proofs:env.PROOFS,identity:execution.source,execution,tickToken:tick.token,job,profile:source.profile});
      if(result.state==='not_claimed')throw Error('source_execution_workflow_source_not_claimed');
      await completeRestorationSourceExecutionTick(db,execution,tick.token);
      return {state:'one_source_completed',deliveries:1,wallMs:Date.now()-started,loopAdmissionBudgetMs:20000};
    }));
  }
  await step.do('source-execution-complete-once',options,()=>once('complete','source',async db=>{
    const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
    if(await restorationSchemaFingerprint(objects)!==source.schemaSha256)throw Error('source_execution_workflow_final_schema_changed');
    if(maximumSources===execution.initialRemaining){
      const completed=await completeRestorationSourceExecution(db,execution);
      return plan.version===1?completed:{...completed,sourceCheckpointed:false,
        sourceReceipts:execution.initialProcessed+maximumSources,remainingPlanned:0,
        journalSha256:await fingerprint(await restorationExecutionJournalSnapshot(db))};
    }
    const current=await inspectRestorationSourceExecution(db,execution),counts=(await db.prepare(
      "SELECT (SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase='planned') planned,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')) unresolved,(SELECT COUNT(*) FROM beedle_restore_execution_history WHERE execution_id=? AND (state!='completed' OR deliveries!=1)) incompleteHistory,(SELECT COUNT(*) FROM beedle_restore_execution_history WHERE execution_id=?) deliveries"
    ).bind(execution.executionId,execution.executionId).all<{receipts:number;planned:number;unresolved:number;incompleteHistory:number;deliveries:number}>()).results[0];
    if(!counts||current.run.state!=='building'||current.tick.state!=='idle'||current.tick.token!==null||
      counts.receipts!==execution.initialProcessed+maximumSources||counts.planned!==execution.initialRemaining-maximumSources||
      counts.unresolved!==0||counts.incompleteHistory!==0||counts.deliveries!==maximumSources)
      throw Error('source_execution_bounded_checkpoint_changed');
    const handoffReasonSha256=await fingerprint('Planned bounded source handoff after '+maximumSources+' deliveries');
    await holdRestorationSourceExecution(db,execution,handoffReasonSha256);
    const held=await inspectRestorationSourceExecution(db,execution);
    if(held.run.state!=='held'||held.tick.state!=='held'||held.tick.token!==null||held.run.error_sha256!==handoffReasonSha256)
      throw Error('source_execution_bounded_handoff_unacknowledged');
    return {completed:false,sourceCheckpointed:true,sourceReceipts:counts.receipts,remainingPlanned:counts.planned,
      handoffReasonSha256,journalSha256:await fingerprint(await restorationExecutionJournalSnapshot(db))};
  }));
  return {state:maximumSources===execution.initialRemaining?'source_complete':'source_checkpointed',executionId:execution.executionId,
    steps:maximumSources+2,sourceReceipts:execution.initialProcessed+maximumSources,publicationAndIntegrityApproval:false};
}
