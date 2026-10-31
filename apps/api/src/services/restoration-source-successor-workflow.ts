import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {advanceRestorationSourceSuccessorTick,parseRestorationSourceSuccessorSpec} from '../restoration-source-successor-worker';
import {initializeRestorationSourceSuccessor} from './restoration-source-successor-control';

const hash=z.string().regex(/^[a-f0-9]{64}$/);
const schema=z.object({version:z.literal(1),successorSpecJson:z.string().min(1).max(20000),beforeSchemaSha256:hash,
  notBefore:z.string().datetime(),notAfter:z.string().datetime(),completionDeadline:z.string().datetime(),
  maximumTicks:z.number().int().min(1).max(1000)}).strict();
export interface SourceSuccessorWorkflowBindings {
  DB:D1Database;ORIGINALS:R2Bucket;TARGET_ORIGINALS:R2Bucket;PROOFS:R2Bucket;
  OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export interface SourceSuccessorParams {operationPlanSha256:string}
export function parseSourceSuccessorWorkflowPlan(text:string){
  if(new TextEncoder().encode(text).length>24000)throw Error('source_successor_workflow_size');
  const plan=schema.parse(JSON.parse(text)),spec=parseRestorationSourceSuccessorSpec(plan.successorSpecJson);
  const begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter),deadline=Date.parse(plan.completionDeadline);
  if(end<=begin||end-begin>30*60000||deadline<=end||deadline-begin>4*60*60000||
    spec.identity.initialRemaining>plan.maximumTicks||plan.beforeSchemaSha256===spec.source.schemaSha256)
    throw Error('source_successor_workflow_bounds');
  return {plan,spec};
}
export async function runSourceSuccessorWorkflow(env:SourceSuccessorWorkflowBindings,
  event:Pick<WorkflowEvent<SourceSuccessorParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true')throw Error('source_successor_workflow_disabled');
  const {plan,spec}=parseSourceSuccessorWorkflowPlan(env.OPERATION_JSON),planSha256=await fingerprint(plan);
  if(event.instanceId!==spec.identity.successorId||!event.payload||Object.keys(event.payload).join('|')!=='operationPlanSha256'||
    event.payload.operationPlanSha256!==planSha256)throw Error('source_successor_workflow_instance_or_plan');
  if(env.ORIGINALS===env.TARGET_ORIGINALS||env.ORIGINALS===env.PROOFS||env.TARGET_ORIGINALS===env.PROOFS)
    throw Error('source_successor_workflow_binding_alias');
  const prefix=`source-successor/${spec.identity.successorId}/`,metadata={successorid:spec.identity.successorId,plansha256:planSha256};
  async function put(name:string,value:unknown){
    const text=canonical(value),written=await env.PROOFS.put(prefix+name,text,{onlyIf:{etagDoesNotMatch:'*'},
      customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('source_successor_workflow_conditional_conflict');
    const read=await env.PROOFS.get(prefix+name);
    if(!read||read.size!==new TextEncoder().encode(text).length||read.version!==written.version||read.etag!==written.etag||
      !read.version||!read.etag||read.customMetadata?.successorid!==metadata.successorid||read.customMetadata.plansha256!==planSha256||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await read.arrayBuffer())!==text)throw Error('source_successor_workflow_proof_unacknowledged');
  }
  async function once<T>(name:string,run:()=>Promise<T>){
    // Even a duplicate callback after a lost Workflow step acknowledgment may
    // not claim a different source tick. Existing claims are never cleared.
    if(await env.PROOFS.get(prefix+name+'-claim.json'))throw Error('source_successor_workflow_step_already_claimed');
    await put(name+'-claim.json',{version:1,successorId:spec.identity.successorId,planSha256,step:name});
    try{const result=await run();await put(name+'-outcome.json',{version:1,successorId:spec.identity.successorId,planSha256,step:name,result,at:new Date().toISOString()});return result;}
    catch(error){
      // The claim alone is enough to prohibit automatic replay. Do not put
      // arbitrary exception messages or case values in a public error.
      try{await put(name+'-failure.json',{version:1,successorId:spec.identity.successorId,planSha256,step:name,
        errorSha256:await fingerprint(error instanceof Error?error.message:String(error)),noRetry:true,at:new Date().toISOString()});}catch{/* Preserve the original unknown acknowledgment. */}
      throw Error('source_successor_workflow_step_held');
    }
  }
  const options={retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const};
  await step.do('source-successor-initialize-once',options,()=>once('initialize',async()=>{
    const now=Date.now();if(now<Date.parse(plan.notBefore)||now>=Date.parse(plan.notAfter))throw Error('source_successor_workflow_admission_expired');
    return initializeRestorationSourceSuccessor(env.DB,spec.identity,plan.beforeSchemaSha256,spec.source.schemaSha256);
  }));
  const sourceEnv={DB:env.DB,ORIGINALS:env.ORIGINALS,TARGET_ORIGINALS:env.TARGET_ORIGINALS,PROOFS:env.PROOFS,
    RESTORE_ENABLED:'true',RESTORE_SPEC_JSON:plan.successorSpecJson};
  for(let index=0;index<plan.maximumTicks;index++){
    const name='tick-'+String(index).padStart(4,'0');
    const result=await step.do('source-successor-'+name,options,()=>once(name,async()=>{
      if(Date.now()>=Date.parse(plan.completionDeadline))throw Error('source_successor_workflow_completion_deadline');
      const outcome=await advanceRestorationSourceSuccessorTick(sourceEnv);
      if(!['building','source_complete'].includes(outcome.state)||outcome.state==='building'&&outcome.deliveries===0)
        throw Error('source_successor_workflow_tick_not_positive');
      return outcome;
    }));
    if(result.state==='source_complete')return {state:'source_complete',successorId:spec.identity.successorId,ticks:index+1,publicationAndIntegrityApproval:false};
  }
  throw Error('source_successor_workflow_tick_bound_exhausted');
}
