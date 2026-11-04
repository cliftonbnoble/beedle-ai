import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {sha256Bytes} from '../lib/digests';
import {parseSourceExecutionWorkflowPlan,runSourceExecutionWorkflow,type SourceExecutionWorkflowBindings} from './restoration-source-execution-workflow';
import {parseRestorationSourceExecutionIdentity} from './restoration-source-execution-control';
import {restorationExecutionJournalSnapshot} from './restoration-held-execution-lineage';
const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid();
const headerSchema=z.object({version:z.literal(1),epochId:uuid,rootPlanSha256:hash}).strict();
const rootBaseSchema=z.object({epochId:uuid,sourceSpecJson:z.string().min(1).max(16384),executionBase:z.unknown(),
  beforeSchemaSha256:hash,initialJournalSha256:hash,notBefore:z.string().datetime(),notAfter:z.string().datetime(),
  maximumBatches:z.number().int().min(1).max(16)}).strict();
const rootSchema=z.discriminatedUnion('version',[
  rootBaseSchema.extend({version:z.literal(1),batchSize:z.literal(32)}).strict(),
  rootBaseSchema.extend({version:z.literal(2),batchSize:z.literal(64)}).strict(),
  rootBaseSchema.extend({version:z.literal(3),batchSize:z.literal(128)}).strict(),
  rootBaseSchema.extend({version:z.literal(4),batchSize:z.literal(512)}).strict()
]);
export interface SourceBatchParams {sequence:number;operationPlanSha256:string;operationPlanJson?:string;rootPlanJson?:string}
export interface SourceBatchBindings extends SourceExecutionWorkflowBindings {OPERATION:Workflow<SourceBatchParams>}
const options={retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const};
export async function sourceBatchInstanceId(epochId:string,sequence:number){
  uuid.parse(epochId);z.number().int().min(0).max(15).parse(sequence);
  const digest=await fingerprint([epochId,sequence]);
  return digest.slice(0,8)+'-'+digest.slice(8,12)+'-4'+digest.slice(13,16)+'-a'+digest.slice(17,20)+'-'+digest.slice(20,32);
}
// The separately owned local controller supplies each hash-pinned plan and creates
// its fixed instance once. This private entry point cannot choose another job,
// expand a batch, invent a baseline, create a child or resume a predecessor.
export async function runSourceBatchWorkflow(env:SourceBatchBindings,
  event:Pick<WorkflowEvent<SourceBatchParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true'||new TextEncoder().encode(env.OPERATION_JSON).length>5000)throw Error('source_batch_disabled_or_header_size');
  const header=headerSchema.parse(JSON.parse(env.OPERATION_JSON)),params=z.object({sequence:z.number().int().min(0).max(15),operationPlanSha256:hash,operationPlanJson:z.string().max(20000).optional(),rootPlanJson:z.string().max(32768).optional()}).strict().parse(event.payload),
    rootPrefix='source-batches/'+header.epochId+'/',prefix=rootPrefix+params.sequence+'/',id=await sourceBatchInstanceId(header.epochId,params.sequence);
  if(event.instanceId!==id||env.ORIGINALS===env.TARGET_ORIGINALS||env.ORIGINALS===env.PROOFS||env.TARGET_ORIGINALS===env.PROOFS)
    throw Error('source_batch_instance_or_binding');
  async function read(key:string,maximum:number){
    const object=await env.PROOFS.get(key);
    if(!object||!object.version||!object.etag||object.size>maximum||object.customMetadata?.epochid!==header.epochId||
      object.customMetadata.rootplansha256!==header.rootPlanSha256)throw Error('source_batch_object_identity');
    const bytes=new Uint8Array(await object.arrayBuffer());
    if(bytes.length!==object.size||bytes.length>maximum)throw Error('source_batch_object_size');
    return {object,bytes,text:new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(bytes)};
  }
  if(params.rootPlanJson!==undefined&&(params.sequence!==0||params.operationPlanJson===undefined))
    throw Error('source_batch_root_delivery');
  const rootText=params.rootPlanJson??(await read(rootPrefix+'root.json',32768)).text;
  if(new TextEncoder().encode(rootText).length>32768||await sha256Bytes(new TextEncoder().encode(rootText))!==header.rootPlanSha256)
    throw Error('source_batch_root_digest');
  const root=rootSchema.parse(JSON.parse(rootText)),base=parseRestorationSourceExecutionIdentity(root.executionBase),begin=Date.parse(root.notBefore),end=Date.parse(root.notAfter);
  if(root.epochId!==header.epochId||await fingerprint(root)!==header.rootPlanSha256||end<=begin||end-begin>8*60*60000||
    Date.now()<begin||Date.now()>=end||params.sequence>=root.maximumBatches||root.maximumBatches>Math.max(1,Math.ceil(base.initialRemaining/root.batchSize)))
    throw Error('source_batch_root_bounds');
  async function validateChild(text:string,sequence:number,expectedDigest?:string){
    const bytes=new TextEncoder().encode(text),digest=await sha256Bytes(bytes);
    if(bytes.length>20000||expectedDigest&&digest!==expectedDigest)throw Error('source_batch_plan_digest');
    const parsed=parseSourceExecutionWorkflowPlan(text),{plan,execution}=parsed,remaining=base.initialRemaining-sequence*root.batchSize;
    if(plan.version!==(root.version===1?2:root.version===2?3:root.version===3?4:5)||await fingerprint(plan)!==digest||remaining<0||sequence>0&&remaining===0||
      execution.executionId!==await sourceBatchInstanceId(header.epochId,sequence)||
      execution.executionPlanSha256!==await fingerprint({epochId:header.epochId,rootPlanSha256:header.rootPlanSha256,sequence})||
      plan.sourceSpecJson!==root.sourceSpecJson||canonical(execution.source)!==canonical(base.source)||
      canonical(execution.parent)!==canonical(base.parent)||canonical(execution.preservedCases)!==canonical(base.preservedCases)||
      execution.initialProcessed!==base.initialProcessed+sequence*root.batchSize||execution.initialRemaining!==remaining||
      plan.maximumSources!==Math.min(root.batchSize,remaining)||plan.maximumSteps!==Math.min(root.batchSize,remaining)+2||
      plan.beforeSchemaSha256!==(sequence===0?root.beforeSchemaSha256:parsed.source.schemaSha256)||
      Date.parse(plan.notBefore)<begin||Date.parse(plan.notAfter)>end||Date.parse(plan.completionDeadline)>end)
      throw Error('source_batch_plan_not_rooted');
    return {...parsed,digest};
  }
  if(params.operationPlanJson!==undefined){
    const delivered=await validateChild(params.operationPlanJson,params.sequence,params.operationPlanSha256),
      metadata={epochid:header.epochId,rootplansha256:header.rootPlanSha256,sequence:String(params.sequence),operationplansha256:delivered.digest};
    await step.do('source-batch-delivered-plan-once',options,async()=>{
      if(await env.PROOFS.get(prefix+'plan-claim.json'))throw Error('source_batch_plan_already_claimed');
      async function create(key:string,text:string){
        const written=await env.PROOFS.put(key,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
        if(!written)throw Error('source_batch_plan_conflict');
        const got=await read(key,32768);
        if(got.object.version!==written.version||got.object.etag!==written.etag||got.text!==text||
          canonical(got.object.customMetadata)!==canonical(metadata))throw Error('source_batch_plan_unacknowledged');
      }
      await create(prefix+'plan-claim.json',canonical({version:1,epochId:header.epochId,sequence:params.sequence,operationPlanSha256:delivered.digest}));
      if(params.rootPlanJson!==undefined)await create(rootPrefix+'root.json',rootText);
      await create(prefix+'plan.json',canonical(delivered.plan));
      return {acknowledged:true};
    });
  }
  const acknowledgedRoot=await read(rootPrefix+'root.json',32768);
  if(acknowledgedRoot.text!==rootText)throw Error('source_batch_root_unacknowledged');
  async function planned(sequence:number,expectedDigest?:string){
    const object=await read(rootPrefix+sequence+'/plan.json',20000),parsed=await validateChild(object.text,sequence,expectedDigest);
    if(object.object.customMetadata?.operationplansha256!==parsed.digest)throw Error('source_batch_plan_digest');
    return parsed;
  }
  const child=await planned(params.sequence,params.operationPlanSha256),metadata={epochid:header.epochId,rootplansha256:header.rootPlanSha256,
    sequence:String(params.sequence),operationplansha256:child.digest};
  async function put(name:string,value:unknown){
    const text=canonical(value),written=await env.PROOFS.put(prefix+name,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('source_batch_conditional_conflict');
    const got=await read(prefix+name,32768);
    if(got.object.version!==written.version||got.object.etag!==written.etag||got.text!==text||
      got.object.customMetadata?.sequence!==String(params.sequence)||got.object.customMetadata.operationplansha256!==child.digest)
      throw Error('source_batch_proof_unacknowledged');
  }
  async function claim(name:string){
    if(await env.PROOFS.get(prefix+name+'-claim.json'))throw Error('source_batch_callback_already_claimed');
    await put(name+'-claim.json',{version:1,epochId:header.epochId,sequence:params.sequence,operationPlanSha256:child.digest});
  }
  await step.do('source-batch-admission-once',options,async()=>{
    await claim('admission');
    const journal=await restorationExecutionJournalSnapshot(env.DB);
    if(params.sequence===0){
      if(await fingerprint(journal)!==root.initialJournalSha256||child.execution.priorJournalSha256!==base.priorJournalSha256||
        child.execution.previousCurrentSha256!==base.previousCurrentSha256)throw Error('source_batch_initial_anchor_changed');
    }else{
      const previous=await planned(params.sequence-1),object=await read(rootPrefix+(params.sequence-1)+'/outcome.json',32768),
        outcome=z.object({version:z.literal(1),epochId:uuid,sequence:z.number().int(),operationPlanSha256:hash,
          journalSha256:hash,nextPriorJournalSha256:hash,nextCurrentSha256:hash,engineOutput:z.unknown()}).strict().parse(JSON.parse(object.text));
      const instance=await env.OPERATION.get(previous.execution.executionId),status=await instance.status();
      if(outcome.epochId!==header.epochId||outcome.sequence!==params.sequence-1||outcome.operationPlanSha256!==previous.digest||
        object.object.customMetadata?.operationplansha256!==previous.digest||object.object.customMetadata.sequence!==String(params.sequence-1)||
        status.status!=='complete'||await fingerprint(status.output)!==await fingerprint(outcome.engineOutput)||
        !outcome.engineOutput||typeof outcome.engineOutput!=='object'||!('state' in outcome.engineOutput)||outcome.engineOutput.state!=='source_checkpointed'||
        await fingerprint(journal)!==outcome.journalSha256||child.execution.priorJournalSha256!==outcome.nextPriorJournalSha256||
        child.execution.previousCurrentSha256!==outcome.nextCurrentSha256)throw Error('source_batch_predecessor_not_acknowledged');
    }
    return {admitted:true,operationPlanSha256:child.digest};
  });
  const result=await runSourceExecutionWorkflow({...env,OPERATION_JSON:canonical(child.plan)},
    {instanceId:id,payload:{operationPlanSha256:child.digest}},step);
  const output={state:result.state,epochId:header.epochId,sequence:params.sequence,executionId:id,
    sourceReceipts:result.sourceReceipts,publicationAndIntegrityApproval:false};
  await step.do('source-batch-outcome-once',options,async()=>{
    await claim('outcome');
    const object=await env.PROOFS.get('source-execution/'+id+'/complete-outcome.json');
    if(!object||object.size>32768||object.customMetadata?.executionid!==id||object.customMetadata.plansha256!==child.digest)
      throw Error('source_batch_checkpoint_proof_identity');
    const value:unknown=JSON.parse(new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await object.arrayBuffer()));
    const completed=z.object({executionId:uuid,planSha256:hash,step:z.literal('complete'),result:z.object({journalSha256:hash}).passthrough()}).passthrough().parse(value),
      journal=await restorationExecutionJournalSnapshot(env.DB),current=journal?.beedle_restore_execution_current?.[0];
    if(completed.executionId!==id||completed.planSha256!==child.digest||!journal||!current||current.execution_id!==id||
      await fingerprint(journal)!==completed.result.journalSha256)throw Error('source_batch_checkpoint_changed');
    await put('outcome.json',{version:1,epochId:header.epochId,sequence:params.sequence,operationPlanSha256:child.digest,
      journalSha256:await fingerprint(journal),nextPriorJournalSha256:await fingerprint({runs:journal.beedle_restore_execution_runs,
        ticks:journal.beedle_restore_execution_ticks,history:journal.beedle_restore_execution_history}),nextCurrentSha256:await fingerprint(current),engineOutput:output});
    return {acknowledged:true};
  });
  return output;
}
