import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {parseRestorationSourceSuccessorSpec} from '../restoration-source-successor-worker';
import {parseRestorationTerminalDispositionPlan,terminalizeRestorationAttempt} from './restoration-terminal-disposition';

const schema=z.object({version:z.literal(2),operationId:z.string().uuid(),targets:z.array(z.object({
  database:z.enum(['DB0','DB2']),planSha256:z.string().regex(/^[a-f0-9]{64}$/)}).strict()).min(1).max(2)}).strict();
export interface RestorationTerminalBindings{
  DB0:D1Database;DB2:D1Database;PROOFS:R2Bucket;OPERATION_ENABLED:string;OPERATION_JSON:string;
  TARGET0_PLAN_JSON:string;TARGET2_PLAN_JSON:string;
}
export interface RestorationTerminalParams{operationPlanSha256:string}
export async function runRestorationTerminalWorkflow(env:RestorationTerminalBindings,
  event:Pick<WorkflowEvent<RestorationTerminalParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true')throw Error('terminal_workflow_disabled');
  if(new TextEncoder().encode(env.OPERATION_JSON).length>5000)throw Error('terminal_workflow_plan_size');
  const plan=schema.parse(JSON.parse(env.OPERATION_JSON)),planSha256=await fingerprint(plan);
  if(event.instanceId!==plan.operationId||!event.payload||Object.keys(event.payload).join('|')!=='operationPlanSha256'||
    event.payload.operationPlanSha256!==planSha256||env.DB0===env.DB2)throw Error('terminal_workflow_identity_or_binding_alias');
  if(new Set(plan.targets.map(t=>t.database)).size!==plan.targets.length)throw Error('terminal_workflow_target_scope');
  const targets=[];
  for(const target of plan.targets){
    const text=target.database==='DB0'?env.TARGET0_PLAN_JSON:env.TARGET2_PLAN_JSON;
    if(typeof text!=='string'||new TextEncoder().encode(text).length>5000)throw Error('terminal_workflow_target_size');
    const child=parseRestorationTerminalDispositionPlan(JSON.parse(text));
    if(await fingerprint(child)!==target.planSha256)throw Error('terminal_workflow_target_plan_changed');
    targets.push({...target,plan:child});
  }
  if(new Set(targets.map(t=>t.plan.operationId)).size!==targets.length||
    targets.some(t=>t.plan.operationId===plan.operationId||parseRestorationSourceSuccessorSpec(t.plan.successorSpecJson).identity.source.partitionIndex!==(t.database==='DB0'?0:2)))
    throw Error('terminal_workflow_target_scope');
  const results=[];
  for(const target of targets){
    const result=await step.do('terminal-source-'+target.database+'-once',
      {retries:{limit:0,delay:'1 second',backoff:'constant'},timeout:'10 minutes'},async()=>{
        const prefix=`terminal-disposition/${plan.operationId}/${target.database}/`,metadata={operationid:plan.operationId,
          plansha256:planSha256,targetoperationid:target.plan.operationId};
        const put=async(name:string,value:unknown)=>{
          const text=canonical(value),written=await env.PROOFS.put(prefix+name,text,{onlyIf:{etagDoesNotMatch:'*'},
            customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
          if(!written)throw Error('terminal_workflow_conditional_conflict');
          const read=await env.PROOFS.get(prefix+name);
          if(!read||!read.version||!read.etag||read.version!==written.version||read.etag!==written.etag||
            read.size!==new TextEncoder().encode(text).length||Object.entries(metadata).some(([k,v])=>read.customMetadata?.[k]!==v)||
            new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await read.arrayBuffer())!==text)
            throw Error('terminal_workflow_proof_unacknowledged');
        };
        if(await env.PROOFS.get(prefix+'claim.json'))throw Error('terminal_workflow_already_claimed');
        await put('claim.json',{version:1,operationId:plan.operationId,targetOperationId:target.plan.operationId,
          planSha256,parentTerminalProofSha256:target.plan.parentTerminalProofSha256});
        try{
          const value=await terminalizeRestorationAttempt(env[target.database],target.plan);
          await put('outcome.json',{version:1,operationId:plan.operationId,targetOperationId:target.plan.operationId,planSha256,
            result:value,at:new Date().toISOString()});return value;
        }catch(error){
          try{await put('failure.json',{version:1,operationId:plan.operationId,targetOperationId:target.plan.operationId,planSha256,
            errorSha256:await fingerprint(error instanceof Error?error.message:String(error)),noRetry:true,at:new Date().toISOString()});}catch{/* Preserve an unacknowledged immutable claim. */}
          throw Error('terminal_workflow_held_no_replay');
        }
      });
    results.push(result);
  }
  return {state:'terminal_dispositions_complete',operationId:plan.operationId,results,sourceOrPublicationSuccess:false};
}
