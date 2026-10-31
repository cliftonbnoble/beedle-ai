import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {completeRestorationSourceForward} from './restoration-source-forward-completion';

export interface SourceForwardBindings {
  DB:D1Database;ORIGINALS:R2Bucket;TARGET_ORIGINALS:R2Bucket;PROOFS:R2Bucket;
  OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export interface SourceForwardParams {operationPlanSha256:string}
export async function runSourceForwardWorkflow(env:SourceForwardBindings,
  event:Pick<WorkflowEvent<SourceForwardParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true'||new TextEncoder().encode(env.OPERATION_JSON).length>5000)
    throw Error('source_forward_workflow_disabled_or_size');
  const plan:unknown=JSON.parse(env.OPERATION_JSON);
  if(!plan||typeof plan!=='object'||Array.isArray(plan)||!('operationId' in plan)||plan.operationId!==event.instanceId||
    !event.payload||Object.keys(event.payload).sort().join('|')!=='operationPlanSha256'||
    event.payload.operationPlanSha256!==await fingerprint(plan))throw Error('source_forward_workflow_instance_or_plan');
  return step.do('source-forward-completion-once',{
    retries:{limit:0,delay:'1 second',backoff:'constant'},timeout:'5 minutes'
  },()=>completeRestorationSourceForward(plan,{
    db:env.DB,originals:env.ORIGINALS,target:env.TARGET_ORIGINALS,proofs:env.PROOFS
  }));
}
