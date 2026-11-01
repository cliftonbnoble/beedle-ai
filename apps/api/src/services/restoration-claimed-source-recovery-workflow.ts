import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {recoverClaimedRestorationSource} from './restoration-claimed-source-recovery';

export interface ClaimedSourceRecoveryBindings {
  DB:D1Database;ORIGINALS:Pick<R2Bucket,'get'>;TARGET_ORIGINALS:R2Bucket;PROOFS:Pick<R2Bucket,'get'|'put'>;
  OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export interface ClaimedSourceRecoveryParams {operationPlanSha256:string}
export async function runClaimedSourceRecoveryWorkflow(env:ClaimedSourceRecoveryBindings,
  event:Pick<WorkflowEvent<ClaimedSourceRecoveryParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true'||new TextEncoder().encode(env.OPERATION_JSON).length>5000)
    throw Error('claimed_source_recovery_workflow_disabled_or_size');
  const plan:unknown=JSON.parse(env.OPERATION_JSON);
  if(!plan||typeof plan!=='object'||Array.isArray(plan)||!('operationId'in plan)||plan.operationId!==event.instanceId||
    !event.payload||Object.keys(event.payload).join('|')!=='operationPlanSha256'||event.payload.operationPlanSha256!==await fingerprint(plan))
    throw Error('claimed_source_recovery_workflow_instance_or_plan');
  return step.do('recover-original-claimed-source-forward-once',{
    retries:{limit:0,delay:'1 second',backoff:'constant'},timeout:'10 minutes'
  },()=>recoverClaimedRestorationSource(plan,{db:env.DB,originals:env.ORIGINALS,target:env.TARGET_ORIGINALS,proofs:env.PROOFS}));
}
