import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {reconcilePublishedRestorationSource} from './restoration-published-source-receipt';

export interface PublishedSourceReceiptBindings {
  DB:D1Database;ORIGINALS:R2Bucket;TARGET_ORIGINALS:R2Bucket;PROOFS:R2Bucket;
  OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export interface PublishedSourceReceiptParams {operationPlanSha256:string}
export async function runPublishedSourceReceiptWorkflow(env:PublishedSourceReceiptBindings,
  event:Pick<WorkflowEvent<PublishedSourceReceiptParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true'||new TextEncoder().encode(env.OPERATION_JSON).length>8000)
    throw Error('published_source_workflow_disabled_or_size');
  const plan:unknown=JSON.parse(env.OPERATION_JSON);
  if(!plan||typeof plan!=='object'||Array.isArray(plan)||!('operationId' in plan)||plan.operationId!==event.instanceId||
    !event.payload||Object.keys(event.payload).join('|')!=='operationPlanSha256'||event.payload.operationPlanSha256!==await fingerprint(plan))
    throw Error('published_source_workflow_instance_or_plan');
  return step.do('reconcile-published-source-receipt-once',{
    retries:{limit:0,delay:'1 second',backoff:'constant'},timeout:'10 minutes'
  },()=>reconcilePublishedRestorationSource(plan,{db:env.DB,originals:env.ORIGINALS,target:env.TARGET_ORIGINALS,proofs:env.PROOFS}));
}
