import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {advanceSelectedReprocess,type SelectedReprocessBindings} from '../restoration-selected-reprocess-worker';

export interface SelectedWorkflowParams {operationPlanSha256:string}
export async function runSelectedWorkflowStep(env:SelectedReprocessBindings,
  event:Pick<WorkflowEvent<SelectedWorkflowParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true'||new TextEncoder().encode(env.OPERATION_JSON).length>5000)throw Error('selected_workflow_disabled_or_size');
  const plan:unknown=JSON.parse(env.OPERATION_JSON);
  if(!plan||typeof plan!=='object'||Array.isArray(plan)||!('operationId' in plan)||plan.operationId!==event.instanceId||
    !event.payload||Object.keys(event.payload).sort().join('|')!=='operationPlanSha256'||
    event.payload.operationPlanSha256!==await fingerprint(plan))throw Error('selected_workflow_instance_or_plan_changed');
  // Workflows normally retry a step. This non-idempotent operation has ZERO
  // configured retries, plus its own immutable R2 claim for lost acknowledgments.
  // A completed Workflow carrying "held" is still a domain failure, not a pass.
  return step.do('selected-restoration-reprocess-once',{
    retries:{limit:0,delay:'1 second',backoff:'constant'},timeout:'10 minutes'
  },()=>advanceSelectedReprocess(env));
}
