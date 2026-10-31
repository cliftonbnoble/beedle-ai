import {z} from 'zod';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../scripts/lib/owned-restoration-resources.mjs';
import {parseRestorationSourceWorkerSpec,readRestorationPartitionJobs} from './restoration-source-worker';
import {restorationBindingBudget} from './services/restoration-binding-budget';
import {recordRestorationFailure} from './services/restoration-failure-proof';
import {runOwnedRestorationSourceJob} from './services/restoration-source-job';
import {RestorationControlError} from './services/restoration-job-control';
import {inspectRestorationSourceSuccessor,claimRestorationSourceSuccessorTick,completeRestorationSourceSuccessorTick,
  holdRestorationSourceSuccessorTick,holdRestorationSourceSuccessorPreflight,completeRestorationSourceSuccessorSources} from './services/restoration-source-successor-control';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid();
const schema=z.object({version:z.literal(1),sourceSpecJson:z.string().min(1).max(16384),
  successorId:uuid,successorPlanSha256:hash,predecessorOwnerSha256:hash,predecessorTickSha256:hash,predecessorHistorySha256:hash,
  forwardOperationId:uuid,forwardResultSha256:hash,failedGenerationId:uuid,failedGenerationSha256:hash,
  initialProcessed:z.number().int().min(1),initialRemaining:z.number().int().min(1)}).strict();
export function parseRestorationSourceSuccessorSpec(text:string){
  if(new TextEncoder().encode(text).length>20000)throw new RestorationControlError('source_successor_spec_size');
  const parsed=schema.parse(JSON.parse(text)),source=parseRestorationSourceWorkerSpec(parsed.sourceSpecJson);
  const {sourceSpecJson,...identity}=parsed;void sourceSpecJson;
  return {source,identity:{source:source.identity,...identity}};
}
export async function advanceRestorationSourceSuccessorTick(env:RestorationSourceBindings){
  if(env.RESTORE_ENABLED!=='true')return {state:'disabled',deliveries:0};
  const spec=parseRestorationSourceSuccessorSpec(env.RESTORE_SPEC_JSON),identity=spec.identity;
  if(env.ORIGINALS===env.TARGET_ORIGINALS||env.ORIGINALS===env.PROOFS||env.TARGET_ORIGINALS===env.PROOFS)
    throw new RestorationControlError('source_successor_binding_alias');
  const meter=restorationBindingBudget(env.DB),db=meter.db,state=await inspectRestorationSourceSuccessor(db,identity);
  if(state.row.state!=='building')return {state:state.row.state==='completed'?'source_complete':'held',deliveries:0};
  if(state.tick.state!=='idle')return {state:state.tick.state==='held'?'held':'busy',deliveries:0};
  const evidence=async(stage:'preflight'|'claim'|'source',error:unknown,attemptId=crypto.randomUUID())=>{
    try{await recordRestorationFailure({proofs:env.PROOFS,identity:identity.source,attemptId,stage,bindingCalls:meter.calls,
      bindingFailure:await meter.failureEvidence(),error});}catch{/* A missing diagnostic never clears the successor or old hold. */}
  };
  let jobs:Awaited<ReturnType<typeof readRestorationPartitionJobs>>;
  try{
    const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
    if(await restorationSchemaFingerprint(objects)!==spec.source.schemaSha256)throw new RestorationControlError('source_successor_schema_changed');
    jobs=await readRestorationPartitionJobs(env.PROOFS,spec.source);
    if(jobs.length!==state.owner.expected_documents||jobs.filter(j=>j.extractionPolicy!=='held-unreadable').length!==state.owner.expected_published_documents)
      throw new RestorationControlError('source_successor_plan_counts_changed');
  }catch(error){
    try{await holdRestorationSourceSuccessorPreflight(env.DB,identity,await fingerprint(error instanceof Error?error.message:String(error)));}catch{/* Retain unknown ownership/outcome. */}
    await evidence('preflight',error);throw new RestorationControlError('source_successor_preflight_held');
  }
  const tick=await claimRestorationSourceSuccessorTick(db,identity).catch(async error=>{
    try{await holdRestorationSourceSuccessorPreflight(env.DB,identity,await fingerprint(error instanceof Error?error.message:String(error)));}catch{/* An unknown running claim cannot be cleared. */}
    await evidence('claim',error);throw new RestorationControlError('source_successor_claim_held');
  });
  if(!tick)return {state:'busy',deliveries:0};
  let deliveries=0;const began=Date.now();
  try{
    const pending=(await db.prepare("SELECT document_id FROM beedle_restore_jobs WHERE phase='planned' ORDER BY document_id LIMIT 4").all<{document_id:string}>()).results;
    for(const row of pending){
      if(Date.now()-began>=20000||meter.remaining<250)break;
      const job=jobs.find(j=>j.documentId===row.document_id);
      if(!job)throw new RestorationControlError('source_successor_job_not_in_plan');
      if(job.extractionPolicy!=='verified-source-restoration-v1'&&deliveries>0)break;
      const result=await runOwnedRestorationSourceJob({db,originals:env.ORIGINALS,target:env.TARGET_ORIGINALS,proofs:env.PROOFS,
        identity:identity.source,tickToken:tick.token,successor:identity,job,profile:spec.source.profile});
      if(result.state==='not_claimed')throw new RestorationControlError('source_successor_job_claim_unconfirmed');
      deliveries++;if(job.extractionPolicy!=='verified-source-restoration-v1')break;
    }
    await completeRestorationSourceSuccessorTick(db,identity,tick.token,deliveries);
    const remaining=await db.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1").first<{n:number}>();
    if(!remaining)throw new RestorationControlError('source_successor_remaining_unacknowledged');
    if(remaining.n===0)await completeRestorationSourceSuccessorSources(db,identity);
    return {state:remaining.n===0?'source_complete':'building',deliveries,bindingCalls:meter.calls};
  }catch(error){
    try{await holdRestorationSourceSuccessorTick(env.DB,identity,tick.token,await fingerprint(error instanceof Error?error.message:String(error)));}catch{/* Preserve any unknown or already-terminal result. */}
    await evidence('source',error,tick.token);throw new RestorationControlError('source_successor_tick_held');
  }
}
export default {
  fetch(){return new Response('Private source restoration successor',{status:503});},
  async scheduled(_event:ScheduledController,env:RestorationSourceBindings){
    const result=await advanceRestorationSourceSuccessorTick(env);
    if(result.state==='held')throw new Error('Source successor held; inspect private evidence');
  }
} satisfies ExportedHandler<RestorationSourceBindings>;
