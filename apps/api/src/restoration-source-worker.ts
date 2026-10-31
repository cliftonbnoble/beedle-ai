import {fingerprint} from '../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../scripts/lib/owned-restoration-resources.mjs';
import {sha256Bytes} from './lib/digests';
import {corpusPartitionIndex} from './services/corpus-partitions';
import {restorationBindingBudget} from './services/restoration-binding-budget';
import {recordRestorationFailure} from './services/restoration-failure-proof';
import {inspectRestorationOwner,claimRestorationTick,completeRestorationTick,holdRestorationTick,holdRestorationPreflight,RestorationControlError,type RestorationIdentity} from './services/restoration-job-control';
import {runOwnedRestorationSourceJob,type RestorationSourceJob} from './services/restoration-source-job';

interface SourceWorkerSpec {
  version:1;identity:RestorationIdentity;partitionPlanSha256:string;schemaSha256:string;
  originalBucket:string;targetBucket:string;proofBucket:string;
  profile:{AI_EMBEDDING_MODEL:string;VECTOR_NAMESPACE:string;AI_EMBEDDING_DIMENSIONS:string};
}
function record(value:unknown):value is Record<string,unknown>{return typeof value==='object'&&value!==null&&!Array.isArray(value);}
function keys(value:Record<string,unknown>,names:string[]){return Object.keys(value).sort().join('|')===names.sort().join('|');}
const digest=/^[a-f0-9]{64}$/,uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
export function parseRestorationSourceWorkerSpec(text:string):SourceWorkerSpec{
  if(text.length>16384)throw new RestorationControlError('source_spec_size');const value:unknown=JSON.parse(text);
  if(!record(value)||!keys(value,['version','identity','partitionPlanSha256','schemaSha256','originalBucket','targetBucket','proofBucket','profile'])||value.version!==1||
    !record(value.identity)||!record(value.profile))throw new RestorationControlError('source_spec_shape');
  const row=value.identity,profile=value.profile;
  if(!keys(row,['ownerId','planSha256','resourcePlanSha256','resourceId','partitionIndex','sourceManifestSha256','catalogSha256'])||
    typeof row.ownerId!=='string'||!uuid.test(row.ownerId)||typeof row.resourceId!=='string'||!uuid.test(row.resourceId)||
    typeof row.partitionIndex!=='number'||!Number.isSafeInteger(row.partitionIndex)||row.partitionIndex<0||row.partitionIndex>31||
    typeof row.planSha256!=='string'||!digest.test(row.planSha256)||typeof row.resourcePlanSha256!=='string'||!digest.test(row.resourcePlanSha256)||
    typeof row.sourceManifestSha256!=='string'||!digest.test(row.sourceManifestSha256)||typeof row.catalogSha256!=='string'||!digest.test(row.catalogSha256)||
    typeof value.partitionPlanSha256!=='string'||!digest.test(value.partitionPlanSha256)||typeof value.schemaSha256!=='string'||!digest.test(value.schemaSha256)||
    value.originalBucket!=='beedle-sources-migration-20261027'||typeof value.targetBucket!=='string'||!/^beedle-restored-[a-f0-9-]{8,50}$/.test(value.targetBucket)||
    typeof value.proofBucket!=='string'||!/^beedle-restoration-proof-[a-f0-9-]{8,40}$/.test(value.proofBucket)||value.targetBucket===value.proofBucket||
    !keys(profile,['AI_EMBEDDING_MODEL','VECTOR_NAMESPACE','AI_EMBEDDING_DIMENSIONS'])||typeof profile.AI_EMBEDDING_MODEL!=='string'||!profile.AI_EMBEDDING_MODEL||profile.AI_EMBEDDING_MODEL.length>200||
    typeof profile.VECTOR_NAMESPACE!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(profile.VECTOR_NAMESPACE)||typeof profile.AI_EMBEDDING_DIMENSIONS!=='string'||!/^[1-9]\d{0,3}$/.test(profile.AI_EMBEDDING_DIMENSIONS)||Number(profile.AI_EMBEDDING_DIMENSIONS)>4096)throw new RestorationControlError('source_spec_values');
  return{version:1,identity:{ownerId:row.ownerId,resourceId:row.resourceId,partitionIndex:row.partitionIndex,planSha256:row.planSha256,
    resourcePlanSha256:row.resourcePlanSha256,sourceManifestSha256:row.sourceManifestSha256,catalogSha256:row.catalogSha256},
    partitionPlanSha256:value.partitionPlanSha256,schemaSha256:value.schemaSha256,originalBucket:value.originalBucket,targetBucket:value.targetBucket,proofBucket:value.proofBucket,
    profile:{AI_EMBEDDING_MODEL:profile.AI_EMBEDDING_MODEL,VECTOR_NAMESPACE:profile.VECTOR_NAMESPACE,AI_EMBEDDING_DIMENSIONS:profile.AI_EMBEDDING_DIMENSIONS}};
}
export async function readRestorationPartitionJobs(proofs:Pick<R2Bucket,'get'>,spec:SourceWorkerSpec){
  const key=`plans/${spec.partitionPlanSha256}.json`,object=await proofs.get(key);
  if(!object||object.size>2*1024*1024)throw new RestorationControlError('source_partition_plan_missing_or_size');
  const bytes=new Uint8Array(await object.arrayBuffer());
  if(bytes.byteLength!==object.size||await sha256Bytes(bytes)!==spec.partitionPlanSha256)throw new RestorationControlError('source_partition_plan_bytes');
  const value:unknown=JSON.parse(new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(bytes)),identity=spec.identity;
  if(!record(value)||!keys(value,['version','ownerId','resourcePlanSha256','resourceId','partitionIndex','sourceManifestSha256','catalogSha256','jobs'])||value.version!==1||
    value.ownerId!==identity.ownerId||value.resourcePlanSha256!==identity.resourcePlanSha256||value.resourceId!==identity.resourceId||value.partitionIndex!==identity.partitionIndex||
    value.sourceManifestSha256!==identity.sourceManifestSha256||value.catalogSha256!==identity.catalogSha256||!Array.isArray(value.jobs)||value.jobs.length>1000)throw new RestorationControlError('source_partition_plan_identity');
  const jobs:RestorationSourceJob[]=[],ids=new Set<string>();
  for(const item of value.jobs){
    if(!record(item)||!keys(item,['documentId','sourceSha256','sourceBytes','originalKey','extractionPolicy','initialMetadataSha256','initialRevision',...(item.originalVersion===undefined?[]:['originalVersion'])])||
      typeof item.documentId!=='string'||!item.documentId||item.documentId.length>200||ids.has(item.documentId)||await corpusPartitionIndex(item.documentId)!==identity.partitionIndex||
      typeof item.sourceSha256!=='string'||!digest.test(item.sourceSha256)||typeof item.sourceBytes!=='number'||!Number.isSafeInteger(item.sourceBytes)||item.sourceBytes<1||item.sourceBytes>1000000||
      typeof item.originalKey!=='string'||!item.originalKey||new TextEncoder().encode(item.originalKey).length>1024||/[\u0000-\u001f\u007f]/.test(item.originalKey)||
      !['verified-source-restoration-v1','recovered-markdown-v1','held-unreadable'].includes(String(item.extractionPolicy))||
      typeof item.initialMetadataSha256!=='string'||!digest.test(item.initialMetadataSha256)||typeof item.initialRevision!=='number'||!Number.isSafeInteger(item.initialRevision)||item.initialRevision<1||
      item.originalVersion!==undefined&&(typeof item.originalVersion!=='string'||!item.originalVersion||item.originalVersion.length>1024))throw new RestorationControlError('source_partition_job_values');
    const extractionPolicy=item.extractionPolicy;
    if(extractionPolicy!=='verified-source-restoration-v1'&&extractionPolicy!=='recovered-markdown-v1'&&extractionPolicy!=='held-unreadable')throw new RestorationControlError('source_partition_job_policy');
    ids.add(item.documentId);jobs.push({documentId:item.documentId,sourceSha256:item.sourceSha256,sourceBytes:item.sourceBytes,originalKey:item.originalKey,
      extractionPolicy,initialMetadataSha256:item.initialMetadataSha256,initialRevision:item.initialRevision,planObjectKey:key,originalVersion:item.originalVersion});
  }
  return jobs;
}
export async function advanceRestorationSourceTick(env:RestorationSourceBindings){
  if(env.RESTORE_ENABLED!=='true')return{state:'disabled',deliveries:0};
  const spec=parseRestorationSourceWorkerSpec(env.RESTORE_SPEC_JSON),meter=restorationBindingBudget(env.DB),db=meter.db;
  const failureProof=async(stage:'preflight'|'claim'|'source',error:unknown,attemptId=crypto.randomUUID())=>{
    try{await recordRestorationFailure({proofs:env.PROOFS,identity:spec.identity,attemptId,stage,bindingCalls:meter.calls,
      bindingFailure:await meter.failureEvidence(),error});}catch{/* A lost diagnostic acknowledgment cannot clear a hold or replay a source job. */}
  };
  if(env.ORIGINALS===env.TARGET_ORIGINALS||env.ORIGINALS===env.PROOFS||env.TARGET_ORIGINALS===env.PROOFS)throw new RestorationControlError('source_binding_alias');
  const owner=await inspectRestorationOwner(db,spec.identity);
  if(owner.phase!=='building')return{state:owner.phase==='held'?'held':owner.phase==='seeded'?'awaiting_start':'source_complete',deliveries:0};
  const control=await db.prepare('SELECT owner_id,plan_sha256,state FROM beedle_restore_tick WHERE singleton=1').first<{owner_id:string;plan_sha256:string;state:string}>();
  if(!control||control.owner_id!==spec.identity.ownerId||control.plan_sha256!==spec.identity.planSha256)throw new RestorationControlError('source_tick_owner');
  if(control.state!=='idle')return{state:control.state==='held'?'held':'busy',deliveries:0};
  let jobs:RestorationSourceJob[];
  try{
  const schema=(await db.prepare("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
  if(await restorationSchemaFingerprint(schema)!==spec.schemaSha256)throw new RestorationControlError('source_schema_changed');
  jobs=await readRestorationPartitionJobs(env.PROOFS,spec);
  if(jobs.length!==owner.expected_documents||jobs.filter(x=>x.extractionPolicy!=='held-unreadable').length!==owner.expected_published_documents)throw new RestorationControlError('source_partition_expected_counts');
  }catch(error){try{await holdRestorationPreflight(env.DB,spec.identity,await fingerprint(error instanceof Error?error.message:String(error)));}catch{/* Preserve unconfirmed guard outcome. */}
    await failureProof('preflight',error);
    throw new RestorationControlError('source_preflight_held');}
  const tick=await claimRestorationTick(db,spec.identity).catch(async error=>{try{await holdRestorationPreflight(env.DB,spec.identity,await fingerprint(error instanceof Error?error.message:String(error)));}catch{/* Retain unknown owner state. */}await failureProof('claim',error);throw new RestorationControlError('source_claim_guard_held');});if(!tick)return{state:'busy',deliveries:0};
  let deliveries=0;const began=Date.now();
  try{
    const pending=(await db.prepare("SELECT document_id FROM beedle_restore_jobs WHERE phase='planned' ORDER BY document_id LIMIT 4").all<{document_id:string}>()).results;
    for(const row of pending){if(Date.now()-began>=20000||meter.remaining<250)break;const job=jobs.find(x=>x.documentId===row.document_id);if(!job)throw new RestorationControlError('source_job_not_in_pinned_plan');
      if(job.extractionPolicy!=='verified-source-restoration-v1'&&deliveries>0)break;
      const result=await runOwnedRestorationSourceJob({db,originals:env.ORIGINALS,target:env.TARGET_ORIGINALS,proofs:env.PROOFS,identity:spec.identity,tickToken:tick.token,job,profile:spec.profile});
      if(result.state==='not_claimed')throw new RestorationControlError('source_job_claim_unconfirmed');deliveries++;if(job.extractionPolicy!=='verified-source-restoration-v1')break;
    }
    await completeRestorationTick(db,spec.identity,tick.token,'building',deliveries);
    const remaining=await db.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1").first<{n:number}>();
    if(remaining?.n===0)await db.prepare(`UPDATE beedle_restore_owner SET phase='indexing',updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='building'
      AND NOT EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE phase NOT IN ('processed','held') OR source_verified!=1)`)
      .bind(new Date().toISOString(),spec.identity.ownerId,spec.identity.planSha256).run();
    return{state:remaining?.n===0?'source_complete':'building',deliveries,bindingCalls:meter.calls};
  }catch(error){
    const hash=await fingerprint(error instanceof Error?error.message:String(error));
    try{await holdRestorationTick(env.DB,spec.identity,tick.token,hash);}catch{/* Preserve uncertain claims and original failure; never reclaim. */}
    await failureProof('source',error,tick.token);
    throw new RestorationControlError('owned_source_tick_held');
  }
}
export default{
  fetch(){return new Response('Private corpus source restoration',{status:503});},
  async scheduled(_event:ScheduledController,env:RestorationSourceBindings){
    try{const result=await advanceRestorationSourceTick(env);console.log(JSON.stringify({event:'owned_source_restoration',state:result.state,deliveries:result.deliveries}));
    if(result.state==='held')throw new Error('Held');}catch{throw new Error('Owned source restoration guard or checkpoint failed; inspect private evidence');}
  }
} satisfies ExportedHandler<RestorationSourceBindings>;
