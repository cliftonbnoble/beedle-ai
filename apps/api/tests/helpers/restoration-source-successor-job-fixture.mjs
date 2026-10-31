import {createHash} from 'node:crypto';
import {loadTs} from './load-ts.mjs';
import {restorationSourceJobFixture} from './restoration-source-job-fixture.mjs';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';

const control=loadTs('src/services/restoration-source-successor-control.ts');
const {runOwnedRestorationSourceJob,restorationSeedMetadataSha256}=loadTs('src/services/restoration-source-job.ts');
const {holdRestorationTick,claimRestorationTick}=loadTs('src/services/restoration-job-control.ts');
const sha=value=>createHash('sha256').update(value).digest('hex');
function partitionZeroIds(){
  const ids=[];
  for(let i=0;ids.length<2;i++){
    const id='successor-fixture-'+i;
    if(createHash('sha256').update(id).digest().readUInt32BE(0)%32===0)ids.push(id);
  }
  return ids;
}

// Real source/generation services over synthetic Markdown, with one completed
// predecessor and a distinct untouched record. This is not remote evidence.
export async function successorJobFixture({initialize=true}={}){
  const [first,next]=partitionZeroIds(),x=await restorationSourceJobFixture(undefined,{documentId:first,extraDocumentId:next,idle:true});
  const firstJob={...x.job};
  const initial=await restorationSeedMetadataSha256(x.db,next);
  const job={...firstJob,documentId:next,originalKey:'next.md',initialMetadataSha256:initial.sha256,initialRevision:initial.row.document_revision};
  const planned=entry=>{const {planObjectKey,...rest}=entry;void planObjectKey;return rest;};
  const plan={version:1,ownerId:x.identity.ownerId,resourcePlanSha256:x.identity.resourcePlanSha256,
    resourceId:x.identity.resourceId,partitionIndex:0,sourceManifestSha256:x.identity.sourceManifestSha256,
    catalogSha256:x.identity.catalogSha256,jobs:[planned(firstJob),planned(job)]};
  const text=JSON.stringify(plan),planSha256=sha(text);job.planObjectKey='plans/'+planSha256+'.json';
  x.sqlite.prepare("INSERT INTO beedle_restore_jobs(document_id,source_sha256,source_bytes,original_key,plan_object_key,extraction_policy,phase) VALUES(?,?,?,?,?,?,'planned')")
    .run(job.documentId,job.sourceSha256,job.sourceBytes,job.originalKey,job.planObjectKey,job.extractionPolicy);
  await x.proofs.put(job.planObjectKey,text,{});
  x.tickToken=(await claimRestorationTick(x.db,x.identity)).token;
  await runOwnedRestorationSourceJob(x);
  await holdRestorationTick(x.db,x.identity,x.tickToken,'e'.repeat(64));
  const failedId='44444444-4444-4444-8444-444444444444';
  x.sqlite.prepare("INSERT INTO document_generations(id,document_id,base_revision,status,source_key,source_sha256,snapshot_json,manifest_json,created_at,manifest_version) VALUES(?,?,1,'failed','synthetic.md',?,'{}','{}','old',3)")
    .run(failedId,first,firstJob.sourceSha256);
  const old={owner:x.sqlite.prepare('SELECT * FROM beedle_restore_owner').get(),tick:x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),
    history:x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history').get(),failed:x.sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(failedId)};
  const identity={source:x.identity,successorId:'55555555-5555-4555-8555-555555555555',successorPlanSha256:'1'.repeat(64),
    predecessorOwnerSha256:await fingerprint(old.owner),predecessorTickSha256:await fingerprint(old.tick),predecessorHistorySha256:await fingerprint(old.history),
    forwardOperationId:'66666666-6666-4666-8666-666666666666',forwardResultSha256:'2'.repeat(64),failedGenerationId:failedId,
    failedGenerationSha256:await fingerprint(old.failed),initialProcessed:1,initialRemaining:1};
  const schemaRows=()=>x.sqlite.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all();
  const beforeSchemaSha256=await restorationSchemaFingerprint(schemaRows());
  x.sqlite.exec('BEGIN');let afterSchemaSha256;
  try{for(const sql of control.restorationSourceSuccessorSql)x.sqlite.exec(sql);afterSchemaSha256=await restorationSchemaFingerprint(schemaRows());}
  finally{x.sqlite.exec('ROLLBACK');}
  if(initialize)await control.initializeRestorationSourceSuccessor(x.db,identity,beforeSchemaSha256,afterSchemaSha256);
  const source={version:1,identity:x.identity,partitionPlanSha256:planSha256,
    schemaSha256:afterSchemaSha256,
    originalBucket:'beedle-sources-migration-20261027',targetBucket:'beedle-restored-11111111',proofBucket:'beedle-restoration-proof-11111111',profile:x.profile};
  const {source:ignored,...fields}=identity;void ignored;
  const env={DB:x.db,ORIGINALS:x.originals,TARGET_ORIGINALS:x.target,PROOFS:x.proofs,RESTORE_ENABLED:'true',
    RESTORE_SPEC_JSON:JSON.stringify({version:1,sourceSpecJson:JSON.stringify(source),...fields})};
  return {...x,first,next,old,successor:identity,job,source,env,beforeSchemaSha256,afterSchemaSha256};
}
