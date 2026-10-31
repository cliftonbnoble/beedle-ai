import {fingerprint} from './release-contract.mjs';
import {validateRestorationResourcePlan,restorationDatabaseName,restorationSchemaFingerprint} from './owned-restoration-resources.mjs';
import {restorationCatalogDigest} from './owned-restoration-catalog.mjs';
const digest=/^[a-f0-9]{64}$/,uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
function declaration(resource,plan,index){
 validateRestorationResourcePlan(resource);
 if(plan?.version!==1||plan.kind!=='owned-original-source-metadata-seed'||plan.ownerId!==resource.ownerId||plan.resourcePlanSha256===undefined||
  ![plan.resourcePlanSha256,plan.sourcePlanSha256,plan.storageCompletionSha256,plan.beforeSchemaSha256,plan.afterSchemaSha256].every(x=>digest.test(x))||
  plan.catalogSha256!==resource.catalogSha256||plan.sourceManifestSha256!==resource.sourceManifestSha256||!Number.isFinite(Date.parse(plan.seededAt))||
  !Array.isArray(plan.migrations)||plan.migrations.length!==33||plan.migrations.some((name,i)=>!new RegExp('^'+String(i+1).padStart(4,'0')+'_[a-z0-9_]+\\.sql$').test(name))||
  !Array.isArray(plan.targets)||plan.targets.length!==32||plan.targets.some((x,i)=>x.index!==i||!uuid.test(x.resourceId)||!Number.isSafeInteger(x.documents)||x.documents<1||x.documents>1000||!Number.isSafeInteger(x.expectedPublished)||x.expectedPublished<0||x.expectedPublished>x.documents||
   ![x.sqlSha256,x.metadataSha256,x.jobsSha256,x.partitionPlanSha256].every(y=>digest.test(y)))||new Set(plan.targets.map(x=>x.resourceId)).size!==32||!Number.isInteger(index)||index<0||index>31)throw Error('Complete immutable source metadata seed plan required');
 return plan.targets[index];
}
function fresh(resource,g,now){const age=now-Date.parse(g?.observedAt);if(!g||g.ownerId!==resource.ownerId||g.passed!==true||g.sourceFrozen!==true||g.candidateUnchanged!==true||g.originalAnchorMatched!==true||g.productionPaused!==true||g.targetsUnbound!==true||g.storageOwnedPrivateVerified!==true||g.localReserveBytes<20*1024**3||!Number.isFinite(age)||age<0||age>300000)throw Error('Fresh source, storage, isolation and reserve guards required');}
async function inspect(resource,plan,row,cloud,after){
 const metadata=await cloud.getDatabase(row.resourceId),claim=await cloud.readResourceClaim(row.resourceId);
 if(plan.resourcePlanSha256!==await fingerprint(resource)||!metadata||metadata.uuid!==row.resourceId||metadata.name!==restorationDatabaseName(resource.ownerId,row.index)||resource.forbiddenDatabaseIds.includes(row.resourceId)||
  !claim||claim.owner_id!==resource.ownerId||claim.resource_plan_sha256!==plan.resourcePlanSha256||claim.resource_id!==row.resourceId||claim.partition_index!==row.index)throw Error('Source seed physical owner differs');
 if(JSON.stringify(await cloud.readMigrationLedger(row.resourceId))!==JSON.stringify(plan.migrations)||await restorationSchemaFingerprint(await cloud.listApplicationObjects(row.resourceId))!==(after?plan.afterSchemaSha256:plan.beforeSchemaSha256)||restorationCatalogDigest(await cloud.readCatalogSnapshot(row.resourceId))!==plan.catalogSha256)throw Error('Source seed exact schema, ledger or catalog differs');
 const state=await cloud.readSourceSeedState(row.resourceId);
 if(state.initializerCompleted!==true||state.leasesQuiescent!==true||state.approvals!==0||state.publications!==0||state.textArtifacts!==0)throw Error('Source seed review, empty text or initializer guard differs');
 if(!after){if(state.documents!==0||state.operationalPresent!==false)throw Error('Source seed requires an untouched empty target');return;}
 if(state.documents!==row.documents||state.operationalPresent!==true||state.metadataSha256!==row.metadataSha256||state.jobsSha256!==row.jobsSha256||state.jobs!==row.documents||state.unplannedJobs!==0||state.history!==0||state.sourceResults!==0||state.integrity!==0||state.capacity!==0)throw Error('Source seed typed metadata/jobs readback differs');
 const owner=state.owner,tick=state.tick;
 if(!owner||owner.singleton!==1||owner.owner_id!==resource.ownerId||owner.plan_sha256!==plan.sourcePlanSha256||owner.source_manifest_sha256!==plan.sourceManifestSha256||owner.catalog_sha256!==plan.catalogSha256||owner.partition_index!==row.index||owner.partition_count!==32||owner.expected_documents!==row.documents||owner.expected_published_documents!==row.expectedPublished||owner.phase!=='seeded'||owner.started_at!==plan.seededAt||owner.updated_at!==plan.seededAt||owner.last_outcome!==null||
  !tick||tick.owner_id!==resource.ownerId||tick.plan_sha256!==plan.sourcePlanSha256||tick.state!=='idle'||tick.token!==null||tick.started_at!==null||tick.updated_at!==plan.seededAt)throw Error('Source seed operational owner/tick differs');
}
export async function seedOwnedRestorationSource({resource,plan,index,store,cloud,guard,runSql,now=Date.now()}){
 const row=declaration(resource,plan,index);fresh(resource,guard,now);const id=`database-${String(index).padStart(2,'0')}-source-seed`;
 if(await store.exists(id+'-start'))throw Error('Existing source seed intent requires observation, never replay');
 await inspect(resource,plan,row,cloud,false);
 const intent={ownerId:resource.ownerId,index,resourceId:row.resourceId,planSha256:await fingerprint(plan),sourcePlanSha256:plan.sourcePlanSha256,sqlSha256:row.sqlSha256,startedAt:new Date(now).toISOString()};await store.reserve(id+'-start',intent);
 try{if((await runSql()).acknowledged!==true)throw Error('No source seed acknowledgment');}catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:await fingerprint(String(error?.message||error)),noRetryAuthorized:true});throw Error('Source seed outcome unknown; observe without SQL replay');}
 await inspect(resource,plan,row,cloud,true);const outcome={...intent,phase:'owned_source_metadata_seed_verified',documents:row.documents,noApprovalOrPublication:true};await store.reserve(id+'-verified',outcome);return outcome;
}
export async function observeOwnedRestorationSourceSeed({resource,plan,index,store,cloud}){
 const row=declaration(resource,plan,index),id=`database-${String(index).padStart(2,'0')}-source-seed`,intent=await store.read(id+'-start');
 if(intent.ownerId!==resource.ownerId||intent.resourceId!==row.resourceId||intent.planSha256!==await fingerprint(plan)||intent.sqlSha256!==row.sqlSha256)throw Error('Source seed observation intent differs');
 await inspect(resource,plan,row,cloud,true);const outcome={...intent,phase:'owned_source_metadata_seed_reconciled',sqlReissued:false};await store.reserve(id+'-observed-'+crypto.randomUUID(),outcome);return outcome;
}
