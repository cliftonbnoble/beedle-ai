import {fingerprint} from './release-contract.mjs';
import {validateRestorationResourcePlan,restorationDatabaseName,restorationSchemaFingerprint} from './owned-restoration-resources.mjs';
import {restorationCatalogDigest} from './owned-restoration-catalog.mjs';
const digest=/^[a-f0-9]{64}$/;
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
function declaration(resource,plan){
  validateRestorationResourcePlan(resource);
  if(plan?.version!==1||plan.ownerId!==resource.ownerId||!uuid.test(plan.generation)||!digest.test(plan.resourcePlanSha256)||!digest.test(plan.initializerPlanSha256)||
    !digest.test(plan.expectedBeforeSchemaSha256)||!digest.test(plan.expectedAfterSchemaSha256)||!digest.test(plan.sqlTemplatesSha256)||
    plan.catalogSha256!==resource.catalogSha256||!Number.isSafeInteger(plan.catalogRows)||plan.catalogRows<0||
    !Array.isArray(plan.migrations)||plan.migrations.length!==33||plan.migrations.some((name,index)=>
      typeof name!=='string'||!new RegExp('^'+String(index+1).padStart(4,'0')+'_[a-z0-9_]+\\.sql$').test(name))||!Array.isArray(plan.sqlFiles)||plan.sqlFiles.length!==32||
    plan.sqlFiles.some((row,index)=>row.index!==index||!digest.test(row.sha256)))throw Error('Incomplete new-target initializer bootstrap');
}
function fresh(resource,guard,now){const age=now-Date.parse(guard?.observedAt);
 if(!guard||guard.ownerId!==resource.ownerId||guard.passed!==true||guard.sourceFrozen!==true||guard.candidateUnchanged!==true||guard.productionPaused!==true||
   guard.originalAnchorMatched!==true||guard.targetsUnbound!==true||guard.localReserveBytes<20*1024**3||!Number.isFinite(age)||age<0||age>300_000)throw Error('Fresh source/ownership/isolation/reserve guard required');}
async function inspect(resource,plan,index,id,cloud,after){
 if(!uuid.test(id))throw Error('Invalid initializer physical resource');
 const metadata=await cloud.getDatabase(id),claim=await cloud.readResourceClaim(id);
 if(!metadata||metadata.uuid!==id||metadata.name!==restorationDatabaseName(resource.ownerId,index)||resource.forbiddenDatabaseIds.includes(id)||
  !claim||claim.owner_id!==resource.ownerId||claim.resource_plan_sha256!==await fingerprint(resource)||claim.resource_id!==id||claim.partition_index!==index||plan.resourcePlanSha256!==await fingerprint(resource))throw Error('Initializer physical resource or owner differs');
 if(JSON.stringify(await cloud.readMigrationLedger(id))!==JSON.stringify(plan.migrations)||
  await restorationSchemaFingerprint(await cloud.listApplicationObjects(id))!==(after?plan.expectedAfterSchemaSha256:plan.expectedBeforeSchemaSha256))throw Error('Initializer exact ledger/schema differs');
 const snapshot=await cloud.readLegacySnapshot(id);
 const catalog=await cloud.readCatalogSnapshot(id);
 if(snapshot.documents!==0||snapshot.rows!==plan.catalogRows||snapshot.policy!=='unknown'||
  restorationCatalogDigest(catalog)!==plan.catalogSha256||catalog.reduce((n,table)=>n+table.rows.length,0)!==plan.catalogRows)throw Error('Initializer empty corpus/catalog snapshot guard failed');
 if(after){const rows=await cloud.readInitializerClaims(id),row=rows.initializer,tick=rows.tick;
  if(!row||row.owner_id!==resource.ownerId||row.resource_plan_sha256!==plan.resourcePlanSha256||row.resource_id!==id||row.partition_index!==index||row.plan_sha256!==plan.initializerPlanSha256||row.generation!==plan.generation||row.catalog_sha256!==plan.catalogSha256||row.catalog_rows!==plan.catalogRows||row.state!=='planned'||row.deliveries!==0||row.started_at!==null||row.completed_at!==null||row.error_sha256!==null||
   !tick||tick.owner_id!==resource.ownerId||tick.plan_sha256!==plan.initializerPlanSha256||tick.state!=='idle'||tick.token!==null||rows.history!==0||rows.errors!==0)throw Error('Initializer control claim/readback differs');
 }
}
export async function bootstrapOwnedIndexInitializer({resource,plan,index,resourceId,store,cloud,guard,runSql,now=Date.now()}){
 declaration(resource,plan);fresh(resource,guard,now);restorationDatabaseName(resource.ownerId,index);
 const id=`database-${String(index).padStart(2,'0')}-index-init-bootstrap`;
 if(await store.exists(id+'-start'))throw Error('Existing initializer bootstrap requires observation, never replay');
 await inspect(resource,plan,index,resourceId,cloud,false);
 const intent={ownerId:resource.ownerId,index,resourceId,planSha256:await fingerprint(plan),startedAt:new Date(now).toISOString(),sqlSha256:plan.sqlFiles[index].sha256};
 await store.reserve(id+'-start',intent);
 try{const result=await runSql();if(result.acknowledged!==true)throw Error('No engine acknowledgment');}
 catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:await fingerprint(String(error?.message||error)),noRetryAuthorized:true});throw Error('Initializer bootstrap outcome unknown; independent observation required');}
 await inspect(resource,plan,index,resourceId,cloud,true);
 const outcome={...intent,phase:'owned_empty_initializer_bootstrap_verified',applicationMigrations:33,documents:0};await store.reserve(id+'-verified',outcome);return outcome;
}
export async function observeOwnedIndexInitializerBootstrap({resource,plan,index,resourceId,store,cloud,now=Date.now()}){
 declaration(resource,plan);const id=`database-${String(index).padStart(2,'0')}-index-init-bootstrap`,intent=await store.read(id+'-start');
 if(intent.ownerId!==resource.ownerId||intent.index!==index||intent.resourceId!==resourceId||intent.planSha256!==await fingerprint(plan))throw Error('Initializer bootstrap intent differs');
 await inspect(resource,plan,index,resourceId,cloud,true);const outcome={...intent,phase:'owned_empty_initializer_bootstrap_reconciled',observedAt:new Date(now).toISOString(),sqlReissued:false};await store.reserve(id+'-observed-'+crypto.randomUUID(),outcome);return outcome;
}
