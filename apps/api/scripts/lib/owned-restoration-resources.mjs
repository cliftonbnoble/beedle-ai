import {fingerprint} from './release-contract.mjs';

const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const digest=/^[a-f0-9]{64}$/;
export function restorationDatabaseName(ownerId,index){
  if(!uuid.test(ownerId)||!Number.isInteger(index)||index<0||index>31)throw Error('Invalid restoration resource identity');
  return `beedle-corpus-${ownerId}-p${String(index).padStart(2,'0')}`;
}
export function validateRestorationResourcePlan(plan){
  if(!plan||plan.version!==1||!uuid.test(plan.ownerId)||!/^[a-f0-9]{32}$/.test(plan.accountId)||plan.partitionCount!==32||
    !digest.test(plan.sourceManifestSha256)||!digest.test(plan.catalogSha256)||!Array.isArray(plan.forbiddenDatabaseIds)||
    plan.forbiddenDatabaseIds.length<3||plan.forbiddenDatabaseIds.some(id=>!uuid.test(id))||
    !Array.isArray(plan.databases)||plan.databases.length!==32||plan.databases.some((row,index)=>row.index!==index||row.name!==restorationDatabaseName(plan.ownerId,index)))
    throw Error('Incomplete isolated restoration resource plan');
  return plan;
}
function validateResult(plan,index,row){
  if(!row||!uuid.test(row.uuid)||row.name!==plan.databases[index].name||plan.forbiddenDatabaseIds.includes(row.uuid))
    throw Error('Restoration resource identity differs; no claim or write is authorized');
  return row;
}
export function restorationApplicationObjects(rows){
  if(!Array.isArray(rows))throw Error('Incomplete resource schema inspection');
  // D1 documents this exact reserved storage table. Do not query, delete or
  // broadly ignore provider-prefixed objects that have not been identified.
  const providerSql='CREATETABLE_cf_KV(keyTEXTPRIMARYKEY,valueBLOB)WITHOUTROWID';
  return rows.filter(row=>!(row.name==='_cf_KV'&&row.type==='table'&&
    String(row.sql).replace(/\s/g,'').replace(/;$/,'')===providerSql));
}
function assertGuard(plan,guard,now){
  const age=now-Date.parse(guard?.observedAt);
  if(!guard||guard.ownerId!==plan.ownerId||guard.passed!==true||guard.sourceFrozen!==true||guard.candidateUnchanged!==true||guard.productionPaused!==true||
    guard.originalAnchorMatched!==true||guard.localReserveBytes<20*1024**3||!Number.isFinite(age)||age<0||age>300_000)
    throw Error('Fresh owned source/candidate/production/local-reserve guards are required');
}

// store.reserve is an exclusive, fsynced receipt, never an upsert. A request may
// have succeeded despite a lost acknowledgment, so create never retries itself.
export async function createIsolatedRestorationDatabase({plan,index,store,cloud,guard,now=Date.now()}){
  validateRestorationResourcePlan(plan);restorationDatabaseName(plan.ownerId,index);assertGuard(plan,guard,now);
  const id=`database-${String(index).padStart(2,'0')}`;
  if(await store.exists(id+'-start'))throw Error('An existing resource attempt must be observed, never replayed');
  const existing=await cloud.listDatabases();
  if(existing.some(row=>row.name===plan.databases[index].name))throw Error('Planned database name already exists; ownership review is required');
  const intent={version:1,ownerId:plan.ownerId,planSha256:await fingerprint(plan),index,name:plan.databases[index].name,
    startedAt:new Date(now).toISOString(),preinventoryNameAbsent:true,kind:'new_empty_database'};
  await store.reserve(id+'-start',intent);
  let result;
  try{result=await cloud.createDatabase({name:intent.name});}
  catch(error){
    await store.reserve(id+'-unacknowledged',{...intent,errorClass:error?.name||'Error',errorSha256:await fingerprint(String(error?.message||error)),
      phase:'unacknowledged_requires_observation',noRetryAuthorized:true});
    throw Error('New database outcome is unacknowledged; observe the existing resource attempt');
  }
  validateResult(plan,index,result);
  await store.reserve(id+'-acknowledged',{...intent,uuid:result.uuid,phase:'created_requires_independent_readback'});
  const readback=validateResult(plan,index,await cloud.getDatabase(result.uuid));
  await store.reserve(id+'-readback',{...intent,uuid:readback.uuid,createdAt:readback.created_at??null,phase:'new_empty_resource_read_back',
    schemaApplied:false,documentsInserted:false,ownerClaimStillRequired:true});
  return readback;
}

export async function observeIsolatedRestorationDatabase({plan,index,store,cloud,now=Date.now()}){
  validateRestorationResourcePlan(plan);restorationDatabaseName(plan.ownerId,index);
  const id=`database-${String(index).padStart(2,'0')}`,intent=await store.read(id+'-start');
  if(intent.ownerId!==plan.ownerId||intent.planSha256!==await fingerprint(plan)||intent.index!==index||intent.name!==plan.databases[index].name||!intent.preinventoryNameAbsent)
    throw Error('Restoration resource intent differs');
  const ack=await store.exists(id+'-acknowledged')?await store.read(id+'-acknowledged'):null;
  const rows=(await cloud.listDatabases()).filter(row=>row.name===intent.name);
  if(rows.length!==1)throw Error('No unique acknowledged restoration resource; preserve the uncertain attempt');
  const row=validateResult(plan,index,await cloud.getDatabase(rows[0].uuid));
  if(ack&&ack.uuid!==row.uuid)throw Error('Acknowledged resource was replaced; ownership review is required');
  const created=Date.parse(row.created_at),started=Date.parse(intent.startedAt);
  if(!ack&&(!Number.isFinite(created)||created<started-1000||created>now||now-started>3_600_000))
    throw Error('Unacknowledged resource time/identity cannot be reconciled');
  const objects=restorationApplicationObjects(await cloud.listApplicationObjects(row.uuid));
  if(objects.length)throw Error('Observed database contains application objects; do not overwrite or claim a differing owner');
  const outcome={ownerId:plan.ownerId,planSha256:intent.planSha256,index,name:intent.name,uuid:row.uuid,
    observedAt:new Date(now).toISOString(),phase:'observed_empty_unclaimed_resource',createReissued:false,ownerClaimStillRequired:true};
  await store.reserve(id+'-observed-'+crypto.randomUUID(),outcome);
  return outcome;
}

export function restorationResourceClaim(plan,index,resourceId,createdAt){
  validateRestorationResourcePlan(plan);restorationDatabaseName(plan.ownerId,index);
  if(!uuid.test(resourceId)||plan.forbiddenDatabaseIds.includes(resourceId)||typeof createdAt!=='string'||!Number.isFinite(Date.parse(createdAt))||new Date(createdAt).toISOString()!==createdAt)throw Error('Invalid resource claim');
  return{singleton:1,owner_id:plan.ownerId,resource_plan_sha256:null,resource_id:resourceId,partition_index:index,created_at:createdAt};
}
export const restorationResourceClaimObjects=[
  {name:'beedle_restore_resource_claim',type:'table',sql:`CREATE TABLE beedle_restore_resource_claim(singleton INTEGER PRIMARY KEY CHECK(singleton=1),owner_id TEXT NOT NULL,resource_plan_sha256 TEXT NOT NULL,resource_id TEXT NOT NULL,partition_index INTEGER NOT NULL CHECK(partition_index BETWEEN 0 AND 31),created_at TEXT NOT NULL)`},
  {name:'beedle_restore_resource_claim_update',type:'trigger',sql:`CREATE TRIGGER beedle_restore_resource_claim_update BEFORE UPDATE ON beedle_restore_resource_claim BEGIN SELECT RAISE(ABORT,'restoration_resource_owner_immutable'); END`},
  {name:'beedle_restore_resource_claim_delete',type:'trigger',sql:`CREATE TRIGGER beedle_restore_resource_claim_delete BEFORE DELETE ON beedle_restore_resource_claim BEGIN SELECT RAISE(ABORT,'restoration_resource_owner_immutable'); END`}
];
const normalizedSql=sql=>String(sql).replace(/\s+/g,' ').trim().replace(/;$/,'');
async function inspectClaim(plan,index,resourceId,intent,cloud){
  validateResult(plan,index,await cloud.getDatabase(resourceId));
  const objects=restorationApplicationObjects(await cloud.listApplicationObjects(resourceId));
  if(objects.length!==restorationResourceClaimObjects.length||restorationResourceClaimObjects.some(want=>!objects.some(row=>row.name===want.name&&row.type===want.type&&normalizedSql(row.sql)===normalizedSql(want.sql))))
    throw Error('Resource claim schema is partial or differs; do not replay its DDL');
  const row=await cloud.readResourceClaim(resourceId);
  if(!row||row.singleton!==1||row.owner_id!==plan.ownerId||row.resource_plan_sha256!==intent.planSha256||row.resource_id!==resourceId||row.partition_index!==index||row.created_at!==intent.startedAt)
    throw Error('Resource claim owner or exact intent differs');
}
export async function claimIsolatedRestorationDatabase({plan,index,resourceId,store,cloud,guard,now=Date.now()}){
  validateRestorationResourcePlan(plan);assertGuard(plan,guard,now);
  if(guard.targetsUnbound!==true)throw Error('New target bindings must be independently proven quiescent');
  const id=`database-${String(index).padStart(2,'0')}-claim`;
  if(await store.exists(id+'-start'))throw Error('An existing resource claim must be observed, never replayed');
  const created=await store.read(`database-${String(index).padStart(2,'0')}-readback`);
  if(created.ownerId!==plan.ownerId||created.uuid!==resourceId||created.planSha256!==await fingerprint(plan))throw Error('The resource create/readback receipt differs');
  validateResult(plan,index,await cloud.getDatabase(resourceId));
  if(restorationApplicationObjects(await cloud.listApplicationObjects(resourceId)).length)throw Error('New target is not empty; a differing owner must never be overwritten');
  const row=restorationResourceClaim(plan,index,resourceId,new Date(now).toISOString());row.resource_plan_sha256=await fingerprint(plan);
  const intent={ownerId:plan.ownerId,planSha256:row.resource_plan_sha256,index,resourceId,startedAt:row.created_at,onlyNewResourceClaim:true};
  // All interpolated values have strict UUID/digest/integer/ISO validation.
  const insert=`INSERT INTO beedle_restore_resource_claim VALUES(1,'${row.owner_id}','${row.resource_plan_sha256}','${resourceId}',${index},'${row.created_at}')`;
  const sql=[restorationResourceClaimObjects[0].sql,insert,...restorationResourceClaimObjects.slice(1).map(x=>x.sql)].join(';\n')+';';
  await store.reserve(id+'-start',{...intent,sqlSha256:await fingerprint(sql)});
  try{const result=await cloud.executeClaimSql(resourceId,sql);if(result.acknowledged!==true)throw Error('Missing D1 acknowledgment');}
  catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:await fingerprint(String(error?.message||error)),noRetryAuthorized:true});throw Error('Resource claim outcome is unacknowledged; observe exact schema/owner before any further write');}
  await inspectClaim(plan,index,resourceId,intent,cloud);
  const outcome={...intent,phase:'owned_empty_target_claim_verified',independentSchemaOwnerReadback:true,applicationMigrationsApplied:0,documentsInserted:0};
  await store.reserve(id+'-verified',outcome);return outcome;
}
export async function observeIsolatedRestorationClaim({plan,index,resourceId,store,cloud,now=Date.now()}){
  validateRestorationResourcePlan(plan);restorationDatabaseName(plan.ownerId,index);
  const id=`database-${String(index).padStart(2,'0')}-claim`,intent=await store.read(id+'-start');
  if(intent.ownerId!==plan.ownerId||intent.planSha256!==await fingerprint(plan)||intent.index!==index||intent.resourceId!==resourceId)throw Error('Resource claim intent differs');
  await inspectClaim(plan,index,resourceId,intent,cloud);
  const outcome={...intent,observedAt:new Date(now).toISOString(),phase:'owned_empty_target_claim_reconciled',claimReissued:false,applicationMigrationsApplied:0};
  await store.reserve(id+'-observed-'+crypto.randomUUID(),outcome);return outcome;
}

function validateBootstrap(plan,bootstrap){
  validateRestorationResourcePlan(plan);
  if(!bootstrap||bootstrap.version!==1||bootstrap.ownerId!==plan.ownerId||bootstrap.through!==18||
    !digest.test(bootstrap.resourcePlanSha256)||!digest.test(bootstrap.configSha256)||!digest.test(bootstrap.expectedSchemaSha256)||
    !Array.isArray(bootstrap.migrations)||bootstrap.migrations.length!==18||bootstrap.migrations.some((row,index)=>
      !new RegExp('^'+String(index+1).padStart(4,'0')+'_[a-z0-9_]+\\.sql$').test(row.name)||!digest.test(row.sha256)))
    throw Error('Incomplete fresh-prefix bootstrap declaration');
}
export async function restorationSchemaFingerprint(objects){
  return fingerprint(restorationApplicationObjects(objects).map(row=>({name:row.name,type:row.type,sql:normalizedSql(row.sql)}))
    .sort((a,b)=>a.name.localeCompare(b.name)));
}
async function inspectBootstrap(plan,bootstrap,index,resourceId,cloud){
  validateResult(plan,index,await cloud.getDatabase(resourceId));
  const claim=await cloud.readResourceClaim(resourceId);
  if(!claim||claim.owner_id!==plan.ownerId||claim.resource_plan_sha256!==await fingerprint(plan)||claim.resource_id!==resourceId||claim.partition_index!==index)
    throw Error('Bootstrap resource owner differs');
  const ledger=await cloud.readMigrationLedger(resourceId);
  if(JSON.stringify(ledger)!==JSON.stringify(bootstrap.migrations.map(row=>row.name)))throw Error('Bootstrap migration prefix is incomplete or differs');
  if(await restorationSchemaFingerprint(await cloud.listApplicationObjects(resourceId))!==bootstrap.expectedSchemaSha256)
    throw Error('Bootstrap schema differs from the frozen native prefix');
}
export async function applyFreshRestorationPrefix({plan,bootstrap,index,resourceId,store,cloud,guard,runMigrations,now=Date.now()}){
  validateBootstrap(plan,bootstrap);restorationDatabaseName(plan.ownerId,index);assertGuard(plan,guard,now);
  if(bootstrap.resourcePlanSha256!==await fingerprint(plan)||guard.targetsUnbound!==true)throw Error('Fresh-prefix ownership/quiescence declaration differs');
  const id=`database-${String(index).padStart(2,'0')}-migrations-18`;
  if(await store.exists(id+'-start'))throw Error('Existing migration execution must be reconciled, never blindly reapplied');
  const claimIntent=await store.read(`database-${String(index).padStart(2,'0')}-claim-start`);
  await inspectClaim(plan,index,resourceId,claimIntent,cloud);
  if((await cloud.readMigrationLedger(resourceId)).length)throw Error('The new target already has application migrations');
  const intent={ownerId:plan.ownerId,resourceId,index,bootstrapSha256:await fingerprint(bootstrap),startedAt:new Date(now).toISOString(),
    migrations:bootstrap.migrations.map(row=>row.name),newTargetOnly:true};
  await store.reserve(id+'-start',intent);
  let result;
  try{result=await runMigrations({index,resourceId,configSha256:bootstrap.configSha256});}
  catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:await fingerprint(String(error?.message||error)),noRetryAuthorized:true});throw Error('Migration execution is unacknowledged; inspect its exact owner/ledger/schema before another action');}
  await store.reserve(id+'-cli-outcome',{...intent,exitCode:result.exitCode,logSha256:result.logSha256??null});
  if(result.exitCode!==0)throw Error('Migration command did not acknowledge success; preserve and reconcile, never blindly reapply');
  await inspectBootstrap(plan,bootstrap,index,resourceId,cloud);
  const outcome={...intent,phase:'fresh_prefix_18_applied_verified',applicationMigrationsApplied:18,catalogSeeded:false,documentsInserted:0};
  await store.reserve(id+'-verified',outcome);return outcome;
}
export async function observeFreshRestorationPrefix({plan,bootstrap,index,resourceId,store,cloud,now=Date.now()}){
  validateBootstrap(plan,bootstrap);restorationDatabaseName(plan.ownerId,index);
  const id=`database-${String(index).padStart(2,'0')}-migrations-18`,intent=await store.read(id+'-start');
  if(intent.ownerId!==plan.ownerId||intent.resourceId!==resourceId||intent.bootstrapSha256!==await fingerprint(bootstrap))throw Error('Migration execution intent differs');
  await inspectBootstrap(plan,bootstrap,index,resourceId,cloud);
  const outcome={...intent,phase:'fresh_prefix_18_reconciled',observedAt:new Date(now).toISOString(),migrationCommandReissued:false};
  await store.reserve(id+'-observed-'+crypto.randomUUID(),outcome);return outcome;
}
