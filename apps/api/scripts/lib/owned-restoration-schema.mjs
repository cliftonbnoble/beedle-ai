import {fingerprint} from './release-contract.mjs';
import {validateRestorationResourcePlan,restorationDatabaseName,restorationSchemaFingerprint} from './owned-restoration-resources.mjs';
import {restorationCatalogDigest} from './owned-restoration-catalog.mjs';
const digest=/^[a-f0-9]{64}$/;
function validate(plan,prefix,seed,completion){
  validateRestorationResourcePlan(plan);
  if(prefix?.through!==18||prefix.migrations?.length!==18||completion?.version!==1||completion.ownerId!==plan.ownerId||completion.through!==33||
    !digest.test(completion.resourcePlanSha256)||!digest.test(completion.prefixBootstrapSha256)||!digest.test(completion.seedPlanSha256)||
    !digest.test(completion.configSha256)||!digest.test(completion.expectedSchemaSha256)||completion.catalogSha256!==plan.catalogSha256||
    !Array.isArray(completion.migrations)||completion.migrations.length!==33||completion.migrations.some((row,index)=>
      !new RegExp('^'+String(index+1).padStart(4,'0')+'_[a-z0-9_]+\\.sql$').test(row.name)||!digest.test(row.sha256))||
    JSON.stringify(completion.migrations.slice(0,18))!==JSON.stringify(prefix.migrations)||seed.catalogSha256!==plan.catalogSha256)
    throw Error('Incomplete owned schema-completion declaration');
}
function guardPassed(plan,guard,now){
  const age=now-Date.parse(guard?.observedAt);
  if(!guard||guard.ownerId!==plan.ownerId||guard.passed!==true||guard.sourceFrozen!==true||guard.candidateUnchanged!==true||
    guard.productionPaused!==true||guard.originalAnchorMatched!==true||guard.targetsUnbound!==true||guard.localReserveBytes<20*1024**3||
    !Number.isFinite(age)||age<0||age>300_000)throw Error('Fresh owned schema-completion guards required');
}
async function inspect({plan,prefix,seed,completion,index,resourceId,cloud,final}){
  const metadata=await cloud.getDatabase(resourceId),claim=await cloud.readResourceClaim(resourceId);
  if(!metadata||metadata.uuid!==resourceId||metadata.name!==restorationDatabaseName(plan.ownerId,index)||plan.forbiddenDatabaseIds.includes(resourceId)||
    !claim||claim.owner_id!==plan.ownerId||claim.resource_id!==resourceId||claim.partition_index!==index||claim.resource_plan_sha256!==await fingerprint(plan))
    throw Error('Schema-completion resource owner differs');
  if(completion.resourcePlanSha256!==await fingerprint(plan)||completion.prefixBootstrapSha256!==await fingerprint(prefix)||completion.seedPlanSha256!==await fingerprint(seed))
    throw Error('Schema-completion plan anchor differs');
  if(JSON.stringify(await cloud.readMigrationLedger(resourceId))!==JSON.stringify((final?completion:prefix).migrations.map(row=>row.name))||
    await restorationSchemaFingerprint(await cloud.listApplicationObjects(resourceId))!==(final?completion:prefix).expectedSchemaSha256)
    throw Error('Schema-completion ledger or exact schema differs');
  if(restorationCatalogDigest(await cloud.readCatalogSnapshot(resourceId))!==plan.catalogSha256)throw Error('Preserved catalog differs');
  if(await cloud.readDocumentCount(resourceId)!==0)throw Error('Schema completion requires the still-empty owned corpus');
  if(final){
    const snapshot=await cloud.readLegacySnapshot(resourceId);
    if(snapshot.rows!==seed.rows||snapshot.policy!=='unknown'||snapshot.documents!==0)throw Error('Immutable legacy snapshot, legal uncertainty or empty corpus differs');
  }
}
export async function completeOwnedRestorationSchema(args){
  const{plan,prefix,seed,completion,index,resourceId,store,runMigrations,guard,now=Date.now()}=args;
  validate(plan,prefix,seed,completion);guardPassed(plan,guard,now);
  const id=`database-${String(index).padStart(2,'0')}-migrations-33`;
  if(await store.exists(id+'-start'))throw Error('Existing final migration attempt requires observation, never replay');
  const copied=await store.read(`database-${String(index).padStart(2,'0')}-catalog-seed-verified`);
  const copiedPositive=copied.phase==='owned_catalog_seed_verified'&&copied.catalogSha256===plan.catalogSha256||copied.phase==='owned_catalog_seed_reconciled'&&copied.seedCommandReissued===false;
  if(!copiedPositive||copied.ownerId!==plan.ownerId||copied.index!==index||copied.resourceId!==resourceId||copied.seedPlanSha256!==await fingerprint(seed))
    throw Error('Acknowledged catalog seed receipt differs');
  await inspect({...args,final:false});
  const intent={ownerId:plan.ownerId,index,resourceId,completionPlanSha256:await fingerprint(completion),startedAt:new Date(now).toISOString(),onlyCanonical19Through33:true};
  await store.reserve(id+'-start',intent);let result;
  try{result=await runMigrations();}catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:await fingerprint(String(error?.message||error)),noRetryAuthorized:true});throw Error('Final migration outcome requires independent observation');}
  await store.reserve(id+'-cli-outcome',{...intent,exitCode:result.exitCode,logSha256:result.logSha256??null});
  if(result.exitCode!==0)throw Error('Final migration command did not acknowledge success; do not replay');
  await inspect({...args,final:true});
  const outcome={...intent,phase:'owned_application_schema_33_verified',applicationMigrations:33,copiedCatalogRows:seed.rows,immutableLegacySnapshotVerified:true,documentsInserted:0};
  await store.reserve(id+'-verified',outcome);return outcome;
}
export async function observeOwnedRestorationSchemaCompletion(args){
  const{plan,prefix,seed,completion,index,resourceId,store,now=Date.now()}=args;validate(plan,prefix,seed,completion);
  const id=`database-${String(index).padStart(2,'0')}-migrations-33`,intent=await store.read(id+'-start');
  if(intent.ownerId!==plan.ownerId||intent.resourceId!==resourceId||intent.index!==index||intent.completionPlanSha256!==await fingerprint(completion))throw Error('Final migration intent differs');
  await inspect({...args,final:true});
  const outcome={...intent,phase:'owned_application_schema_33_reconciled',observedAt:new Date(now).toISOString(),migrationCommandReissued:false};
  await store.reserve(id+'-observed-'+crypto.randomUUID(),outcome);return outcome;
}
