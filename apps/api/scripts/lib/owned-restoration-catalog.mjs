import {createHash} from 'node:crypto';
import {fingerprint} from './release-contract.mjs';
import {validateRestorationResourcePlan,restorationDatabaseName,restorationSchemaFingerprint} from './owned-restoration-resources.mjs';

export const restorationCatalogTables=Object.freeze(['legal_index_codes','legal_ordinance_sections','legal_rules_sections','legal_reference_crosswalk','legal_reference_sources']);
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const digest=/^[a-f0-9]{64}$/;
const identifier=/^[a-z][a-z0-9_]*$/;
function literal(value){
  if(value===null)return 'NULL';
  if(typeof value==='number'&&Number.isSafeInteger(value))return String(value);
  if(typeof value==='string'&&Buffer.from(value,'utf8').toString('utf8')===value)
    return `CAST(X'${Buffer.from(value,'utf8').toString('hex')}' AS TEXT)`;
  throw Error('Catalog values require exact UTF-8 text, safe integers or null');
}
// This preserves the original column order, row order and values. Text is a
// typed UTF-8 hex literal: quotes, NULs and newlines cannot become SQL syntax.
export function restorationCatalogDigest(snapshot){
  if(!Array.isArray(snapshot)||snapshot.length!==5)throw Error('Complete five-table catalog required');
  if(snapshot.reduce((n,entry)=>n+(Array.isArray(entry?.rows)?entry.rows.length:0),0)>5000||Buffer.byteLength(JSON.stringify(snapshot),'utf8')>4*1024*1024)
    throw Error('Catalog exceeds the reviewed input bound');
  for(const [index,entry] of snapshot.entries()){
    if(entry.table!==restorationCatalogTables[index]||!Array.isArray(entry.rows)||!entry.rows.length)
      throw Error('Catalog table order or coverage differs');
    const columns=Object.keys(entry.rows[0]);
    if(!columns.length||columns.some(column=>!identifier.test(column)))throw Error('Invalid catalog columns');
    for(const row of entry.rows){
      if(JSON.stringify(Object.keys(row))!==JSON.stringify(columns))throw Error('Catalog row columns differ');
      for(const value of Object.values(row))literal(value);
    }
  }
  return sha(JSON.stringify(snapshot));
}
export async function compileOwnedRestorationCatalog({plan,index,resourceId,snapshot}){
  validateRestorationResourcePlan(plan);restorationDatabaseName(plan.ownerId,index);
  if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(resourceId)||plan.forbiddenDatabaseIds.includes(resourceId))
    throw Error('Protected or invalid catalog target');
  const catalogSha256=restorationCatalogDigest(snapshot);
  if(catalogSha256!==plan.catalogSha256)throw Error('Catalog differs from the sealed resource plan');
  const resourcePlanSha256=await fingerprint(plan);
  const condition=`EXISTS(SELECT 1 FROM beedle_restore_resource_claim WHERE singleton=1 AND owner_id='${plan.ownerId}' AND resource_plan_sha256='${resourcePlanSha256}' AND resource_id='${resourceId}' AND partition_index=${index})`;
  const statements=snapshot.flatMap(({table,rows})=>rows.map(row=>{
    const columns=Object.keys(row),sql=`INSERT INTO ${table}(${columns.join(',')}) SELECT ${columns.map(column=>literal(row[column])).join(',')} WHERE ${condition};`;
    if(Buffer.byteLength(sql,'utf8')>=100_000)throw Error('Catalog statement exceeds the D1 SQL bound');
    return sql;
  }));
  return{sql:statements.join('\n')+'\n',catalogSha256,rows:statements.length,
    tableCounts:snapshot.map(({table,rows})=>({table,rows:rows.length})),maxStatementBytes:Math.max(...statements.map(sql=>Buffer.byteLength(sql,'utf8')))};
}
function declaration(plan,bootstrap,seed){
  validateRestorationResourcePlan(plan);
  if(bootstrap?.through!==18||bootstrap.migrations?.length!==18||seed?.version!==1||seed.ownerId!==plan.ownerId||seed.catalogSha256!==plan.catalogSha256||
    !digest.test(seed.resourcePlanSha256)||!digest.test(seed.prefixBootstrapSha256)||!digest.test(seed.expectedSchemaSha256)||
    seed.expectedSchemaSha256!==bootstrap.expectedSchemaSha256||!Number.isSafeInteger(seed.rows)||seed.rows<5||
    !Array.isArray(seed.tableCounts)||seed.tableCounts.length!==5||seed.tableCounts.some((row,index)=>
      row.table!==restorationCatalogTables[index]||!Number.isSafeInteger(row.rows)||row.rows<1)||seed.tableCounts.reduce((n,row)=>n+row.rows,0)!==seed.rows)
    throw Error('Incomplete catalog seed declaration');
}
function freshGuard(plan,guard,now){
  const age=now-Date.parse(guard?.observedAt);
  if(!guard||guard.ownerId!==plan.ownerId||guard.passed!==true||guard.sourceFrozen!==true||guard.candidateUnchanged!==true||
    guard.productionPaused!==true||guard.originalAnchorMatched!==true||guard.targetsUnbound!==true||
    guard.localReserveBytes<20*1024**3||!Number.isFinite(age)||age<0||age>300_000)throw Error('Fresh isolated catalog guards required');
}
async function inspect(plan,bootstrap,seed,index,resourceId,cloud){
  const resource=await cloud.getDatabase(resourceId),claim=await cloud.readResourceClaim(resourceId);
  if(!resource||resource.uuid!==resourceId||resource.name!==restorationDatabaseName(plan.ownerId,index)||plan.forbiddenDatabaseIds.includes(resourceId)||
    !claim||claim.owner_id!==plan.ownerId||claim.resource_id!==resourceId||claim.partition_index!==index||claim.resource_plan_sha256!==await fingerprint(plan))
    throw Error('Catalog target owner differs');
  if(seed.resourcePlanSha256!==await fingerprint(plan)||seed.prefixBootstrapSha256!==await fingerprint(bootstrap))throw Error('Catalog plan anchor differs');
  if(JSON.stringify(await cloud.readMigrationLedger(resourceId))!==JSON.stringify(bootstrap.migrations.map(row=>row.name))||
    await restorationSchemaFingerprint(await cloud.listApplicationObjects(resourceId))!==seed.expectedSchemaSha256)
    throw Error('Catalog requires the exact acknowledged migration-18 schema');
}
function sameSnapshot(seed,snapshot){
  if(restorationCatalogDigest(snapshot)!==seed.catalogSha256||JSON.stringify(snapshot.map(({table,rows})=>({table,rows:rows.length})))!==JSON.stringify(seed.tableCounts))
    throw Error('Catalog byte/row readback differs');
}
export async function seedOwnedRestorationCatalog({plan,bootstrap,seed,index,resourceId,store,cloud,guard,runSeed,now=Date.now()}){
  declaration(plan,bootstrap,seed);freshGuard(plan,guard,now);
  const id=`database-${String(index).padStart(2,'0')}-catalog-seed`;
  if(await store.exists(id+'-start'))throw Error('An existing catalog attempt must be reconciled, never replayed');
  await inspect(plan,bootstrap,seed,index,resourceId,cloud);
  const counts=await cloud.readCatalogCounts(resourceId);
  if(JSON.stringify(counts)!==JSON.stringify(restorationCatalogTables.map(table=>({table,rows:0}))))throw Error('Catalog seed target is not empty');
  const intent={ownerId:plan.ownerId,index,resourceId,seedPlanSha256:await fingerprint(seed),startedAt:new Date(now).toISOString()};
  await store.reserve(id+'-start',intent);
  let result;
  try{result=await runSeed();}catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:sha(String(error?.message||error)),noRetryAuthorized:true});throw Error('Unacknowledged catalog seed requires independent readback');}
  await store.reserve(id+'-cli-outcome',{...intent,exitCode:result.exitCode,logSha256:result.logSha256??null});
  if(result.exitCode!==0)throw Error('Catalog seed did not acknowledge success; reconcile without replay');
  await inspect(plan,bootstrap,seed,index,resourceId,cloud);sameSnapshot(seed,await cloud.readCatalogSnapshot(resourceId));
  const outcome={...intent,phase:'owned_catalog_seed_verified',catalogSha256:seed.catalogSha256,rows:seed.rows,applicationMigrations:18,
    sourceCatalogPreserved:true,humanApprovalGranted:false,authoritativeEditionClaimed:false};
  await store.reserve(id+'-verified',outcome);return outcome;
}
export async function observeOwnedRestorationCatalog({plan,bootstrap,seed,index,resourceId,store,cloud,now=Date.now()}){
  declaration(plan,bootstrap,seed);const id=`database-${String(index).padStart(2,'0')}-catalog-seed`,intent=await store.read(id+'-start');
  if(intent.ownerId!==plan.ownerId||intent.index!==index||intent.resourceId!==resourceId||intent.seedPlanSha256!==await fingerprint(seed))throw Error('Catalog intent differs');
  await inspect(plan,bootstrap,seed,index,resourceId,cloud);sameSnapshot(seed,await cloud.readCatalogSnapshot(resourceId));
  const outcome={...intent,phase:'owned_catalog_seed_reconciled',observedAt:new Date(now).toISOString(),seedCommandReissued:false};
  await store.reserve(id+'-observed-'+crypto.randomUUID(),outcome);return outcome;
}
