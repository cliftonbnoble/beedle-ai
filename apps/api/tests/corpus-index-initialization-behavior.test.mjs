import test from'node:test';import assert from'node:assert/strict';import{readFileSync}from'node:fs';
import{memoryD1}from'./helpers/d1-memory.mjs';import{loadTs}from'./helpers/load-ts.mjs';
import{restorationResourceClaimObjects,restorationSchemaFingerprint}from'../scripts/lib/owned-restoration-resources.mjs';
const{corpusIndexInitializationSql,advanceOwnedIndexInitialization,inspectOwnedIndexInitialization}=loadTs('src/services/corpus-index-initialization.ts');
const{advanceIndexInitializationTick,parseIndexInitializationSpec,default:initializerWorker}=loadTs('src/corpus-index-initializer.ts');
const{startIndexRecovery,indexRecoveryStatus,advanceIndexRecovery}=loadTs('src/services/search-index-recovery.ts');
const{advanceFacetBuild,facetBuildStatus}=loadTs('src/services/search-facet-build.ts');const{advanceReviewerQueue,inspectReviewerQueue}=loadTs('src/services/reviewer-queue-index.ts');const{buildReviewerRows}=loadTs('src/services/reviewer-read-model.ts');const{inspectSearchReadiness}=loadTs('src/services/search-readiness.ts');
const contract=JSON.parse(readFileSync(new URL('../release/contract.json',import.meta.url),'utf8'));
function fixture(){const{DB,sqlite}=memoryD1({finalizeEmptyPhraseIndex:false});
 for(const x of restorationResourceClaimObjects)sqlite.exec(x.sql);for(const sql of corpusIndexInitializationSql)sqlite.exec(sql);
 const spec={ownerId:'11111111-1111-4111-8111-111111111111',resourcePlanSha256:'a'.repeat(64),resourceId:'99999999-9999-4999-8999-999999999999',partitionIndex:0,generation:'22222222-2222-4222-8222-222222222222',planSha256:'b'.repeat(64),catalogSha256:'c'.repeat(64),catalogRows:0};
 sqlite.prepare('INSERT INTO beedle_restore_resource_claim VALUES(1,?,?,?,?,?)').run(spec.ownerId,spec.resourcePlanSha256,spec.resourceId,0,'now');
 sqlite.prepare("INSERT INTO beedle_restore_index_init(singleton,owner_id,resource_plan_sha256,resource_id,partition_index,generation,plan_sha256,catalog_sha256,catalog_rows,state,updated_at) VALUES(1,?,?,?,?,?,?,?,0,'planned','now')").run(spec.ownerId,spec.resourcePlanSha256,spec.resourceId,0,spec.generation,spec.planSha256,spec.catalogSha256);
 sqlite.prepare("INSERT INTO beedle_restore_index_init_tick VALUES(1,?,?,'idle',NULL,'now')").run(spec.ownerId,spec.planSha256);
 sqlite.exec('CREATE TABLE d1_migrations(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE)');for(const name of contract.requiredMigrations)sqlite.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(name);
 return{DB,sqlite,spec};}
const objects=sqlite=>sqlite.prepare("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all();
// Discover expected transitional schemas ONLY by running frozen production
// maintenance functions on disposable native data, never from a remote target.
async function blueprint(){const x=fixture(),set=new Set();try{
 for(let i=0;i<64;i++){
  set.add(await restorationSchemaFingerprint(objects(x.sqlite)));const env={DB:x.DB},state=await indexRecoveryStatus(env);
  if(state.generation==='legacy')await startIndexRecovery(env,{dryRun:false,generation:x.spec.generation,expectedGeneration:'legacy',replaceArchive:true,restart:false});
  else if(state.status!=='completed')await advanceIndexRecovery(env,x.spec.generation);
  else if((await facetBuildStatus(env)).phase!=='ready')await advanceFacetBuild(env);
  else if(!(await inspectReviewerQueue(env)).ready)await advanceReviewerQueue(env,buildReviewerRows);
  else{assert.equal((await inspectSearchReadiness(env)).ready,true);return{allowedSchemaSha256:[...set],finalSchemaSha256:await restorationSchemaFingerprint(objects(x.sqlite))};}
 }
 throw Error('Native initializer did not finish');
 }finally{x.sqlite.close();}}
const frozenBlueprint=await blueprint();
test('owned empty initialization reaches current posting/facet/reviewer policy without a fake recovery receipt or case rows',async()=>{const x=fixture();try{
 const spec={...x.spec,...frozenBlueprint};let result;for(let i=0;i<64;i++){result=await advanceOwnedIndexInitialization(x.DB,spec);if(result.state==='completed')break;}
 assert.equal(result.state,'completed');assert.equal((await inspectSearchReadiness({DB:x.DB})).ready,true);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents').get().n,0);assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='beedle_recovery_receipt'").get().n,0);assert.equal((await indexRecoveryStatus({DB:x.DB})).generation,spec.generation);
 assert.equal((await inspectOwnedIndexInitialization(x.DB,spec)).schemaSha256,spec.finalSchemaSha256);assert.equal((await advanceOwnedIndexInitialization(x.DB,spec)).deliveries,result.deliveries);assert.throws(()=>x.sqlite.exec("UPDATE beedle_restore_index_init SET state='running'"),/immutable/);assert.throws(()=>x.sqlite.exec('DELETE FROM beedle_restore_index_init'),/immutable/);
 }finally{x.sqlite.close();}});
test('different owner, schema, physical claim, ledger or nonempty corpus holds before any recovery start',async()=>{for(const change of[x=>x.spec.resourceId='88888888-8888-4888-8888-888888888888',x=>x.sqlite.exec('CREATE TABLE foreign_writer(id TEXT)'),x=>x.sqlite.exec('DELETE FROM d1_migrations WHERE id=33'),x=>x.sqlite.exec("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES('fixture','decision_docx','Test','Fixture','F-1','fixture.md','https://fixture.invalid','now','now')")]){const x=fixture();try{change(x);await assert.rejects(advanceOwnedIndexInitialization(x.DB,{...x.spec,...frozenBlueprint}));assert.equal((await indexRecoveryStatus({DB:x.DB})).generation,'legacy');assert.equal(x.sqlite.prepare('SELECT deliveries FROM beedle_restore_index_init').get().deliveries,0);}finally{x.sqlite.close();}}});
test('existing leases and prior coverage errors cannot be expired, cleared or silently advanced',async()=>{for(const kind of['lease','error']){const x=fixture();try{
 if(kind==='lease')x.sqlite.exec("UPDATE search_index_rebuild SET owner_token='other-owner',lease_expires_at='2000-01-01'");else x.sqlite.exec("UPDATE search_index_rebuild SET last_error='coverage_verification_failed'");
 await assert.rejects(advanceOwnedIndexInitialization(x.DB,{...x.spec,...frozenBlueprint}),/checkpoint_owner|prior_checkpoint_error/);
 const state=await indexRecoveryStatus({DB:x.DB});assert.equal(state.generation,'legacy');if(kind==='lease')assert.equal(state.owner_token,'other-owner');else assert.equal(state.last_error,'coverage_verification_failed');
 }finally{x.sqlite.close();}}});
test('a new generation differing from the reviewed plan is held rather than adopted as a fresh baseline',async()=>{const x=fixture();try{
 x.sqlite.prepare("UPDATE search_index_rebuild SET generation=?").run('33333333-3333-4333-8333-333333333333');await assert.rejects(advanceOwnedIndexInitialization(x.DB,{...x.spec,...frozenBlueprint}),/generation_changed/);assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_index_init').get().state,'held');assert(x.sqlite.prepare('SELECT error_sha256 FROM beedle_restore_index_init').get().error_sha256);
 }finally{x.sqlite.close();}});
test('a committed start with lost acknowledgment is reconciled by the supported generation transaction, without another start',async()=>{const x=fixture();try{
 const batch=x.DB.batch;let starts=0;x.DB.batch=async statements=>{const result=await batch(statements);if(statements.some(s=>s.sql.includes('INSERT INTO search_index_start_guards'))){starts++;throw Error('Lost start response');}return result;};
 const result=await advanceOwnedIndexInitialization(x.DB,{...x.spec,...frozenBlueprint});assert.equal(result.state,'running');assert.equal(starts,1);assert.equal((await indexRecoveryStatus({DB:x.DB})).generation,x.spec.generation);x.DB.batch=batch;
 }finally{x.sqlite.close();}});
test('disabled and malformed initializer configurations cannot reach the database; HTTP stays503',async()=>{
 assert.deepEqual(await advanceIndexInitializationTick({RESTORE_ENABLED:'false',get DB(){throw Error('No access permitted');}}),{state:'disabled',deliveries:0});assert.equal(initializerWorker.fetch().status,503);
 for(const text of['null','{}',JSON.stringify({extra:'not-allowed'}),'x'.repeat(16385)])assert.throws(()=>parseIndexInitializationSpec(text));
});
test('the scheduled-only initializer finishes within its bounded tick and cannot replay completed work',async()=>{const x=fixture();try{
 const env={DB:x.DB,RESTORE_ENABLED:'true',RESTORE_SPEC_JSON:JSON.stringify({...x.spec,...frozenBlueprint})};const result=await advanceIndexInitializationTick(env);assert.equal(result.state,'completed');assert(result.deliveries<=20);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_index_init_history').get().n,1);assert.deepEqual(await advanceIndexInitializationTick(env),{state:'completed',deliveries:0});
 x.sqlite.prepare('UPDATE search_index_rebuild SET generation=?').run('33333333-3333-4333-8333-333333333333');await assert.rejects(advanceIndexInitializationTick(env),/completion_changed/);
 }finally{x.sqlite.close();}});
test('a duplicate scheduled delivery respects the existing tick before inspecting its live builder lease',async()=>{const x=fixture();try{
 x.sqlite.exec("UPDATE beedle_restore_index_init_tick SET state='running',token='existing-owned-tick'; UPDATE search_index_rebuild SET owner_token='existing-index-owner',lease_expires_at='2099-01-01'");
 const env={DB:x.DB,RESTORE_ENABLED:'true',RESTORE_SPEC_JSON:JSON.stringify({...x.spec,...frozenBlueprint})};assert.deepEqual(await advanceIndexInitializationTick(env),{state:'busy',deliveries:0});assert.equal(x.sqlite.prepare('SELECT token FROM beedle_restore_index_init_tick').get().token,'existing-owned-tick');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_index_init_history').get().n,0);
 }finally{x.sqlite.close();}});
test('lost final tick acknowledgment preserves an immutable error and independently acknowledged completion',async()=>{const x=fixture();try{
 const batch=x.DB.batch;let releases=0;x.DB.batch=async statements=>{const result=await batch(statements);if(statements.some(s=>s.sql.startsWith("UPDATE beedle_restore_index_init_history SET state='completed'"))){releases++;throw Error('Lost final root acknowledgment');}return result;};
 const env={DB:x.DB,RESTORE_ENABLED:'true',RESTORE_SPEC_JSON:JSON.stringify({...x.spec,...frozenBlueprint})};await assert.rejects(advanceIndexInitializationTick(env),/Lost final/);x.DB.batch=batch;assert.equal(releases,1);assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_index_init_tick').get().state,'completed');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_index_init_errors').get().n,1);assert.deepEqual(await advanceIndexInitializationTick(env),{state:'completed',deliveries:0});assert.throws(()=>x.sqlite.exec('DELETE FROM beedle_restore_index_init_errors'),/immutable/);
 }finally{x.sqlite.close();}});
