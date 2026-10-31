import test from'node:test';import assert from'node:assert/strict';import{readFileSync}from'node:fs';
import{memoryD1}from'./helpers/d1-memory.mjs';import{loadTs}from'./helpers/load-ts.mjs';
import{restorationResourceClaimObjects}from'../scripts/lib/owned-restoration-resources.mjs';
const{corpusRestorationSql}=loadTs('src/services/corpus-restoration-schema.ts');
const{restorationTickSql,inspectRestorationOwner,claimRestorationTick,claimRestorationJob,completeRestorationTick,holdRestorationTick,holdRestorationPreflight,observeRestorationTick}=loadTs('src/services/restoration-job-control.ts');
const contract=JSON.parse(readFileSync(new URL('../release/contract.json',import.meta.url),'utf8'));
function setup(){const{DB,sqlite}=memoryD1(),identity={ownerId:'11111111-1111-4111-8111-111111111111',planSha256:'b'.repeat(64),resourcePlanSha256:'a'.repeat(64),resourceId:'99999999-9999-4999-8999-999999999999',partitionIndex:7,sourceManifestSha256:'c'.repeat(64),catalogSha256:'d'.repeat(64)};
 for(const x of restorationResourceClaimObjects)sqlite.exec(x.sql);for(const sql of[...corpusRestorationSql,...restorationTickSql])sqlite.exec(sql);
 sqlite.prepare('INSERT INTO beedle_restore_resource_claim VALUES(1,?,?,?,?,?)').run(identity.ownerId,identity.resourcePlanSha256,identity.resourceId,7,'now');
 sqlite.prepare('INSERT INTO beedle_restore_owner VALUES(1,?,?,?,?,?,32,1,1,?,?,?,?)').run(identity.ownerId,identity.planSha256,identity.sourceManifestSha256,identity.catalogSha256,7,'building','now','now',null);
 sqlite.prepare("INSERT INTO beedle_restore_tick VALUES(1,?,?,'idle',NULL,NULL,'now')").run(identity.ownerId,identity.planSha256);
 sqlite.exec('CREATE TABLE d1_migrations(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE)');for(const name of contract.requiredMigrations)sqlite.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(name);
 sqlite.exec("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES('fixture','decision_docx','Test','Fixture','F-1','synthetic.md','https://fixture.invalid','now','now')");
 const job={documentId:'fixture',sourceSha256:'e'.repeat(64),sourceBytes:100,originalKey:'synthetic.md',planObjectKey:'plan/fixture',extractionPolicy:'verified-source-restoration-v1'};
 sqlite.prepare("INSERT INTO beedle_restore_jobs(document_id,source_sha256,source_bytes,original_key,plan_object_key,extraction_policy,phase) VALUES(?,?,?,?,?,?,'planned')").run(job.documentId,job.sourceSha256,job.sourceBytes,job.originalKey,job.planObjectKey,job.extractionPolicy);
 return{DB,sqlite,identity,job};}
test('two scheduled deliveries obtain only one durable tick and one immutable source-job claim',async()=>{const x=setup();try{
 const ticks=await Promise.all([claimRestorationTick(x.DB,x.identity),claimRestorationTick(x.DB,x.identity)]),tick=ticks.find(Boolean);assert.equal(ticks.filter(Boolean).length,1);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_tick_history').get().n,1);
 const claims=await Promise.all([claimRestorationJob(x.DB,x.identity,tick.token,x.job),claimRestorationJob(x.DB,x.identity,tick.token,x.job)]);assert.equal(claims.filter(Boolean).length,1);assert.throws(()=>x.sqlite.exec('UPDATE beedle_restore_jobs SET claim_token=NULL'),/immutable/);assert.equal((await claimRestorationJob(x.DB,x.identity,tick.token,x.job)),null);
 }finally{x.sqlite.close();}});
test('tick completion refuses an in-flight job and completed history cannot be reset or deleted',async()=>{const x=setup();try{
 const tick=await claimRestorationTick(x.DB,x.identity);await claimRestorationJob(x.DB,x.identity,tick.token,x.job);await assert.rejects(completeRestorationTick(x.DB,x.identity,tick.token,'building',1),/unconfirmed/);
 assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_tick').get().state,'running');
 x.sqlite.exec("UPDATE beedle_restore_jobs SET phase='processed',source_verified=1");await completeRestorationTick(x.DB,x.identity,tick.token,'building',1);assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_tick').get().state,'idle');
 assert.throws(()=>x.sqlite.exec("UPDATE beedle_restore_tick_history SET state='running'"),/immutable/);assert.throws(()=>x.sqlite.exec('DELETE FROM beedle_restore_tick_history'),/immutable/);
 }finally{x.sqlite.close();}});
test('an older tick failure cannot hold a later healthy delivery',async()=>{const x=setup();try{
 const first=await claimRestorationTick(x.DB,x.identity);await completeRestorationTick(x.DB,x.identity,first.token,'building',0);const second=await claimRestorationTick(x.DB,x.identity);assert(second);
 await holdRestorationTick(x.DB,x.identity,first.token,'f'.repeat(64));assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'building');assert.equal(x.sqlite.prepare('SELECT token FROM beedle_restore_tick').get().token,second.token);
 await holdRestorationTick(x.DB,x.identity,second.token,'f'.repeat(64));assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'held');assert.equal(await claimRestorationTick(x.DB,x.identity),null);
 }finally{x.sqlite.close();}});
test('unresolved work never becomes eligible again merely because a tick clock is old',async()=>{const x=setup();try{
 const tick=await claimRestorationTick(x.DB,x.identity);await claimRestorationJob(x.DB,x.identity,tick.token,x.job);
 // A broken external pointer must not grant another document attempt.
 x.sqlite.exec("UPDATE beedle_restore_tick SET state='idle',token=NULL,started_at='2000-01-01'");await assert.rejects(claimRestorationTick(x.DB,x.identity),/unresolved_prior_job/);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_tick_history').get().n,1);
 }finally{x.sqlite.close();}});
test('foreign resource/source identities, incomplete ledgers and human approvals refuse all work',async()=>{for(const change of[x=>x.identity.resourceId='88888888-8888-4888-8888-888888888888',x=>x.identity.sourceManifestSha256='f'.repeat(64),x=>x.sqlite.exec('DELETE FROM d1_migrations WHERE id=33'),x=>x.sqlite.exec("UPDATE documents SET approved_at='now',approved_revision=document_revision")]){const x=setup();try{change(x);await assert.rejects(claimRestorationTick(x.DB,x.identity));assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_tick_history').get().n,0);}finally{x.sqlite.close();}}});
test('wrong source plan and foreign tick cannot claim a source job',async()=>{const x=setup();try{
 const tick=await claimRestorationTick(x.DB,x.identity);await assert.rejects(claimRestorationJob(x.DB,x.identity,tick.token,{...x.job,sourceSha256:'f'.repeat(64)}),/plan_changed/);assert.equal(await claimRestorationJob(x.DB,x.identity,crypto.randomUUID(),x.job),null);assert.equal(x.sqlite.prepare('SELECT claim_token FROM beedle_restore_jobs').get().claim_token,null);
 }finally{x.sqlite.close();}});
test('later index/proof stages require every source outcome to be verified',async()=>{const x=setup();try{
 x.sqlite.exec("UPDATE beedle_restore_owner SET phase='indexing'");await assert.rejects(inspectRestorationOwner(x.DB,x.identity),/sources_incomplete/);
 x.sqlite.exec("UPDATE beedle_restore_jobs SET phase='held',source_verified=1");const tick=await claimRestorationTick(x.DB,x.identity);assert.equal(tick.stage,'indexing');await assert.rejects(claimRestorationJob(x.DB,x.identity,tick.token,x.job),/source_build_phase_changed/);
 }finally{x.sqlite.close();}});
test('missing claim/release acknowledgments are observed from exact durable history, never reclaimed',async()=>{const x=setup();try{
 const batch=x.DB.batch;x.DB.batch=async statements=>{await batch(statements);return[];};await assert.rejects(claimRestorationTick(x.DB,x.identity),/unacknowledged/);
 x.DB.batch=batch;const token=x.sqlite.prepare('SELECT token FROM beedle_restore_tick').get().token;assert.equal((await observeRestorationTick(x.DB,x.identity,token)).state,'running');assert.equal(await claimRestorationTick(x.DB,x.identity),null);
 x.DB.batch=async statements=>{await batch(statements);throw Error('Lost release response');};await assert.rejects(completeRestorationTick(x.DB,x.identity,token,'building',0),/Lost release/);x.DB.batch=batch;
 assert.equal((await observeRestorationTick(x.DB,x.identity,token)).state,'completed');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_tick_history').get().n,1);
 }finally{x.sqlite.close();}});

test('a failed preflight holds only the exact idle owner and cannot interrupt a healthy tick or a foreign resource',async()=>{for(const kind of['idle','busy','foreign']){const x=setup();try{if(kind==='busy')await claimRestorationTick(x.DB,x.identity);if(kind==='foreign')x.identity.resourceId='88888888-8888-4888-8888-888888888888';const result=await holdRestorationPreflight(x.DB,x.identity,'f'.repeat(64));assert.equal(result.held,kind==='idle');assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,kind==='idle'?'held':'building');if(kind==='idle')assert.equal(await claimRestorationTick(x.DB,x.identity),null);}finally{x.sqlite.close();}}});
