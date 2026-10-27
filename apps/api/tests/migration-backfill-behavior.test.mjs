import test from 'node:test';
import assert from 'node:assert/strict';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
import {readFileSync} from 'node:fs';
const contract=JSON.parse(readFileSync(new URL('../release/contract.json',import.meta.url),'utf8'));
const {advanceMigrationBackfill,default:worker}=loadTs('src/migration-backfill.ts');
const {inspectSearchReadiness}=loadTs('src/services/search-readiness.ts');
const {inspectReviewerQueue}=loadTs('src/services/reviewer-queue-index.ts');
function fixture(){
 const f=memoryD1({finalizeEmptyPhraseIndex:false});
 const env={DB:f.DB,MIGRATION_ENABLED:'true',MIGRATION_RUN_ID:crypto.randomUUID(),MIGRATION_TARGET_SHA256:'a'.repeat(64),MIGRATION_BUNDLE_SHA256:'b'.repeat(64)};
 f.sqlite.exec('CREATE TABLE beedle_recovery_receipt(singleton INTEGER PRIMARY KEY,run_id TEXT,target_sha256 TEXT,bundle_sha256 TEXT)');
 f.sqlite.prepare('INSERT INTO beedle_recovery_receipt VALUES(1,?,?,?)').run(env.MIGRATION_RUN_ID,env.MIGRATION_TARGET_SHA256,env.MIGRATION_BUNDLE_SHA256);
 f.sqlite.exec('CREATE TABLE d1_migrations(id INTEGER PRIMARY KEY,name TEXT)');
 for(const name of contract.requiredMigrations)f.sqlite.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(name);
 return {...f,env};
}
function seed(sqlite,id){sqlite.prepare("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES(?,'decision_docx','Test','Held case',?,'original.docx','https://example.invalid','now','now')").run(id,id);}
test('disabled, wrong-target and wrong-bundle runners cannot initialize progress or mutate a database',async()=>{
 await assert.rejects(advanceMigrationBackfill({MIGRATION_ENABLED:'false',get DB(){assert.fail('disabled runner touched DB');}}),/disabled/);
 const {DB,sqlite,env}=fixture();try{
  const original=DB.prepare;DB.prepare=sql=>{assert.doesNotMatch(sql,/^\s*(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP)\b/i);return original(sql);};
  await assert.rejects(advanceMigrationBackfill({...env,MIGRATION_TARGET_SHA256:'c'.repeat(64)}),/receipt differs/);
  await assert.rejects(advanceMigrationBackfill({...env,MIGRATION_BUNDLE_SHA256:'c'.repeat(64)}),/receipt differs/);
  assert.equal((await worker.fetch()).status,503);
 }finally{sqlite.close();}
});
test('scheduled maintenance builds real postings/facets/reviewer rows and preserves all publication holds',async()=>{
 const {sqlite,env}=fixture();try{
  seed(sqlite,'held');
  sqlite.exec("INSERT INTO document_sections(id,document_id,canonical_key,heading,section_order) VALUES('s','held','conclusions_of_law','Conclusions',0); INSERT INTO section_paragraphs(id,section_id,anchor,paragraph_order,text) VALUES('p','s','p1',0,'legacyneedle rent'); INSERT INTO document_chunks(id,document_id,section_id,paragraph_id,paragraph_anchor,citation_anchor,section_label,chunk_order,chunk_text,token_estimate,created_at) VALUES('c','held','s','p','p1','L:p1','Conclusions',0,'legacyneedle rent',5,'now');");
  const before=sqlite.prepare('SELECT * FROM documents').get();let state;
  for(let tick=0;tick<10;tick++){state=await advanceMigrationBackfill(env);if(state.state==='indexes_ready')break;}
  assert.equal(state.state,'indexes_ready');assert.equal((await inspectSearchReadiness(env)).ready,true);assert.equal((await inspectReviewerQueue(env)).ready,true);
  assert.equal(sqlite.prepare("SELECT COUNT(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'legacyneedle'").get().n,1);
  assert.deepEqual(sqlite.prepare('SELECT * FROM documents').get(),before,'index maintenance confers no approval, confirmation or source provenance');
  assert.equal((await advanceMigrationBackfill(env)).deliveries,state.deliveries,'completed cron replay performs no extra work');
 }finally{sqlite.close();}
});
test('missing release migrations prevent all writes even with a matching recovery receipt',async()=>{
 const {DB,sqlite,env}=fixture();try{
  sqlite.exec('DELETE FROM d1_migrations WHERE id=(SELECT MAX(id) FROM d1_migrations)');
  const original=DB.prepare;DB.prepare=sql=>{assert.doesNotMatch(sql,/^\s*(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP)\b/i);return original(sql);};
  await assert.rejects(advanceMigrationBackfill(env),/ledger/);
 }finally{sqlite.close();}
});
test('an operator hold during a checkpoint survives both success and failure',async()=>{
 for(const fail of [false,true]){
  const {DB,sqlite,env}=fixture();try{
   seed(sqlite,'held');const batch=DB.batch;let calls=0;
   DB.batch=async statements=>{
    calls++;sqlite.prepare("UPDATE beedle_recovery_backfill_status SET state='held'").run();
    if(fail)throw Error('checkpoint failed after operator hold');
    return batch(statements);
   };
   assert.equal((await advanceMigrationBackfill(env)).state,'held');
   assert.equal(calls,1,'no later checkpoint runs after a hold');
   assert.equal((await advanceMigrationBackfill(env)).state,'held');
  }finally{sqlite.close();}
 }
});
test('one tick is bounded and five consecutive checkpoint failures hold the run for inspection',async()=>{
 const {DB,sqlite,env}=fixture();try{
  for(let i=0;i<250;i++)seed(sqlite,'held-'+String(i).padStart(3,'0'));
  const first=await advanceMigrationBackfill(env);assert.equal(first.deliveries,20);assert.equal(first.state,'running');
  const batch=DB.batch;DB.batch=async()=>{throw Error('injected checkpoint failure');};
  let held;for(let i=0;i<5;i++)held=await advanceMigrationBackfill(env);
  assert.equal(held.state,'held');assert.equal(held.failures,5);
  DB.batch=batch;assert.equal((await advanceMigrationBackfill(env)).state,'held','a successful provider does not silently resume a held run');
 }finally{sqlite.close();}
});
