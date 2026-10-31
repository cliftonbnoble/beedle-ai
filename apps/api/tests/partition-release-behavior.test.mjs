import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {createPartitionFixture} from './helpers/corpus-partitions.mjs';
import {readyReviewerQueue} from './helpers/reviewer-queue.mjs';
import {loadTs} from './helpers/load-ts.mjs';
import {contract,preflight} from '../scripts/lib/release-preflight.mjs';
import {corpusPartitionPolicy,parseCorpusPartitionManifest,partitionProofQuery,partitionIntegrityCommands} from '../scripts/lib/corpus-partition-contract.mjs';
const {corpusRestorationSql}=loadTs('src/services/corpus-restoration-schema.ts');
const {corpusReviewCacheSql}=loadTs('src/services/corpus-review-cache.ts');
const {inspectReleaseReadiness}=loadTs('src/services/release-readiness.ts');
const config=JSON.parse(readFileSync(new URL('../wrangler.json',import.meta.url),'utf8'));
const hash=x=>createHash('sha256').update(x).digest('hex');

async function setup(){
 const f=await createPartitionFixture();
 const manifest=JSON.parse(f.env.CORPUS_PARTITIONS_JSON);
 for(const [index,s] of f.stores.entries()){
  await readyReviewerQueue({DB:s.DB});
  s.sqlite.exec('CREATE TABLE d1_migrations(id INTEGER PRIMARY KEY,name TEXT NOT NULL)');
  for(const name of readdirSync(new URL('../migrations/',import.meta.url)).filter(x=>x.endsWith('.sql')).sort())s.sqlite.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(name);
  for(const sql of corpusRestorationSql)s.sqlite.exec(sql);
  s.sqlite.prepare('INSERT INTO beedle_restore_owner VALUES(1,?,?,?,?,?,32,1,1,?,?,?,?)').run(manifest.restoreOwnerId,manifest.restorePlanSha256,manifest.sourceManifestSha256,manifest.catalogSha256,index,'sealed','now','now',null);
  const commands={metadata:"INSERT INTO search_metadata_substrings(search_metadata_substrings,rank) VALUES('integrity-check',1)",body:"INSERT INTO search_body_substrings(search_body_substrings,rank) VALUES('integrity-check',1)",phrase:"INSERT INTO search_chunks_fts(search_chunks_fts,rank) VALUES('integrity-check',1)",physical:'PRAGMA quick_check',foreign_keys:'PRAGMA foreign_key_check'};
  for(const [kind,sql] of Object.entries(commands)){
   const rows=/^PRAGMA/.test(sql)?s.sqlite.prepare(sql).all():(s.sqlite.exec(sql),[]);
   if(kind==='physical')assert.deepEqual(rows.map(x=>Object.values(x)[0]),['ok']);
   if(kind==='foreign_keys')assert.equal(rows.length,0);
   s.sqlite.prepare('INSERT INTO beedle_restore_integrity VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(kind,manifest.restoreOwnerId,manifest.restorePlanSha256,manifest.partitions[index].revision,crypto.randomUUID(),hash(sql),'passed',1,hash(JSON.stringify(rows)),'now','now');
  }
  s.sqlite.prepare('INSERT INTO beedle_restore_integrity VALUES(?,?,?,?,?,?,?,?,?,?,?)').run('source_sha',manifest.restoreOwnerId,manifest.restorePlanSha256,manifest.partitions[index].revision,crypto.randomUUID(),partitionIntegrityCommands.source_sha.sha256,'passed',1,hash('fixture-original-sha-checked-in-source-tests'),'now','now');
 }
 const sqlite=f.stores[0].sqlite;for(const sql of corpusReviewCacheSql)sqlite.exec(sql);
 for(const s of f.stores)for(const row of s.sqlite.prepare('SELECT * FROM reviewer_queue_state').all()){
  const columns=Object.keys(row);sqlite.prepare(`INSERT INTO beedle_corpus_review_rows(${columns.join(',')}) VALUES(${columns.map(()=>'?').join(',')})`).run(...columns.map(k=>row[k]));
 }
 sqlite.exec('INSERT INTO beedle_corpus_review_groups SELECT group_key,COUNT(*) FROM beedle_corpus_review_rows GROUP BY group_key');
 sqlite.prepare('INSERT INTO beedle_corpus_review_control VALUES(1,?,?,?,?,?,?,?)').run(manifest.partitions[0].revision,contract.reviewerPolicyVersion,'legacy',hash(f.env.CORPUS_PARTITIONS_JSON),hash('synthetic-review-rows'),'ready',32);
 const selected=structuredClone(config);const target=selected.env.preview;target.vars={...target.vars,DEPLOYMENT_READY:'true',CORPUS_PARTITIONS_JSON:f.env.CORPUS_PARTITIONS_JSON,
  CORS_ALLOWED_ORIGINS:'https://preview.example.com',SOURCE_PROXY_BASE_URL:'https://preview-api.example.com'};
 target.d1_databases=manifest.partitions.map((part,index)=>({binding:part.binding,database_name:'synthetic-partition-'+index,database_id:'00000000-0000-4000-8000-'+String(100+index).padStart(12,'0')}));
 delete target.ai;target.vectorize=[];target.queues.consumers=[];target.triggers={crons:[]};
 f.env={...target.vars,...f.env,APP_ENV:'preview',RELEASE_REVISION:'a'.repeat(40),RELEASE_CONFIG_SHA256:'b'.repeat(64)};
 const execute=async(queries,binding='DB')=>{const index=manifest.partitions.findIndex(p=>p.binding===binding);return queries.map(sql=>f.stores[index].sqlite.prepare(sql).all());};
 return{...f,manifest,config:selected,execute};
}

test('runtime and CLI require every complete owned partition and the global cache, without writes',async()=>{
 const f=await setup();try{
  assert.deepEqual(contract.partitionPolicy,corpusPartitionPolicy);
  for(const row of Object.values(partitionIntegrityCommands))assert.equal(hash(row.command),row.sha256);
  const changes=f.stores.map(s=>s.sqlite.prepare('SELECT total_changes() n').get().n);
  const runtime=await inspectReleaseReadiness(f.env);assert.equal(runtime.ready,true,JSON.stringify(runtime));assert.equal(runtime.partitionCount,32);assert.equal(runtime.documents,32);
  const cli=await preflight(f.config,'preview',{execute:f.execute});assert.equal(cli.ready,true);assert.equal(cli.partitionContract,runtime.partitionContract);assert.equal(cli.corpusRevision,runtime.corpusRevision);
  assert.deepEqual(f.stores.map(s=>s.sqlite.prepare('SELECT total_changes() n').get().n),changes);
  f.stores[31].sqlite.exec('DROP TRIGGER documents_substring_insert');
  assert.equal((await inspectReleaseReadiness(f.env)).ready,false);await assert.rejects(preflight(f.config,'preview',{execute:f.execute}),/schema/);
 }finally{f.close();}
});

test('unknown owner, missing acknowledgment, partial count and any unsealed partition refuse promotion',async()=>{
 const f=await setup();try{
  for(const change of [row=>({...row,owner_id:crypto.randomUUID()}),row=>({...row,acknowledged_proofs:5}),row=>({...row,actual_documents:0}),row=>({...row,phase:'verifying'})]){
   const original=f.stores[31].DB.prepare;
   f.stores[31].DB.prepare=sql=>{const prepared=original(sql);if(sql===partitionProofQuery){const first=prepared.first;prepared.first=async()=>change(await first.call(prepared));}return prepared;};
   assert.equal((await inspectReleaseReadiness(f.env)).ready,false);
   f.stores[31].DB.prepare=original;
   await assert.rejects(preflight(f.config,'preview',{execute:async(queries,binding)=>{const rows=await f.execute(queries,binding);return binding==='CORPUS_DB_31'&&queries[0]===partitionProofQuery?[[change(rows[0][0])]]:rows;}}),/restoration_proof/);
  }
  const duplicate=structuredClone(f.config);duplicate.env.preview.d1_databases[31].database_id=duplicate.env.preview.d1_databases[0].database_id;
  await assert.rejects(preflight(duplicate,'preview',{execute:f.execute}),/Aliased/);
  const manifest=structuredClone(f.manifest);manifest.partitions[31].revision++;
  const drift={...f.env,CORPUS_PARTITIONS_JSON:JSON.stringify(manifest)};assert.equal((await inspectReleaseReadiness(drift)).ready,false);
  f.stores[0].sqlite.exec("UPDATE beedle_corpus_review_control SET phase='building'");
  assert.equal((await inspectReleaseReadiness(f.env)).ready,false);await assert.rejects(preflight(f.config,'preview',{execute:f.execute}),/review_cache/);
 }finally{f.close();}
});

test('restoration claims and acknowledged results cannot be rewritten, reset or removed',async()=>{
 const f=await setup();try{
  const s=f.stores[0].sqlite;
  for(const sql of ["UPDATE beedle_restore_owner SET owner_id='different'","DELETE FROM beedle_restore_owner","UPDATE beedle_restore_integrity SET status='claimed' WHERE kind='body'","DELETE FROM beedle_restore_integrity WHERE kind='body'","UPDATE beedle_restore_integrity SET corpus_revision=2 WHERE kind='body'"])
   assert.throws(()=>s.exec(sql),/immutable/);
  const m=JSON.parse(f.env.CORPUS_PARTITIONS_JSON);m.restoreOwnerId='unowned';assert.throws(()=>parseCorpusPartitionManifest(JSON.stringify(m)),/Invalid/);
 }finally{f.close();}
});
