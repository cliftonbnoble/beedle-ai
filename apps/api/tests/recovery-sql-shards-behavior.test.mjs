import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {spawnSync} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
import {writeRestoreSql,sha,fileSha,describe,claimStatements} from '../scripts/lib/recovery-bundle.mjs';
import {loadSqlShardPlan,applySqlShards} from '../scripts/lib/recovery-sql-shards.mjs';
async function fixture(fn){
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'beedle-sql-shards-'));
 const source=new DatabaseSync(path.join(root,'source.sqlite')),target=new DatabaseSync(':memory:');
 try{
  source.exec("PRAGMA foreign_keys=ON;CREATE TABLE parent(id INTEGER PRIMARY KEY,value TEXT);CREATE TABLE child(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parent(id),value BLOB);CREATE TABLE stats(n INTEGER);INSERT INTO stats VALUES(0);CREATE TRIGGER child_count AFTER INSERT ON child BEGIN\n UPDATE stats SET n=n+1;\n END;");
  source.exec("INSERT INTO parent VALUES(9223372036854775807,CAST(X'610062F09F9880' AS TEXT));");
  const put=source.prepare('INSERT INTO child VALUES(?,9223372036854775807,?)');
  for(let i=0;i<36;i++)put.run(i,Buffer.alloc(i===5?150000:5000,i));
  const manifest={runId:crypto.randomUUID()},parentSha='a'.repeat(64),bundleSha='b'.repeat(64),targetSha='c'.repeat(64);
  await writeRestoreSql(source,path.join(root,'restore.sql'),{runId:manifest.runId,bundleSha256:bundleSha,targetSha256:targetSha});
  const input=await fs.readFile(path.join(root,'restore.sql'));
  const result=spawnSync('python3',['-c',"import importlib.machinery,json,sys; m=importlib.machinery.SourceFileLoader('planner',sys.argv[1]).load_module();print(json.dumps(m.plan_shards(*sys.argv[2:5],max_bytes=512000,max_statements=32)))",path.resolve(import.meta.dirname,'../scripts/plan-recovery-sql-shards.py'),path.join(root,'restore.sql'),path.join(root,'source.sqlite'),parentSha],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);const plan=JSON.parse(result.stdout),derivedSha=sha(JSON.stringify(plan)),file=path.join(root,'shards.json');await fs.writeFile(file,JSON.stringify(plan));
  const ctx={approvedPlanSha256:parentSha,manifest,plan:{bundleSha256:bundleSha,targetSha256:targetSha,sql:{sha256:sha(input),bytes:input.length}}};
  assert.deepEqual(await loadSqlShardPlan(file,derivedSha,ctx),plan);assert.ok(plan.chunks.length>1);
  target.exec('PRAGMA foreign_keys=ON;'+claimStatements({runId:manifest.runId,bundleSha256:bundleSha,targetSha256:targetSha}).join(';')+';');
  const dir=path.join(root,'execution');await fs.mkdir(dir);
  let writes=0;
  const options={bundle:root,ctx,plan,planSha256:derivedSha,dir,readQuery:async sql=>target.prepare(sql).all().map(row=>({...row})),progress:async()=>{},importFile:async file=>{
   writes++;target.exec('BEGIN;');try{target.exec(await fs.readFile(file,'utf8'));target.exec('COMMIT;');}catch(e){target.exec('ROLLBACK;');throw e;}
  }};
  await fn({root,source,target,plan,file,ctx,input,options,get writes(){return writes;}});
 }finally{source.close();target.close();await fs.rm(root,{recursive:true,force:true});}
}
test('bounded ranges preserve typed data, staged values, trigger schema and original SQL; exact receipts replay without writes',()=>fixture(async f=>{
 await applySqlShards(f.options);assert.deepEqual(describe(f.target),describe(f.source));
 assert.equal(await fileSha(path.join(f.root,'restore.sql')),sha(f.input));
 const writes=f.writes;await applySqlShards(f.options);assert.equal(f.writes,writes);
 assert.equal(f.target.prepare('SELECT count(*) n FROM beedle_recovery_sql_chunks').get().n,f.plan.chunks.length);
}));
test('a lost acknowledgment after an atomic commit resumes by its exact receipt without replaying that chunk',()=>fixture(async f=>{
 const original=f.options.importFile;let first=true;
 await assert.rejects(applySqlShards({...f.options,importFile:async(...args)=>{await original(...args);if(first){first=false;throw Error('lost acknowledgment');}}}),/lost acknowledgment/);
 assert.equal(f.writes,1);await applySqlShards(f.options);assert.equal(f.writes,f.plan.chunks.length);assert.deepEqual(describe(f.target),describe(f.source));
}));
test('an uncommitted failed shard holds and cannot be blindly retried',()=>fixture(async f=>{
 let attempted=0;
 await assert.rejects(applySqlShards({...f.options,importFile:async()=>{attempted++;throw Error('provider failure');}}),/provider failure/);
 await assert.rejects(applySqlShards(f.options),/terminal provider review required/);assert.equal(attempted,1);assert.equal(f.writes,0);
}));
test('missing/gapped or altered receipts fail before another write',async()=>{
 for(const mutation of ["UPDATE beedle_recovery_sql_chunks SET sql_sha256='bad'","DELETE FROM beedle_recovery_sql_chunks WHERE chunk_index=0"]){await fixture(async f=>{
  await applySqlShards(f.options);f.target.exec(mutation);const writes=f.writes;await assert.rejects(applySqlShards(f.options),/receipt prefix differs/);assert.equal(f.writes,writes);
 });}
});
test('wrong parent plan, gapped byte inventory, mutated SQL and excessive limits fail closed',async()=>{
 for(const mutation of ['parent','offset','budget','sql'])await fixture(async f=>{
  if(mutation==='sql'){await fs.appendFile(path.join(f.root,'restore.sql'),'SELECT 1;');await assert.rejects(applySqlShards(f.options),/Original SQL changed/);}
  else{const p=structuredClone(f.plan);if(mutation==='parent')p.originalPlanSha256='0'.repeat(64);if(mutation==='offset')p.chunks[1].offset++;if(mutation==='budget')p.maxChunkBytes=9*1024*1024;await fs.writeFile(f.file,JSON.stringify(p));await assert.rejects(loadSqlShardPlan(f.file,sha(JSON.stringify(p)),f.ctx));}
  assert.equal(f.writes,0);
 });
});
test('wrong target claim rolls the whole first shard back',()=>fixture(async f=>{
 f.target.exec("UPDATE beedle_recovery_claim SET run_id='other-owner'");await assert.rejects(applySqlShards(f.options),/constraint/i);
 assert.deepEqual(f.target.prepare("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").all().map(r=>r.name),['beedle_recovery_claim']);
}));
test('planner rejects cyclic foreign keys instead of disabling their integrity checks',()=>fixture(async f=>{
 f.source.exec('CREATE TABLE cycle(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES cycle(id));');
 const result=spawnSync('python3',[path.resolve(import.meta.dirname,'../scripts/plan-recovery-sql-shards.py'),path.join(f.root,'restore.sql'),path.join(f.root,'source.sqlite'),f.ctx.approvedPlanSha256,path.join(f.root,'bad-plan.json')],{encoding:'utf8'});
 assert.notEqual(result.status,0);assert.match(result.stderr,/Cyclic\/self foreign keys/);
}));
test('cancellation and a bad range hash stop before any SQL write',async()=>{
 for(const mode of ['cancel','range-hash'])await fixture(async f=>{
  if(mode==='cancel'){const abort=new AbortController();abort.abort();await assert.rejects(applySqlShards({...f.options,signal:abort.signal}),/Operator interrupted/);}
  else{const plan=structuredClone(f.plan);plan.chunks[0].sha256='0'.repeat(64);await assert.rejects(applySqlShards({...f.options,plan}),/byte range differs/);}
  assert.equal(f.writes,0);
 });
});
test('planner rejects trailing incomplete statements instead of silently dropping SQL',()=>fixture(async f=>{
 await fs.appendFile(path.join(f.root,'restore.sql'),'SELECT ');
 const result=spawnSync('python3',[path.resolve(import.meta.dirname,'../scripts/plan-recovery-sql-shards.py'),path.join(f.root,'restore.sql'),path.join(f.root,'source.sqlite'),f.ctx.approvedPlanSha256,path.join(f.root,'incomplete-plan.json')],{encoding:'utf8'});
 assert.notEqual(result.status,0);assert.match(result.stderr,/Incomplete sealed SQL/);
}));
