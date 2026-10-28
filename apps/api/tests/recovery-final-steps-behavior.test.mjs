import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {spawnSync} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
import {writeRestoreSql,sha,describe,claimStatements} from '../scripts/lib/recovery-bundle.mjs';
import {applySqlShards} from '../scripts/lib/recovery-sql-shards.mjs';
import {loadFinalStepPlan,applyFinalSqlSteps} from '../scripts/lib/recovery-final-steps.mjs';
async function fixture(fn){
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'beedle-final-steps-'));
 const source=new DatabaseSync(path.join(root,'source.sqlite')),db=new DatabaseSync(':memory:');
 try{
  source.exec('PRAGMA foreign_keys=ON;CREATE TABLE parent(id INTEGER PRIMARY KEY,value TEXT);CREATE TABLE child(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parent(id),value BLOB);CREATE INDEX child_parent ON child(parent_id);CREATE INDEX child_value ON child(value);CREATE TABLE stats(n INTEGER);INSERT INTO stats VALUES(0);CREATE TRIGGER child_count AFTER INSERT ON child BEGIN UPDATE stats SET n=n+1; END;');
  source.exec("INSERT INTO parent VALUES(9223372036854775807,CAST(X'610062f09f9880' AS TEXT));");
  const insert=source.prepare('INSERT INTO child VALUES(?,9223372036854775807,?)');for(let i=0;i<36;i++)insert.run(i,Buffer.alloc(i===35?150000:5000,i));
  const ctx={approvedPlanSha256:'a'.repeat(64),manifest:{runId:crypto.randomUUID()},plan:{bundleSha256:'b'.repeat(64),targetSha256:'c'.repeat(64)}};
  await writeRestoreSql(source,path.join(root,'restore.sql'),{runId:ctx.manifest.runId,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256});
  const original=await fs.readFile(path.join(root,'restore.sql'));ctx.plan.sql={sha256:sha(original),bytes:original.length};
  const parser=path.resolve(import.meta.dirname,'../scripts/plan-recovery-sql-shards.py');
  const planned=spawnSync('python3',['-c',"import importlib.machinery,json,sys;m=importlib.machinery.SourceFileLoader('p',sys.argv[1]).load_module();print(json.dumps(m.plan_shards(*sys.argv[2:5],max_bytes=512000,max_statements=32)))",parser,path.join(root,'restore.sql'),path.join(root,'source.sqlite'),ctx.approvedPlanSha256],{encoding:'utf8'});assert.equal(planned.status,0,planned.stderr);
  const base=JSON.parse(planned.stdout),baseSha=sha(JSON.stringify(base));await fs.writeFile(path.join(root,'shards.json'),JSON.stringify(base));
  const file=path.join(root,'final.json'),planResult=spawnSync('python3',[path.resolve(import.meta.dirname,'../scripts/plan-recovery-final-steps.py'),path.join(root,'restore.sql'),path.join(root,'source.sqlite'),path.join(root,'shards.json'),baseSha,file],{encoding:'utf8'});assert.equal(planResult.status,0,planResult.stderr);
  const p=JSON.parse(await fs.readFile(file,'utf8')),approved=sha(JSON.stringify(p)),plan=await loadFinalStepPlan(file,approved,ctx,base,baseSha);assert.ok(plan.steps.filter(s=>s.kind==='ddl').length>=2);
  db.exec('PRAGMA foreign_keys=ON;'+claimStatements({runId:ctx.manifest.runId,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256}).join(';')+';');
  const readQuery=async sql=>db.prepare(sql).all().map(row=>({...row}));
  const initial=path.join(root,'initial');await fs.mkdir(initial);
  async function execute(file){db.exec('BEGIN;');try{db.exec(await fs.readFile(file,'utf8'));db.exec('COMMIT;');}catch(e){db.exec('ROLLBACK;');throw e;}}
  await assert.rejects(applySqlShards({bundle:root,ctx,plan:base,planSha256:baseSha,dir:initial,readQuery,progress:async()=>{},importFile:async(file,index)=>{if(index===base.chunks.length-1)throw Error('simulated provider reset');await execute(file);}}),/simulated provider reset/);
  const parentCheckpoint=await fs.readFile(path.join(root,'sql-shard-checkpoint.json'));
  const dir=path.join(root,'final-execution');await fs.mkdir(dir);let writes=0;
  const resetProof={providerTerminalReset:'D1_RESET_DO',exactCommittedPrefix:base.chunks.length-1,pendingFinalRangeCommitted:false,targetClaimMatches:true,originalPlanSha256:ctx.approvedPlanSha256,sqlShardPlanSha256:baseSha};
  const options={bundle:root,ctx,base,baseSha,plan,approved,resetProof,dir,readQuery,progress:async()=>{},importFile:async(file)=>{writes++;await execute(file);}};
  await fn({root,source,db,original,parentCheckpoint,ctx,base,baseSha,plan,approved,file,options,get writes(){return writes;}});
 }finally{source.close();db.close();await fs.rm(root,{recursive:true,force:true});}
}
test('final steps preserve every source byte, all data/schema and original pending checkpoint; DDL commits individually',()=>fixture(async f=>{
 const ranges=f.plan.steps.map(s=>f.original.subarray(s.offset,s.offset+s.bytes));assert.equal(sha(Buffer.concat(ranges)),f.plan.parentChunk.sha256);
 await applyFinalSqlSteps(f.options);assert.deepEqual(describe(f.db),describe(f.source));
 assert.deepEqual(await fs.readFile(path.join(f.root,'restore.sql')),f.original);assert.deepEqual(await fs.readFile(path.join(f.root,'sql-shard-checkpoint.json')),f.parentCheckpoint);
 const writes=f.writes;await applyFinalSqlSteps(f.options);assert.equal(f.writes,writes);
 assert.equal(f.db.prepare('SELECT count(*) n FROM beedle_recovery_sql_chunks').get().n,f.base.chunks.length);
}));
test('lost acknowledgment resumes only from its committed exact child receipt',()=>fixture(async f=>{
 const run=f.options.importFile;let first=true;await assert.rejects(applyFinalSqlSteps({...f.options,importFile:async(...args)=>{await run(...args);if(first){first=false;throw Error('lost acknowledgment');}}}),/lost acknowledgment/);
 await applyFinalSqlSteps(f.options);assert.equal(f.writes,f.plan.steps.length);assert.deepEqual(describe(f.db),describe(f.source));
}));
test('an uncommitted final step cannot be automatically retried using the old parent-reset proof',()=>fixture(async f=>{
 await assert.rejects(applyFinalSqlSteps({...f.options,importFile:async()=>{throw Error('uncertain child outcome');}}),/uncertain child/);
 await assert.rejects(applyFinalSqlSteps(f.options),/terminal provider review/);assert.equal(f.writes,0);
}));
test('missing or altered child receipts reject completion and preserve the original prefix',async()=>{
 for(const mutation of ["UPDATE beedle_recovery_sql_final_steps SET sql_sha256='bad'","DELETE FROM beedle_recovery_sql_final_steps WHERE step_index=0"])await fixture(async f=>{
  await applyFinalSqlSteps(f.options);f.db.exec(mutation);const writes=f.writes;await assert.rejects(applyFinalSqlSteps(f.options),/receipt|completion/i);assert.equal(f.writes,writes);
 });
});
test('wrong claim or missing terminal provider proof stops before a write',async()=>{
 for(const kind of ['claim','proof'])await fixture(async f=>{
  if(kind==='claim')f.db.exec("UPDATE beedle_recovery_claim SET run_id='other-owner'");else f.options.resetProof={};
  await assert.rejects(applyFinalSqlSteps(f.options),/claim|parent-reset proof/);assert.equal(f.writes,0);
 });
});
test('foreign-key violations still roll back a child transaction and its receipt',()=>fixture(async f=>{
 await assert.rejects(applyFinalSqlSteps({...f.options,importFile:async file=>{
  f.db.exec('BEGIN;');try{f.db.exec(await fs.readFile(file,'utf8'));f.db.exec('INSERT INTO child VALUES(9999,-1,NULL);COMMIT;');}catch(e){f.db.exec('ROLLBACK;');throw e;}
 }}),/FOREIGN KEY/i);
 assert.equal(f.db.prepare('SELECT count(*) n FROM beedle_recovery_sql_chunks').get().n,f.base.chunks.length-1);assert.equal(f.db.prepare('SELECT count(*) n FROM beedle_recovery_receipt').get().n,0);assert.deepEqual(f.db.prepare('PRAGMA foreign_key_check').all(),[]);
}));
test('changed parent, gapped ranges and grouped DDL cannot acquire a newly approved plan hash',async()=>{
 for(const kind of ['parent','gap','ddl'])await fixture(async f=>{
  const plan=structuredClone(f.plan);if(kind==='parent')plan.parentChunk.index--;if(kind==='gap')plan.steps[1].offset++;if(kind==='ddl')plan.steps.find(s=>s.kind==='ddl').statements=2;
  await fs.writeFile(f.file,JSON.stringify(plan));await assert.rejects(loadFinalStepPlan(f.file,sha(JSON.stringify(plan)),f.ctx,f.base,f.baseSha));assert.equal(f.writes,0);
 });
});
test('changed SQL and cancellation hold without another write',async()=>{
 for(const kind of ['sql','cancel'])await fixture(async f=>{
  if(kind==='sql'){await fs.appendFile(path.join(f.root,'restore.sql'),'SELECT 1;');await assert.rejects(applyFinalSqlSteps(f.options),/Original SQL changed/);}
  else{const abort=new AbortController();abort.abort();await assert.rejects(applyFinalSqlSteps({...f.options,signal:abort.signal}),/Operator interrupted/);}
  assert.equal(f.writes,0);
 });
});
test('an orphaned child helper without its atomic receipt is not accepted as an owned resume',()=>fixture(async f=>{
 f.db.exec('CREATE TABLE beedle_recovery_sql_final_steps(step_index INTEGER PRIMARY KEY,step_plan_sha256 TEXT,original_plan_sha256 TEXT,shard_plan_sha256 TEXT,parent_chunk_index INTEGER,sql_sha256 TEXT,sql_offset INTEGER,sql_bytes INTEGER)');
 await assert.rejects(applyFinalSqlSteps(f.options),/Incomplete final-step ownership/);assert.equal(f.writes,0);
}));
