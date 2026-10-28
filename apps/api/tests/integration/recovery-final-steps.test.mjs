import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawnSync,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {createRequire} from 'node:module';
import {createWorkerFixture} from './fixture.mjs';
import {seedFixture} from './seeds.mjs';
import {main,query} from '../../scripts/recovery.mjs';
import {loadParallelPlan} from '../../scripts/recovery-parallel.mjs';
import {loadSqlShardPlan,applySqlShards} from '../../scripts/lib/recovery-sql-shards.mjs';
import {loadFinalStepPlan,applyFinalSqlSteps} from '../../scripts/lib/recovery-final-steps.mjs';
import {runFinalStepRecovery} from '../../scripts/recovery-final-steps.mjs';
import {sha,claimStatements} from '../../scripts/lib/recovery-bundle.mjs';
const run=promisify(execFile),require=createRequire(import.meta.url),api=path.resolve(import.meta.dirname,'../..');
async function sqliteFile(dir){for(const item of await fs.readdir(dir,{withFileTypes:true})){const p=path.join(dir,item.name);if(item.isDirectory()){const result=await sqliteFile(p);if(result)return result;}else if(p.endsWith('.sqlite'))return p;}return null;}
async function json(file,value){await fs.writeFile(file,JSON.stringify(value));return file;}
test('native final-step recovery keeps originals/parent checkpoint intact and passes the original independent D1/R2 verifier',{timeout:900000},async()=>{
 const source=await createWorkerFixture({lexicalOnly:true,consumers:false});let target;
 try{
  await seedFixture(source);await source.pause();
  const spec=await json(path.join(source.dir,'source.json'),{mode:'local',config:source.configPath,persistTo:source.persistTo,sqlite:await sqliteFile(path.join(source.persistTo,'v3/d1'))});
  const bundle=path.join(source.dir,'bundle');await main(['capture',spec,bundle]);
  target=await createWorkerFixture({lexicalOnly:true,maintenanceOnly:true,consumers:false,initialize:false,autoStart:false});
  const targetSpec=await json(path.join(target.dir,'target.json'),{mode:'local',config:target.configPath,persistTo:target.persistTo,purpose:'isolated-recovery',writersQuiesced:true,freshBucket:true});
  const original=await main(['plan',bundle,targetSpec]),ctx=await loadParallelPlan(bundle,targetSpec,original.planSha256);ctx.approvedPlanSha256=original.planSha256;
  const baseFile=path.join(source.dir,'shards.json');
  const planning=spawnSync('python3',[path.join(api,'scripts/plan-recovery-sql-shards.py'),path.join(bundle,'restore.sql'),path.join(bundle,'restore.sqlite'),original.planSha256,baseFile],{encoding:'utf8'});assert.equal(planning.status,0,planning.stderr);
  const rawBase=JSON.parse(await fs.readFile(baseFile,'utf8')),baseSha=sha(JSON.stringify(rawBase)),base=await loadSqlShardPlan(baseFile,baseSha,ctx);assert.ok(base.chunks.length>1);
  const stepFile=path.join(source.dir,'final-steps.json'),stepPlanning=spawnSync('python3',[path.join(api,'scripts/plan-recovery-final-steps.py'),path.join(bundle,'restore.sql'),path.join(bundle,'restore.sqlite'),baseFile,baseSha,stepFile],{encoding:'utf8'});assert.equal(stepPlanning.status,0,stepPlanning.stderr);
  const raw=JSON.parse(await fs.readFile(stepFile,'utf8')),approved=sha(JSON.stringify(raw)),plan=await loadFinalStepPlan(stepFile,approved,ctx,base,baseSha);assert.ok(plan.steps.length>1);
  const names=['bundle.json','plan.json','restore.sql'],before=await Promise.all(names.map(n=>fs.readFile(path.join(bundle,n))));
  const env={...process.env,WRANGLER_SEND_METRICS:'false',WRANGLER_LOG_PATH:path.join(target.dir,'native-final-step-private.log')};for(const k of ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL'])delete env[k];
  const cli=args=>run(process.execPath,[require.resolve('wrangler'),...args],{cwd:api,env,timeout:120000,maxBuffer:16*1024*1024});
  const flags=['--config',target.configPath,'--local','--persist-to',target.persistTo];
  for(const object of ctx.manifest.objects.filter(o=>o.present))await cli(['r2','object','put',ctx.ep.identity.bucket+'/'+object.key,...flags,'--file',path.join(bundle,'objects',object.sha256)]);
  for(const sql of claimStatements({runId:ctx.manifest.runId,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256}))await query(ctx.ep,sql,target.dir);
  const initial=path.join(source.dir,'initial-execution');await fs.mkdir(initial);const readQuery=sql=>query(ctx.ep,sql,target.dir),importFile=file=>cli(['d1','execute','DB',...flags,'--yes','--file',file]);
  await assert.rejects(applySqlShards({bundle,ctx,plan:base,planSha256:baseSha,dir:initial,readQuery,progress:async()=>{},importFile:async(file,index)=>{if(index===base.chunks.length-1)throw Error('simulated provider reset');await importFile(file);}}),/simulated provider reset/);
  const checkpoint=await fs.readFile(path.join(bundle,'sql-shard-checkpoint.json')),dir=path.join(source.dir,'final-execution');await fs.mkdir(dir);let writes=0;
  const args={bundle,ctx,base,baseSha,plan,approved,dir,readQuery,progress:async()=>{},resetProof:{providerTerminalReset:'D1_RESET_DO',exactCommittedPrefix:base.chunks.length-1,pendingFinalRangeCommitted:false,targetClaimMatches:true,originalPlanSha256:original.planSha256,sqlShardPlanSha256:baseSha},importFile:async file=>{writes++;await importFile(file);}};
  await applyFinalSqlSteps(args);assert.equal(writes,plan.steps.length);await applyFinalSqlSteps(args);assert.equal(writes,plan.steps.length,'replay must not write');
  assert.deepEqual(await fs.readFile(path.join(bundle,'sql-shard-checkpoint.json')),checkpoint);assert.deepEqual(await Promise.all(names.map(n=>fs.readFile(path.join(bundle,n)))),before);
  assert.equal((await main(['verify',bundle,targetSpec])).dataVerified,true,'original verifier independently checks every ordinary row/schema and original file');
  const resetProof=await json(path.join(source.dir,'reset-proof.json'),args.resetProof);
  const result=await runFinalStepRecovery({bundle,target:targetSpec,originalPlanSha256:original.planSha256,options:{sqlShardPlan:baseFile,sqlShardPlanSha256:baseSha,finalStepPlan:stepFile,finalStepPlanSha256:approved,resetProof,resetProofSha256:sha(JSON.stringify(args.resetProof)),concurrency:4,requestsPerMinute:180}});
  assert.equal(result.dataVerified,true,'public runner reacquires ownership and requires full original D1/R2 verification');
  const state=JSON.parse(await fs.readFile(path.join(bundle,'final-step-status.json'),'utf8'));assert.equal(state.phase,'data_verified_not_promoted');
  for(const name of ['controller-source.mjs','final-step-library-source.mjs','final-step-planner-source.py'])assert.ok((await fs.stat(path.join(state.executionDirectory,name))).size>0,'each provenance source has a distinct frozen filename');
  await query(ctx.ep,"UPDATE beedle_recovery_sql_final_steps SET sql_sha256='injected corruption' WHERE step_index=0",target.dir);
  await assert.rejects(applyFinalSqlSteps(args),/receipt prefix differs/);assert.equal(writes,plan.steps.length);
 }finally{await target?.stop();await source.stop();}
});
