import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createWorkerFixture} from './fixture.mjs';
import {seedFixture} from './seeds.mjs';
import {main,query} from '../../scripts/recovery.mjs';
import {runResumableVerification} from '../../scripts/recovery-resumable-verify.mjs';
async function sqliteFile(dir){for(const item of await fs.readdir(dir,{withFileTypes:true})){const p=path.join(dir,item.name);if(item.isDirectory()){const found=await sqliteFile(p);if(found)return found;}else if(p.endsWith('.sqlite'))return p;}return null;}
async function json(file,value){await fs.writeFile(file,JSON.stringify(value));return file;}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
test('native resumable controller survives an interrupted real D1 pass, agrees with independent full verification, and rejects changed target',{timeout:900000},async()=>{
 const source=await createWorkerFixture({lexicalOnly:true,consumers:false});let target,child;
 try{
  const ids=await seedFixture(source);await source.pause();
  const sourceSpec=await json(path.join(source.dir,'source.json'),{mode:'local',config:source.configPath,persistTo:source.persistTo,sqlite:await sqliteFile(path.join(source.persistTo,'v3/d1'))}),bundle=path.join(source.dir,'bundle');await main(['capture',sourceSpec,bundle]);
  target=await createWorkerFixture({lexicalOnly:true,maintenanceOnly:true,consumers:false,initialize:false,autoStart:false});
  const targetSpec=await json(path.join(target.dir,'target.json'),{mode:'local',config:target.configPath,persistTo:target.persistTo,purpose:'isolated-recovery',writersQuiesced:true,freshBucket:true}),original=await main(['plan',bundle,targetSpec]);await main(['apply',bundle,targetSpec,original.planSha256]);
  const originals=['bundle.json','plan.json','restore.sql'],before=await Promise.all(originals.map(n=>fs.readFile(path.join(bundle,n)))),verificationPlan=path.join(source.dir,'verification-pages.json'),made=await runResumableVerification({action:'plan',bundle,target:targetSpec,originalPlanSha256:original.planSha256,options:{verificationPlan}}),options={verificationPlan,verificationPlanSha256:made.sha256,concurrency:4,requestsPerMinute:180,fencePlan:path.join(source.dir,'fence-plan.json')};
  const fence=await runResumableVerification({action:'fence-plan',bundle,target:targetSpec,originalPlanSha256:original.planSha256,options});options.fencePlanSha256=fence.fencePlanSha256;
  assert.equal((await runResumableVerification({action:'fence-apply',bundle,target:targetSpec,originalPlanSha256:original.planSha256,options})).fenceInstalled,true);
  const optionsFile=await json(path.join(source.dir,'verification-options.json'),options);
  const env={...process.env,WRANGLER_SEND_METRICS:'false'};for(const k of ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL'])delete env[k];
  child=spawn(process.execPath,[path.resolve(import.meta.dirname,'../../scripts/recovery-resumable-verify.mjs'),'verify',bundle,targetSpec,original.planSha256,optionsFile],{env,stdio:['ignore','ignore','pipe']});let output='';child.stderr.on('data',b=>output+=b.toString());const exited=new Promise(resolve=>child.once('exit',resolve));
  let interrupted=false;const until=Date.now()+120000;
  while(Date.now()<until&&child.exitCode===null){const state=await fs.readFile(path.join(bundle,'resumable-verification-status.json'),'utf8').then(JSON.parse).catch(()=>null);if(state?.verifiedPages>0){child.kill('SIGTERM');interrupted=true;break;}if(state?.phase==='held')break;await sleep(100);}
  if(!interrupted)child.kill('SIGTERM');await exited;assert.equal(interrupted,true,output+' '+await fs.readFile(path.join(bundle,'resumable-verification-status.json'),'utf8'));assert.equal(child.exitCode,1);child=null;
  assert.equal(JSON.parse(await fs.readFile(path.join(bundle,'verification.json'),'utf8')).dataVerified,false);await assert.rejects(fs.access(path.join(bundle,'operation.lock')));
  const result=await runResumableVerification({bundle,target:targetSpec,originalPlanSha256:original.planSha256,options});assert.equal(result.dataVerified,true);assert.ok(result.coverage.resumedPages>0);assert.equal(result.coverage.pages,made.plan.pageCount);
  assert.equal((await main(['verify',bundle,targetSpec])).dataVerified,true,'original independent verifier re-reads all typed rows and R2 files');assert.deepEqual(await Promise.all(originals.map(n=>fs.readFile(path.join(bundle,n)))),before);
  const ep={mode:'local',config:target.configPath,persistTo:target.persistTo};await assert.rejects(query(ep,"UPDATE documents SET case_number='changed synthetic value' WHERE id='"+ids.alpha+"'",target.dir));
  const fencePlan=JSON.parse(await fs.readFile(options.fencePlan,'utf8')),trigger=fencePlan.expectedSchema.find(o=>o.type==='trigger'&&o.tbl_name==='documents'&&o.sql.includes(' BEFORE UPDATE '));await query(ep,'DROP TRIGGER "'+trigger.name+'"',target.dir);
  await assert.rejects(runResumableVerification({bundle,target:targetSpec,originalPlanSha256:original.planSha256,options}),/fence\/schema differs/);assert.equal(JSON.parse(await fs.readFile(path.join(bundle,'verification.json'),'utf8')).dataVerified,false);
 }catch(error){
  const state=await fs.readFile(path.join(source.dir,'bundle/resumable-verification-status.json'),'utf8').then(JSON.parse).catch(()=>null);
  if(state?.executionDirectory)for(const name of ['objects-private.log','objects-detail-private.log']){const data=await fs.readFile(path.join(state.executionDirectory,name)).catch(()=>null);if(data)await fs.writeFile('/tmp/beedle-resumable-native-failure-'+name,data,{mode:0o600});}
  throw error;
 }finally{if(child&&child.exitCode===null&&child.signalCode===null){const done=new Promise(resolve=>child.once('exit',resolve));child.kill('SIGTERM');await done;}await target?.stop();await source.stop();}
});
