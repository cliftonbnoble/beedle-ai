import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createWorkerFixture} from './fixture.mjs';
import {seedFixture} from './seeds.mjs';
import {main,query} from '../../scripts/recovery.mjs';
import {parallelRecovery} from '../../scripts/recovery-parallel.mjs';
async function sqliteFile(dir){for(const item of await fs.readdir(dir,{withFileTypes:true})){const file=path.join(dir,item.name);if(item.isDirectory()){const found=await sqliteFile(file);if(found)return found;}else if(file.endsWith('.sqlite'))return file;}return null;}
async function json(file,value){await fs.writeFile(file,JSON.stringify(value));return file;}
test('parallel native R2 copy and full verification preserve the original sealed plan, replay, and reject changed data', {timeout:900000},async()=>{
 const source=await createWorkerFixture({lexicalOnly:true,consumers:false});let target;
 try{
  const ids=await seedFixture(source);await source.pause();
  const sourceSpec=await json(path.join(source.dir,'source.json'),{mode:'local',config:source.configPath,persistTo:source.persistTo,sqlite:await sqliteFile(path.join(source.persistTo,'v3/d1'))});
  const bundle=path.join(source.dir,'bundle');await main(['capture',sourceSpec,bundle]);
  target=await createWorkerFixture({lexicalOnly:true,maintenanceOnly:true,consumers:false,initialize:false,autoStart:false});
  const targetSpec=await json(path.join(target.dir,'target.json'),{mode:'local',config:target.configPath,persistTo:target.persistTo,purpose:'isolated-recovery',writersQuiesced:true,freshBucket:true});
  const planned=await main(['plan',bundle,targetSpec]);
  const sealedFiles=['bundle.json','plan.json','restore.sql'];const before=await Promise.all(sealedFiles.map(n=>fs.readFile(path.join(bundle,n))));
  assert.equal((await parallelRecovery({bundle,target:targetSpec,planSha256:planned.planSha256,options:{concurrency:4,requestsPerMinute:180}})).dataVerified,true);
  assert.deepEqual(await Promise.all(sealedFiles.map(n=>fs.readFile(path.join(bundle,n)))),before,'original seals are untouched');
  const status=JSON.parse(await fs.readFile(path.join(bundle,'parallel-status.json')));assert.equal(status.phase,'data_verified_not_promoted');
  const copy=JSON.parse(await fs.readFile(path.join(status.executionDirectory,'copy-status.json')));assert.equal(copy.completed,copy.total);assert.ok(copy.maxActive>1&&copy.maxActive<=4);
  assert.equal((await main(['verify',bundle,targetSpec])).dataVerified,true,'independent original verifier accepts all content and originals');
  assert.equal((await parallelRecovery({action:'verify',bundle,target:targetSpec,planSha256:planned.planSha256,options:{concurrency:4}})).dataVerified,true,'completed receipt replays without importing or rewriting');
  const ep={mode:'local',config:target.configPath,persistTo:target.persistTo};await query(ep,"UPDATE documents SET case_number='injected content mismatch' WHERE id='"+ids.alpha+"'",target.dir);
  await assert.rejects(parallelRecovery({action:'verify',bundle,target:targetSpec,planSha256:planned.planSha256}),/typed table content differs/);
  assert.equal(JSON.parse(await fs.readFile(path.join(bundle,'verification.json'))).dataVerified,false,'failed revalidation cannot leave a stale positive report');
 }finally{await target?.stop();await source.stop();}
});
