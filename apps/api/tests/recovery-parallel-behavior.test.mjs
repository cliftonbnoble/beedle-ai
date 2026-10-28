import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {loadParallelPlan,parallelRecovery,transientReadFailure} from '../scripts/recovery-parallel.mjs';
import {sha,fileSha} from '../scripts/lib/recovery-bundle.mjs';
const json=async(file,value)=>fs.writeFile(file,JSON.stringify(value),{mode:0o600});
test('read-only retry classifier does not turn content or SQL errors into transport retries',()=>{
 for(const s of ['request - 500: Internal Server Error','HTTP 503','fetch failed','ECONNRESET'])assert.equal(transientReadFailure(s),true);
 for(const s of ['SQLITE_ERROR: record has 500 words','Conflicting R2 key content','HTTP 403','no such table','constraint failed'])assert.equal(transientReadFailure(s),false);
});
async function fixture(fn){
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'parallel-recovery-unit-'));
 try{
  const bundle=path.join(root,'bundle'),persist=path.join(root,'target'),source=path.join(root,'source');await Promise.all([bundle,persist,source].map(d=>fs.mkdir(d)));await fs.mkdir(path.join(bundle,'objects'));
  const config=path.join(root,'target-wrangler.json'),target=path.join(root,'target.json');
  await json(config,{name:'synthetic-only',vars:{VECTOR_LIFECYCLE_READY:'false',VECTOR_FILTERS_READY:'false',RELEASE_MAINTENANCE_ONLY:'true'},d1_databases:[{binding:'DB',database_name:'synthetic',database_id:'00000000-0000-0000-0000-000000000002'}],r2_buckets:[{binding:'SOURCE_BUCKET',bucket_name:'synthetic-target'}]});
  await json(target,{mode:'local',config,persistTo:persist,purpose:'isolated-recovery',writersQuiesced:true,freshBucket:true});
  const identity={mode:'local',accountId:null,databaseId:'00000000-0000-0000-0000-000000000002',databaseName:'synthetic',bucket:'synthetic-target',configSha256:await fileSha(config),persistTo:await fs.realpath(persist)};
  await fs.writeFile(path.join(bundle,'source.sqlite'),'synthetic source');await fs.writeFile(path.join(bundle,'restore.sqlite'),'synthetic restore');
  const body=Buffer.from('exact\0original'),hash=sha(body);await fs.writeFile(path.join(bundle,'objects',hash),body);await fs.writeFile(path.join(bundle,'restore.sql'),'SELECT 1;\n');
  const api=path.resolve(import.meta.dirname,'..');
  const manifest={version:1,runId:crypto.randomUUID(),source:{mode:'local',persistTo:source,databaseId:'00000000-0000-0000-0000-000000000001',bucket:'synthetic-source'},toolSha256:sha(await fs.readFile(path.join(api,'scripts/recovery.mjs'),'utf8')+await fs.readFile(path.join(api,'scripts/lib/recovery-bundle.mjs'),'utf8')),contractSha256:await fileSha(path.join(api,'release/contract.json')),objects:[{key:'synthetic-only',present:true,sha256:hash,bytes:body.length}],files:{'source.sqlite':await fileSha(path.join(bundle,'source.sqlite')),'restore.sqlite':await fileSha(path.join(bundle,'restore.sqlite'))},restore:{tables:[],schemaSha256:sha('[]')}};
  const plan={version:1,runId:manifest.runId,bundleSha256:sha(JSON.stringify(manifest)),target:identity,targetSha256:sha(JSON.stringify(identity)),sql:{sha256:await fileSha(path.join(bundle,'restore.sql')),bytes:10,maxSqlBytes:536870912},importTimeoutMs:120000};
  const seal=async()=>{plan.bundleSha256=sha(JSON.stringify(manifest));await json(path.join(bundle,'bundle.json'),manifest);await json(path.join(bundle,'plan.json'),plan);return sha(JSON.stringify(plan));};
  const approved=await seal();await fn({root,bundle,target,config,manifest,plan,hash,seal,approved});
 }finally{await fs.rm(root,{recursive:true,force:true});}
}
test('parallel executor validates the original seal without changing any plan or bundle bytes',()=>fixture(async f=>{
 const files=['bundle.json','plan.json','restore.sql','source.sqlite','restore.sqlite'];const before=await Promise.all(files.map(n=>fs.readFile(path.join(f.bundle,n))));
 const ctx=await loadParallelPlan(f.bundle,f.target,f.approved);assert.equal(ctx.plan.runId,f.manifest.runId);
 const after=await Promise.all(files.map(n=>fs.readFile(path.join(f.bundle,n))));assert.deepEqual(after,before);
}));
test('wrong approval, changed local original, SQL, snapshot and target config all fail before execution',async()=>{
 for(const mutation of ['approval','original','sql','snapshot','config'])await fixture(async f=>{
  let approved=f.approved;
  if(mutation==='approval')approved='0'.repeat(64);
  if(mutation==='original')await fs.writeFile(path.join(f.bundle,'objects',f.hash),'altered');
  if(mutation==='sql')await fs.appendFile(path.join(f.bundle,'restore.sql'),'DROP TABLE synthetic;');
  if(mutation==='snapshot')await fs.appendFile(path.join(f.bundle,'source.sqlite'),'changed');
  if(mutation==='config')await fs.appendFile(f.config,' ');
  await assert.rejects(loadParallelPlan(f.bundle,f.target,approved));
  await assert.rejects(fs.access(path.join(f.bundle,'parallel-status.json')));
 });
});
test('source resources cannot be selected even with a newly approved matching identity',()=>fixture(async f=>{
 f.manifest.source={...f.plan.target};const approved=await f.seal();await assert.rejects(loadParallelPlan(f.bundle,f.target,approved),/distinct/);
}));
test('legacy tool and release contract hashes are mandatory',async()=>{
 for(const key of ['toolSha256','contractSha256'])await fixture(async f=>{f.manifest[key]='a'.repeat(64);await f.seal();await assert.rejects(loadParallelPlan(f.bundle,f.target,f.approved),/Original sealed/);});
});
test('concurrency/rate bounds and an existing owner lock prevent a new execution',()=>fixture(async f=>{
 for(const options of [{concurrency:0},{concurrency:5},{requestsPerMinute:181}])await assert.rejects(parallelRecovery({bundle:f.bundle,target:f.target,planSha256:f.approved,options}),/Bounded/);
 await fs.writeFile(path.join(f.bundle,'operation.lock'),JSON.stringify({pid:process.pid}));await assert.rejects(parallelRecovery({bundle:f.bundle,target:f.target,planSha256:f.approved}),/locked/);
}));
