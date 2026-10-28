import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {sha,fileSha,describe} from '../scripts/lib/recovery-bundle.mjs';
import {makeVerificationPlan,loadVerificationPlan,checkpointStore,verifyPages,verifyObjects,objectVersions,verifiedPrefix,durableJson} from '../scripts/lib/recovery-verification.mjs';
import {retryRead,apiReadRetryable,VerificationReadFailure} from '../scripts/lib/recovery-verification-transport.mjs';
import inspector from '../scripts/lib/recovery-version-inspector.mjs';
import capture from '../scripts/lib/recovery-version-capture.mjs';

async function fixture(fn){
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'resumable-verification-test-')),file=path.join(root,'restore.sqlite'),db=new DatabaseSync(file);
 try{
  db.exec('CREATE TABLE typed(id INTEGER PRIMARY KEY,v); CREATE TABLE empty(v); CREATE TABLE composite(a TEXT,b BLOB,v,PRIMARY KEY(a,b)) WITHOUT ROWID;');
  const insert=db.prepare('INSERT INTO typed VALUES(?,?)');for(let i=1;i<=383;i++)insert.run(i,i===2?null:i===3?Buffer.from([0,255]):i===4?'nul\u0000text':i===5?9223372036854775807n:i===6?1.2345678901234567:'row '+i);insert.run(9223372036854775807n,'int64 cursor');
  const composite=db.prepare('INSERT INTO composite VALUES(?,?,?)');for(let i=0;i<130;i++)composite.run("a\u0000b'☃",Buffer.from([i]),'value '+i);
  const restore=describe(db),snapshotSha256=await fileSha(file),ctx={approvedPlanSha256:sha('plan'),plan:{bundleSha256:sha('bundle'),targetSha256:sha('target')},originalToolSha:sha('original'),manifest:{files:{'restore.sqlite':snapshotSha256},restore,objects:[{present:true,key:'fixture',sha256:sha('original'),bytes:8}]}},planFile=path.join(root,'verification-plan.json');
  const made=await makeVerificationPlan(ctx,root,planFile),plan=await loadVerificationPlan(planFile,made.sha256,ctx);await fn({root,db,ctx,plan,planFile,approved:made.sha256});
 }finally{db.close();await fs.rm(root,{recursive:true,force:true});}
}
test('derived pages preserve every typed byte, composite cursor, empty and exact-multiple terminal page',()=>fixture(async({plan,db,root})=>{
 const typed=plan.tables.find(e=>e.table.name==='typed');assert.deepEqual(typed.pages.map(p=>p.count),[128,128,128,0]);assert.deepEqual(plan.tables.find(e=>e.table.name==='composite').pages.map(p=>p.count),[128,2]);assert.equal(plan.tables.find(e=>e.table.name==='empty').pages[0].count,0);
 const store=await checkpointStore(path.join(root,'check.sqlite'),{test:1},{create:true});try{const result=await verifyPages({plan,store,read:async sql=>db.prepare(sql).all()});assert.equal(result.rows,514);assert.equal(result.pages,plan.pageCount);}finally{store.close();}
}));
test('interruption persists exact prefix and resumes only the unfinished page, including a lost local acknowledgment',()=>fixture(async({plan,db,root})=>{
 const file=path.join(root,'check.sqlite'),identity={plan:'a',bookmark:'unchanged'},store=await checkpointStore(file,identity,{create:true});let calls=0;
 await assert.rejects(verifyPages({plan,store,read:async sql=>{if(++calls===3)throw new VerificationReadFailure(true);return db.prepare(sql).all();}}),/transport exhausted/);assert.equal(store.pages().length,2);store.close();
 const reopened=await checkpointStore(file,identity);calls=0;let progress;
 const result=await verifyPages({plan,store:reopened,read:async sql=>{calls++;return db.prepare(sql).all();},progress:async p=>{progress=p;}});assert.equal(calls,plan.pageCount-2);assert.equal(result.resumedPages,2);assert.equal(progress.verifiedPages,plan.pageCount);reopened.close();
}));
test('wrong plan, changed bookmark, file versions, or controller cannot reuse a checkpoint',()=>fixture(async({root})=>{
 const file=path.join(root,'check.sqlite'),identity={plan:'a',bookmark:'b',versions:'c',controller:'d'};(await checkpointStore(file,identity,{create:true})).close();
 for(const key of Object.keys(identity))await assert.rejects(checkpointStore(file,{...identity,[key]:'changed'}),/identity differs/);
}));
test('file epoch is durable and independent of database receipts; a changed version never resets it',()=>fixture(async({root,plan})=>{
 const file=path.join(root,'epoch.sqlite'),identity={plan:1,bookmark:'stable'},store=await checkpointStore(file,identity,{create:true});store.save('pages',0,plan.tables[0].pages[0]);store.bindObjects({versions:'original'});store.close();
 const resumed=await checkpointStore(file,identity);assert.equal(resumed.pages().length,1);resumed.bindObjects({versions:'original'});assert.throws(()=>resumed.bindObjects({versions:'changed'}),/epoch differs/);resumed.close();
 const damaged=await checkpointStore(path.join(root,'no-epoch.sqlite'),identity,{create:true});damaged.save('objects',0,{test:1});assert.throws(()=>damaged.bindObjects({versions:'original'}),/no verification epoch/);damaged.close();
}));
test('gapped, altered, extra and corrupt checkpoint receipts never count as progress',()=>fixture(async({root,plan})=>{
 const file=path.join(root,'check.sqlite'),store=await checkpointStore(file,{plan:1},{create:true}),pages=plan.tables.flatMap(e=>e.pages);
 store.save('pages',1,pages[1]);await assert.rejects(verifyPages({plan,store,read:async()=>{throw Error('must not read');}}),/prefix differs/);
 store.db.exec('DELETE FROM pages');store.db.prepare('INSERT INTO pages VALUES(?,?)').run(0,sha('bad'));assert.throws(()=>verifiedPrefix(store.pages(),pages),/prefix differs/);
 store.db.exec('DELETE FROM pages');for(const page of pages)store.save('pages',page.index,page);store.db.prepare('INSERT INTO pages VALUES(?,?)').run(pages.length,sha('extra'));assert.throws(()=>verifiedPrefix(store.pages(),pages),/Unexpected/);store.close();
 await fs.writeFile(file,'damaged');await assert.rejects(checkpointStore(file,{plan:1}));
}));
test('changed typed rows, missing rows, additional rows, wrong order and non-advancing cursors hold before credit',()=>fixture(async({root,plan,db})=>{
 const target=plan.tables.find(e=>e.table.name==='typed');
 for(const mode of ['value','missing','extra','order','cursor']){
  const store=await checkpointStore(path.join(root,mode+'.sqlite'),{mode},{create:true});
  await assert.rejects(verifyPages({plan,store,read:async sql=>{const rows=db.prepare(sql).all();if(sql.includes('FROM "typed"')&&rows.length){if(mode==='value')rows[0].value='["text:00"]';if(mode==='missing')rows.pop();if(mode==='extra')rows.push(rows[0]);if(mode==='order')rows.reverse();if(mode==='cursor')rows[rows.length-1].cursor=target.pages[0].after||'["integer:0"]';}return rows;}}),/typed page content differs/);assert.ok(store.pages().length<plan.pageCount);store.close();
 }
}));
test('uncommitted receipt is re-read and a committed receipt survives a subsequent progress write failure',()=>fixture(async({root,plan,db})=>{
 const store=await checkpointStore(path.join(root,'check.sqlite'),{test:1},{create:true}),save=store.save;let failed=false;
 store.save=(...args)=>{if(!failed){failed=true;throw Error('disk full before commit');}return save(...args);};
 await assert.rejects(verifyPages({plan,store,read:async sql=>db.prepare(sql).all()}),/disk full/);assert.equal(store.pages().length,0);store.save=save;
 await assert.rejects(verifyPages({plan,store,read:async sql=>db.prepare(sql).all(),progress:async()=>{throw Error('status write failed');}}),/status write failed/);assert.equal(store.pages().length,1);
 const result=await verifyPages({plan,store,read:async sql=>db.prepare(sql).all()});assert.equal(result.resumedPages,1);store.close();
}));
test('object receipts survive partial parallel failure and every active operation drains before returning',()=>fixture(async({root})=>{
 const objects=Array.from({length:7},(_,i)=>({key:String(i),sha256:sha(String(i)),bytes:1,version:'v'+i})),store=await checkpointStore(path.join(root,'files.sqlite'),{test:1},{create:true});let active=0,peak=0;
 await assert.rejects(verifyObjects({objects,store,get:async key=>{active++;peak=Math.max(peak,active);try{await new Promise(r=>setTimeout(r,10));if(key==='1')throw new VerificationReadFailure(true);return {sha256:sha(key),bytes:1};}finally{active--;}}}),/transport exhausted/);assert.equal(active,0);assert.equal(peak,4);assert.equal(store.objects().length,3);
 const calls=[];const result=await verifyObjects({objects,store,get:async key=>{calls.push(key);return {sha256:sha(key),bytes:1};}});assert.equal(result.objects,7);assert.equal(result.resumedObjects,3);assert.deepEqual(calls,['1','4','5','6']);store.close();
}));
test('missing or conflicting original never earns a receipt; changed upload version rejects prior receipts',()=>fixture(async({root,plan})=>{
 const rows=[{key:'fixture',version:'first',size:8}],objects=objectVersions(plan,rows),store=await checkpointStore(path.join(root,'files.sqlite'),{test:1},{create:true});
 for(const actual of [null,{sha256:sha('wrong'),bytes:8},{sha256:sha('original'),bytes:7}])await assert.rejects(verifyObjects({objects,store,get:async()=>actual}),/original SHA256\/bytes differ/);
 assert.equal(store.objects().length,0);await verifyObjects({objects,store,get:async()=>({sha256:sha('original'),bytes:8})});
 await assert.rejects(verifyObjects({objects:objectVersions(plan,[{...rows[0],version:'replacement'}]),store,get:async()=>{throw Error('must not read');}}),/object receipt differs/);store.close();
 for(const inventory of [[],[...rows,...rows],[{...rows[0],version:''}],[{...rows[0],size:9}]])assert.throws(()=>objectVersions(plan,inventory));
}));
test('plans are exclusive, exactly approved, and reject altered coverage/cursors',()=>fixture(async({root,ctx,plan,planFile,approved})=>{
 await assert.rejects(makeVerificationPlan(ctx,root,planFile),/already exists/);await assert.rejects(loadVerificationPlan(planFile,sha('wrong'),ctx),/Exact/);
 const changed=structuredClone(plan);changed.tables[0].pages[0].count++;await durableJson(path.join(root,'bad.json'),changed);await assert.rejects(loadVerificationPlan(path.join(root,'bad.json'),sha(JSON.stringify(changed)),ctx),/coverage/);
 assert.equal((await loadVerificationPlan(planFile,approved,ctx)).pageCount,plan.pageCount);
}));
test('linked plan/checkpoint paths reject access and durable writes remain private',()=>fixture(async({root,ctx,planFile,approved})=>{
 const link=path.join(root,'linked.json');await fs.symlink(planFile,link);await assert.rejects(loadVerificationPlan(link,approved,ctx),/regular/);
 const cp=path.join(root,'cp.sqlite');(await checkpointStore(cp,{test:1},{create:true})).close();await fs.link(cp,path.join(root,'hard.sqlite'));await assert.rejects(checkpointStore(cp,{test:1}),/regular/);
 assert.equal((await fs.stat(planFile)).mode&0o777,0o600);
}));
test('transport retries are finite and limited to explicit transient read failures; cancellation interrupts cooldown',async()=>{
 let now=0,calls=0;const cfg={clock:()=>now,wait:async ms=>{now+=ms;}};
 await assert.rejects(retryRead(async()=>{calls++;throw new VerificationReadFailure(true);},cfg),/exhausted/);assert.equal(calls,4);assert.equal(now,65000);
 for(const error of [new VerificationReadFailure(false),Error('integrity'),Error('auth')]){calls=0;await assert.rejects(retryRead(async()=>{calls++;throw error;},cfg));assert.equal(calls,1);}
 const abort=new AbortController();calls=0;await assert.rejects(retryRead(async()=>{calls++;throw new VerificationReadFailure(true,300000);},{...cfg,signal:abort.signal,wait:async()=>abort.abort()}),/interrupted/);assert.equal(calls,1);
});
test('API status codes cannot disguise permission, content or integrity errors as retryable transports',()=>{
 for(const status of [200,429,500])for(const [code,message]of [[7403,'not authorized'],[10000,'authentication error'],[7500,'SQLITE_CORRUPT'],[7500,'integrity mismatch']])assert.equal(apiReadRetryable(status,JSON.stringify({success:false,errors:[{code,message}]})),false);
 assert.equal(apiReadRetryable(500,'<html>SQLITE_CORRUPT</html>'),false);assert.equal(apiReadRetryable(500,'upstream unavailable'),true);assert.equal(apiReadRetryable(429,'rate limit'),true);
 assert.equal(apiReadRetryable(200,JSON.stringify({success:false,errors:[{code:7429,message:'internal error; reference = synthetic'}]})),true);
});
test('version inspector exposes only bounded read-only metadata on its owned loopback listener',async()=>{
 let calls=0;const env={EXECUTION_ID:'owned',SOURCE_BUCKET:{list:async options=>{calls++;assert.equal(options.limit,1000);return {objects:[{key:'fixture',version:'unique',size:8,httpMetadata:{private:'omitted'}}],truncated:false};}}};
 const request=(url='http://127.0.0.1/versions',init={})=>new Request(url,{headers:{'x-recovery-execution':'owned',...init.headers},...init});
 assert.equal((await inspector.fetch(request(),env)).status,200);assert.deepEqual(await (await inspector.fetch(request(),env)).json(),{objects:[{key:'fixture',version:'unique',size:8}],truncated:false,cursor:null});
 for(const r of [request('http://outside.example/versions'),request(undefined,{headers:{origin:'http://outside','x-recovery-execution':'owned'}}),request(undefined,{headers:{'x-recovery-execution':'other'}}),request(undefined,{method:'PUT'}),request('http://127.0.0.1/versions?limit=10000')])assert.notEqual((await inspector.fetch(r,env)).status,200);
 assert.equal(calls,2);
});
test('scheduled metadata capture is one-shot, complete, read-only on originals, and conditional on a new private receipt',async()=>{
 const objects=[{key:'fixture',version:'unique-upload',size:8}],calls=[],env={CAPTURE_ID:'11111111-2222-4333-8444-555555555555',TARGET_SHA256:sha('target'),TOOL_SHA256:sha('tool'),SOURCE_BUCKET:{list:async options=>{calls.push(options);return {objects,truncated:false};}},PROOF_BUCKET:{head:async()=>null,put:async(key,body,options)=>{assert.equal(key,'versions/'+env.CAPTURE_ID+'.json');assert.deepEqual(options.onlyIf,{etagDoesNotMatch:'*'});const proof=JSON.parse(body);assert.deepEqual(proof.objects,objects);assert.equal(proof.captureId,env.CAPTURE_ID);assert.equal(proof.targetSha256,env.TARGET_SHA256);calls.push('put');}}};
 await capture.scheduled({},env);assert.equal(calls.length,2);assert.equal(calls[0].limit,1000);
 env.PROOF_BUCKET.head=async()=>({version:'already sealed'});await capture.scheduled({},env);assert.equal(calls.length,2,'a lost acknowledgment cannot rewrite a completed capture');
 await assert.rejects(capture.scheduled({},{...env,CAPTURE_ID:'wrong'}),/identity/);
});
test('scheduled version capture follows all pages and holds on repeated or incomplete provider cursors',async()=>{
 let reads=0,put=false;const env={CAPTURE_ID:'11111111-2222-4333-8444-555555555555',TARGET_SHA256:sha('target'),TOOL_SHA256:sha('tool'),SOURCE_BUCKET:{list:async()=>{reads++;return {objects:[],truncated:true,cursor:'same'};}},PROOF_BUCKET:{head:async()=>null,put:async()=>{put=true;}}};
 await assert.rejects(capture.scheduled({},env),/cursor did not advance/);assert.equal(reads,2);assert.equal(put,false);
 env.SOURCE_BUCKET.list=async()=>({objects:[],truncated:true});await assert.rejects(capture.scheduled({},env),/cursor/);assert.equal(put,false);
});
