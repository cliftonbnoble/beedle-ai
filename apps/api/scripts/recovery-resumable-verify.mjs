// A separate, read-only successor to the sealed recovery's full verifier.
import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {spawn} from 'node:child_process';
import {createRequire} from 'node:module';
import {loadParallelPlan,readQuery,transientReadFailure} from './recovery-parallel.mjs';
import {sha,fileSha,locked,open,describe} from './lib/recovery-bundle.mjs';
import {loadSqlShardPlan,verifyShardPrefix} from './lib/recovery-sql-shards.mjs';
import {loadFinalStepPlan,verifyFinalStepPrefix} from './lib/recovery-final-steps.mjs';
import {durableJson,regular,makeVerificationPlan,loadVerificationPlan,checkpointStore,schemaSql,objectVersions,verifyPages,verifyObjects} from './lib/recovery-verification.mjs';
import {remoteReader,startVersionInspector,captureScheduledVersions,retryRead,VerificationReadFailure} from './lib/recovery-verification-transport.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),require=createRequire(import.meta.url),apiRoot=path.resolve(here,'..');
const read=async file=>JSON.parse(await fs.readFile(file,'utf8'));
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
export async function inspectRecovery(ctx,inspect,options){
 const expected={singleton:1,run_id:ctx.manifest.runId,bundle_sha256:ctx.plan.bundleSha256,target_sha256:ctx.plan.targetSha256};
 for(const table of ['beedle_recovery_claim','beedle_recovery_receipt']){
  const rows=await inspect('SELECT * FROM '+table);if(rows.length!==1||!same(rows[0],expected))throw Error('Exact recovery ownership/completion required');
 }
 if(sha(JSON.stringify(await inspect(schemaSql)))!==ctx.manifest.restore.schemaSha256)throw Error('Restored schema differs');
 for(const name of ['sqlShardPlan','finalStepPlan'])if(Boolean(options[name])!==Boolean(options[name+'Sha256'])||(options[name]&&!path.isAbsolute(options[name])))throw Error('Explicit receipt plans and checksums required');
 if(options.sqlShardPlan){
  const base=await loadSqlShardPlan(options.sqlShardPlan,options.sqlShardPlanSha256,ctx);
  const rows=await inspect('SELECT chunk_index,original_plan_sha256,shard_plan_sha256,sql_sha256,sql_offset,sql_bytes FROM beedle_recovery_sql_chunks ORDER BY chunk_index');
  if(verifyShardPrefix(rows,base,ctx.approvedPlanSha256,options.sqlShardPlanSha256)!==base.chunks.length)throw Error('Incomplete committed SQL ranges');
  if(options.finalStepPlan){const plan=await loadFinalStepPlan(options.finalStepPlan,options.finalStepPlanSha256,ctx,base,options.sqlShardPlanSha256),steps=await inspect('SELECT step_index,step_plan_sha256,original_plan_sha256,shard_plan_sha256,parent_chunk_index,sql_sha256,sql_offset,sql_bytes FROM beedle_recovery_sql_final_steps ORDER BY step_index');if(verifyFinalStepPrefix(steps,plan,options.finalStepPlanSha256)!==plan.steps.length)throw Error('Incomplete committed final steps');}
 }else if(options.finalStepPlan)throw Error('Final steps require their parent plan');
}
async function localDatabaseRevision(ep){
 const directory=path.join(ep.persistTo,'v3/d1');let files=[];
 const walk=async dir=>{for(const item of await fs.readdir(dir,{withFileTypes:true})){const file=path.join(dir,item.name);if(item.isDirectory())await walk(file);else if(file.endsWith('.sqlite')){await regular(file);files.push(file);}}};
 await walk(directory);if(files.length!==1)throw Error('Single isolated local candidate database required');
 // Native Wrangler may write its own internal metadata on a read. Local mode
 // therefore re-hashes ALL ordinary typed content/schema, rather than trusting
 // a file mtime or ignoring WAL bytes. Cloud mode uses the provider bookmark.
 const db=open(files[0]);try{return sha(JSON.stringify(describe(db)));}finally{db.close();}
}
async function localObject(ctx,key,dir,signal){
 const file=path.join(dir,'object-read-'+randomUUID()),log=await fs.open(path.join(dir,'objects-private.log'),'a',0o600),env={...process.env,WRANGLER_SEND_METRICS:'false',WRANGLER_LOG_PATH:path.join(dir,'objects-detail-private.log')};
 for(const k of ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL'])delete env[k];
 const offset=(await log.stat()).size,child=spawn(process.execPath,[require.resolve('wrangler'),'r2','object','get',ctx.ep.identity.bucket+'/'+key,'--config',ctx.ep.config,'--local','--persist-to',ctx.ep.persistTo,'--file',file],{cwd:apiRoot,env,stdio:['ignore',log.fd,log.fd]});
 const stop=()=>child.kill('SIGTERM');signal.addEventListener('abort',stop,{once:true});const timer=setTimeout(stop,120000);
 try{const code=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});if(code!==0||signal.aborted){const text=(await fs.readFile(path.join(dir,'objects-private.log'),'utf8')).slice(offset);if(!signal.aborted&&/SQLITE_BUSY|database is locked|ECONNRESET|ETIMEDOUT|fetch failed/i.test(text))throw new VerificationReadFailure(true);throw Error('Local original read failed');}const s=await regular(file);if(s.size>32*1024*1024)throw Error('Original exceeds byte bound');return {sha256:await fileSha(file),bytes:s.size};}
 finally{clearTimeout(timer);signal.removeEventListener('abort',stop);await log.close();await fs.rm(file,{force:true});}
}
export async function runResumableVerification({action='verify',bundle,target,originalPlanSha256,options}){
 bundle=path.resolve(bundle);target=path.resolve(target);
 if(!['plan','verify'].includes(action)||!path.isAbsolute(options.verificationPlan||''))throw Error('Explicit verification plan/action required');
 if(action==='plan'){
  // Derivation is local/read-only and may run while the old verifier owns the
  // target. It creates only its distinct new derived artifact, never old files.
  const ctx=await loadParallelPlan(bundle,target,originalPlanSha256);ctx.approvedPlanSha256=originalPlanSha256;
  return makeVerificationPlan(ctx,bundle,options.verificationPlan);
 }
 const abort=new AbortController(),stop=()=>abort.abort();process.on('SIGINT',stop);process.on('SIGTERM',stop);
 try{return await locked(bundle,async()=>{
  const executionId=randomUUID(),dir=path.join(bundle,'resumable-verification-executions',executionId);await fs.mkdir(dir,{recursive:true,mode:0o700});
  let state={executionId,pid:process.pid,startedAt:new Date().toISOString(),phase:'guarding',dataVerified:false,backfillStarted:false,executionDirectory:dir};
  const status=async patch=>{state={...state,...patch,updatedAt:new Date().toISOString()};await durableJson(path.join(bundle,'resumable-verification-status.json'),state);};
  let store,inspector,remote;
  // Invalidate any previous positive result even if a subsequent plan/guard fails.
  await durableJson(path.join(bundle,'verification.json'),{dataVerified:false,executor:{executionId},readiness:'Full resumable verification running; not promoted'});await status({});
  try{
   const ctx=await loadParallelPlan(bundle,target,originalPlanSha256);ctx.approvedPlanSha256=originalPlanSha256;ctx.ep.recoveryOAuthFile=options.oauthFile;ctx.ep.recoverySignal=abort.signal;
   const concurrency=options.concurrency??4,rate=options.requestsPerMinute??180;
   if(!Number.isSafeInteger(concurrency)||concurrency<1||concurrency>4||!Number.isSafeInteger(rate)||rate<1||rate>180)throw Error('Reviewed concurrency/rate required');
   const sources=['recovery-resumable-verify.mjs','lib/recovery-verification.mjs','lib/recovery-verification-transport.mjs','lib/recovery-version-inspector.mjs','lib/recovery-version-capture.mjs','recovery-parallel.mjs','lib/recovery-sql-shards.mjs','lib/recovery-final-steps.mjs'];let sourceIdentity='';
   for(const file of sources){const bytes=await fs.readFile(path.join(here,file));sourceIdentity+=file+'\n'+sha(bytes)+'\n';await fs.writeFile(path.join(dir,file.replaceAll('/','-')),bytes,{flag:'wx',mode:0o600});}
   const controllerSha256=sha(sourceIdentity),plan=await loadVerificationPlan(options.verificationPlan,options.verificationPlanSha256,ctx);
   const inspect=async sql=>{
    const log=path.join(dir,'wrangler-private.log'),offset=(await fs.stat(log).catch(()=>({size:0}))).size;
    try{return await readQuery(ctx.ep,sql,dir);}catch(error){
     const current=(await fs.readFile(log).catch(()=>Buffer.alloc(0))).subarray(offset).toString('utf8');
     if(transientReadFailure(current))throw new VerificationReadFailure(true);throw error;
    }
   };
   const versionInventoryMode=ctx.ep.mode==='remote'?options.versionInventoryMode:'loopback';if(!['loopback','scheduled-private'].includes(versionInventoryMode))throw Error('Explicit file version inspection mode required');
   if(versionInventoryMode==='scheduled-private'&&(!/^beedle-recovery-proofs-[a-z0-9-]+$/.test(options.proofBucket||'')||[ctx.ep.identity.bucket,ctx.manifest.source.bucket].includes(options.proofBucket)))throw Error('Distinct private proof bucket required');
   const provenance={runId:ctx.manifest.runId,originalPlanSha256,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256,originalToolSha256:ctx.originalToolSha,verificationPlanSha256:options.verificationPlanSha256,controllerSha256,sqlShardPlanSha256:options.sqlShardPlanSha256??null,finalStepPlanSha256:options.finalStepPlanSha256??null,versionInventoryMode,proofBucket:versionInventoryMode==='scheduled-private'?options.proofBucket:null};
   await status(provenance);await durableJson(path.join(dir,'execution.json'),state);
   if(ctx.ep.mode==='remote')remote=remoteReader(ctx.ep,dir,{caFile:options.caFile,requestsPerMinute:rate,signal:abort.signal});
   const bookmark=()=>remote?remote.bookmark():localDatabaseRevision(ctx.ep);
   const initialBookmark=await bookmark();await inspectRecovery(ctx,inspect,options);
   if(remote){const base='/r2/buckets/'+encodeURIComponent(ctx.ep.identity.bucket)+'/domains/';if((await remote.api(base+'managed')).enabled!==false||!same((await remote.api(base+'custom')).domains,[]))throw Error('Candidate originals bucket is public');}
   if(await bookmark()!==initialBookmark)throw Error('Candidate changed during verification guards');
   const identity={version:1,...provenance,databaseBookmark:initialBookmark},checkpointFile=path.join(bundle,'resumable-verification.sqlite');
   store=await checkpointStore(checkpointFile,identity,{create:true});
   await status({phase:'d1_verify',checkpointFile,databaseBookmark:initialBookmark});
   const d1=await verifyPages({plan,store,read:inspect,progress:status,signal:abort.signal});
   if((await inspect('PRAGMA foreign_key_check')).length)throw Error('Restored foreign keys differ');
   if(await bookmark()!==initialBookmark)throw Error('Candidate changed during typed verification');
   await status({phase:'objects_verify',d1Summary:d1});
   // R2 has its own epoch: a file-inspection startup failure must not discard
   // already verified D1 pages. Short-lived inspectors also avoid stale preview
   // tokens across the many-hour database and object readback stages.
   const versions=async label=>{
    const inspectionDir=path.join(dir,label);await fs.mkdir(inspectionDir,{mode:0o700});
    if(versionInventoryMode==='scheduled-private')return objectVersions(plan,await captureScheduledVersions(ctx,inspectionDir,remote,{sourceFile:path.join(dir,'lib-recovery-version-capture.mjs'),proofBucket:options.proofBucket,caFile:options.caFile,signal:abort.signal,progress:status}));
    inspector=await startVersionInspector(ctx,inspectionDir,executionId,{signal:abort.signal,sourceFile:path.join(dir,'lib-recovery-version-inspector.mjs')});await status({inspectorPid:inspector.pid});
    try{return objectVersions(plan,await inspector.versions());}finally{await inspector.close();inspector=null;await status({inspectorPid:null});}
   };
   const objects=await versions('versions-before'),objectVersionsSha256=sha(JSON.stringify(objects));store.bindObjects({objectVersionsSha256});await status({objectVersionsSha256});
   const files=await verifyObjects({objects,store,get:key=>remote?remote.get(key):retryRead(()=>localObject(ctx,key,dir,abort.signal),{signal:abort.signal}),progress:status,signal:abort.signal,concurrency:ctx.ep.mode==='local'?1:concurrency});
   await status({phase:'final_guards',objectSummary:files});
   if(!same(await versions('versions-after'),objects))throw Error('Candidate original versions changed during verification');
   await inspectRecovery(ctx,inspect,options);
   if((await inspect('PRAGMA foreign_key_check')).length)throw Error('Restored foreign keys differ');
   if(await bookmark()!==initialBookmark)throw Error('Candidate changed before verification completion');
   if(remote){const base='/r2/buckets/'+encodeURIComponent(ctx.ep.identity.bucket)+'/domains/';if((await remote.api(base+'managed')).enabled!==false||!same((await remote.api(base+'custom')).domains,[]))throw Error('Candidate originals bucket privacy changed');}
   if(abort.signal.aborted)throw Error('Operator interrupted');
   const report={runId:ctx.manifest.runId,verifiedAt:new Date().toISOString(),dataVerified:true,mismatches:[],readiness:ctx.manifest.readiness,executor:{kind:'resumable-full-verification',executionId,...provenance,concurrency,requestsPerMinute:rate,databaseBookmark:initialBookmark,objectVersionsSha256},coverage:{...d1,...files}};
   await durableJson(path.join(dir,'verification.json'),report);await durableJson(path.join(bundle,'verification.json'),report);await status({phase:'data_verified_not_promoted',dataVerified:true,completedAt:new Date().toISOString()});return report;
  }catch(e){await status({phase:'held',dataVerified:false,error:e instanceof Error?e.message:'Verification held',retryableTransportFailure:e instanceof VerificationReadFailure&&e.retryable,resumePolicy:'same-plan-after-fresh-guards-only',heldAt:new Date().toISOString()});throw e;}
  finally{store?.close();await inspector?.close();remote?.close();}
 });}finally{process.removeListener('SIGINT',stop);process.removeListener('SIGTERM',stop);}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const [action,bundle,target,originalPlanSha256,optionsFile,...extra]=process.argv.slice(2);
 if(!optionsFile||extra.length)throw Error('Usage: recovery-resumable-verify.mjs plan|verify BUNDLE TARGET ORIGINAL_PLAN_SHA OPTIONS');
 runResumableVerification({action,bundle,target,originalPlanSha256,options:await read(path.resolve(optionsFile))}).then(r=>console.log(JSON.stringify(action==='plan'?{verificationPlanSha256:r.sha256,pages:r.plan.pageCount}:{dataVerified:r.dataVerified,coverage:r.coverage}))).catch(()=>{console.error('Resumable verification held; inspect private status and checkpoints.');process.exitCode=1;});
}
