// Additive executor for an existing sealed recovery plan. The original recovery
// tool, bundle, plan, SQL and target claim remain unchanged and are all checked.
import fs from 'node:fs/promises';
import {createReadStream,createWriteStream} from 'node:fs';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {createGzip,createGunzip} from 'node:zlib';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import {createHash,randomUUID} from 'node:crypto';
import {query} from './recovery.mjs';
import {sha,fileSha,atomicJson,locked,rowExpression,ident,typedSql,canonicalRow,claimStatements} from './lib/recovery-bundle.mjs';
import {loadSqlShardPlan,applySqlShards} from './lib/recovery-sql-shards.mjs';
const require=createRequire(import.meta.url),here=path.dirname(fileURLToPath(import.meta.url)),apiRoot=path.resolve(here,'..');
const read=async file=>JSON.parse(await fs.readFile(file,'utf8'));
export function transientReadFailure(message){
 const text=message.replace(/\u001b\[[0-9;]*m/g,'');
 if(/SQLITE_\w+|no such (?:table|column|function)|constraint (?:failed|violation)|corrupt|malformed|checksum|integrity|conflicting|authentication error|not authorized|unauthorized|forbidden|\[code:\s*(?:7403|10000)\]|HTTP\s*(?:401|403)\b/i.test(text))return false;
 return / - (?:429|5\d\d)\b|HTTP\s*(?:429|5\d\d)\b|fetch failed|ECONNRESET|ETIMEDOUT/i.test(text)
  ||(/\binternal error;\s*reference\s*=/i.test(text)&&/\[code:\s*7500\]/i.test(text))
  ||(/\bUpstream service unavailable\b/i.test(text)&&/\[code:\s*7009\]/i.test(text));
}
// Wrangler debug files include response bodies. Keep them losslessly, bounded
// between sequential reads, rather than consuming the recovery disk reserve.
// Never truncate until the durable archive has passed full decompressed readback.
export async function archiveRecoveryDebugLog(dir,threshold=64*1024*1024){
 if(!Number.isSafeInteger(threshold)||threshold<1)throw Error('Positive debug archive bound required');
 const file=path.join(dir,'wrangler-detail-private.log');let before;
 try{before=await fs.lstat(file);}catch(e){if(e.code==='ENOENT')return;throw e;}
 if(!before.isFile()||before.nlink!==1)throw Error('Recovery debug log must be an owned regular file');
 if(before.size<threshold||before.size===0)return;
 const archives=path.join(dir,'debug-log-archives');await fs.mkdir(archives,{recursive:true,mode:0o700});
 if(!(await fs.lstat(archives)).isDirectory())throw Error('Recovery debug archive directory differs');
 const name=randomUUID(),temporary=path.join(archives,name+'.tmp'),archive=path.join(archives,name+'.log.gz');
 const sourceHash=createHash('sha256');let bytes=0;
 const input=createReadStream(file);input.on('data',chunk=>{sourceHash.update(chunk);bytes+=chunk.length;});
 await pipeline(input,createGzip({level:1}),createWriteStream(temporary,{flags:'wx',mode:0o600}));
 const hash=sourceHash.digest('hex'),readback=createHash('sha256');let verifiedBytes=0;
 await pipeline(createReadStream(temporary),createGunzip(),new Writable({write(chunk,encoding,callback){readback.update(chunk);verifiedBytes+=chunk.length;callback();}}));
 if(bytes!==before.size||verifiedBytes!==bytes||readback.digest('hex')!==hash)throw Error('Recovery debug archive readback differs');
 const unchanged=stat=>stat.isFile()&&stat.nlink===1&&stat.ino===before.ino&&stat.size===before.size&&stat.mtimeMs===before.mtimeMs;
 if(!unchanged(await fs.lstat(file)))throw Error('Recovery debug log changed during archival');
 const compressed=await fs.open(temporary,'r');try{await compressed.sync();}finally{await compressed.close();}
 await fs.rename(temporary,archive);
 const proof=path.join(archives,name+'.json');await atomicJson(proof,{archivedAt:new Date().toISOString(),source:'wrangler-detail-private.log',bytes,sha256:hash,archive:path.basename(archive),archiveSha256:await fileSha(archive),decompressedReadbackVerified:true});
 const metadata=await fs.open(proof,'r');try{await metadata.sync();}finally{await metadata.close();}
 const directory=await fs.open(archives,'r');try{await directory.sync();}finally{await directory.close();}
 if(!unchanged(await fs.lstat(file)))throw Error('Recovery debug log changed before truncation');
 const original=await fs.open(file,'r+');try{
  if(!unchanged(await original.stat()))throw Error('Recovery debug log identity differs');
  await original.truncate(0);await original.sync();
 }finally{await original.close();}
}
// Do not begin a remote inspection near the existing CLI token's expiry. Drain
// new starts, let pinned Wrangler refresh its existing grant, and prove the file
// advanced before querying. Authorization errors themselves are NEVER retried.
export async function ensureRecoveryReadSession(ep,dir,{clock=Date.now,sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms)),refresh,env=process.env}={}){
 const check=()=>{if(ep.recoverySignal?.aborted)throw Error('Operator interrupted before read session');};check();
 if(ep.mode!=='remote'||!ep.recoveryOAuthFile||env.CLOUDFLARE_API_TOKEN||env.CLOUDFLARE_API_KEY)return;
 if(!path.isAbsolute(ep.recoveryOAuthFile))throw Error('Explicit existing Wrangler session path required');
 const expiry=async()=>{
  const text=await fs.readFile(ep.recoveryOAuthFile,'utf8'),matches=[...text.matchAll(/^expiration_time\s*=\s*"([^"\r\n]+)"\s*$/gm)];
  const value=matches.length===1?Date.parse(matches[0][1]):NaN;
  if(!Number.isFinite(value)||!/^oauth_token\s*=\s*"[^"\r\n]+"\s*$/m.test(text)||!/^refresh_token\s*=\s*"[^"\r\n]+"\s*$/m.test(text))throw Error('Existing Wrangler session metadata is invalid');
  return value;
 };
 const minimumRemainingMs=125000,expires=await expiry();if(expires>clock()+minimumRemainingMs)return;
 let waits=0;
 while(expires>=clock()){
  check();if(++waits>Math.ceil(minimumRemainingMs/1000)+5)throw Error('Wrangler session clock did not advance');
  await sleep(Math.min(1000,Math.max(1,expires-clock()+1)));
 }
 check();
 if(refresh)await refresh();
 else await runProcess(process.execPath,[require.resolve('wrangler'),'whoami'],{env:{CI:'1',WRANGLER_LOG_PATH:path.join(dir,'session-refresh-detail-private.log')},log:path.join(dir,'session-refresh-private.log'),timeout:120000,signal:ep.recoverySignal});
 check();const renewed=await expiry();if(renewed<=expires||renewed<=clock()+minimumRemainingMs)throw Error('Existing Wrangler session needs sign-in');
}
export async function readQuery(ep,sql,dir,{execute=query,sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms)),archive=archiveRecoveryDebugLog,session=ensureRecoveryReadSession}={}){
 if(!/^\s*(?:SELECT\b|PRAGMA\s+foreign_key_check\b)/i.test(sql))throw Error('Read retry permits only fixed inspections');
 const log=path.join(dir,'wrangler-private.log');
 for(let attempt=0;attempt<4;attempt++){
  await session(ep,dir); // Also recheck after a long rate-limit/transport backoff.
  const offset=(await fs.stat(log).catch(()=>({size:0}))).size;
  let result;
  try{result=await execute(ep,sql,dir);}catch(error){
   const bytes=await fs.readFile(log).catch(()=>Buffer.alloc(0));const current=bytes.subarray(offset,offset+128*1024).toString('utf8').replace(/\u001b\[[0-9;]*m/g,'');
   if(attempt===3||!transientReadFailure(current))throw error;
   await sleep(/ - 429\b|HTTP\s*429\b/.test(current)?300000:5000*3**attempt);
   continue;
  }
  await archive(dir);return result;
 }
}
export async function loadParallelPlan(bundle,targetFile,approved){
 const manifest=await read(path.join(bundle,'bundle.json')),plan=await read(path.join(bundle,'plan.json'));
 const originalToolSha=sha(await fs.readFile(path.join(here,'recovery.mjs'),'utf8')+await fs.readFile(path.join(here,'lib/recovery-bundle.mjs'),'utf8'));
 if(manifest.version!==1||plan.version!==1||manifest.toolSha256!==originalToolSha||manifest.contractSha256!==await fileSha(path.join(apiRoot,'release/contract.json')))throw Error('Original sealed tool or contract differs');
 if(sha(JSON.stringify(plan))!==approved||plan.runId!==manifest.runId||plan.bundleSha256!==sha(JSON.stringify(manifest)))throw Error('Exact original plan checksum/identity required');
 if(JSON.stringify(Object.keys(manifest.files).sort())!==JSON.stringify(['restore.sqlite','source.sqlite']))throw Error('Incomplete snapshot inventory');
 for(const [name,hash]of Object.entries(manifest.files))if(await fileSha(path.join(bundle,name))!==hash)throw Error('Bundle snapshot checksum mismatch');
 if(await fileSha(path.join(bundle,'restore.sql'))!==plan.sql.sha256||!Number.isSafeInteger(plan.sql.bytes)||(await fs.stat(path.join(bundle,'restore.sql'))).size!==plan.sql.bytes||plan.sql.bytes>plan.sql.maxSqlBytes)throw Error('Sealed SQL differs');
 const spec=await read(targetFile);if(!['local','remote'].includes(spec.mode)||!path.isAbsolute(spec.config))throw Error('Explicit target mode/config required');
 const config=await read(spec.config);if(config.env||config.d1_databases?.length!==1||config.r2_buckets?.filter(b=>b.binding==='SOURCE_BUCKET').length!==1)throw Error('Dedicated flat target DB/bucket required');
 const db=config.d1_databases.find(x=>x.binding==='DB'),bucket=config.r2_buckets.find(x=>x.binding==='SOURCE_BUCKET');
 if(!db?.database_id||!db.database_name||!bucket?.bucket_name||spec.purpose!=='isolated-recovery'||spec.writersQuiesced!==true||spec.freshBucket!==true||config.vars?.VECTOR_LIFECYCLE_READY!=='false'||config.vars?.VECTOR_FILTERS_READY!=='false'||config.vars?.RELEASE_MAINTENANCE_ONLY!=='true'||config.queues?.consumers?.length||config.triggers?.crons?.length)throw Error('Quiesced private lexical-only recovery target required');
 if(spec.mode==='remote'&&!/^[a-f0-9]{32}$/.test(config.account_id||''))throw Error('Explicit remote account required');
 if(spec.mode==='local'&&!path.isAbsolute(spec.persistTo||''))throw Error('Explicit local persistence required');
 const identity={mode:spec.mode,accountId:config.account_id||null,databaseId:db.database_id,databaseName:db.database_name,bucket:bucket.bucket_name,configSha256:await fileSha(spec.config),persistTo:spec.mode==='local'?await fs.realpath(spec.persistTo):null};
 if(sha(JSON.stringify(identity))!==plan.targetSha256||JSON.stringify(identity)!==JSON.stringify(plan.target))throw Error('Sealed target binding differs');
 const source=manifest.source,same=source.mode===identity.mode&&(source.mode==='remote'?source.accountId===identity.accountId:source.persistTo===identity.persistTo);
 if(same&&(source.databaseId===identity.databaseId||source.bucket===identity.bucket))throw Error('Source and target must remain distinct');
 const keys=new Set();for(const o of manifest.objects){
  if(keys.has(o.key)||typeof o.key!=='string'||o.key.includes('"')||o.key.includes('\0'))throw Error('Invalid/duplicate object key');keys.add(o.key);
  if(o.present&&(!/^[a-f0-9]{64}$/.test(o.sha256)||!Number.isSafeInteger(o.bytes)||o.bytes<0||o.bytes>32*1024*1024||await fileSha(path.join(bundle,'objects',o.sha256))!==o.sha256))throw Error('Bundle object checksum/size mismatch');
 }
 if(!Number.isSafeInteger(plan.importTimeoutMs)||plan.importTimeoutMs<120000||plan.importTimeoutMs>7200000)throw Error('Invalid sealed import deadline');
 return {manifest,plan,ep:{...spec,identity},originalToolSha};
}
async function runProcess(command,args,{cwd=apiRoot,env={},log,timeout=120000,signal,drainOnAbort=false}={}){
 if(signal?.aborted)throw Error('Operator interrupted');
 const file=await fs.open(log,'a',0o600);let timedOut=false;const child=spawn(command,args,{cwd,env:{...process.env,WRANGLER_SEND_METRICS:'false',...env},stdio:['ignore',file.fd,file.fd]});
 let force;const stop=()=>{child.kill('SIGTERM');if(!drainOnAbort)force=setTimeout(()=>child.kill('SIGKILL'),5000);};
 signal?.addEventListener('abort',stop,{once:true});const timer=setTimeout(()=>{timedOut=true;stop();},timeout);
 try{const code=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});if(code!==0||timedOut||signal?.aborted)throw Error('Private execution failed or timed out; inspect its status/logs');}
 finally{clearTimeout(timer);clearTimeout(force);signal?.removeEventListener('abort',stop);await file.close();}
}
async function schemaNames(ep,dir){return readQuery(ep,"SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' ORDER BY name",dir);}
function expectedReceipt(ctx){return {singleton:1,run_id:ctx.manifest.runId,bundle_sha256:ctx.plan.bundleSha256,target_sha256:ctx.plan.targetSha256};}
function assertReceipt(row,ctx){const expected=expectedReceipt(ctx);if(!row||Object.entries(expected).some(([key,value])=>row[key]!==value))throw Error('Target recovery owner differs');}
async function existingReceipt(ctx,dir){
 const names=await schemaNames(ctx.ep,dir);if(!names.some(x=>x.name==='beedle_recovery_receipt'))return null;
 const rows=await readQuery(ctx.ep,'SELECT * FROM beedle_recovery_receipt',dir);if(rows.length!==1)throw Error('Incomplete recovery receipt');assertReceipt(rows[0],ctx);return rows[0];
}
async function verifyD1(ctx,dir,progress,signal){
 if(!await existingReceipt(ctx,dir))throw Error('Committed recovery receipt required');
 const schema=await readQuery(ctx.ep,"SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' AND name NOT GLOB 'beedle_recovery_*' AND name NOT IN(SELECT name FROM pragma_table_list WHERE type='shadow') ORDER BY type,name",dir);
 if(sha(JSON.stringify(schema))!==ctx.manifest.restore.schemaSha256)throw Error('Restored schema differs');
 for(const table of ctx.manifest.restore.tables){
  const hash=createHash('sha256'),keys=table.cursor.columns.map(ident).join(',');
  const pageSize=Math.max(1,Math.min(128,Math.floor(8*1024*1024/Math.max(table.maxRowBytes,1))));let after=null,count=0;
  for(let page=0;page<=Math.ceil(table.count/pageSize);page++){
   if(signal?.aborted)throw Error('Operator interrupted');
   const where=after===null?'':`WHERE (${keys})>(${JSON.parse(after).map(typedSql).join(',')})`;
   const rows=await readQuery(ctx.ep,`SELECT ${rowExpression(table.cursor.columns)} AS cursor,${rowExpression(table.columns)} AS value FROM ${ident(table.name)} ${where} ORDER BY ${keys} LIMIT ${pageSize}`,dir);
   for(const row of rows){hash.update(canonicalRow(row.cursor)+':'+canonicalRow(row.value)+'\n');count++;}
   if(page%10===0)await progress({table:table.name,tableRows:count});
   if(rows.length<pageSize)break;
   const next=rows.at(-1).cursor;if(next===after)throw Error('Verification cursor did not advance');after=next;
  }
  if(count!==table.count||hash.digest('hex')!==table.sha256)throw Error('Restored typed table content differs');
  await progress({table:table.name,tableRows:count});
 }
 if((await readQuery(ctx.ep,'PRAGMA foreign_key_check',dir)).length)throw Error('Restored foreign keys differ');
}
export async function parallelRecovery({action='apply',bundle,target,planSha256,options={}}){
 bundle=path.resolve(bundle);target=path.resolve(target);
 const concurrency=options.concurrency??4,rate=options.requestsPerMinute??180;
 if(!['apply','verify'].includes(action)||!Number.isSafeInteger(concurrency)||concurrency<1||concurrency>4||!Number.isSafeInteger(rate)||rate<1||rate>180)throw Error('Bounded action/concurrency/rate required');
 if(Boolean(options.sqlShardPlan)!==Boolean(options.sqlShardPlanSha256)||(options.sqlShardPlan&&!path.isAbsolute(options.sqlShardPlan)))throw Error('Explicit absolute SQL shard plan and checksum required together');
 return locked(bundle,async()=>{
  const abort=new AbortController();const stop=()=>abort.abort();process.on('SIGTERM',stop);process.on('SIGINT',stop);
  const execute=async()=>{
  const ctx=await loadParallelPlan(bundle,target,planSha256);
  ctx.ep.recoveryOAuthFile=options.oauthFile;ctx.ep.recoverySignal=abort.signal;
  ctx.approvedPlanSha256=planSha256;
  const sqlShardPlan=options.sqlShardPlan?await loadSqlShardPlan(options.sqlShardPlan,options.sqlShardPlanSha256,ctx):null;
  if(abort.signal.aborted)throw Error('Operator interrupted');
  const executions=path.join(bundle,'parallel-executions');await fs.mkdir(executions,{recursive:true,mode:0o700});const executionId=randomUUID(),dir=path.join(executions,executionId);await fs.mkdir(dir,{mode:0o700});
  const controllerSource=await fs.readFile(fileURLToPath(import.meta.url),'utf8'),workerSource=await fs.readFile(path.join(here,'parallel-recovery-objects.py'),'utf8');
  const shardSource=await fs.readFile(path.join(here,'lib/recovery-sql-shards.mjs'),'utf8');
  const controllerSha=sha(controllerSource+workerSource+shardSource),workerFile=path.join(dir,'parallel-recovery-objects.py');
  await fs.writeFile(path.join(dir,'controller-source.mjs'),controllerSource,{mode:0o600,flag:'wx'});await fs.writeFile(workerFile,workerSource,{mode:0o600,flag:'wx'});
  await fs.writeFile(path.join(dir,'sql-shards-source.mjs'),shardSource,{mode:0o600,flag:'wx'});
  if(sqlShardPlan)await atomicJson(path.join(dir,'sql-shard-plan.json'),sqlShardPlan);
  const provenance={executionId,startedAt:new Date().toISOString(),pid:process.pid,runId:ctx.manifest.runId,planSha256,originalToolSha256:ctx.originalToolSha,controllerSha256:controllerSha,concurrency,requestsPerMinute:rate,targetSha256:ctx.plan.targetSha256,bundleSha256:ctx.plan.bundleSha256,...(sqlShardPlan?{sqlShardPlanSha256:options.sqlShardPlanSha256}:{})};
  await fs.writeFile(path.join(dir,'execution.json'),JSON.stringify(provenance,null,2)+'\n',{flag:'wx',mode:0o600});
  const state={...provenance,phase:'guarding',executionDirectory:dir,backfillStarted:false};const statusFile=path.join(bundle,'parallel-status.json');
  const status=async patch=>{Object.assign(state,patch,{updatedAt:new Date().toISOString()});await atomicJson(statusFile,state);};
  await status({});
  const objects=async operation=>{
   const cfg={bundle,mode:ctx.ep.mode,config:ctx.ep.config,configSha256:ctx.ep.identity.configSha256,persistTo:ctx.ep.persistTo,accountId:ctx.ep.identity.accountId,databaseId:ctx.ep.identity.databaseId,bucket:ctx.ep.identity.bucket,runId:ctx.manifest.runId,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256,manifestFileSha256:await fileSha(path.join(bundle,'bundle.json')),workerSha256:sha(workerSource),concurrency,requestsPerMinute:rate,oauthFile:options.oauthFile,caFile:options.caFile,logDir:dir,node:process.execPath,wrangler:require.resolve('wrangler'),apiRoot,operation,statusFile:path.join(dir,operation+'-status.json')};
   const configFile=path.join(dir,operation+'-config.json');await atomicJson(configFile,cfg);await status({phase:operation==='copy'?'objects_copy':'objects_verify',objectStatusFile:cfg.statusFile});
   await runProcess('python3',[workerFile,configFile],{log:path.join(dir,operation+'-private.log'),timeout:24*60*60*1000,signal:abort.signal,drainOnAbort:true});
   const result=await read(cfg.statusFile);if(result.phase!=='complete'||result.completed!==ctx.manifest.objects.filter(o=>o.present).length)throw Error('Incomplete parallel object coverage');
   await status({objectSummary:{operation,completed:result.completed,total:result.total,retries:result.retries,maxActive:result.maxActive}});
  };
  try{
   const checkpointFile=path.join(bundle,'checkpoint.json');let checkpoint;
   try{checkpoint=await read(checkpointFile);}catch(error){if(error.code!=='ENOENT')throw error;}
   if(checkpoint&&(checkpoint.runId!==ctx.manifest.runId||checkpoint.planSha256!==planSha256))throw Error('Checkpoint belongs to another run/plan');
   const verificationFile=path.join(bundle,'verification.json');
   try{await fs.copyFile(verificationFile,path.join(dir,'previous-verification.json'));}catch(error){if(error.code!=='ENOENT')throw error;}
   await atomicJson(verificationFile,{runId:ctx.manifest.runId,dataVerified:false,mismatches:[],readiness:'Verification running; never promoted',executor:{executionId,controllerSha256:controllerSha,planSha256}});
   // Partial sharded imports deliberately have an EMPTY completion-receipt table.
   // They resume only through exact contiguous shard receipts, never raw SQL replay.
   const namesBefore=await schemaNames(ctx.ep,dir);
   const shardedPartial=sqlShardPlan&&namesBefore.some(x=>x.name==='beedle_recovery_sql_chunks');
   let committed;
   if(shardedPartial){const rows=await readQuery(ctx.ep,'SELECT * FROM beedle_recovery_receipt',dir);if(rows.length){if(rows.length!==1)throw Error('Incomplete recovery receipt');assertReceipt(rows[0],ctx);committed=rows[0];}}
   else committed=await existingReceipt(ctx,dir);
   if(!committed||sqlShardPlan){
    if(action==='verify'&&!committed)throw Error('Committed recovery receipt required');
    if(abort.signal.aborted)throw Error('Operator interrupted');
    if(!shardedPartial&&namesBefore.some(row=>row.name!=='beedle_recovery_claim'))throw Error('Conflicting target schema/data');
    for(const sql of claimStatements({runId:ctx.manifest.runId,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256}))await query(ctx.ep,sql,dir);
    const rows=await query(ctx.ep,'SELECT * FROM beedle_recovery_claim',dir);if(rows.length!==1)throw Error('Incomplete target claim');assertReceipt(rows[0],ctx);
    if(!shardedPartial)await objects('copy');
    if(sqlShardPlan){
     await status({phase:'sql_sharded_import',sqlShardPlanSha256:options.sqlShardPlanSha256});
     await applySqlShards({bundle,ctx,plan:sqlShardPlan,planSha256:options.sqlShardPlanSha256,dir,readQuery:sql=>readQuery(ctx.ep,sql,dir),signal:abort.signal,progress:patch=>status(patch),importFile:async(file,index)=>{
      const args=['d1','execute','DB','--config',ctx.ep.config,ctx.ep.mode==='local'?'--local':'--remote',...(ctx.ep.mode==='local'?['--persist-to',ctx.ep.persistTo]:[]),'--yes','--file',file];
      const env={WRANGLER_LOG_PATH:path.join(dir,`sql-shard-${index}-detail-private.log`)};
      if(ctx.ep.mode==='local')for(const key of ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL'])env[key]='';
      await runProcess(process.execPath,[require.resolve('wrangler'),...args],{env,log:path.join(dir,`sql-shard-${index}-private.log`),timeout:Math.min(ctx.plan.importTimeoutMs,1200000),signal:abort.signal});
     }});
    }else{
    await atomicJson(checkpointFile,{runId:ctx.manifest.runId,planSha256,phase:'sql_acknowledgment_pending',executorSha256:controllerSha,updatedAt:new Date().toISOString()});
    await status({phase:'sql_import'});
    const args=['d1','execute','DB','--config',ctx.ep.config,ctx.ep.mode==='local'?'--local':'--remote',...(ctx.ep.mode==='local'?['--persist-to',ctx.ep.persistTo]:[]),'--yes','--file',path.join(bundle,'restore.sql')];
    const env={WRANGLER_LOG_PATH:path.join(dir,'import-detail-private.log')};
    if(ctx.ep.mode==='local')for(const key of ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL'])env[key]='';
    await runProcess(process.execPath,[require.resolve('wrangler'),...args],{env,log:path.join(dir,'import-private.log'),timeout:ctx.plan.importTimeoutMs,signal:abort.signal});
    }
   }
   await status({phase:'d1_verify'});await verifyD1(ctx,dir,patch=>status(patch),abort.signal);await objects('verify');
   const report={runId:ctx.manifest.runId,verifiedAt:new Date().toISOString(),dataVerified:true,mismatches:[],readiness:ctx.manifest.readiness,executor:{executionId,controllerSha256:controllerSha,originalToolSha256:ctx.originalToolSha,planSha256,concurrency,...(sqlShardPlan?{sqlShardPlanSha256:options.sqlShardPlanSha256}:{})}};
   await atomicJson(path.join(bundle,'verification.json'),report);
   await atomicJson(path.join(bundle,'checkpoint.json'),{runId:ctx.manifest.runId,planSha256,phase:'data_verified_not_promoted',executorSha256:controllerSha,updatedAt:new Date().toISOString()});
   await status({phase:'data_verified_not_promoted',dataVerified:true,completedAt:new Date().toISOString()});return report;
  }catch(error){await status({phase:'held',error:error instanceof Error?error.message:'Private execution failure',heldAt:new Date().toISOString()});throw error;}
  };
  try{return await execute();}finally{process.removeListener('SIGTERM',stop);process.removeListener('SIGINT',stop);}
 });
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const [action,bundle,target,planSha256,optionsFile,...extra]=process.argv.slice(2);
 if(!optionsFile||extra.length)throw Error('Usage: recovery-parallel.mjs apply|verify <bundle> <target.json> <plan-sha256> <options.json>');
 parallelRecovery({action,bundle,target,planSha256,options:await read(path.resolve(optionsFile))}).then(r=>console.log(JSON.stringify({dataVerified:r.dataVerified,runId:r.runId,readiness:r.readiness}))).catch(()=>{console.error('Parallel recovery held; inspect private parallel-status.json and execution logs.');process.exitCode=1;});
}
