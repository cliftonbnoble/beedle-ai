import fs from 'node:fs/promises';
import path from 'node:path';
import https from 'node:https';
import net from 'node:net';
import {spawn} from 'node:child_process';
import {createRequire} from 'node:module';
import {createHash,randomUUID} from 'node:crypto';
import {ensureRecoveryReadSession,transientReadFailure} from '../recovery-parallel.mjs';
import {durableJson} from './recovery-verification.mjs';
import {fileSha} from './recovery-bundle.mjs';
const require=createRequire(import.meta.url),apiRoot=path.resolve(import.meta.dirname,'../..');
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
export class VerificationReadFailure extends Error{
 constructor(retryable,delay=0){super(retryable?'Verification transport exhausted':'Verification request rejected');this.retryable=retryable;this.delay=delay;}
}
export function apiReadRetryable(status,body){
 const text=Buffer.isBuffer(body)?body.toString('utf8'):String(body);let value;
 try{value=JSON.parse(text);}catch{/* Preserve non-JSON error diagnostics too. */}
 const diagnostics=Array.isArray(value?.errors)?value.errors.map(e=>String(e.message)+' [code: '+String(e.code)+']').join('\n'):text;
 return transientReadFailure('HTTP '+status+'\n'+diagnostics);
}
export async function retryRead(fn,{wait=sleep,clock=Date.now,signal}={}){
 for(let attempt=0;attempt<4;attempt++){
  if(signal?.aborted)throw Error('Operator interrupted');
  try{return await fn();}catch(e){
   if(!(e instanceof VerificationReadFailure)||!e.retryable||attempt===3)throw e;
   const end=clock()+(e.delay||5000*3**attempt);while(clock()<end){if(signal?.aborted)throw Error('Operator interrupted');await wait(Math.min(1000,end-clock()));}
  }
 }
}
export function remoteReader(ep,dir,{caFile,requestsPerMinute=180,signal}={}){
 if(ep.mode!=='remote'||!Number.isSafeInteger(requestsPerMinute)||requestsPerMinute<1||requestsPerMinute>180)throw Error('Explicit bounded remote verification required');
 const agent=new https.Agent({keepAlive:true,maxSockets:4});let session=Promise.resolve(),starts=Promise.resolve(),next=0,until=0;
 const credentials=async()=>{
  session=session.then(()=>ensureRecoveryReadSession(ep,dir));await session;
  if(process.env.CLOUDFLARE_API_TOKEN)return process.env.CLOUDFLARE_API_TOKEN;
  const text=await fs.readFile(ep.recoveryOAuthFile,'utf8'),matches=[...text.matchAll(/^oauth_token\s*=\s*"([^"\r\n]+)"\s*$/gm)];
  if(matches.length!==1)throw Error('Explicit existing CLI session required');return matches[0][1];
 };
 const begin=()=>{
  // Serialize only dispatch, not reads. Set the next start AFTER any credential
  // drain so four waiting tasks cannot burst when a session resumes.
  const current=starts.then(async()=>{
   await credentials();
   while(Date.now()<Math.max(next,until)){if(signal?.aborted)throw Error('Operator interrupted');await sleep(Math.min(1000,Math.max(next,until)-Date.now()));}
   const token=await credentials();next=Date.now()+60000/requestsPerMinute;return token;
  });starts=current.then(()=>{},()=>{});return current;
 };
 const ca=caFile?fs.readFile(caFile):Promise.resolve(undefined);
 const raw=async(endpoint,limit,inspectEnvelope=false,allowMissing=false)=>retryRead(async()=>{
  const token=await begin(),loadedCA=await ca;
  const result=await new Promise((resolve,reject)=>{
   const request=https.request({hostname:'api.cloudflare.com',path:'/client/v4/accounts/'+ep.identity.accountId+endpoint,method:'GET',headers:{Authorization:'Bearer '+token},agent,ca:loadedCA,signal},response=>{
    const chunks=[];let bytes=0;response.on('data',chunk=>{bytes+=chunk.length;if(bytes>limit){response.destroy();reject(Error('Verification response exceeds byte bound'));}else chunks.push(chunk);});
    response.on('end',()=>resolve({status:response.statusCode,headers:response.headers,body:Buffer.concat(chunks)}));response.on('error',()=>reject(new VerificationReadFailure(true)));
   });
   // The approved CA is loaded before connecting, without global TLS overrides.
   request.on('error',e=>reject(new VerificationReadFailure(['ECONNRESET','ETIMEDOUT','EPIPE','ENETUNREACH','EAI_AGAIN'].includes(e.code))));
   request.setTimeout(60000,()=>request.destroy(Object.assign(Error('timeout'),{code:'ETIMEDOUT'})));request.end();
  });
  if(allowMissing&&result.status===404)return null;
  if(result.status===429){if(!apiReadRetryable(result.status,result.body))throw new VerificationReadFailure(false);const value=Number(result.headers['retry-after']),delay=Number.isFinite(value)&&value>=1?Math.min(value,3600)*1000:300000;until=Math.max(until,Date.now()+delay);throw new VerificationReadFailure(true,delay);}
  let envelope;const text=inspectEnvelope||result.status!==200?result.body.toString('utf8'):'';if((inspectEnvelope||result.status!==200)&&result.headers['content-type']?.includes('json'))try{envelope=JSON.parse(text);}catch{/* Invalid JSON is checked by the caller, never credited. */}
  if(result.status!==200||envelope?.success===false){
   await fs.appendFile(path.join(dir,'api-errors-private.log'),JSON.stringify({at:new Date().toISOString(),status:result.status,body:text.slice(0,65536)})+'\n',{mode:0o600});
   throw new VerificationReadFailure(apiReadRetryable(result.status,result.body));
  }
  return result.body;
 },{signal});
 // Pass CA per request via the private agent, not NODE_TLS_REJECT_UNAUTHORIZED.
 const ready=ca.then(value=>{if(value)agent.options.ca=value;});
 const api=async endpoint=>{await ready;const body=await raw(endpoint,4*1024*1024,true);let value;try{value=JSON.parse(body);}catch{throw Error('Invalid verification API JSON');}if(value.success!==true||!('result'in value))throw Error('Incomplete verification API response');return value.result;};
 return {
  api,
  optionalApi:async endpoint=>{await ready;const body=await raw(endpoint,4*1024*1024,true,true);if(body===null)return null;const v=JSON.parse(body);if(v.success!==true||!('result'in v))throw Error('Incomplete optional inspection response');return v.result;},
  bookmark:async()=>{const r=await api('/d1/database/'+ep.identity.databaseId+'/time_travel/bookmark');if(typeof r.bookmark!=='string'||!/^[a-f0-9-]{32,128}$/i.test(r.bookmark))throw Error('Current database bookmark missing');return r.bookmark;},
  get:async key=>{await ready;const bytes=await raw('/r2/buckets/'+encodeURIComponent(ep.identity.bucket)+'/objects/'+encodeURIComponent(key),32*1024*1024);return {sha256:createHash('sha256').update(bytes).digest('hex'),bytes:bytes.length};},
  proof:async(bucket,key)=>{await ready;const body=await raw('/r2/buckets/'+encodeURIComponent(bucket)+'/objects/'+encodeURIComponent(key),32*1024*1024,false,true);return body===null?null:JSON.parse(body);},
  close:()=>agent.destroy()
 };
}

// Only the separate, uniquely named auxiliary Worker is mutated by these CLI
// commands. The original target config, candidate DB and original files are not.
export async function captureScheduledVersions(ctx,dir,remote,{sourceFile,proofBucket,caFile,signal,progress=async()=>{}}){
 if(ctx.ep.mode!=='remote'||!/^beedle-recovery-proofs-[a-z0-9-]+$/.test(proofBucket||'')||[ctx.ep.identity.bucket,ctx.manifest.source.bucket].includes(proofBucket))throw Error('Distinct private recovery-proof bucket required');
 const domains='/r2/buckets/'+encodeURIComponent(proofBucket)+'/domains/';
 if((await remote.api(domains+'managed')).enabled!==false||JSON.stringify((await remote.api(domains+'custom')).domains)!=='[]')throw Error('Version proof bucket is public');
 const captureId=randomUUID(),name='beedle-recovery-verify-'+captureId.replaceAll('-','').slice(0,16),toolSha256=await fileSha(sourceFile),configFile=path.join(dir,'scheduled-version-config.json');
 const config={name,main:sourceFile,account_id:ctx.ep.identity.accountId,compatibility_date:'2026-10-28',workers_dev:false,preview_urls:false,observability:{enabled:false},triggers:{crons:['* * * * *']},vars:{CAPTURE_ID:captureId,TARGET_SHA256:ctx.plan.targetSha256,TOOL_SHA256:toolSha256},r2_buckets:[{binding:'SOURCE_BUCKET',bucket_name:ctx.ep.identity.bucket},{binding:'PROOF_BUCKET',bucket_name:proofBucket}]};
 await durableJson(configFile,config);const configSha256=await fileSha(configFile);
 await durableJson(path.join(dir,'scheduled-version-ownership.json'),{name,captureId,toolSha256,configSha256,proofBucket,targetSha256:ctx.plan.targetSha256,phase:'deploy_pending'});
 const cli=async(args,{cleanup=false}={})=>{
  if(await fileSha(configFile)!==configSha256||await fileSha(sourceFile)!==toolSha256)throw Error('Scheduled version inspector changed');
  await ensureRecoveryReadSession(cleanup?{...ctx.ep,recoverySignal:undefined}:ctx.ep,dir);
  const log=await fs.open(path.join(dir,'scheduled-version-private.log'),'a',0o600),env={...process.env,CI:'1',WRANGLER_SEND_METRICS:'false',WRANGLER_LOG_PATH:path.join(dir,'scheduled-version-detail-private.log')};delete env.CLOUDFLARE_ACCOUNT_ID;
  const child=spawn(process.execPath,[require.resolve('wrangler'),...args,'--config',configFile],{cwd:apiRoot,env,stdio:['ignore',log.fd,log.fd]});let timedOut=false;
  const timer=setTimeout(()=>{timedOut=true;child.kill('SIGTERM');},120000);
  try{const code=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});if(code!==0||timedOut)throw Error('Private version helper CLI held; inspect owned resource');}finally{clearTimeout(timer);await log.close();}
 };
 let deployed=false;
 try{
  if(signal?.aborted)throw Error('Operator interrupted');
  await progress({versionCapture:{name,captureId,phase:'deploy_pending'}});
  // A collision, including a lost previous owner, is a hold. Never overwrite it.
  if(await remote.optionalApi('/workers/scripts/'+name+'/settings')!==null)throw Error('Version helper name already owned');
  deployed=true;await cli(['deploy']);
  const settings=await remote.api('/workers/scripts/'+name+'/settings'),binding=settings.bindings?.find(b=>b.name==='CAPTURE_ID');
  if(binding?.text!==captureId||settings.bindings?.find(b=>b.name==='TOOL_SHA256')?.text!==toolSha256||settings.bindings?.find(b=>b.name==='SOURCE_BUCKET')?.bucket_name!==ctx.ep.identity.bucket||settings.bindings?.find(b=>b.name==='PROOF_BUCKET')?.bucket_name!==proofBucket)throw Error('Scheduled helper ownership differs');
  const access=await remote.api('/workers/scripts/'+name+'/subdomain');if(access.enabled!==false||access.previews_enabled!==false)throw Error('Scheduled version helper has public access');
  await progress({versionCapture:{name,captureId,phase:'waiting_for_private_receipt'}});
  // Newly created Cron Triggers can take up to 15 minutes to propagate.
  const end=Date.now()+20*60*1000;
  while(Date.now()<end){
   if(signal?.aborted)throw Error('Operator interrupted');
   const proof=await remote.proof(proofBucket,'versions/'+captureId+'.json');
   if(proof!==null){if(proof.version!==1||proof.captureId!==captureId||proof.targetSha256!==ctx.plan.targetSha256||proof.toolSha256!==toolSha256||!Number.isFinite(Date.parse(proof.capturedAt))||!Array.isArray(proof.objects))throw Error('Private version capture identity differs');await durableJson(path.join(dir,'version-capture-proof.json'),proof);return proof.objects;}
   await sleep(5000);
  }throw Error('Private version capture deadline exceeded');
 }finally{
  if(deployed){
   // Inspect exact ownership even after a lost deploy acknowledgment. A missing
   // deployment or differing owner needs operator review; never blind-delete.
   const cleanupReader=remoteReader({...ctx.ep,recoverySignal:undefined},dir,{caFile});
   try{
    const settings=await cleanupReader.optionalApi('/workers/scripts/'+name+'/settings');
    if(settings!==null){if(settings.bindings?.find(b=>b.name==='CAPTURE_ID')?.text!==captureId||settings.bindings?.find(b=>b.name==='TOOL_SHA256')?.text!==toolSha256)throw Error('Version helper cleanup ownership unresolved');await cli(['delete','--force'],{cleanup:true});}
    await progress({versionCapture:{name,captureId,phase:'deleted'}});
    await durableJson(path.join(dir,'scheduled-version-ownership.json'),{name,captureId,toolSha256,configSha256,proofBucket,targetSha256:ctx.plan.targetSha256,phase:'deleted',completedAt:new Date().toISOString()});
   }finally{cleanupReader.close();}
  }
 }
}
async function availablePort(){const server=net.createServer();await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});const port=server.address().port;await new Promise(resolve=>server.close(resolve));return port;}
export async function startVersionInspector(ctx,dir,executionId,{signal,sourceFile}={}){
 if(signal?.aborted)throw Error('Operator interrupted');
 const port=await availablePort(),configFile=path.join(dir,'version-inspector-config.json');
 const config={name:'beedle-private-recovery-inspector',main:sourceFile,compatibility_date:'2026-10-28',workers_dev:false,preview_urls:false,...(ctx.ep.mode==='remote'?{account_id:ctx.ep.identity.accountId}:{}),vars:{EXECUTION_ID:executionId},r2_buckets:[{binding:'SOURCE_BUCKET',bucket_name:ctx.ep.identity.bucket,...(ctx.ep.mode==='remote'?{remote:true}:{})}],observability:{enabled:false}};
 await durableJson(configFile,config);
 const log=await fs.open(path.join(dir,'version-inspector-private.log'),'a',0o600),env={...process.env,CI:'1',WRANGLER_SEND_METRICS:'false',WRANGLER_LOG_PATH:path.join(dir,'version-inspector-detail-private.log')};
 delete env.CLOUDFLARE_ACCOUNT_ID;if(ctx.ep.mode==='local')for(const key of ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL'])delete env[key];
 const child=spawn(process.execPath,[require.resolve('wrangler'),'dev','--config',configFile,'--ip','127.0.0.1','--port',String(port),'--inspector-port','0',...(ctx.ep.mode==='local'?['--persist-to',ctx.ep.persistTo]:[])],{cwd:apiRoot,env,detached:true,stdio:['ignore',log.fd,log.fd]});
 let spawnError;child.on('error',e=>{spawnError=e;});
 const close=async()=>{
  if(child.exitCode===null&&child.signalCode===null&&!spawnError){const exited=new Promise(resolve=>child.once('close',resolve));try{process.kill(-child.pid,'SIGTERM');}catch(e){if(e.code!=='ESRCH')throw e;}await Promise.race([exited,sleep(5000)]);if(child.exitCode===null&&child.signalCode===null){try{process.kill(-child.pid,'SIGKILL');}catch(e){if(e.code!=='ESRCH')throw e;}await exited;}}
  await log.close();
 };
 const request=async route=>{
  if(signal?.aborted)throw Error('Operator interrupted');
  const response=await fetch('http://127.0.0.1:'+port+route,{headers:{'x-recovery-execution':executionId},signal:AbortSignal.timeout(60000)}).catch(()=>{throw new VerificationReadFailure(true);});
  if(!response.ok){await response.body?.cancel();throw new VerificationReadFailure(response.status>=500||response.status===429);}
  const chunks=[];let bytes=0;for await(const chunk of response.body){bytes+=chunk.length;if(bytes>2*1024*1024)throw Error('Version inventory exceeds page bound');chunks.push(chunk);}return JSON.parse(Buffer.concat(chunks).toString('utf8'));
 };
 try{
  const until=Date.now()+90000;let ready=false;
  while(Date.now()<until){if(signal?.aborted||spawnError||child.exitCode!==null)throw Error('Private version inspector unavailable');try{const r=await request('/ready');if(r.executionId!==executionId)throw Error('Private inspector owner differs');ready=true;break;}catch(e){if(!(e instanceof VerificationReadFailure))throw e;}await sleep(500);}
  if(!ready)throw Error('Private version inspector startup deadline exceeded');
  return {pid:child.pid,close,versions:async()=>{
   const all=[],seen=new Set();let cursor=null;
   for(let page=0;page<10000;page++){
    const r=await retryRead(()=>request('/versions'+(cursor===null?'':'?cursor='+encodeURIComponent(cursor))),{signal});
    if(!Array.isArray(r.objects)||r.objects.length>1000||typeof r.truncated!=='boolean')throw Error('Incomplete version inventory');all.push(...r.objects);
    if(!r.truncated)return all;
    if(typeof r.cursor!=='string'||!r.cursor||seen.has(r.cursor))throw Error('Version inventory cursor did not advance');seen.add(r.cursor);cursor=r.cursor;
   }throw Error('Version inventory exceeds coverage bound');
  }};
 }catch(e){await close();throw e;}
}
