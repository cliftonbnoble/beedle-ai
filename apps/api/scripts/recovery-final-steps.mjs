// Reviewed final-range recovery followed by the unchanged full D1/R2 verifier.
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {query} from './recovery.mjs';
import {loadParallelPlan,parallelRecovery,transientReadFailure} from './recovery-parallel.mjs';
import {loadSqlShardPlan} from './lib/recovery-sql-shards.mjs';
import {loadFinalStepPlan,applyFinalSqlSteps} from './lib/recovery-final-steps.mjs';
import {sha,atomicJson,locked} from './lib/recovery-bundle.mjs';
const require=createRequire(import.meta.url),here=path.dirname(fileURLToPath(import.meta.url)),apiRoot=path.resolve(here,'..');
const read=async file=>JSON.parse(await fs.readFile(file,'utf8'));
async function inspect(ep,sql,dir){
 if(!/^SELECT\b/.test(sql))throw Error('Final-step inspections must be read-only');
 const file=path.join(dir,'wrangler-private.log');
 for(let attempt=0;attempt<4;attempt++){
  const offset=(await fs.stat(file).catch(()=>({size:0}))).size;
  try{return await query(ep,sql,dir);}catch(e){
   const data=await fs.readFile(file).catch(()=>Buffer.alloc(0));const latest=data.subarray(offset,offset+128*1024).toString();
   if(attempt===3||!transientReadFailure(latest))throw e;
   await new Promise(resolve=>setTimeout(resolve,/ - 429\b|HTTP\s*429\b/.test(latest)?300000:1000*2**attempt));
  }
 }
}
async function importOnce(ep,file,dir,index,timeout,signal){
 if(signal.aborted)throw Error('Operator interrupted');
 const out=await fs.open(path.join(dir,`final-step-${index}-private.log`),'a',0o600);
 const args=[require.resolve('wrangler'),'d1','execute','DB','--config',ep.config,ep.mode==='local'?'--local':'--remote',...(ep.mode==='local'?['--persist-to',ep.persistTo]:[]),'--yes','--file',file];
 const env={...process.env,WRANGLER_SEND_METRICS:'false',WRANGLER_LOG_PATH:path.join(dir,`final-step-${index}-detail-private.log`)};
 delete env.CLOUDFLARE_ACCOUNT_ID; // The sealed explicit target config owns account selection.
 if(ep.mode==='local')for(const name of ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL'])delete env[name];
 const child=spawn(process.execPath,args,{cwd:apiRoot,env,stdio:['ignore',out.fd,out.fd]});let force,timedOut=false;
 const stop=()=>{child.kill('SIGTERM');force=setTimeout(()=>child.kill('SIGKILL'),5000);};signal.addEventListener('abort',stop,{once:true});
 // Honor the ORIGINAL sealed import deadline. Never infer cancellation of the
 // remote operation from local termination; the pending checkpoint is retained.
 const timer=setTimeout(()=>{timedOut=true;stop();},timeout);
 try{const code=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});if(code!==0||timedOut||signal.aborted)throw Error('Final import outcome uncertain; inspect private status and provider receipt');}
 finally{clearTimeout(timer);clearTimeout(force);signal.removeEventListener('abort',stop);await out.close();}
}
export async function runFinalStepRecovery({bundle,target,originalPlanSha256,options}){
 bundle=path.resolve(bundle);target=path.resolve(target);
 const concurrency=options.concurrency??4,rate=options.requestsPerMinute??180;
 if(!Number.isSafeInteger(concurrency)||concurrency<1||concurrency>4||!Number.isSafeInteger(rate)||rate<1||rate>180)throw Error('Bounded file verification concurrency/rate required');
 for(const name of ['sqlShardPlan','finalStepPlan','resetProof'])if(!path.isAbsolute(options[name]||'')||!/^[a-f0-9]{64}$/.test(options[name+'Sha256']||''))throw Error('Explicit final-step plan/proof paths and checksums required');
 const abort=new AbortController(),stop=()=>abort.abort();process.on('SIGTERM',stop);process.on('SIGINT',stop);
 let state,status;
 try{
  await locked(bundle,async()=>{
   const ctx=await loadParallelPlan(bundle,target,originalPlanSha256);ctx.approvedPlanSha256=originalPlanSha256;
   const base=await loadSqlShardPlan(options.sqlShardPlan,options.sqlShardPlanSha256,ctx),plan=await loadFinalStepPlan(options.finalStepPlan,options.finalStepPlanSha256,ctx,base,options.sqlShardPlanSha256),proof=await read(options.resetProof);
   if(sha(JSON.stringify(proof))!==options.resetProofSha256)throw Error('Exact reviewed reset proof required');
   const executionId=randomUUID(),dir=path.join(bundle,'final-step-executions',executionId);await fs.mkdir(dir,{recursive:true,mode:0o700});
   const sources=[[fileURLToPath(import.meta.url),'controller-source.mjs'],[path.join(here,'lib/recovery-final-steps.mjs'),'final-step-library-source.mjs'],[path.join(here,'plan-recovery-final-steps.py'),'final-step-planner-source.py']];let controller='';
   for(const [file,name]of sources){const bytes=await fs.readFile(file);controller+=bytes.toString();await fs.writeFile(path.join(dir,name),bytes,{flag:'wx',mode:0o600});}
   state={executionId,pid:process.pid,startedAt:new Date().toISOString(),runId:ctx.manifest.runId,originalPlanSha256,shardPlanSha256:options.sqlShardPlanSha256,stepPlanSha256:options.finalStepPlanSha256,resetProofSha256:options.resetProofSha256,controllerSha256:sha(controller),originalToolSha256:ctx.originalToolSha,targetSha256:ctx.plan.targetSha256,executionDirectory:dir,phase:'guarding',backfillStarted:false};
   status=async patch=>{Object.assign(state,patch,{updatedAt:new Date().toISOString()});await atomicJson(path.join(bundle,'final-step-status.json'),state);};
   await atomicJson(path.join(dir,'execution.json'),state);await atomicJson(path.join(dir,'step-plan.json'),plan);await atomicJson(path.join(dir,'reset-proof.json'),proof);await status({phase:'final_step_import'});
   // Invalidate any earlier positive report before touching a recovery target.
   await atomicJson(path.join(bundle,'verification.json'),{runId:ctx.manifest.runId,dataVerified:false,readiness:'Final transport running; full D1/R2 verification required',executor:{executionId,controllerSha256:state.controllerSha256}});
   await applyFinalSqlSteps({bundle,ctx,base,baseSha:options.sqlShardPlanSha256,plan,approved:options.finalStepPlanSha256,resetProof:proof,dir,readQuery:sql=>inspect(ctx.ep,sql,dir),progress:status,signal:abort.signal,importFile:(file,index)=>importOnce(ctx.ep,file,dir,index,ctx.plan.importTimeoutMs,abort.signal)});
   await status({phase:'sql_complete_full_verification_required',sqlFinalized:true});
  });
  // Release/reacquire the SAME bundle lock in sequence, in this live process.
  // The original verifier must still pass every table and original file.
  if(abort.signal.aborted)throw Error('Operator interrupted before full verification');
  await status({phase:'original_full_verification'});
  const result=await parallelRecovery({action:'verify',bundle,target,planSha256:originalPlanSha256,options});
  await status({phase:'data_verified_not_promoted',dataVerified:result.dataVerified,completedAt:new Date().toISOString()});return result;
 }catch(e){if(status)await status({phase:'held',error:e instanceof Error?e.message:'Final recovery held',heldAt:new Date().toISOString()});throw e;}
 finally{process.removeListener('SIGTERM',stop);process.removeListener('SIGINT',stop);}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const [bundle,target,originalPlanSha256,optionsFile,...extra]=process.argv.slice(2);
 if(!optionsFile||extra.length)throw Error('Usage: recovery-final-steps.mjs BUNDLE TARGET ORIGINAL_PLAN_SHA OPTIONS');
 runFinalStepRecovery({bundle,target,originalPlanSha256,options:await read(path.resolve(optionsFile))}).then(result=>console.log(JSON.stringify({dataVerified:result.dataVerified,runId:result.runId}))).catch(()=>{console.error('Final recovery held; inspect private final-step-status.json and provider receipts.');process.exitCode=1;});
}
