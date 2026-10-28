// A reviewed subdivision of ONLY an uncommitted final range. Original SQL and
// parent receipts stay intact. No automatic retry of any uncertain write.
import fs from 'node:fs/promises';
import path from 'node:path';
import {sha,fileSha,atomicJson,literal} from './recovery-bundle.mjs';
import {shardReceipt,verifyShardPrefix} from './recovery-sql-shards.mjs';
const hash=/^[a-f0-9]{64}$/;
const outerColumns='chunk_index,original_plan_sha256,shard_plan_sha256,sql_sha256,sql_offset,sql_bytes';
const innerColumns='step_index,step_plan_sha256,original_plan_sha256,shard_plan_sha256,parent_chunk_index,sql_sha256,sql_offset,sql_bytes';
const same=(a,b)=>Object.keys(a).length===Object.keys(b).length&&Object.entries(b).every(([k,v])=>a[k]===v);
export async function loadFinalStepPlan(file,approved,ctx,base,baseSha){
 const p=JSON.parse(await fs.readFile(file,'utf8')),last=base.chunks.at(-1);
 if(!hash.test(approved||'')||sha(JSON.stringify(p))!==approved||p.version!==1||p.kind!=='isolated-final-range-steps'||p.originalPlanSha256!==ctx.approvedPlanSha256||p.shardPlanSha256!==baseSha||p.originalSqlSha256!==ctx.plan.sql.sha256||p.foreignKeyGraphAcyclic!==true||!same(p.parentChunk,last)||last.index<1||!p.steps?.length)throw Error('Exact final-step plan and parent required');
 let offset=last.offset,count=0;
 for(const [index,s]of p.steps.entries()){
  if(s.index!==index||s.offset!==offset||!Number.isSafeInteger(s.bytes)||s.bytes<1||s.bytes>8*1024*1024||!Number.isSafeInteger(s.statements)||s.statements<1||s.statements>128||!hash.test(s.sha256)||!['data','ddl','large-value','finalize'].includes(s.kind))throw Error('Invalid final-step coverage');
  if((s.kind==='data'&&(s.bytes>1024*1024||s.statements>8))||(s.kind==='ddl'&&s.statements!==1)||(s.kind==='finalize')!==(index===p.steps.length-1))throw Error('Final-step bounds/finalization differ');
  offset+=s.bytes;count+=s.statements;
 }
 if(offset!==last.offset+last.bytes||count!==last.statements)throw Error('Incomplete final parent coverage');
 return p;
}
export function finalStepReceipt(s,p,approved){return {step_index:s.index,step_plan_sha256:approved,original_plan_sha256:p.originalPlanSha256,shard_plan_sha256:p.shardPlanSha256,parent_chunk_index:p.parentChunk.index,sql_sha256:s.sha256,sql_offset:s.offset,sql_bytes:s.bytes};}
export function verifyFinalStepPrefix(rows,p,approved){
 if(rows.length>p.steps.length)throw Error('Unexpected final-step receipts');
 rows.forEach((row,i)=>{if(!same(row,finalStepReceipt(p.steps[i],p,approved)))throw Error('Final-step receipt prefix differs');});return rows.length;
}
export function finalStepSql(body,s,p,approved,ctx){
 const outer=shardReceipt(p.parentChunk,p.originalPlanSha256,p.shardPlanSha256),inner=finalStepReceipt(s,p,approved),values=row=>Object.values(row).map(v=>typeof v==='number'?v:literal(v)).join(',');
 const create=s.index===0?'CREATE TABLE beedle_recovery_sql_final_steps(step_index INTEGER PRIMARY KEY,step_plan_sha256 TEXT NOT NULL,original_plan_sha256 TEXT NOT NULL,shard_plan_sha256 TEXT NOT NULL,parent_chunk_index INTEGER NOT NULL,sql_sha256 TEXT NOT NULL,sql_offset INTEGER NOT NULL,sql_bytes INTEGER NOT NULL);\n':'';
 const claim=`EXISTS(SELECT 1 FROM beedle_recovery_claim WHERE singleton=1 AND run_id=${literal(ctx.manifest.runId)} AND bundle_sha256=${literal(ctx.plan.bundleSha256)} AND target_sha256=${literal(ctx.plan.targetSha256)})`;
 const guard=`CREATE TABLE beedle_recovery_final_step_assert(ok INTEGER CHECK(ok=1));\nINSERT INTO beedle_recovery_final_step_assert SELECT CASE WHEN ${claim} AND (SELECT count(*) FROM beedle_recovery_sql_chunks)=${p.parentChunk.index} AND NOT EXISTS(SELECT 1 FROM beedle_recovery_receipt) AND (SELECT count(*) FROM beedle_recovery_sql_final_steps)=${s.index} AND NOT EXISTS(SELECT 1 FROM beedle_recovery_sql_final_steps WHERE step_plan_sha256<>${literal(approved)} OR shard_plan_sha256<>${literal(p.shardPlanSha256)} OR original_plan_sha256<>${literal(p.originalPlanSha256)}) THEN 1 ELSE 0 END;\n`;
 return 'PRAGMA defer_foreign_keys=ON;\n'+create+guard+body+`INSERT INTO beedle_recovery_sql_final_steps VALUES(${values(inner)});\n`+(s.index===p.steps.length-1?`INSERT INTO beedle_recovery_sql_chunks VALUES(${values(outer)});\n`:'')+'DROP TABLE beedle_recovery_final_step_assert;\n';
}
export async function applyFinalSqlSteps({bundle,ctx,base,baseSha,plan,approved,resetProof,dir,readQuery,importFile,progress,signal}){
 if(await fileSha(path.join(bundle,'restore.sql'))!==plan.originalSqlSha256)throw Error('Original SQL changed before final steps');
 const outer=await readQuery(`SELECT ${outerColumns} FROM beedle_recovery_sql_chunks ORDER BY chunk_index`),prefix=verifyShardPrefix(outer,base,ctx.approvedPlanSha256,baseSha);
 if(prefix!==plan.parentChunk.index&&prefix!==base.chunks.length)throw Error('Final steps require the exact preceding parent prefix');
 const claim=await readQuery('SELECT * FROM beedle_recovery_claim');
 if(claim.length!==1||!same(claim[0],{singleton:1,run_id:ctx.manifest.runId,bundle_sha256:ctx.plan.bundleSha256,target_sha256:ctx.plan.targetSha256}))throw Error('Final-step target claim differs');
 const names=await readQuery("SELECT name FROM sqlite_schema WHERE type='table' AND name='beedle_recovery_sql_final_steps'");
 const getRows=()=>readQuery(`SELECT ${innerColumns} FROM beedle_recovery_sql_final_steps ORDER BY step_index`);
 let start=names.length?verifyFinalStepPrefix(await getRows(),plan,approved):0;
 if(names.length&&start===0)throw Error('Incomplete final-step ownership receipt');
 const completion=await readQuery('SELECT * FROM beedle_recovery_receipt');
 if(prefix===base.chunks.length){
  if(start!==plan.steps.length||completion.length!==1||!same(completion[0],{singleton:1,run_id:ctx.manifest.runId,bundle_sha256:ctx.plan.bundleSha256,target_sha256:ctx.plan.targetSha256}))throw Error('Final completion proof differs');return;
 }
 if(completion.length)throw Error('Premature completion receipt');
 const checkpointFile=path.join(bundle,'final-step-checkpoint.json');let prior;
 try{prior=JSON.parse(await fs.readFile(checkpointFile,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
 if(prior&&prior.stepPlanSha256!==approved)throw Error('Final checkpoint belongs to another plan');
 if(prior?.pendingStep!==undefined&&start<=prior.pendingStep)throw Error('Unacknowledged final step requires terminal provider review');
 if(!names.length){
  const pending=JSON.parse(await fs.readFile(path.join(bundle,'sql-shard-checkpoint.json'),'utf8'));
  if(pending.originalPlanSha256!==ctx.approvedPlanSha256||pending.shardPlanSha256!==baseSha||pending.pendingIndex!==plan.parentChunk.index)throw Error('Exact pending parent checkpoint required');
  if(resetProof?.providerTerminalReset!=='D1_RESET_DO'||resetProof.exactCommittedPrefix!==prefix||resetProof.pendingFinalRangeCommitted!==false||resetProof.targetClaimMatches!==true||resetProof.originalPlanSha256!==ctx.approvedPlanSha256||resetProof.sqlShardPlanSha256!==baseSha)throw Error('Reviewed terminal parent-reset proof required');
 }
 const handle=await fs.open(path.join(bundle,'restore.sql'),'r');
 try{
  for(const s of plan.steps.slice(start)){
   if(signal?.aborted)throw Error('Operator interrupted before final step');
   const body=Buffer.alloc(s.bytes),r=await handle.read(body,0,s.bytes,s.offset);if(r.bytesRead!==s.bytes||sha(body)!==s.sha256)throw Error('Final source byte range differs');
   const file=path.join(dir,`final-step-${String(s.index).padStart(4,'0')}.sql`);await fs.writeFile(file,finalStepSql(body.toString('utf8'),s,plan,approved,ctx),{mode:0o600,flag:'wx'});
   await atomicJson(checkpointFile,{stepPlanSha256:approved,originalPlanSha256:ctx.approvedPlanSha256,shardPlanSha256:baseSha,pendingStep:s.index,updatedAt:new Date().toISOString()});
   await progress({step:s.index,steps:plan.steps.length,kind:s.kind});
   await importFile(file,s.index); // Exactly one write attempt; uncertain outcomes hold.
   if(verifyFinalStepPrefix(await getRows(),plan,approved)!==s.index+1)throw Error('Final-step acknowledgment differs');
   await atomicJson(checkpointFile,{stepPlanSha256:approved,originalPlanSha256:ctx.approvedPlanSha256,shardPlanSha256:baseSha,completed:s.index+1,updatedAt:new Date().toISOString()});await fs.unlink(file);
  }
 }finally{await handle.close();}
 const end=await readQuery(`SELECT ${outerColumns} FROM beedle_recovery_sql_chunks ORDER BY chunk_index`);
 if(verifyShardPrefix(end,base,ctx.approvedPlanSha256,baseSha)!==base.chunks.length)throw Error('Final parent receipt not committed');
 const receipt=await readQuery('SELECT * FROM beedle_recovery_receipt');if(receipt.length!==1||!same(receipt[0],{singleton:1,run_id:ctx.manifest.runId,bundle_sha256:ctx.plan.bundleSha256,target_sha256:ctx.plan.targetSha256}))throw Error('Final recovery completion differs');
 await progress({step:plan.steps.length,steps:plan.steps.length,phase:'sql_complete_full_verification_required'});
}
