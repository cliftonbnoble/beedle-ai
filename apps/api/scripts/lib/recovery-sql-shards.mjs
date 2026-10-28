// Additive bounded transactions over exact byte ranges of the ORIGINAL sealed SQL.
// A receipt commits atomically with each range. Any failed/uncertain write holds;
// a restart can skip only a complete, exact, contiguous remote receipt prefix.
import fs from 'node:fs/promises';
import path from 'node:path';
import {sha,fileSha,atomicJson,literal} from './recovery-bundle.mjs';
const hashPattern=/^[a-f0-9]{64}$/;
export async function loadSqlShardPlan(file,approved,ctx){
 const plan=JSON.parse(await fs.readFile(file,'utf8'));
 if(!hashPattern.test(approved||'')||sha(JSON.stringify(plan))!==approved||plan.version!==1||plan.originalPlanSha256!==ctx.approvedPlanSha256||plan.originalSqlSha256!==ctx.plan.sql.sha256||plan.originalSqlBytes!==ctx.plan.sql.bytes||plan.foreignKeyGraphAcyclic!==true)throw Error('Exact SQL shard plan/parent required');
 if(!Number.isSafeInteger(plan.maxChunkBytes)||plan.maxChunkBytes<1024||plan.maxChunkBytes>8*1024*1024||!Number.isSafeInteger(plan.maxChunkStatements)||plan.maxChunkStatements<8||plan.maxChunkStatements>128||!Array.isArray(plan.chunks)||!plan.chunks.length)throw Error('Invalid SQL shard bounds');
 let offset=0,statements=0;
 for(const [index,c]of plan.chunks.entries()){
  if(c.index!==index||c.offset!==offset||!Number.isSafeInteger(c.bytes)||c.bytes<1||c.bytes>plan.maxChunkBytes||!Number.isSafeInteger(c.statements)||c.statements<1||c.statements>plan.maxChunkStatements||!hashPattern.test(c.sha256))throw Error('Noncontiguous/invalid SQL shard inventory');
  offset+=c.bytes;statements+=c.statements;
 }
 if(offset!==plan.originalSqlBytes||statements!==plan.statementCount)throw Error('Incomplete SQL shard inventory');
 return plan;
}
export function shardReceipt(c,original,derived){return {chunk_index:c.index,original_plan_sha256:original,shard_plan_sha256:derived,sql_sha256:c.sha256,sql_offset:c.offset,sql_bytes:c.bytes};}
export function verifyShardPrefix(rows,plan,original,derived){
 if(rows.length>plan.chunks.length)throw Error('Unexpected SQL shard receipts');
 rows.forEach((row,index)=>{const expected=shardReceipt(plan.chunks[index],original,derived);if(Object.keys(row).length!==Object.keys(expected).length||Object.entries(expected).some(([k,v])=>row[k]!==v))throw Error('SQL shard receipt prefix differs');});
 return rows.length;
}
export function shardSql(body,c,{runId,bundleSha256,targetSha256,originalPlanSha256,shardPlanSha256}){
 const receipt=shardReceipt(c,originalPlanSha256,shardPlanSha256);
 const claim=`EXISTS(SELECT 1 FROM beedle_recovery_claim WHERE singleton=1 AND run_id=${literal(runId)} AND bundle_sha256=${literal(bundleSha256)} AND target_sha256=${literal(targetSha256)})`;
 const prefix=c.index===0?`CREATE TABLE beedle_recovery_sql_chunks(chunk_index INTEGER PRIMARY KEY,original_plan_sha256 TEXT NOT NULL,shard_plan_sha256 TEXT NOT NULL,sql_sha256 TEXT NOT NULL,sql_offset INTEGER NOT NULL,sql_bytes INTEGER NOT NULL);\n`:'';
 const guard=`CREATE TABLE beedle_recovery_chunk_assert(ok INTEGER CHECK(ok=1));\nINSERT INTO beedle_recovery_chunk_assert SELECT CASE WHEN ${claim} AND (SELECT count(*) FROM beedle_recovery_sql_chunks)=${c.index} AND NOT EXISTS(SELECT 1 FROM beedle_recovery_sql_chunks WHERE original_plan_sha256<>${literal(originalPlanSha256)} OR shard_plan_sha256<>${literal(shardPlanSha256)}) THEN 1 ELSE 0 END;\n`;
 // The original empty-schema assertion excludes only beedle_recovery_* helpers.
 // Repeated defer applies to each new transaction; cyclic FK snapshots are rejected.
 return 'PRAGMA defer_foreign_keys=ON;\n'+prefix+guard+body+`INSERT INTO beedle_recovery_sql_chunks VALUES(${Object.values(receipt).map(v=>typeof v==='number'?v:literal(v)).join(',')});\nDROP TABLE beedle_recovery_chunk_assert;\n`;
}
export async function applySqlShards({bundle,ctx,plan,planSha256,dir,readQuery,importFile,progress,signal}){
 const original=ctx.approvedPlanSha256,receiptColumns='chunk_index,original_plan_sha256,shard_plan_sha256,sql_sha256,sql_offset,sql_bytes';
 if(await fileSha(path.join(bundle,'restore.sql'))!==plan.originalSqlSha256)throw Error('Original SQL changed before sharded import');
 const tables=await readQuery("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' ORDER BY name");
 const hasChunks=tables.some(t=>t.name==='beedle_recovery_sql_chunks');
 if(!hasChunks&&tables.some(t=>t.name!=='beedle_recovery_claim'))throw Error('Sharded recovery requires empty claimed target or exact shard receipts');
 const getRows=()=>readQuery(`SELECT ${receiptColumns} FROM beedle_recovery_sql_chunks ORDER BY chunk_index`);
 let rows=hasChunks?await getRows():[],start=verifyShardPrefix(rows,plan,original,planSha256);
 const pendingFile=path.join(bundle,'sql-shard-checkpoint.json');
 let old;try{old=JSON.parse(await fs.readFile(pendingFile,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
 if(old&&(old.originalPlanSha256!==original||old.shardPlanSha256!==planSha256))throw Error('SQL shard checkpoint belongs to another plan');
 if(old?.pendingIndex!==undefined&&start<=old.pendingIndex)throw Error('Unacknowledged SQL shard has no committed receipt; terminal provider review required');
 if(hasChunks){
  const receipt=await readQuery('SELECT * FROM beedle_recovery_receipt');
  if(start<plan.chunks.length&&receipt.length)throw Error('Premature recovery completion receipt');
  if(start===plan.chunks.length&&(receipt.length!==1||receipt[0].singleton!==1||receipt[0].run_id!==ctx.manifest.runId||receipt[0].bundle_sha256!==ctx.plan.bundleSha256||receipt[0].target_sha256!==ctx.plan.targetSha256))throw Error('Final recovery receipt differs');
 }
 const handle=await fs.open(path.join(bundle,'restore.sql'),'r');
 try{
  for(const c of plan.chunks.slice(start)){
   if(signal?.aborted)throw Error('Operator interrupted before SQL shard');
   const bytes=Buffer.alloc(c.bytes),{bytesRead}=await handle.read(bytes,0,c.bytes,c.offset);
   if(bytesRead!==c.bytes||sha(bytes)!==c.sha256)throw Error('Original SQL byte range differs');
   const sql=shardSql(bytes.toString('utf8'),c,{runId:ctx.manifest.runId,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256,originalPlanSha256:original,shardPlanSha256:planSha256});
   const file=path.join(dir,`sql-shard-${String(c.index).padStart(5,'0')}.sql`);
   await fs.writeFile(file,sql,{mode:0o600,flag:'wx'});
   await atomicJson(pendingFile,{originalPlanSha256:original,shardPlanSha256:planSha256,pendingIndex:c.index,updatedAt:new Date().toISOString()});
   await progress({sqlChunk:c.index,sqlChunks:plan.chunks.length,sqlBytesCompleted:c.offset});
   // ONE import attempt. A failed acknowledgment never triggers a second write.
   await importFile(file,c.index);
   rows=await getRows();if(verifyShardPrefix(rows,plan,original,planSha256)!==c.index+1)throw Error('SQL shard commit acknowledgment differs');
   await atomicJson(pendingFile,{originalPlanSha256:original,shardPlanSha256:planSha256,completed:c.index+1,updatedAt:new Date().toISOString()});
   await fs.unlink(file);
  }
  await progress({sqlChunk:plan.chunks.length,sqlChunks:plan.chunks.length,sqlBytesCompleted:plan.originalSqlBytes});
 }finally{await handle.close();}
}
