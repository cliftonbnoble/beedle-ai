// Persistent maintenance fence, using the same reject-write protocol as the
// approved source capture. No original row, original schema object or receipt is
// rewritten. Privileged operator DDL invalidates this epoch, even if later undone.
import fs from 'node:fs/promises';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {sha,ident,literal} from './recovery-bundle.mjs';
import {durableJson,regular} from './recovery-verification.mjs';
export const fullSchemaSql="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' AND name NOT IN(SELECT name FROM pragma_table_list WHERE type='shadow') ORDER BY type,name";
const epochTable='beedle_recovery_verification_epoch';
const tableSql=`CREATE TABLE ${epochTable}(singleton INTEGER PRIMARY KEY CHECK(singleton=1),epoch_id TEXT NOT NULL,run_id TEXT NOT NULL,bundle_sha256 TEXT NOT NULL,target_sha256 TEXT NOT NULL,original_plan_sha256 TEXT NOT NULL,verification_plan_sha256 TEXT NOT NULL,controller_sha256 TEXT NOT NULL,full_schema_sha256 TEXT NOT NULL)`;
const order=(a,b)=>a.type===b.type?(a.name<b.name?-1:a.name>b.name?1:0):a.type<b.type?-1:1;
const equal=(a,b)=>Object.keys(a).length===Object.keys(b).length&&Object.entries(b).every(([k,v])=>a[k]===v);
function identity(ctx){return {runId:ctx.manifest.runId,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256,originalPlanSha256:ctx.approvedPlanSha256,verificationPlanSha256:ctx.verificationPlanSha256,controllerSha256:ctx.controllerSha256};}
export function buildVerificationFence(ctx,base,epochId=randomUUID()){
 if(!/^[a-f0-9-]{36}$/.test(epochId)||base.some(o=>o.name===epochTable||o.name.startsWith('beedle_recovery_verify_')))throw Error('Fresh owned verification fence required');
 const ordinary=ctx.manifest.restore.tables.map(t=>t.name),helpers=base.filter(o=>o.type==='table'&&o.name.startsWith('beedle_recovery_')&&!/^CREATE VIRTUAL TABLE/i.test(o.sql)).map(o=>o.name);
 const protectedTables=[...new Set([...ordinary,...helpers,epochTable])].sort(),objects=[...base,{type:'table',name:epochTable,tbl_name:epochTable,sql:tableSql}];
 for(const table of protectedTables)for(const verb of ['INSERT','UPDATE','DELETE']){
  const name='beedle_recovery_verify_'+epochId.replaceAll('-','')+'_'+sha(table).slice(0,16)+'_'+verb.toLowerCase();
  objects.push({type:'trigger',name,tbl_name:table,sql:`CREATE TRIGGER ${ident(name)} BEFORE ${verb} ON ${ident(table)} BEGIN SELECT RAISE(ABORT,'recovery_verification_read_only'); END`});
 }
 objects.sort(order);if(new Set(objects.map(o=>o.name)).size!==objects.length)throw Error('Verification fence object collision');
 const receipt={singleton:1,epoch_id:epochId,run_id:ctx.manifest.runId,bundle_sha256:ctx.plan.bundleSha256,target_sha256:ctx.plan.targetSha256,original_plan_sha256:ctx.approvedPlanSha256,verification_plan_sha256:ctx.verificationPlanSha256,controller_sha256:ctx.controllerSha256,full_schema_sha256:sha(JSON.stringify(objects))};
 const sql=tableSql+';\n'+`INSERT INTO ${epochTable} VALUES(${Object.values(receipt).map(v=>typeof v==='number'?v:literal(v)).join(',')});\n`+objects.filter(o=>o.type==='trigger'&&o.name.startsWith('beedle_recovery_verify_')).map(o=>o.sql+';').join('\n')+'\n';
 return {version:1,kind:'owned-candidate-verification-fence',...identity(ctx),epochId,baseSchema:base,baseSchemaSha256:sha(JSON.stringify(base)),protectedTables,expectedSchema:objects,receipt,sql,sqlSha256:sha(sql)};
}
export async function writeFencePlan(file,plan){
 try{await fs.lstat(file);throw Error('Verification fence plan already exists');}catch(e){if(e.code!=='ENOENT')throw e;}
 const temporary=file+'.'+randomUUID()+'.tmp';await durableJson(temporary,plan);try{await fs.link(temporary,file);}finally{await fs.unlink(temporary);}
 const dir=await fs.open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}
}
export async function loadVerificationFence(file,approved,ctx){
 await regular(file);const plan=JSON.parse(await fs.readFile(file,'utf8'));
 if(!/^[a-f0-9]{64}$/.test(approved||'')||sha(JSON.stringify(plan))!==approved||plan.version!==1||plan.kind!=='owned-candidate-verification-fence'||Object.entries(identity(ctx)).some(([k,v])=>plan[k]!==v))throw Error('Exact reviewed verification fence required');
 const rebuilt=buildVerificationFence(ctx,plan.baseSchema,plan.epochId);if(JSON.stringify(rebuilt)!==JSON.stringify(plan))throw Error('Verification fence coverage differs');return plan;
}
export async function assertVerificationFence(plan,read){
 const schema=await read(fullSchemaSql);if(JSON.stringify(schema)!==JSON.stringify(plan.expectedSchema))throw Error('Owned verification fence/schema differs');
 const receipt=await read('SELECT * FROM '+epochTable);if(receipt.length!==1||!equal(receipt[0],plan.receipt))throw Error('Owned verification epoch differs');
 return {epochId:plan.epochId,fullSchemaSha256:plan.receipt.full_schema_sha256};
}
export async function applyVerificationFence({plan,bundle,read,write}){
 const schema=await read(fullSchemaSql),pending=path.join(bundle,'verification-fence-apply.json');
 if(schema.some(o=>o.name===epochTable)){
  await assertVerificationFence(plan,read);await durableJson(pending,{epochId:plan.epochId,sqlSha256:plan.sqlSha256,phase:'installed_verified'});return;
 }
 if(sha(JSON.stringify(schema))!==plan.baseSchemaSha256)throw Error('Candidate schema changed before fence installation');
 try{await fs.access(pending);throw Error('Unacknowledged fence installation requires ownership review');}catch(e){if(e.code!=='ENOENT')throw e;}
 await durableJson(pending,{epochId:plan.epochId,sqlSha256:plan.sqlSha256,phase:'write_acknowledgment_pending'});
 await write(plan.sql); // One attempt; never automatically replay uncertain DDL.
 await assertVerificationFence(plan,read);await durableJson(pending,{epochId:plan.epochId,sqlSha256:plan.sqlSha256,phase:'installed_verified'});
}
