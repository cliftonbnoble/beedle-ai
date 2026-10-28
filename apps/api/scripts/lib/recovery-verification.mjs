// Additive verification only: never imports SQL or changes a recovery target.
// Page hashes bind exact typed bytes to the untouched, manifest-checked snapshot.
import fs from 'node:fs/promises';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createHash,randomUUID} from 'node:crypto';
import {open,sha,ident,rowExpression,typedSql,canonicalRow} from './recovery-bundle.mjs';

export const schemaSql="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' AND name NOT GLOB 'beedle_recovery_*' AND name NOT IN(SELECT name FROM pragma_table_list WHERE type='shadow') ORDER BY type,name";
export const pageSize=table=>Math.max(1,Math.min(128,Math.floor(8*1024*1024/Math.max(table.maxRowBytes,1))));
export function pageSql(table,after){
 const keys=table.cursor.columns.map(ident).join(',');
 const where=after===null?'':`WHERE (${keys})>(${JSON.parse(after).map(typedSql).join(',')})`;
 return `SELECT ${rowExpression(table.cursor.columns)} AS cursor,${rowExpression(table.columns)} AS value FROM ${ident(table.name)} ${where} ORDER BY ${keys} LIMIT ${pageSize(table)}`;
}
export function pageDigest(rows){
 const hash=createHash('sha256');let bytes=0;
 for(const row of rows){const line=canonicalRow(row.cursor)+':'+canonicalRow(row.value)+'\n';hash.update(line);bytes+=Buffer.byteLength(line);}
 return {count:rows.length,bytes,sha256:hash.digest('hex'),last:rows.length?canonicalRow(rows.at(-1).cursor):null};
}
export async function durableJson(file,value){
 const tmp=file+'.'+randomUUID()+'.tmp',handle=await fs.open(tmp,'wx',0o600);
 try{await handle.writeFile(JSON.stringify(value)+'\n');await handle.sync();}finally{await handle.close();}
 await fs.rename(tmp,file);const dir=await fs.open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}
}
export async function regular(file){
 const s=await fs.lstat(file);if(!s.isFile()||s.nlink!==1)throw Error('Verification artifact must be an unlinked regular file');return s;
}
export function planIdentity(ctx){return {version:1,kind:'typed-recovery-verification',originalPlanSha256:ctx.approvedPlanSha256,bundleSha256:ctx.plan.bundleSha256,targetSha256:ctx.plan.targetSha256,originalToolSha256:ctx.originalToolSha,snapshotSha256:ctx.manifest.files['restore.sqlite'],schemaSha256:ctx.manifest.restore.schemaSha256};}
export async function makeVerificationPlan(ctx,bundle,file){
 // Creation is exclusive. Never replace an approved verification plan.
 try{await fs.lstat(file);throw Error('Verification plan already exists');}catch(e){if(e.code!=='ENOENT')throw e;}
 const db=open(path.join(bundle,'restore.sqlite')),tables=[];let index=0;
 try{
  if(sha(JSON.stringify(db.prepare(schemaSql).all()))!==ctx.manifest.restore.schemaSha256)throw Error('Local verification schema differs');
  for(const table of ctx.manifest.restore.tables){
   const pages=[],hash=createHash('sha256');let count=0,after=null;
   // Stream once instead of OFFSET scans or reading the whole corpus into RAM.
   const rows=db.prepare(`SELECT ${rowExpression(table.cursor.columns)} AS cursor,${rowExpression(table.columns)} AS value FROM ${ident(table.name)} ORDER BY ${table.cursor.columns.map(ident).join(',')}`).iterate();
   let batch=[];
   const add=()=>{
    const digest=pageDigest(batch);pages.push({index:index++,after,...digest});
    if(batch.length)after=digest.last;batch=[];
   };
   for(const row of rows){hash.update(canonicalRow(row.cursor)+':'+canonicalRow(row.value)+'\n');count++;batch.push(row);if(batch.length===pageSize(table))add();}
   add(); // Includes the original verifier's terminal empty page on exact multiples.
   if(count!==table.count||hash.digest('hex')!==table.sha256)throw Error('Local typed table differs from original manifest');
   tables.push({table,pages});
  }
 }finally{db.close();}
 const plan={...planIdentity(ctx),tables,objects:ctx.manifest.objects.filter(o=>o.present).map(o=>({key:o.key,sha256:o.sha256,bytes:o.bytes})),pageCount:index};
 // wx publish: another planner cannot replace the derived plan in a race.
 const tmp=file+'.'+randomUUID()+'.tmp';await durableJson(tmp,plan);
 try{await fs.link(tmp,file);const dir=await fs.open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}}finally{await fs.unlink(tmp);}
 return {plan,sha256:sha(JSON.stringify(plan))};
}
export async function loadVerificationPlan(file,approved,ctx){
 await regular(file);const plan=JSON.parse(await fs.readFile(file,'utf8'));
 if(!/^[a-f0-9]{64}$/.test(approved||'')||sha(JSON.stringify(plan))!==approved||Object.entries(planIdentity(ctx)).some(([k,v])=>plan[k]!==v)||plan.tables?.length!==ctx.manifest.restore.tables.length)throw Error('Exact derived verification plan required');
 if(JSON.stringify(plan.objects)!==JSON.stringify(ctx.manifest.objects.filter(o=>o.present).map(o=>({key:o.key,sha256:o.sha256,bytes:o.bytes}))))throw Error('Verification object inventory differs');
 let index=0;
 for(const [t,entry]of plan.tables.entries()){
  const table=ctx.manifest.restore.tables[t];if(JSON.stringify(entry.table)!==JSON.stringify(table)||entry.pages?.length!==Math.floor(table.count/pageSize(table))+1)throw Error('Verification table coverage differs');
  let after=null,count=0;
  for(const [p,page]of entry.pages.entries()){
   const expected=Math.min(pageSize(table),table.count-count);
   if(page.index!==index++||page.after!==after||page.count!==expected||!Number.isSafeInteger(page.bytes)||page.bytes<0||!/^[a-f0-9]{64}$/.test(page.sha256)||(expected===0?(page.last!==null||page.bytes!==0||page.sha256!==sha('')):typeof page.last!=='string'))throw Error('Verification page coverage differs');
   if(expected){const cursor=JSON.parse(page.last);if(!Array.isArray(cursor)||cursor.length!==table.cursor.columns.length||canonicalRow(page.last)!==page.last||page.last===after)throw Error('Verification typed cursor differs');cursor.forEach(typedSql);after=page.last;}
   if(p<entry.pages.length-1&&expected!==pageSize(table))throw Error('Premature terminal verification page');count+=page.count;
  }
  if(count!==table.count)throw Error('Incomplete verification table');
 }
 if(index!==plan.pageCount)throw Error('Incomplete verification plan');return plan;
}

// FULL-synchronous SQLite transactions persist receipts before progress advances.
// A crash either commits the exact receipt or re-reads that page/object on resume.
export async function checkpointStore(file,identity,{create=false}={}){
 let fresh=false;
 try{await regular(file);}catch(e){if(e.code!=='ENOENT'||!create)throw e;const h=await fs.open(file,'wx',0o600);await h.sync();await h.close();fresh=true;}
 const db=new DatabaseSync(file);
 try{
  db.exec('PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA trusted_schema=OFF;');
  if(fresh){db.exec('BEGIN; CREATE TABLE identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),value TEXT NOT NULL); CREATE TABLE pages(idx INTEGER PRIMARY KEY,descriptor_sha256 TEXT NOT NULL); CREATE TABLE objects(idx INTEGER PRIMARY KEY,descriptor_sha256 TEXT NOT NULL); CREATE TABLE objects_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),value TEXT NOT NULL);');db.prepare('INSERT INTO identity VALUES(1,?)').run(JSON.stringify(identity));db.exec('COMMIT;');const dir=await fs.open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}}
  if(db.prepare('PRAGMA integrity_check').get().integrity_check!=='ok'||db.prepare('PRAGMA foreign_key_check').all().length)throw Error('Verification checkpoint integrity differs');
  const rows=db.prepare('SELECT * FROM identity').all();if(rows.length!==1||rows[0].singleton!==1||rows[0].value!==JSON.stringify(identity))throw Error('Verification checkpoint identity differs');
  return {db,close:()=>db.close(),pages:()=>db.prepare('SELECT * FROM pages ORDER BY idx').all(),objects:()=>db.prepare('SELECT * FROM objects ORDER BY idx').all(),bindObjects:identity=>{
   const rows=db.prepare('SELECT * FROM objects_identity').all(),value=JSON.stringify(identity);
   if(rows.length){if(rows.length!==1||rows[0].singleton!==1||rows[0].value!==value)throw Error('Verification object epoch differs');}
   else{if(db.prepare('SELECT count(*) AS n FROM objects').get().n)throw Error('Object receipts have no verification epoch');db.prepare('INSERT INTO objects_identity VALUES(1,?)').run(value);}
  },save:(kind,index,descriptor)=>{
   if(!['pages','objects'].includes(kind))throw Error('Invalid receipt kind');
   db.prepare(`INSERT INTO ${kind}(idx,descriptor_sha256) VALUES(?,?)`).run(index,sha(JSON.stringify(descriptor)));
  }};
 }catch(e){db.close();throw e;}
}
export function verifiedPrefix(rows,descriptors){
 if(rows.length>descriptors.length)throw Error('Unexpected verification page receipts');
 rows.forEach((row,index)=>{if(row.idx!==index||row.descriptor_sha256!==sha(JSON.stringify(descriptors[index])))throw Error('Verification page receipt prefix differs');});return rows.length;
}
export function verifiedObjects(rows,objects){
 const done=new Set();for(const row of rows){if(!Number.isSafeInteger(row.idx)||row.idx<0||row.idx>=objects.length||done.has(row.idx)||row.descriptor_sha256!==sha(JSON.stringify(objects[row.idx])))throw Error('Verification object receipt differs');done.add(row.idx);}return done;
}
export function objectVersions(plan,rows){
 const found=new Map();for(const row of rows){if(typeof row.key!=='string'||typeof row.version!=='string'||!row.version||!Number.isSafeInteger(row.size)||row.size<0||found.has(row.key))throw Error('Invalid object version inventory');found.set(row.key,row);}
 return plan.objects.map(o=>{const row=found.get(o.key);if(!row||row.size!==o.bytes)throw Error('Required original version missing or size differs');return {...o,version:row.version};});
}
export async function verifyPages({plan,store,read,progress=async()=>{},signal}){
 const pages=plan.tables.flatMap(entry=>entry.pages),start=verifiedPrefix(store.pages(),pages);let complete=0,rows=0;
 for(const entry of plan.tables){
  for(const page of entry.pages){
   if(signal?.aborted)throw Error('Operator interrupted');
   if(page.index>=start){
    const actual=pageDigest(await read(pageSql(entry.table,page.after)));
    if(Object.entries(actual).some(([k,v])=>page[k]!==v))throw Error('Restored typed page content differs');
    store.save('pages',page.index,page);
   }
   complete++;rows+=page.count;
   if(page.index%10===0||page===entry.pages.at(-1))await progress({table:entry.table.name,verifiedPages:complete,totalPages:pages.length,verifiedRows:rows,resumedPages:start});
  }
 }
 if(verifiedPrefix(store.pages(),pages)!==pages.length)throw Error('Incomplete typed page receipts');
 return {pages:complete,rows,resumedPages:start};
}
export async function verifyObjects({objects,store,get,progress=async()=>{},signal,concurrency=4}){
 if(!Number.isSafeInteger(concurrency)||concurrency<1||concurrency>4)throw Error('Bounded object verification required');
 const done=verifiedObjects(store.objects(),objects),resumed=done.size,pending=objects.map((o,i)=>({o,i})).filter(({i})=>!done.has(i));
 for(let offset=0;offset<pending.length;offset+=concurrency){
  if(signal?.aborted)throw Error('Operator interrupted');
  // Drain the entire bounded wave before releasing ownership on any failure.
  const wave=await Promise.allSettled(pending.slice(offset,offset+concurrency).map(async({o,i})=>{
   const actual=await get(o.key);
   if(!actual||actual.sha256!==o.sha256||actual.bytes!==o.bytes)throw Error('Required original SHA256/bytes differ');
   store.save('objects',i,o);done.add(i);
  }));
  const failure=wave.find(r=>r.status==='rejected');if(failure)throw failure.reason;
  await progress({verifiedObjects:done.size,totalObjects:objects.length,resumedObjects:resumed});
 }
 if(verifiedObjects(store.objects(),objects).size!==objects.length)throw Error('Incomplete original receipts');
 return {objects:done.size,resumedObjects:resumed};
}
