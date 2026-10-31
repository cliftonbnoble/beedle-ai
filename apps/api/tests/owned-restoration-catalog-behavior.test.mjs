import test from 'node:test';import assert from 'node:assert/strict';import{DatabaseSync}from'node:sqlite';
import{readFileSync,readdirSync}from'node:fs';
import{fingerprint}from'../scripts/lib/release-contract.mjs';
import{restorationDatabaseName,restorationResourceClaimObjects,restorationSchemaFingerprint}from'../scripts/lib/owned-restoration-resources.mjs';
import{restorationCatalogTables,restorationCatalogDigest,compileOwnedRestorationCatalog,seedOwnedRestorationCatalog,observeOwnedRestorationCatalog}from'../scripts/lib/owned-restoration-catalog.mjs';
const ownerId='11111111-1111-4111-8111-111111111111',resourceId='99999999-9999-4999-8999-999999999999',now=Date.parse('2026-10-31T10:00:00Z');
async function setup(){
 const db=new DatabaseSync(':memory:');for(const x of restorationResourceClaimObjects)db.exec(x.sql);
 const directory=new URL('../migrations/',import.meta.url),names=readdirSync(directory).filter(x=>x.endsWith('.sql')).sort().slice(0,18);
 db.exec('CREATE TABLE d1_migrations(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE,applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)');
 for(const name of names){db.exec(readFileSync(new URL(name,directory),'utf8'));db.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(name);}
 const snapshot=restorationCatalogTables.map(table=>({table,rows:[Object.fromEntries(db.prepare('PRAGMA table_info('+table+')').all().map(column=>
   [column.name,column.type==='INTEGER'?1:column.name==='source_page_anchor'?null:column.name==='body_text'?"Exact 'quotes';\n雪\u0000end":'fixture_'+column.name]))]}));
 const plan={version:1,ownerId,accountId:'a'.repeat(32),partitionCount:32,sourceManifestSha256:'b'.repeat(64),catalogSha256:restorationCatalogDigest(snapshot),
  forbiddenDatabaseIds:[1,2,3].map(i=>`00000000-0000-4000-8000-${String(i).padStart(12,'0')}`),databases:Array.from({length:32},(_,index)=>({index,name:restorationDatabaseName(ownerId,index)}))};
 const planSha=await fingerprint(plan);db.prepare('INSERT INTO beedle_restore_resource_claim VALUES(1,?,?,?,?,?)').run(ownerId,planSha,resourceId,0,new Date(now).toISOString());
 const objects=()=>db.prepare("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all();
 const bootstrap={through:18,migrations:names.map(name=>({name})),expectedSchemaSha256:await restorationSchemaFingerprint(objects())};
 const seed={version:1,ownerId,catalogSha256:plan.catalogSha256,resourcePlanSha256:planSha,prefixBootstrapSha256:await fingerprint(bootstrap),expectedSchemaSha256:bootstrap.expectedSchemaSha256,rows:5,tableCounts:restorationCatalogTables.map(table=>({table,rows:1}))};
 const records=new Map(),store={async exists(k){return records.has(k);},async reserve(k,v){assert(!records.has(k));records.set(k,structuredClone(v));},async read(k){assert(records.has(k));return records.get(k);}};
 // Hex readback preserves every TEXT byte, including NULs that a native text
 // accessor may truncate. The remote adapter follows this same typed protocol.
 const readSnapshot=()=>restorationCatalogTables.map(table=>{
  const columns=db.prepare('PRAGMA table_info('+table+')').all().map(x=>x.name);
  const rows=db.prepare('SELECT '+columns.flatMap((c,i)=>[`typeof(${c}) t${i}`,`hex(${c}) h${i}`]).join(',')+' FROM '+table+' ORDER BY rowid').all().map(row=>Object.fromEntries(columns.map((c,i)=>{
   const type=row['t'+i],text=Buffer.from(row['h'+i],'hex').toString('utf8');return[c,type==='null'?null:type==='integer'?Number(text):text];
  })));
  return{table,rows};
 });
 const cloud={async getDatabase(){return{uuid:resourceId,name:plan.databases[0].name};},async readResourceClaim(){return db.prepare('SELECT * FROM beedle_restore_resource_claim').get();},
  async readMigrationLedger(){return db.prepare('SELECT name FROM d1_migrations ORDER BY id').all().map(x=>x.name);},async listApplicationObjects(){return objects();},
  async readCatalogCounts(){return restorationCatalogTables.map(table=>({table,rows:db.prepare('SELECT COUNT(*) n FROM '+table).get().n}));},async readCatalogSnapshot(){return readSnapshot();}};
 const guard={ownerId,observedAt:new Date(now).toISOString(),passed:true,sourceFrozen:true,candidateUnchanged:true,productionPaused:true,originalAnchorMatched:true,targetsUnbound:true,localReserveBytes:30*1024**3};
 const compiled=await compileOwnedRestorationCatalog({plan,index:0,resourceId,snapshot});let writes=0;
 const runSeed=async()=>{writes++;db.exec(compiled.sql);return{exitCode:0};};
 return{db,plan,bootstrap,seed,index:0,resourceId,store,cloud,guard,now,snapshot,compiled,runSeed,records,writes:()=>writes};
}
test('owned catalog preserves exact typed source bytes before migration19 takes an immutable snapshot',async()=>{
 const x=await setup();try{
  assert.equal((await seedOwnedRestorationCatalog(x)).rows,5);assert.equal(restorationCatalogDigest(await x.cloud.readCatalogSnapshot()),x.plan.catalogSha256);
  assert(!x.compiled.sql.includes('雪'));assert(x.compiled.maxStatementBytes<100_000);
  assert.equal(x.db.prepare('SELECT COUNT(*) n FROM documents').get().n,0);
  x.db.exec(readFileSync(new URL('../migrations/0019_legal_catalog_versions.sql',import.meta.url),'utf8'));
  assert.equal(x.db.prepare('SELECT COUNT(*) n FROM legal_catalog_rows').get().n,5);
  assert.throws(()=>x.db.exec("UPDATE legal_catalog_rows SET payload_json='{}'"),/immutable/);
 }finally{x.db.close();}
});
test('lost seed acknowledgment is observed from exact values without another insert command',async()=>{
 const x=await setup();try{
  await assert.rejects(seedOwnedRestorationCatalog({...x,runSeed:async()=>{await x.runSeed();throw Error('Lost response');}}),/Unacknowledged/);
  assert.equal((await observeOwnedRestorationCatalog(x)).seedCommandReissued,false);
  await assert.rejects(seedOwnedRestorationCatalog(x),/never replayed/);assert.equal(x.writes(),1);
  x.db.prepare('UPDATE legal_rules_sections SET heading=?').run('different');await assert.rejects(observeOwnedRestorationCatalog(x),/readback differs/);
 }finally{x.db.close();}
});
test('partial seed and failed CLI remain held and cannot authorize insert replay',async()=>{
 const x=await setup();try{
  await assert.rejects(seedOwnedRestorationCatalog({...x,runSeed:async()=>{x.db.exec(x.compiled.sql.split('\n')[0]);return{exitCode:1};}}),/reconcile without replay/);
  await assert.rejects(observeOwnedRestorationCatalog(x),/coverage differs|readback differs/);
  await assert.rejects(seedOwnedRestorationCatalog(x),/never replayed/);
 }finally{x.db.close();}
});
test('different owner, changed prefix/schema, stale guard and occupied catalog fail before writes',async()=>{
 for(const change of [x=>x.guard.targetsUnbound=false,x=>x.guard.localReserveBytes=19*1024**3,x=>x.guard.observedAt=new Date(now-300001).toISOString(),
  x=>x.cloud.readResourceClaim=async()=>({owner_id:'other'}),x=>x.db.exec('CREATE TABLE foreign_writer(id TEXT)'),x=>x.db.exec(x.compiled.sql.split('\n')[0])]){
  const x=await setup();try{change(x);await assert.rejects(seedOwnedRestorationCatalog(x));assert.equal(x.writes(),0);assert.equal(x.records.size,0);}finally{x.db.close();}
 }
});
test('unsupported values, malformed identifiers, changed bytes, oversized SQL and protected targets cannot compile',async()=>{
 const x=await setup();try{
  for(const mutate of [s=>s[0].rows[0].id=1.5,s=>s[0].rows[0].id='\ud800',s=>s[0].rows[0]['sql;--']='x',s=>s[0].rows[0].id='changed',s=>s[0].rows[0].description='x'.repeat(60_000)]){
   const snapshot=structuredClone(x.snapshot);mutate(snapshot);
   // A normal altered source fails its original hash; a reviewed new hash must
   // still obey SQL/value bounds.
   await assert.rejects(compileOwnedRestorationCatalog({...x,snapshot}));
   if(typeof snapshot[0].rows[0].description==='string'&&snapshot[0].rows[0].description.length>50_000){const plan={...x.plan,catalogSha256:restorationCatalogDigest(snapshot)};await assert.rejects(compileOwnedRestorationCatalog({...x,plan,snapshot}),/SQL bound/);}
  }
  await assert.rejects(compileOwnedRestorationCatalog({...x,resourceId:x.plan.forbiddenDatabaseIds[0]}),/Protected/);
 }finally{x.db.close();}
});
