import test from 'node:test';
import assert from 'node:assert/strict';
import {restorationDatabaseName,createIsolatedRestorationDatabase,observeIsolatedRestorationDatabase,
  claimIsolatedRestorationDatabase,observeIsolatedRestorationClaim,restorationApplicationObjects} from '../scripts/lib/owned-restoration-resources.mjs';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
import {applyFreshRestorationPrefix,observeFreshRestorationPrefix,restorationSchemaFingerprint} from '../scripts/lib/owned-restoration-resources.mjs';
const ownerId='11111111-1111-4111-8111-111111111111',now=Date.parse('2026-10-31T08:00:00Z');
function setup(mode='normal'){
 const plan={version:1,ownerId,accountId:'a'.repeat(32),partitionCount:32,sourceManifestSha256:'b'.repeat(64),catalogSha256:'c'.repeat(64),
  forbiddenDatabaseIds:[1,2,3].map(i=>`00000000-0000-4000-8000-${String(i).padStart(12,'0')}`),databases:Array.from({length:32},(_,index)=>({index,name:restorationDatabaseName(ownerId,index)}))};
 const records=new Map(),databases=[];let creates=0,objectReads=0;
 const store={async exists(key){return records.has(key);},async reserve(key,value){assert(!records.has(key));records.set(key,structuredClone(value));},async read(key){assert(records.has(key));return records.get(key);}};
 const cloud={async listDatabases(){return structuredClone(databases);},async createDatabase({name}){creates++;const row={name,uuid:'99999999-9999-4999-8999-999999999999',created_at:new Date(now).toISOString()};databases.push(row);if(mode==='lost-ack')throw Error('Transport response lost');return structuredClone(row);},async getDatabase(id){return structuredClone(databases.find(row=>row.uuid===id));},async listApplicationObjects(){objectReads++;return mode==='foreign-objects'?[{name:'other_owner'}]:[];}};
 const guard={ownerId,observedAt:new Date(now).toISOString(),passed:true,sourceFrozen:true,candidateUnchanged:true,productionPaused:true,originalAnchorMatched:true,localReserveBytes:30*1024**3};
 return{plan,store,cloud,guard,now,records,databases,counts:()=>({creates,objectReads})};
}
test('a new database is created once after fresh guards and independently read back without data writes',async()=>{
 const x=setup();const result=await createIsolatedRestorationDatabase({...x,index:0});assert.equal(result.name,x.plan.databases[0].name);
 assert.deepEqual(x.counts(),{creates:1,objectReads:0});assert.equal(x.records.get('database-00-readback').documentsInserted,false);
 await assert.rejects(createIsolatedRestorationDatabase({...x,index:0}),/observed/);assert.equal(x.counts().creates,1);
});
test('lost acknowledgment is reconciled through exact empty resource identity without a second create',async()=>{
 const x=setup('lost-ack');await assert.rejects(createIsolatedRestorationDatabase({...x,index:0}),/unacknowledged/);
 assert(x.records.has('database-00-unacknowledged'));const observed=await observeIsolatedRestorationDatabase({...x,index:0});
 assert.equal(observed.phase,'observed_empty_unclaimed_resource');assert.equal(observed.createReissued,false);assert.equal(x.counts().creates,1);
});
test('existing names, changed acknowledgments, protected IDs, foreign objects and ambiguous times require review',async()=>{
 const named=setup();named.databases.push({name:named.plan.databases[0].name,uuid:'88888888-8888-4888-8888-888888888888'});
 await assert.rejects(createIsolatedRestorationDatabase({...named,index:0}),/already exists/);assert.equal(named.counts().creates,0);
 const changed=setup();await createIsolatedRestorationDatabase({...changed,index:0});changed.databases[0].uuid='88888888-8888-4888-8888-888888888888';
 await assert.rejects(observeIsolatedRestorationDatabase({...changed,index:0}),/replaced/);
 const foreign=setup('foreign-objects');await createIsolatedRestorationDatabase({...foreign,index:0});await assert.rejects(observeIsolatedRestorationDatabase({...foreign,index:0}),/application objects/);
 const protectedId=setup();protectedId.cloud.createDatabase=async({name})=>({name,uuid:protectedId.plan.forbiddenDatabaseIds[0]});await assert.rejects(createIsolatedRestorationDatabase({...protectedId,index:0}),/identity differs/);
 const old=setup('lost-ack');await assert.rejects(createIsolatedRestorationDatabase({...old,index:0}));old.databases[0].created_at='2020-01-01T00:00:00Z';await assert.rejects(observeIsolatedRestorationDatabase({...old,index:0}),/cannot be reconciled/);
});
test('stale, missing, foreign-owner or low-reserve guards and altered resource names fail before cloud mutation',async()=>{
 for(const change of [x=>x.guard.ownerId=crypto.randomUUID(),x=>x.guard.sourceFrozen=false,x=>x.guard.originalAnchorMatched=false,
  x=>x.guard.observedAt=new Date(now-300001).toISOString(),x=>x.guard.localReserveBytes=19*1024**3,x=>x.plan.databases[0].name='existing-production']){
  const x=setup();change(x);await assert.rejects(createIsolatedRestorationDatabase({...x,index:0}));assert.equal(x.counts().creates,0);assert.equal(x.records.size,0);
 }
});
test('a fresh target claim is immutable and a lost DDL acknowledgment reconciles without replay',async()=>{
 for(const lost of [false,true]){
  const x=setup();const db=new DatabaseSync(':memory:');let attempts=0;
  try{
   x.guard.targetsUnbound=true;const resource=await createIsolatedRestorationDatabase({...x,index:0});
   x.cloud.listApplicationObjects=async()=>db.prepare("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").all();
   x.cloud.readResourceClaim=async()=>db.prepare('SELECT * FROM beedle_restore_resource_claim').get();
   x.cloud.executeClaimSql=async(_,sql)=>{attempts++;db.exec(sql);if(lost)throw Error('Lost SQL response');return{acknowledged:true};};
   const args={...x,index:0,resourceId:resource.uuid};
   if(lost){await assert.rejects(claimIsolatedRestorationDatabase(args),/unacknowledged/);assert.equal((await observeIsolatedRestorationClaim(args)).claimReissued,false);}
   else assert.equal((await claimIsolatedRestorationDatabase(args)).phase,'owned_empty_target_claim_verified');
   await assert.rejects(claimIsolatedRestorationDatabase(args),/observed/);assert.equal(attempts,1);
   assert.throws(()=>db.exec("UPDATE beedle_restore_resource_claim SET owner_id='different'"),/immutable/);
   assert.throws(()=>db.exec('DELETE FROM beedle_restore_resource_claim'),/immutable/);
  }finally{db.close();}
 }
});
test('a partial claim, unexpected object or missing binding quiescence cannot authorize further writes',async()=>{
 const x=setup();const resource=await createIsolatedRestorationDatabase({...x,index:0});
 let writes=0;x.cloud.executeClaimSql=async()=>{writes++;return{acknowledged:true};};x.cloud.listApplicationObjects=async()=>[];
 await assert.rejects(claimIsolatedRestorationDatabase({...x,index:0,resourceId:resource.uuid}),/quiescent/);assert.equal(writes,0);
 x.guard.targetsUnbound=true;x.cloud.executeClaimSql=async()=>{writes++;throw Error('Partial DDL');};
 await assert.rejects(claimIsolatedRestorationDatabase({...x,index:0,resourceId:resource.uuid}),/unacknowledged/);
 x.cloud.listApplicationObjects=async()=>[{name:'beedle_restore_resource_claim',type:'table',sql:'partial'}];
 await assert.rejects(observeIsolatedRestorationClaim({...x,index:0,resourceId:resource.uuid}),/partial or differs/);assert.equal(writes,1);
});
test('only the documented exact D1 reserved storage table is excluded from application ownership checks',()=>{
 const provider={name:'_cf_KV',type:'table',sql:'CREATE TABLE _cf_KV (\n key TEXT PRIMARY KEY, value BLOB\n) WITHOUT ROWID'};
 assert.deepEqual(restorationApplicationObjects([provider]),[]);
 for(const row of [{...provider,name:'_cf_other'},{...provider,type:'view'},{...provider,sql:'CREATE TABLE _cf_KV(owner TEXT)'},{name:'foreign_owner',type:'table',sql:'CREATE TABLE foreign_owner(id TEXT)'}])
  assert.equal(restorationApplicationObjects([row]).length,1);
});

test('canonical migrations apply once only on a verified fresh owner; exact ledger/schema reconcile a lost CLI result',async()=>{
 const directory=new URL('../migrations/',import.meta.url),names=readdirSync(directory).filter(x=>x.endsWith('.sql')).sort().slice(0,18);
 const contents=names.map(name=>readFileSync(new URL(name,directory),'utf8'));
 const migrations=names.map((name,index)=>({name,sha256:createHash('sha256').update(contents[index]).digest('hex')}));
 const ledgerSql='CREATE TABLE IF NOT EXISTS d1_migrations(\n\t\tid         INTEGER PRIMARY KEY AUTOINCREMENT,\n\t\tname       TEXT UNIQUE,\n\t\tapplied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL\n);';
 for(const lost of [false,true]){
  const x=setup();x.guard.targetsUnbound=true;const db=new DatabaseSync(':memory:'),expected=new DatabaseSync(':memory:');let commands=0;
  try{
   const resource=await createIsolatedRestorationDatabase({...x,index:0});
   x.cloud.listApplicationObjects=async()=>db.prepare("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all();
   x.cloud.readResourceClaim=async()=>db.prepare('SELECT * FROM beedle_restore_resource_claim').get();
   x.cloud.executeClaimSql=async(_,sql)=>{db.exec(sql);expected.exec(sql);return{acknowledged:true};};
   await claimIsolatedRestorationDatabase({...x,index:0,resourceId:resource.uuid});
   expected.exec(ledgerSql);for(const sql of contents)expected.exec(sql);
   const expectedSchemaSha256=await restorationSchemaFingerprint(expected.prepare("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all());
   const bootstrap={version:1,ownerId,through:18,resourcePlanSha256:await fingerprint(x.plan),configSha256:'d'.repeat(64),expectedSchemaSha256,migrations};
   x.cloud.readMigrationLedger=async()=>db.prepare("SELECT name FROM sqlite_master WHERE name='d1_migrations'").get()?db.prepare('SELECT name FROM d1_migrations ORDER BY id').all().map(row=>row.name):[];
   const runMigrations=async()=>{
    commands++;db.exec(ledgerSql);for(const [index,sql] of contents.entries()){db.exec('BEGIN');try{db.exec(sql);db.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(names[index]);db.exec('COMMIT');}catch(error){db.exec('ROLLBACK');throw error;}}
    return{exitCode:lost?1:0};
   };
   const args={...x,bootstrap,index:0,resourceId:resource.uuid,runMigrations};
   if(lost){await assert.rejects(applyFreshRestorationPrefix(args),/did not acknowledge/);assert.equal((await observeFreshRestorationPrefix(args)).migrationCommandReissued,false);}
   else assert.equal((await applyFreshRestorationPrefix(args)).applicationMigrationsApplied,18);
   await assert.rejects(applyFreshRestorationPrefix(args),/reconciled/);assert.equal(commands,1);
   assert.equal(db.prepare('SELECT COUNT(*) n FROM documents').get().n,0);
   db.exec('DROP INDEX idx_document_chunks_doc');await assert.rejects(observeFreshRestorationPrefix(args),/schema differs/);
  }finally{db.close();expected.close();}
 }
});
