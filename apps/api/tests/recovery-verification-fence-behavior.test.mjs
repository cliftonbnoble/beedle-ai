import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {DatabaseSync} from 'node:sqlite';
import {sha} from '../scripts/lib/recovery-bundle.mjs';
import {fullSchemaSql,buildVerificationFence,writeFencePlan,loadVerificationFence,assertVerificationFence,applyVerificationFence} from '../scripts/lib/recovery-verification-fence.mjs';
async function fixture(fn){
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'verification-fence-')),db=new DatabaseSync(':memory:');
 try{
  db.exec('CREATE TABLE ordinary(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO ordinary VALUES(1,\'unchanged\'); CREATE TABLE beedle_recovery_claim(v TEXT); INSERT INTO beedle_recovery_claim VALUES(\'owner\');');
  const ctx={manifest:{runId:'original-run',restore:{tables:[{name:'ordinary'}]}},plan:{bundleSha256:sha('bundle'),targetSha256:sha('target')},approvedPlanSha256:sha('original'),verificationPlanSha256:sha('typed-pages'),controllerSha256:sha('controller')},base=db.prepare(fullSchemaSql).all(),plan=buildVerificationFence(ctx,base),read=async sql=>db.prepare(sql).all();await fn({root,db,ctx,plan,read});
 }finally{db.close();await fs.rm(root,{recursive:true,force:true});}
}
test('owned epoch blocks INSERT/UPDATE/DELETE/REPLACE on ordinary and recovery metadata while reads and original bytes remain intact',()=>fixture(async({db,plan,read})=>{
 db.exec(plan.sql);await assertVerificationFence(plan,read);
 for(const sql of ["INSERT INTO ordinary VALUES(2,'changed')","UPDATE ordinary SET v='changed'","DELETE FROM ordinary","INSERT OR REPLACE INTO ordinary VALUES(1,'changed')","UPDATE beedle_recovery_claim SET v='other'","DELETE FROM beedle_recovery_verification_epoch","INSERT INTO beedle_recovery_verification_epoch SELECT * FROM beedle_recovery_verification_epoch"]){assert.throws(()=>db.exec(sql),/recovery_verification_read_only/);}
 assert.equal(db.prepare('SELECT v FROM ordinary').get().v,'unchanged');assert.equal(plan.protectedTables.length,3);
}));
test('fence removal, alteration, wrong epoch or any schema drift holds; a partial installation is never repaired by blind replay',()=>fixture(async({root,db,plan,read})=>{
 db.exec(plan.sql);const trigger=plan.expectedSchema.find(o=>o.type==='trigger');db.exec('DROP TRIGGER "'+trigger.name+'"');await assert.rejects(assertVerificationFence(plan,read),/fence\/schema differs/);
 let writes=0;await assert.rejects(applyVerificationFence({plan,bundle:root,read,write:async()=>writes++}),/fence\/schema differs/);assert.equal(writes,0);
}));
test('one fence write only; a lost acknowledgment resolves by complete owned schema and receipt without another write',()=>fixture(async({root,db,plan,read})=>{
 let writes=0;const args={plan,bundle:root,read,write:async sql=>{writes++;db.exec(sql);throw Error('lost acknowledgment');}};
 await assert.rejects(applyVerificationFence(args),/lost acknowledgment/);await applyVerificationFence(args);await applyVerificationFence(args);assert.equal(writes,1);await assertVerificationFence(plan,read);
}));
test('an uncommitted uncertain fence write never retries and an approved plan cannot change its target or omit a table',()=>fixture(async({root,ctx,plan,read})=>{
 let writes=0;const args={plan,bundle:root,read,write:async()=>{writes++;throw Error('unknown outcome');}};await assert.rejects(applyVerificationFence(args),/unknown outcome/);await assert.rejects(applyVerificationFence(args),/ownership review/);assert.equal(writes,1);
 const file=path.join(root,'fence.json');await writeFencePlan(file,plan);const approved=sha(JSON.stringify(plan));await loadVerificationFence(file,approved,ctx);await assert.rejects(loadVerificationFence(file,approved,{...ctx,verificationPlanSha256:sha('other')}),/Exact reviewed/);
 const changed=structuredClone(plan);changed.protectedTables.pop();await fs.writeFile(file,JSON.stringify(changed));await assert.rejects(loadVerificationFence(file,sha(JSON.stringify(changed)),ctx),/coverage differs/);
}));
