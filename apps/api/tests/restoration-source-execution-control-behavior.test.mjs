import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {sourceExecutionFixture} from './helpers/restoration-source-execution-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
import {createHash} from 'node:crypto';
const c=loadTs('src/services/restoration-source-execution-control.ts');
const {runOwnedRestorationSourceJob:source}=loadTs('src/services/restoration-source-job.ts');
const initialize=x=>c.initializeRestorationSourceExecution(x.db,x.execution,x.beforeSchemaSha256,x.afterSchemaSha256);
function preserved(x,{completed=false}={}){
  const owner=x.sqlite.prepare('SELECT * FROM beedle_restore_owner').get();
  if(completed)assert.deepEqual({...owner,phase:x.oldRows.owner.phase,updated_at:x.oldRows.owner.updated_at,last_outcome:x.oldRows.owner.last_outcome},{...x.oldRows.owner});
  else assert.deepEqual(owner,x.oldRows.owner);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.oldRows.tick);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history ORDER BY token').all(),x.oldRows.histories);
  if(x.oldRows.heldRows)for(const [key,table]of [['control','beedle_restore_source_successor'],['tick','beedle_restore_source_successor_tick'],['history','beedle_restore_source_successor_history']])
    assert.deepEqual(x.sqlite.prepare('SELECT * FROM '+table).get(),x.oldRows.heldRows[key]);
}
test('seeded, held and V2-held roots use a distinct one-case execution without clearing earlier journals',async()=>{
  for(const mode of ['seeded','held','held-successor']){const x=await sourceExecutionFixture(mode);try{
    await initialize(x);preserved(x);const tick=await c.claimRestorationSourceExecutionTick(x.db,x.execution);assert(tick);
    const result=await source({db:x.db,originals:x.originals,target:x.target,proofs:x.proofs,identity:x.identity,execution:x.execution,tickToken:tick.token,job:x.job,profile:x.profile});
    assert.equal(result.state,'processed');preserved(x);
    const proof=JSON.parse(x.sqlite.prepare('SELECT proof_json FROM beedle_restore_source_results WHERE document_id=?').get(x.job.documentId).proof_json);
    assert.equal(proof.version,3);assert.equal(proof.sourceExecution.executionId,x.execution.executionId);assert.equal(proof.humanApproval,false);
    await c.completeRestorationSourceExecutionTick(x.db,x.execution,tick.token);
    assert.equal((await c.completeRestorationSourceExecution(x.db,x.execution)).completed,true);preserved(x,{completed:true});
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'indexing');
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL OR searchable_at IS NOT NULL').get().n,0);
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await c.completeRestorationSourceExecution(x.db,x.execution);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
  }finally{x.sqlite.close();}}
});
test('an immutable held run can be followed by a distinct run while its full journal stays unchanged',async()=>{
  const x=await sourceExecutionFixture('held-successor');try{
    await initialize(x);await c.holdRestorationSourceExecution(x.db,x.execution,'e'.repeat(64));
    const prior=await c.restorationPriorExecutionJournal(x.db,'99999999-9999-4999-8999-999999999999'),current=x.sqlite.prepare('SELECT * FROM beedle_restore_execution_current').get(),id=crypto.randomUUID();
    const next={...x.execution,executionId:id,executionPlanSha256:await fingerprint(id),priorJournalSha256:await fingerprint(prior),previousCurrentSha256:await fingerprint(current)};
    await c.initializeRestorationSourceExecution(x.db,next,x.afterSchemaSha256,x.afterSchemaSha256);
    assert.deepEqual(await c.restorationPriorExecutionJournal(x.db,id),prior);preserved(x);
    assert.throws(()=>x.sqlite.prepare("UPDATE beedle_restore_execution_runs SET state='building' WHERE execution_id=?").run(x.execution.executionId),/terminal_immutable/);
    assert.throws(()=>x.sqlite.prepare('DELETE FROM beedle_restore_execution_ticks WHERE execution_id=?').run(x.execution.executionId),/immutable/);
    await assert.rejects(c.claimRestorationSourceExecutionTick(x.db,x.execution),/identity_changed/);
    assert((await c.claimRestorationSourceExecutionTick(x.db,next)).token);
  }finally{x.sqlite.close();}
});
test('a lost acknowledged bootstrap response cannot recreate its schema or execution',async()=>{
  const x=await sourceExecutionFixture();try{
    const batch=x.db.batch;let calls=0;x.db.batch=async statements=>{calls++;await batch(statements);throw Error('Lost bootstrap acknowledgment');};
    await assert.rejects(initialize(x),/Lost bootstrap/);const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(initialize(x),/before_schema_changed/);assert.equal(calls,1);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);preserved(x);
  }finally{x.sqlite.close();}
});
test('unknown journal, changed parent or wrong source coverage refuses initialization before mutation',async()=>{
  for(const mode of ['journal','parent','coverage','lineage']){const x=await sourceExecutionFixture('held-successor');try{
    if(mode==='journal')x.sqlite.exec('CREATE TABLE beedle_restore_execution_unknown(id TEXT)');
    if(mode==='parent')x.execution.parent.ownerSha256='0'.repeat(64);
    if(mode==='coverage')x.execution.initialProcessed++;
    if(mode==='lineage')x.execution.parent.heldSuccessor.tickSha256='0'.repeat(64);
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(initialize(x));assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);preserved(x);
  }finally{x.sqlite.close();}}
});
test('a claimed or uncertain source remains held and a step cannot receive a second job claim',async()=>{
  const x=await sourceExecutionFixture();try{
    await initialize(x);const tick=await c.claimRestorationSourceExecutionTick(x.db,x.execution),claim=await c.claimRestorationSourceExecutionJob(x.db,x.execution,tick.token,x.job);assert(claim);
    assert.equal(await c.claimRestorationSourceExecutionJob(x.db,x.execution,tick.token,x.job),null);
    await assert.rejects(c.completeRestorationSourceExecutionTick(x.db,x.execution,tick.token),/single_delivery_unconfirmed/);
    await c.holdRestorationSourceExecution(x.db,x.execution,'f'.repeat(64));preserved(x);
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_jobs').get().phase,'claimed');
    assert.equal(await c.claimRestorationSourceExecutionTick(x.db,x.execution),null);
    assert.throws(()=>x.sqlite.exec("UPDATE beedle_restore_execution_history SET state='completed'"),/terminal/);
  }finally{x.sqlite.close();}
});
test('a reconciled final receipt can complete through a distinct zero-source run after lost acknowledgment',async()=>{
  const x=await sourceExecutionFixture();try{
    await initialize(x);const tick=await c.claimRestorationSourceExecutionTick(x.db,x.execution),batch=x.db.batch;
    x.db.batch=async statements=>{const result=await batch(statements);if(statements.some(s=>s.sql.includes('INSERT INTO beedle_restore_source_results')))throw Error('Native lost final receipt acknowledgment');return result;};
    const args={db:x.db,originals:x.originals,target:x.target,proofs:x.proofs,identity:x.identity,execution:x.execution,tickToken:tick.token,job:x.job,profile:x.profile};
    await assert.rejects(source(args),/lost final receipt/);x.db.batch=batch;
    const receipts=x.sqlite.prepare('SELECT * FROM beedle_restore_source_results').all(),generations=x.sqlite.prepare('SELECT * FROM document_generations ORDER BY id').all(),job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs').get(),puts=x.puts();
    assert.equal(receipts.length,1);assert.equal(job.phase,'processed');await assert.rejects(source(args),/seed_changed/);assert.equal(x.puts(),puts);
    await c.holdRestorationSourceExecution(x.db,x.execution,'e'.repeat(64));
    const id=crypto.randomUUID(),prior=await c.restorationPriorExecutionJournal(x.db,id),current=x.sqlite.prepare('SELECT * FROM beedle_restore_execution_current').get(),stages={};
    for(const table of ['document_sections','section_paragraphs','document_chunks','document_reference_links','document_reference_issues'])stages[table]=x.sqlite.prepare('SELECT * FROM staged_'+table+' WHERE generation_id IN(SELECT id FROM document_generations WHERE document_id=?) ORDER BY generation_id,id').all(x.job.documentId);
    const next={...x.execution,executionId:id,executionPlanSha256:await fingerprint(id),initialProcessed:1,initialRemaining:0,priorJournalSha256:await fingerprint(prior),previousCurrentSha256:await fingerprint(current),preservedCases:[{documentId:x.job.documentId,documentSha256:await fingerprint(x.sqlite.prepare('SELECT * FROM documents').get()),jobSha256:await fingerprint(job),generationsSha256:await fingerprint(generations),stagesSha256:await fingerprint(stages)}]};
    await c.initializeRestorationSourceExecution(x.db,next,x.afterSchemaSha256,x.afterSchemaSha256);await c.completeRestorationSourceExecution(x.db,next);preserved(x,{completed:true});
    assert.deepEqual(await c.restorationPriorExecutionJournal(x.db,id),prior);assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_source_results').all(),receipts);assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations ORDER BY id').all(),generations);assert.equal(x.puts(),puts);
  }finally{x.sqlite.close();}
});
test('an unreadable original is preserved without parsing, generations, review or search publication',async()=>{
  const x=await sourceExecutionFixture('seeded',{policy:'held-unreadable'});try{
    await initialize(x);const tick=await c.claimRestorationSourceExecutionTick(x.db,x.execution),result=await source({db:x.db,originals:x.originals,target:x.target,proofs:x.proofs,identity:x.identity,execution:x.execution,tickToken:tick.token,job:x.job,profile:x.profile});
    assert.equal(result.state,'held');await c.completeRestorationSourceExecutionTick(x.db,x.execution,tick.token);await c.completeRestorationSourceExecution(x.db,x.execution);preserved(x,{completed:true});
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM document_generations').get().n,0);const document=x.sqlite.prepare('SELECT * FROM documents').get();assert.equal(document.ingestion_state,'legacy');assert.equal(document.searchable_at,null);assert.equal(document.approved_at,null);
    const proof=JSON.parse(x.sqlite.prepare('SELECT proof_json FROM beedle_restore_source_results').get().proof_json);assert.equal(proof.disposition,'held_unreadable_original_preserved');assert.equal(proof.humanApproval,false);
  }finally{x.sqlite.close();}
});
test('concurrent different-job claims within one step cannot acquire two source records',async()=>{
  const partition=id=>createHash('sha256').update(id).digest().readUInt32BE(0)%32;let extraDocumentId;
  for(let i=0;!extraDocumentId&&i<100000;i++){const id='concurrent-synthetic-case-'+i;if(partition(id)===partition('fixture'))extraDocumentId=id;}
  assert(extraDocumentId);const x=await sourceExecutionFixture('seeded',{extraDocumentId});try{
    await initialize(x);const tick=await c.claimRestorationSourceExecutionTick(x.db,x.execution),results=await Promise.allSettled([c.claimRestorationSourceExecutionJob(x.db,x.execution,tick.token,x.job),c.claimRestorationSourceExecutionJob(x.db,x.execution,tick.token,x.extraJob)]);
    assert.equal(results.filter(r=>r.status==='fulfilled'&&r.value!==null).length,1);assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='claimed'").get().n,1);
    await c.holdRestorationSourceExecution(x.db,x.execution,'f'.repeat(64));preserved(x);
  }finally{x.sqlite.close();}
});
test('a different physical partition cannot receive a job claim',async()=>{
  const x=await sourceExecutionFixture();try{
    await initialize(x);const tick=await c.claimRestorationSourceExecutionTick(x.db,x.execution);let wrong;
    for(let i=0;!wrong&&i<100000;i++){const id='wrong-synthetic-case-'+i;if(createHash('sha256').update(id).digest().readUInt32BE(0)%32!==x.identity.partitionIndex)wrong=id;}
    assert(wrong);await assert.rejects(c.claimRestorationSourceExecutionJob(x.db,x.execution,tick.token,{...x.job,documentId:wrong}),/job_partition_changed/);
    assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase!='planned'").get().n,0);preserved(x);
  }finally{x.sqlite.close();}
});
