import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {publishedSourceReceiptFixture as fixture} from './helpers/restoration-published-source-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
const {reconcilePublishedRestorationSource:reconcile}=loadTs('src/services/restoration-published-source-receipt.ts');
const {runPublishedSourceReceiptWorkflow:runWorkflow}=loadTs('src/services/restoration-published-source-workflow.ts');
function preserved(x){
  for(const key of ['owner','tick','history'])assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_'+(key==='history'?'tick_history':key)).get(),x.old[key]);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations ORDER BY id').all(),x.old.generations);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM documents').get(),x.old.document);
}
test('an already-built generation gains a truthful V3 receipt without reprocessing or clearing its old hold',async()=>{
  const x=await fixture();try{
    const puts=x.puts(),result=await reconcile(x.plan,x.bindings);assert.equal(result.state,'passed');assert.equal(result.sourceSqlReprocessed,false);
    assert.equal(x.puts(),puts);const row=x.sqlite.prepare('SELECT * FROM beedle_restore_source_results').get(),proof=JSON.parse(row.proof_json);
    assert.equal(proof.version,3);assert.equal(proof.publishedAttemptReconciliation.noSourceSqlReprocessed,true);assert.equal(proof.humanApproval,false);
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_jobs').get().phase,'processed');
    assert.equal(x.sqlite.prepare('SELECT claim_token FROM beedle_restore_jobs').get().claim_token,x.old.job.claim_token);preserved(x);
    const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    assert.equal((await reconcile(x.plan,x.bindings)).state,'already_claimed');assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);
  }finally{x.sqlite.close();}
});
test('an active/staged artifact discrepancy is held before any source-receipt SQL',async()=>{
  const x=await fixture();try{
    x.sqlite.exec("UPDATE staged_section_paragraphs SET text='changed artifact'");
    const result=await reconcile(x.plan,x.bindings);assert.equal(result.state,'held');assert.equal(result.stage,'manifest');
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,0);
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_jobs').get().phase,'seeded');preserved(x);
  }finally{x.sqlite.close();}
});
test('schema and original-source mismatches retain every held predecessor row',async()=>{
  for(const mode of ['schema','source']){const x=await fixture();try{
    if(mode==='schema')x.sqlite.exec('CREATE TABLE unplanned_writer(id TEXT)');else x.originals.get=async()=>null;
    assert.equal((await reconcile(x.plan,x.bindings)).state,'held');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,0);preserved(x);
  }finally{x.sqlite.close();}}
});
test('a lost final SQL acknowledgment cannot append another receipt or recreate text',async()=>{
  const x=await fixture();try{
    const batch=x.db.batch;x.db.batch=async rows=>{const result=await batch(rows);if(rows.some(row=>row.sql.startsWith('INSERT INTO beedle_restore_source_results')))throw Error('Lost final receipt response');return result;};
    assert.equal((await reconcile(x.plan,x.bindings)).state,'held');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,1);
    const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;assert.equal((await reconcile(x.plan,x.bindings)).state,'already_claimed');
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);preserved(x);
  }finally{x.sqlite.close();}
});
test('the Workflow binds the exact instance and plan to one zero-retry reconciliation step',async()=>{
  const x=await fixture();try{
    let callbacks=0;const env={DB:x.db,ORIGINALS:x.originals,TARGET_ORIGINALS:x.target,PROOFS:x.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(x.plan)},
      event={instanceId:x.plan.operationId,payload:{operationPlanSha256:await fingerprint(x.plan)}},step={async do(name,config,callback){assert.equal(name,'reconcile-published-source-receipt-once');
        assert.deepEqual(config,{retries:{limit:0,delay:'1 second',backoff:'constant'},timeout:'10 minutes'});callbacks++;return callback();}};
    assert.equal((await runWorkflow(env,event,step)).state,'passed');assert.equal(callbacks,1);preserved(x);
    await assert.rejects(runWorkflow(env,{...event,instanceId:'different'},step),/instance_or_plan/);assert.equal(callbacks,1);
  }finally{x.sqlite.close();}
});
