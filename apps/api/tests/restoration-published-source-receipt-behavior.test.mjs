import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {publishedSourceReceiptFixture as fixture} from './helpers/restoration-published-source-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
import {terminalDispositionFixture} from './helpers/restoration-terminal-disposition-fixture.mjs';
const {reconcilePublishedRestorationSource:reconcile}=loadTs('src/services/restoration-published-source-receipt.ts');
const {runPublishedSourceReceiptWorkflow:runWorkflow}=loadTs('src/services/restoration-published-source-workflow.ts');
const {terminalizeRestorationAttempt:terminalize}=loadTs('src/services/restoration-terminal-disposition.ts');
const {restorationSeedMetadataSha256}=loadTs('src/services/restoration-source-job.ts');
const {verifyHeldSourceLineage}=loadTs('src/services/restoration-held-source-lineage.ts');
async function withHeldSuccessor(){
  const x=await terminalDispositionFixture('published',{partitionIndex:2});await terminalize(x.db,x.plan);
  const get=sql=>x.sqlite.prepare(sql).get(),control=get('SELECT * FROM beedle_restore_source_successor'),tick=get('SELECT * FROM beedle_restore_source_successor_tick'),history=x.sqlite.prepare('SELECT * FROM beedle_restore_source_successor_history WHERE token=?').get(tick.token),
    oldOwner=get('SELECT * FROM beedle_restore_owner'),oldTick=get('SELECT * FROM beedle_restore_tick'),oldHistory=x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history WHERE token=?').get(oldTick.token),
    job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(x.next),generations=x.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').all(x.next),
    current=await restorationSeedMetadataSha256(x.db,x.next),copy=await x.target.get(job.source_copy_key),source=await x.target.get(current.row.source_r2_key);
  const heldSuccessor={successorId:control.successor_id,successorPlanSha256:control.successor_plan_sha256,controlSha256:await fingerprint(control),tickSha256:await fingerprint(tick),historySha256:await fingerprint(history)},now=Date.now();
  const plan={version:2,operationId:'88888888-8888-4888-8888-888888888888',identity:x.identity,notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+20*60000).toISOString(),schemaSha256:x.afterSchemaSha256,
    documentId:x.next,documentRevision:current.row.document_revision,generationId:current.row.active_text_generation,ownerSha256:await fingerprint(oldOwner),tickSha256:await fingerprint(oldTick),historySha256:await fingerprint(oldHistory),jobSha256:await fingerprint(job),currentMetadataSha256:current.sha256,generationsSha256:await fingerprint(generations),expectedSourceResults:1,heldSuccessor,
    source:{sourceSha256:x.job.sourceSha256,sourceBytes:x.job.sourceBytes,originalKey:x.job.originalKey,originalVersion:'original-v1',copyEtag:copy.etag,currentEtag:source.etag}};
  return {...x,receiptPlan:plan,bindings:{db:x.db,originals:x.originals,target:x.target,proofs:x.proofs},heldBefore:{control,tick,history},generationsBefore:generations};
}
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
test('a new published receipt preserves both original and successor held journals',async()=>{
  const x=await withHeldSuccessor();try{
    assert.equal((await reconcile(x.receiptPlan,x.bindings)).state,'passed');
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,2);
    for(const [key,table]of [['control','beedle_restore_source_successor'],['tick','beedle_restore_source_successor_tick'],['history','beedle_restore_source_successor_history']])
      assert.deepEqual(x.sqlite.prepare('SELECT * FROM '+table).get(),x.heldBefore[key]);
    assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').all(x.next),x.generationsBefore);
    assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.old.tick);
  }finally{x.sqlite.close();}
});
test('missing, changed or unknown successor lineage holds before receipt SQL',async()=>{
  for(const mode of ['missing','changed','unknown']){const x=await withHeldSuccessor();try{
    if(mode==='missing')x.receiptPlan.heldSuccessor=null;
    if(mode==='changed')x.receiptPlan.heldSuccessor.tickSha256='0'.repeat(64);
    if(mode==='unknown')x.sqlite.exec('CREATE TABLE beedle_restore_source_unplanned(id TEXT)');
    if(mode==='unknown')await assert.rejects(verifyHeldSourceLineage(x.db,x.identity,x.receiptPlan.heldSuccessor),/lineage_schema/);
    else{const result=await reconcile(x.receiptPlan,x.bindings);assert.equal(result.state,'held');assert.equal(result.stage,'preflight');}
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,1);
  }finally{x.sqlite.close();}}
});
test('a version-2 original-root-only receipt explicitly proves no successor controls exist',async()=>{
  const x=await fixture();try{assert.equal((await reconcile({...x.plan,version:2,heldSuccessor:null},x.bindings)).state,'passed');preserved(x);}finally{x.sqlite.close();}
});
