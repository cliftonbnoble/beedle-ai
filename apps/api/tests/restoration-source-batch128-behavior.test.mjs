import test from 'node:test';
import assert from 'node:assert/strict';
import {sourceBatchWorkflowFixture} from './helpers/restoration-source-batch-workflow-fixture.mjs';
import {loadTs} from './helpers/load-ts.mjs';
import {canonical,fingerprint} from '../scripts/lib/release-contract.mjs';
const {runSourceBatchWorkflow:run}=loadTs('src/services/restoration-source-batch-workflow.ts');
const {parseSourceExecutionWorkflowPlan:parse}=loadTs('src/services/restoration-source-execution-workflow.ts');

test('an explicit 128-record root completes 128/128/2 with unchanged journals, holds, foreign keys and per-step budgets',async()=>{
 const x=await sourceBatchWorkflowFixture(258,{batchSize:128,heldIndexes:[0]});try{
  const counts=[];
  for(let sequence=0;sequence<3;sequence++){
   const child=await x.child(sequence,true),out=await run(x.env,child.event,x.step(child.event.instanceId));counts.push(out.sourceReceipts);x.engines.set(child.event.instanceId,{status:'complete',output:out});
   assert.equal(out.state,sequence===2?'source_complete':'source_checkpointed');
   const complete=JSON.parse(new TextDecoder().decode(await(await x.proofs.get('source-execution/'+child.event.instanceId+'/complete-outcome.json')).arrayBuffer()));
   assert(complete.bindingCalls<=700);assert.equal(complete.capacity.limitReached,false);assert.equal(complete.capacity.unobservedTransientPeakCredited,false);
  }
  assert.deepEqual(counts,[128,256,258]);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_execution_runs').get().n,3);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,258);
  assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='processed' AND source_verified=1").get().n,257);
  assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='held' AND source_verified=1 AND generation_id IS NULL").get().n,1);
  assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM documents WHERE ingestion_state='legacy' AND active_text_generation='initial'").get().n,1);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL OR searchable_at IS NOT NULL OR qc_required_confirmed!=0').get().n,0);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_execution_history WHERE deliveries!=1 OR error_sha256 IS NOT NULL').get().n,0);
  assert.deepEqual(x.sqlite.prepare('PRAGMA foreign_key_check').all(),[]);assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.tick);
 }finally{x.sqlite.close();}
});

test('128 requires the new explicit plan version; old 32/64 plans and over-128 plans stay rejected',async()=>{
 const x=await sourceBatchWorkflowFixture(129,{batchSize:128});try{
  const {plan}=await x.child(0,true);assert.equal(parse(canonical(plan)).plan.maximumSources,128);
  assert.throws(()=>parse(canonical({...plan,version:3})));
  assert.throws(()=>parse(canonical({...plan,maximumSources:129,maximumSteps:131})));
  assert.throws(()=>parse(canonical({...plan,maximumSources:0,maximumSteps:2})),/bounds/);
 }finally{x.sqlite.close();}
});

test('wrong root batch size, excess batches or legacy child version cannot write SQL or immutable proofs',async()=>{
 for(const mode of ['old-root-128','new-root-129','excess-batches','old-child-version']){
  const x=await sourceBatchWorkflowFixture(129,{batchSize:128});try{
   const child=await x.child(0,true);
   if(mode==='old-child-version'){
    const plan={...child.plan,version:3};child.event.payload.operationPlanJson=canonical(plan);child.event.payload.operationPlanSha256=await fingerprint(plan);
   }else{
    const root={...x.root,...(mode==='old-root-128'?{version:2}:mode==='new-root-129'?{batchSize:129}:{maximumBatches:3})};
    child.event.payload.rootPlanJson=canonical(root);x.env.OPERATION_JSON=JSON.stringify({version:1,epochId:root.epochId,rootPlanSha256:await fingerprint(root)});
   }
   const calls=x.state.sqlCalls,puts=x.state.puts;await assert.rejects(run(x.env,child.event,x.step(child.event.instanceId)));assert.equal(x.state.sqlCalls,calls);assert.equal(x.state.puts,puts);
  }finally{x.sqlite.close();}
 }
});

test('a lost acknowledgment after the sixty-fifth source preserves progress and forbids SQL replay',async()=>{
 const x=await sourceBatchWorkflowFixture(129,{batchSize:128});try{
  const child=await x.child(0,true),put=x.proofs.put;x.proofs.put=async(...args)=>{const value=await put(...args);if(args[0].endsWith('/source-0064-outcome.json'))throw Error('Synthetic lost source65 outcome');return value;};
  await assert.rejects(run(x.env,child.event,x.step(child.event.instanceId)),/step_held/);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,65);
  const changes=x.sqlite.prepare('SELECT total_changes() n').get().n,puts=x.state.puts;await assert.rejects(run(x.env,child.event,x.step(child.event.instanceId)),/step_already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);assert.equal(x.state.puts,puts);
 }finally{x.sqlite.close();}
});

test('the 9 GB guard still holds before source65 in a larger batch',async()=>{
 const x=await sourceBatchWorkflowFixture(129,{batchSize:128});try{
  const child=await x.child(0,true),base=x.step(child.event.instanceId),step={async do(name,options,callback){assert.equal(options.retries.limit,0);if(name.endsWith('source-0064'))x.state.sizeOverride=9000000000;return base.do(name,options,callback);}};
  await assert.rejects(run(x.env,child.event,step),/step_held/);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,64);assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_execution_runs').get().state,'held');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE searchable_at IS NOT NULL OR approved_at IS NOT NULL').get().n,0);
 }finally{x.sqlite.close();}
});
