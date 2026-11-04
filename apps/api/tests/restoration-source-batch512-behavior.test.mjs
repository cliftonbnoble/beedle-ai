import test from 'node:test';
import assert from 'node:assert/strict';
import {sourceBatchWorkflowFixture} from './helpers/restoration-source-batch-workflow-fixture.mjs';
import {loadTs} from './helpers/load-ts.mjs';
import {canonical,fingerprint} from '../scripts/lib/release-contract.mjs';
const {runSourceBatchWorkflow:run}=loadTs('src/services/restoration-source-batch-workflow.ts');
const {parseSourceExecutionWorkflowPlan:parse}=loadTs('src/services/restoration-source-execution-workflow.ts');

test('512 deliveries retain individual zero-retry budgets and hand off to a separately acknowledged 258-record remainder',async()=>{
 const x=await sourceBatchWorkflowFixture(770,{batchSize:512,heldIndexes:[0,511,512,769]});try{
  const counts=[];
  for(let sequence=0;sequence<2;sequence++){
   const child=await x.child(sequence,true),base=x.step(child.event.instanceId);let callbacks=0;
   const step={async do(name,options,callback){assert.equal(options.retries.limit,0);assert.equal(options.timeout,'10 minutes');callbacks++;return base.do(name,options,callback);}};
   const out=await run(x.env,child.event,step);counts.push(out.sourceReceipts);x.engines.set(child.event.instanceId,{status:'complete',output:out});
   assert.equal(out.state,sequence===1?'source_complete':'source_checkpointed');assert.equal(callbacks,child.plan.maximumSources+5);
   const outcomes=[...x.maps.proofs].filter(([key])=>key.startsWith('source-execution/'+child.event.instanceId+'/')&&key.endsWith('-outcome.json'));
   assert.equal(outcomes.length,child.plan.maximumSources+2);
   for(const [,row] of outcomes){const value=JSON.parse(new TextDecoder().decode(row.bytes));assert(value.bindingCalls<=700);assert.equal(value.capacity.limitReached,false);assert.equal(value.capacity.unobservedTransientPeakCredited,false);}
  }
  assert.deepEqual(counts,[512,770]);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_execution_runs').get().n,2);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,770);
  assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='processed' AND source_verified=1").get().n,766);
  assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='held' AND source_verified=1 AND generation_id IS NULL").get().n,4);
  assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM documents WHERE ingestion_state='legacy' AND active_text_generation='initial'").get().n,4);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL OR searchable_at IS NOT NULL OR qc_required_confirmed!=0').get().n,0);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_execution_history WHERE deliveries!=1 OR error_sha256 IS NOT NULL').get().n,0);
  assert.deepEqual(x.sqlite.prepare('PRAGMA foreign_key_check').all(),[]);assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.tick);
 }finally{x.sqlite.close();}
});

test('512 requires its explicit new versions and rejects over-512, empty, deadline and step-budget changes before SQL',async()=>{
 const x=await sourceBatchWorkflowFixture(513,{batchSize:512});try{
  const {plan}=await x.child(0,true);assert.equal(parse(canonical(plan)).plan.maximumSources,512);
  for(const version of [2,3,4])assert.throws(()=>parse(canonical({...plan,version})));
  for(const changed of [{maximumSources:513,maximumSteps:515},{maximumSources:0,maximumSteps:2},{maximumSteps:513},
    {completionDeadline:new Date(Date.parse(plan.notBefore)+4*60*60000+1).toISOString()}])assert.throws(()=>parse(canonical({...plan,...changed})));
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,0);
 }finally{x.sqlite.close();}
});

test('legacy roots, wrong sizes and old child versions cannot use the larger ceiling or create claims',async()=>{
 for(const mode of ['legacy-root','oversized-root','excess-batches','old-child-version']){
  const x=await sourceBatchWorkflowFixture(513,{batchSize:512});try{
   const child=await x.child(0,true);
   if(mode==='old-child-version'){
    const plan={...child.plan,version:4};child.event.payload.operationPlanJson=canonical(plan);child.event.payload.operationPlanSha256=await fingerprint(plan);
   }else{
    const root={...x.root,...(mode==='legacy-root'?{version:3}:mode==='oversized-root'?{batchSize:513}:{maximumBatches:3})};
    child.event.payload.rootPlanJson=canonical(root);x.env.OPERATION_JSON=JSON.stringify({version:1,epochId:root.epochId,rootPlanSha256:await fingerprint(root)});
   }
   const calls=x.state.sqlCalls,puts=x.state.puts;await assert.rejects(run(x.env,child.event,x.step(child.event.instanceId)));assert.equal(x.state.sqlCalls,calls);assert.equal(x.state.puts,puts);
  }finally{x.sqlite.close();}
 }
});

test('a lost source257 outcome acknowledgment retains its receipt and prevents SQL or proof replay',async()=>{
 const x=await sourceBatchWorkflowFixture(513,{batchSize:512});try{
  const child=await x.child(0,true),put=x.proofs.put;x.proofs.put=async(...args)=>{const value=await put(...args);if(args[0].endsWith('/source-0256-outcome.json'))throw Error('Synthetic lost source257 outcome');return value;};
  await assert.rejects(run(x.env,child.event,x.step(child.event.instanceId)),/step_held/);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,257);
  const changes=x.sqlite.prepare('SELECT total_changes() n').get().n,puts=x.state.puts;await assert.rejects(run(x.env,child.event,x.step(child.event.instanceId)),/step_already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);assert.equal(x.state.puts,puts);
 }finally{x.sqlite.close();}
});

test('the 9 GB guard still holds before source257 without publishing the first 256',async()=>{
 const x=await sourceBatchWorkflowFixture(513,{batchSize:512});try{
  const child=await x.child(0,true),base=x.step(child.event.instanceId),step={async do(name,options,callback){if(name.endsWith('source-0256'))x.state.sizeOverride=9000000000;return base.do(name,options,callback);}};
  await assert.rejects(run(x.env,child.event,step),/step_held/);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,256);assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_execution_runs').get().state,'held');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE searchable_at IS NOT NULL OR approved_at IS NOT NULL').get().n,0);
  assert.deepEqual(x.sqlite.prepare('PRAGMA foreign_key_check').all(),[]);
 }finally{x.sqlite.close();}
});

test('a larger batch cannot admit the next source after its original completion deadline',async()=>{
 const x=await sourceBatchWorkflowFixture(513,{batchSize:512});try{
  const child=await x.child(0,true),base=x.step(child.event.instanceId),clock=Date.now,step={async do(name,options,callback){
   if(!name.endsWith('source-0256'))return base.do(name,options,callback);
   Date.now=()=>Date.parse(child.plan.completionDeadline);try{return await base.do(name,options,callback);}finally{Date.now=clock;}
  }};
  await assert.rejects(run(x.env,child.event,step),/step_held/);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,256);
  const failure=JSON.parse(new TextDecoder().decode((await x.proofs.get('source-execution/'+child.event.instanceId+'/source-0256-failure.json')).bytes));
  assert.equal(failure.noRetry,true);assert.equal(failure.holdAcknowledged,true);assert.equal(failure.bindingFailure,null);assert(failure.holdBindingCalls<=30);assert.equal(await x.proofs.get('source-execution/'+child.event.instanceId+'/source-0256-outcome.json'),null);
 }finally{x.sqlite.close();}
});
