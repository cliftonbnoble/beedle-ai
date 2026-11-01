import test from 'node:test';import assert from 'node:assert/strict';
import {sourceBatchWorkflowFixture} from './helpers/restoration-source-batch-workflow-fixture.mjs';import {loadTs} from './helpers/load-ts.mjs';
const {runSourceBatchWorkflow:run}=loadTs('src/services/restoration-source-batch-workflow.ts');
test('a private static definition advances only from an acknowledged bounded predecessor and preserves cached callback replay',async()=>{
 const x=await sourceBatchWorkflowFixture();try{
  const first=await x.child(0),out=await run(x.env,first.event,x.step(first.event.instanceId));assert.equal(out.state,'source_checkpointed');assert.equal(out.sourceReceipts,32);
  const before=x.sqlite.prepare('SELECT total_changes() n').get().n,puts=x.state.puts;assert.deepEqual(await run(x.env,first.event,x.step(first.event.instanceId)),out);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.state.puts,puts);
  x.engines.set(first.event.instanceId,{status:'complete',output:out});const next=await x.child(1),done=await run(x.env,next.event,x.step(next.event.instanceId));assert.equal(done.state,'source_complete');assert.equal(done.sourceReceipts,33);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_execution_runs').get().n,2);assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.tick);
  assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL OR searchable_at IS NOT NULL').get().n,0);
 }finally{x.sqlite.close();}
});
test('a different root, wrong instance, unplanned sequence or disabled entry cannot reach SQL',async()=>{
 for(const mode of ['root','instance','sequence','disabled']){const x=await sourceBatchWorkflowFixture(1);try{
  const {event}=await x.child(0);let env=x.env;if(mode==='root')env={...env,OPERATION_JSON:JSON.stringify({version:1,epochId:x.root.epochId,rootPlanSha256:'0'.repeat(64)})};
  if(mode==='instance')event.instanceId=crypto.randomUUID();if(mode==='sequence')event.payload.sequence=1;if(mode==='disabled')env={...env,OPERATION_ENABLED:'false'};
  const calls=x.state.sqlCalls;await assert.rejects(run(env,event,x.step(event.instanceId)));assert.equal(x.state.sqlCalls,calls);
 }finally{x.sqlite.close();}}
});
test('a lost admission-claim acknowledgment cannot write SQL or reissue that callback',async()=>{
 const x=await sourceBatchWorkflowFixture(1);try{const {event}=await x.child(0),put=x.proofs.put;x.proofs.put=async(...args)=>{const value=await put(...args);if(args[0].endsWith('/admission-claim.json'))throw Error('Synthetic lost admission acknowledgment');return value;};
  const calls=x.state.sqlCalls;await assert.rejects(run(x.env,event,x.step(event.instanceId)),/lost admission/);await assert.rejects(run(x.env,event,x.step(event.instanceId)),/already_claimed/);assert.equal(x.state.sqlCalls,calls);
 }finally{x.sqlite.close();}
});
test('a durable checkpoint cannot authorize a next child while its actual predecessor engine is paused or errored',async()=>{
 for(const status of ['paused','errored']){const x=await sourceBatchWorkflowFixture();try{
  const first=await x.child(0),out=await run(x.env,first.event,x.step(first.event.instanceId));x.engines.set(first.event.instanceId,{status,output:out});const next=await x.child(1),before=x.sqlite.prepare('SELECT total_changes() n').get().n;
  await assert.rejects(run(x.env,next.event,x.step(next.event.instanceId)),/predecessor_not_acknowledged/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_execution_runs').get().n,1);
 }finally{x.sqlite.close();}}
});
test('a lost final outcome acknowledgment preserves the completed batch and forbids a callback replay',async()=>{
 const x=await sourceBatchWorkflowFixture(1);try{
  const {event}=await x.child(0),put=x.proofs.put;x.proofs.put=async(...args)=>{const value=await put(...args);if(args[0]===x.prefix+'0/outcome.json')throw Error('Synthetic lost final outcome');return value;};
  await assert.rejects(run(x.env,event,x.step(event.instanceId)),/lost final/);const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
  await assert.rejects(run(x.env,event,x.step(event.instanceId)),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,1);
 }finally{x.sqlite.close();}
});

test('hash-pinned event plans are conditionally saved before SQL and cached across two distinct children',async()=>{
 const x=await sourceBatchWorkflowFixture();try{
  const first=await x.child(0,true),out=await run(x.env,first.event,x.step(first.event.instanceId));assert.equal(out.sourceReceipts,32);
  const changes=x.sqlite.prepare('SELECT total_changes() n').get().n,puts=x.state.puts;assert.deepEqual(await run(x.env,first.event,x.step(first.event.instanceId)),out);assert.equal(x.state.puts,puts);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);
  x.engines.set(first.event.instanceId,{status:'complete',output:out});const next=await x.child(1,true),done=await run(x.env,next.event,x.step(next.event.instanceId));assert.equal(done.state,'source_complete');assert.equal(done.sourceReceipts,33);
  assert.ok(x.maps.proofs.has(x.prefix+'root.json'));assert.ok(x.maps.proofs.has(x.prefix+'0/plan-claim.json'));assert.ok(x.maps.proofs.has(x.prefix+'1/plan-claim.json'));
 }finally{x.sqlite.close();}
});
test('lost delivered-plan acknowledgment cannot replay a plan write or reach SQL',async()=>{
 for(const name of ['plan-claim.json','root.json','plan.json']){const x=await sourceBatchWorkflowFixture(1);try{
  const {event}=await x.child(0,true),put=x.proofs.put;let injected=false;x.proofs.put=async(...args)=>{const result=await put(...args);if(!injected&&args[0].endsWith('/'+name)){injected=true;throw Error('Synthetic lost plan acknowledgment');}return result;};
  const calls=x.state.sqlCalls;await assert.rejects(run(x.env,event,x.step(event.instanceId)),/lost plan acknowledgment/);const puts=x.state.puts;
  await assert.rejects(run(x.env,event,x.step(event.instanceId)),/already_claimed/);assert.equal(x.state.puts,puts);assert.equal(x.state.sqlCalls,calls);
 }finally{x.sqlite.close();}}
});
test('bad delivered plan/root digest or contradictory root delivery cannot write a proof or SQL',async()=>{
 for(const mode of ['plan','root','root-only','next-root']){const x=await sourceBatchWorkflowFixture(1);try{
  const {event}=await x.child(0,true);if(mode==='plan')event.payload.operationPlanJson+=' ';if(mode==='root')event.payload.rootPlanJson+=' ';
  if(mode==='root-only')delete event.payload.operationPlanJson;if(mode==='next-root')event.payload.sequence=1;
  const calls=x.state.sqlCalls,puts=x.state.puts;await assert.rejects(run(x.env,event,x.step(event.instanceId)));assert.equal(x.state.sqlCalls,calls);assert.equal(x.state.puts,puts);
 }finally{x.sqlite.close();}}
});
