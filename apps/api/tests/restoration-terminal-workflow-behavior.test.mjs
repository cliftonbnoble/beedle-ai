import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {terminalDispositionFixture as fixture} from './helpers/restoration-terminal-disposition-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
const {runRestorationTerminalWorkflow:run}=loadTs('src/services/restoration-terminal-workflow.ts');
async function setup(){
  const zero=await fixture(),two=await fixture('published',{partitionIndex:2});
  two.plan.operationId='bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
  const plan={version:1,operationId:'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',targets:[{database:'DB0',plan:zero.plan},{database:'DB2',plan:two.plan}]};
  const env={DB0:zero.db,DB2:two.db,PROOFS:zero.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(plan)},
    event={instanceId:plan.operationId,payload:{operationPlanSha256:await fingerprint(plan)}},calls=[];
  return {zero,two,plan,env,event,calls,step:{async do(name,options,callback){calls.push({name,options});return callback();}},close(){zero.sqlite.close();two.sqlite.close();}};
}
const held=x=>x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor').get().state;
test('two exact targets receive separate one-attempt proofs and preserve their respective generation states',async()=>{
  const x=await setup();try{
    const result=await run(x.env,x.event,x.step);assert.equal(result.state,'terminal_dispositions_complete');assert.equal(result.sourceOrPublicationSuccess,false);
    assert.equal(x.calls.length,2);assert(x.calls.every(c=>c.options.retries.limit===0&&c.options.timeout==='10 minutes'));
    assert.equal(held(x.zero),'held');assert.equal(held(x.two),'held');
    assert.equal(x.zero.sqlite.prepare('SELECT status FROM document_generations WHERE id=?').get(x.zero.plan.abandonStagingGenerationId).status,'failed');
    assert.deepEqual(x.two.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').all(x.two.next),x.two.terminalBefore.generations);
    const before=x.zero.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(run(x.env,x.event,x.step),/already_claimed/);assert.equal(x.zero.sqlite.prepare('SELECT total_changes() n').get().n,before);
  }finally{x.close();}
});
test('wrong instance, changed payload and aliased databases fail before any target mutation',async()=>{
  for(const kind of ['instance','payload','alias']){const x=await setup();try{
    if(kind==='instance')x.event.instanceId='different';else if(kind==='payload')x.event.payload.operationPlanSha256='0'.repeat(64);else x.env.DB2=x.env.DB0;
    await assert.rejects(run(x.env,x.event,x.step),/identity_or_binding_alias/);assert.equal(held(x.zero),'building');assert.equal(held(x.two),'building');
  }finally{x.close();}}
});
test('an unacknowledged conditional claim blocks SQL and cannot be reset by a repeated callback',async()=>{
  const x=await setup();try{
    const put=x.env.PROOFS.put;x.env.PROOFS.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/claim.json'))throw Error('Lost immutable claim acknowledgment');return result;};
    await assert.rejects(run(x.env,x.event,x.step),/Lost immutable claim/);await assert.rejects(run(x.env,x.event,x.step),/already_claimed/);
    assert.equal(held(x.zero),'building');assert.equal(held(x.two),'building');
  }finally{x.close();}
});
test('lost SQL acknowledgment stops the next target and never repeats the committed disposition',async()=>{
  const x=await setup();try{
    const batch=x.zero.db.batch;let calls=0;x.zero.db.batch=async statements=>{calls++;await batch(statements);throw Error('Lost terminal SQL acknowledgment');};
    await assert.rejects(run(x.env,x.event,x.step),/held_no_replay/);await assert.rejects(run(x.env,x.event,x.step),/already_claimed/);
    assert.equal(calls,1);assert.equal(held(x.zero),'held');assert.equal(held(x.two),'building');
  }finally{x.close();}
});
test('a target selector cannot impersonate another partition',async()=>{
  const x=await setup();try{
    x.plan.targets[0].database='DB2';x.env.OPERATION_JSON=JSON.stringify(x.plan);x.event.payload.operationPlanSha256=await fingerprint(x.plan);
    await assert.rejects(run(x.env,x.event,x.step),/target_scope/);assert.equal(held(x.zero),'building');assert.equal(held(x.two),'building');
  }finally{x.close();}
});
test('an expired target admission is held without modifying its executor or advancing the next target',async()=>{
  const x=await setup();try{
    x.plan.targets[0].plan.notBefore=new Date(Date.now()-25*60000).toISOString();x.plan.targets[0].plan.notAfter=new Date(Date.now()-1000).toISOString();
    x.env.OPERATION_JSON=JSON.stringify(x.plan);x.event.payload.operationPlanSha256=await fingerprint(x.plan);
    await assert.rejects(run(x.env,x.event,x.step),/held_no_replay/);assert.equal(held(x.zero),'building');assert.equal(held(x.two),'building');
  }finally{x.close();}
});
