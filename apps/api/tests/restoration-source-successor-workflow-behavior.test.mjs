import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {successorJobFixture as fixture} from './helpers/restoration-source-successor-job-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
import {partitionIntegrityCommands} from '../scripts/lib/corpus-partition-contract.mjs';
const {runSourceSuccessorWorkflow:run}=loadTs('src/services/restoration-source-successor-workflow.ts');
const {initializeRestorationSourceSuccessor:initialize}=loadTs('src/services/restoration-source-successor-control.ts');
async function setup(){
  const x=await fixture({initialize:false}),now=Date.now();
  const plan={version:1,successorSpecJson:x.env.RESTORE_SPEC_JSON,beforeSchemaSha256:x.beforeSchemaSha256,
    notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+20*60000).toISOString(),
    completionDeadline:new Date(now+3*60*60000).toISOString(),maximumTicks:2};
  const calls=[],step={async do(name,options,callback){calls.push({name,options});return callback();}};
  const env={DB:x.db,ORIGINALS:x.originals,TARGET_ORIGINALS:x.target,PROOFS:x.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(plan)};
  const event={instanceId:x.successor.successorId,payload:{operationPlanSha256:await fingerprint(plan)}};
  return {...x,plan,calls,step,env,event};
}
function preserved(x){
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.old.tick);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history').get(),x.old.history);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(x.old.failed.id),x.old.failed);
}
test('the private Workflow atomically adds separate controls then runs finite zero-retry source ticks',async()=>{
  const x=await setup();try{
    const result=await run(x.env,x.event,x.step);assert.equal(result.state,'source_complete');assert.equal(result.publicationAndIntegrityApproval,false);
    assert.equal(x.calls.length,2);for(const call of x.calls){assert.equal(call.options.retries.limit,0);assert.equal(call.options.timeout,'10 minutes');}
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor').get().state,'completed');preserved(x);
    for(const kind of ['metadata','body','phrase'])x.sqlite.exec(partitionIntegrityCommands[kind].command);
    assert.deepEqual(x.sqlite.prepare('PRAGMA quick_check').all().map(row=>row.quick_check),['ok']);
    assert.deepEqual(x.sqlite.prepare('PRAGMA foreign_key_check').all(),[]);
    const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(run(x.env,x.event,x.step),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);
  }finally{x.sqlite.close();}
});
test('an initialization acknowledgment lost after commit retains its claim and prohibits DDL replay',async()=>{
  const x=await setup();try{
    const batch=x.db.batch;x.db.batch=async rows=>{const result=await batch(rows);if(rows.some(row=>row.sql.startsWith('CREATE TABLE beedle_restore_source_successor(')))throw Error('Lost schema batch response');return result;};
    await assert.rejects(run(x.env,x.event,x.step),/step_held/);x.db.batch=batch;
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor').get().state,'building');
    const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(run(x.env,x.event,x.step),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);
    assert.equal(x.sqlite.prepare('SELECT claim_token FROM beedle_restore_jobs WHERE document_id=?').get(x.next).claim_token,null);preserved(x);
  }finally{x.sqlite.close();}
});
test('a durable step claim blocks another callback even when the tick later becomes idle',async()=>{
  const x=await setup();try{
    let repeat;
    const step={async do(name,options,callback){const value=await callback();if(name.endsWith('tick-0000'))repeat=callback;return value;}};
    await run(x.env,x.event,step);const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(repeat(),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);preserved(x);
  }finally{x.sqlite.close();}
});
test('wrong instance, changed payload and expired admission never initialize controls',async()=>{
  for(const mode of ['instance','payload','expired']){const x=await setup();try{
    if(mode==='instance')x.event.instanceId='other';else if(mode==='payload')x.event.payload.operationPlanSha256='0'.repeat(64);
    else {x.plan.notBefore=new Date(Date.now()-25*60000).toISOString();x.plan.notAfter=new Date(Date.now()-1000).toISOString();x.env.OPERATION_JSON=JSON.stringify(x.plan);x.event.payload.operationPlanSha256=await fingerprint(x.plan);}
    await assert.rejects(run(x.env,x.event,x.step));assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM sqlite_schema WHERE name='beedle_restore_source_successor'").get().n,0);preserved(x);
  }finally{x.sqlite.close();}}
});
test('one failing initialization statement rolls back all additive controls',async()=>{
  const x=await setup();try{
    const prepare=x.db.prepare;x.db.prepare=sql=>prepare(sql.startsWith('CREATE TRIGGER beedle_restore_source_successor_delete')?'SELECT * FROM deliberately_missing_table':sql);
    await assert.rejects(initialize(x.db,x.successor,x.beforeSchemaSha256,x.afterSchemaSha256),/missing_table/);
    assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM sqlite_schema WHERE name LIKE 'beedle_restore_source_successor%'").get().n,0);preserved(x);
  }finally{x.sqlite.close();}
});
test('an unacknowledged immutable initialization claim authorizes no SQL',async()=>{
  const x=await setup();try{
    const put=x.proofs.put;x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/initialize-claim.json'))throw Error('Lost claim response');return result;};
    const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(run(x.env,x.event,x.step),/Lost claim/);await assert.rejects(run(x.env,x.event,x.step),/already_claimed/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);preserved(x);
  }finally{x.sqlite.close();}
});
test('a lost completed-tick outcome remains uncredited and cannot run another tick',async()=>{
  const x=await setup();try{
    const put=x.proofs.put;x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/tick-0000-outcome.json'))throw Error('Lost completed tick response');return result;};
    await assert.rejects(run(x.env,x.event,x.step),/step_held/);
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor').get().state,'completed');
    const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(run(x.env,x.event,x.step),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);preserved(x);
  }finally{x.sqlite.close();}
});
