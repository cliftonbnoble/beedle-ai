import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {sourceExecutionWorkflowFixture} from './helpers/restoration-source-execution-workflow-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
import {createHash} from 'node:crypto';
const {runSourceExecutionWorkflow:run,parseSourceExecutionWorkflowPlan:parse}=loadTs('src/services/restoration-source-execution-workflow.ts');
const event=async x=>({instanceId:x.plan.execution.executionId,payload:{operationPlanSha256:await fingerprint(x.plan)}});
const step=before=>({async do(name,options,callback){assert.equal(options.retries.limit,0);assert.equal(options.timeout,'10 minutes');if(before)await before(name);return callback();}});
test('the finite Workflow processes one case per step and preserves seeded and held parent journals',async()=>{
  for(const mode of ['seeded','held','held-successor']){const x=await sourceExecutionWorkflowFixture(mode);try{
    const names=[],result=await run(x.env,await event(x),step(name=>names.push(name)));
    assert.equal(result.state,'source_complete');assert.equal(names.length,3);assert.equal(names.filter(name=>name.includes('source-0000')).length,1);
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_execution_runs').get().state,'completed');assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.oldRows.tick);
    const outcome=JSON.parse(new TextDecoder().decode(await(await x.proofs.get('source-execution/'+x.execution.executionId+'/source-0000-outcome.json')).arrayBuffer()));
    assert(outcome.capacity.samples>0);assert(outcome.capacity.peakBytes>0);assert.equal(outcome.capacity.unobservedTransientPeakCredited,false);assert(outcome.bindingCalls<=700);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL OR searchable_at IS NOT NULL').get().n,0);
  }finally{x.sqlite.close();}}
});
test('expired admission, wrong instance, payload and disabled delivery cannot initialize execution controls',async()=>{
  for(const mode of ['expired','instance','payload','disabled']){const x=await sourceExecutionWorkflowFixture();try{
    const e=await event(x);if(mode==='expired'){x.plan.notBefore=new Date(Date.now()-25*60000).toISOString();x.plan.notAfter=new Date(Date.now()-1000).toISOString();x.env.OPERATION_JSON=JSON.stringify(x.plan);e.payload.operationPlanSha256=await fingerprint(x.plan);}
    if(mode==='instance')e.instanceId=crypto.randomUUID();if(mode==='payload')e.payload.extra=true;if(mode==='disabled')x.env.OPERATION_ENABLED='false';
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,e,step()));assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
    assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM sqlite_schema WHERE name LIKE 'beedle_restore_execution_%'").get().n,0);
  }finally{x.sqlite.close();}}
});
test('a lost immutable step-claim acknowledgment cannot issue DDL or repeat any SQL',async()=>{
  const x=await sourceExecutionWorkflowFixture();try{
    const put=x.proofs.put;x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/initialize-claim.json'))throw Error('Native lost claim acknowledgment');return result;};
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,await event(x),step()),/lost claim/);x.proofs.put=put;
    await assert.rejects(run(x.env,await event(x),step()),/step_already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
  }finally{x.sqlite.close();}
});
test('missing capacity metadata and a reached capacity guard hold without claiming source text',async()=>{
  for(const mode of ['missing','limit']){const x=await sourceExecutionWorkflowFixture();try{
    if(mode==='missing')x.capacityFixtureState.omitSize=true;
    await assert.rejects(run(x.env,await event(x),step(name=>{if(mode==='limit'&&name.includes('source-0000'))x.capacityFixtureState.sizeOverride=9000000000;})),/step_held/);
    assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase!='planned'").get().n,0);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM document_generations').get().n,0);
    if(mode==='limit'){assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_execution_runs').get().state,'held');assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'seeded');}
  }finally{x.sqlite.close();}}
});
test('a lost source-step outcome cannot replay a completed generation even when its tick is idle',async()=>{
  const x=await sourceExecutionWorkflowFixture();try{
    const put=x.proofs.put;x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/source-0000-outcome.json'))throw Error('Native lost source outcome');return result;};
    await assert.rejects(run(x.env,await event(x),step()),/step_held/);x.proofs.put=put;
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n,puts=x.puts();await assert.rejects(run(x.env,await event(x),step()),/step_already_claimed/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.puts(),puts);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,1);assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_execution_runs').get().state,'held');
  }finally{x.sqlite.close();}
});
test('step count, start window and completion deadline cannot be broadened by a malformed plan',async()=>{
  const x=await sourceExecutionWorkflowFixture();try{
    for(const plan of [{...x.plan,maximumSteps:4},{...x.plan,notAfter:new Date(Date.parse(x.plan.notBefore)+31*60000).toISOString()},{...x.plan,completionDeadline:new Date(Date.parse(x.plan.notBefore)+5*60*60000).toISOString()}])assert.throws(()=>parse(JSON.stringify(plan)),/bounds/);
  }finally{x.sqlite.close();}
});

async function twoSourceFixture(){
  const partition=id=>createHash('sha256').update(id).digest().readUInt32BE(0)%32;
  let suffix=0;while(partition('next-'+suffix)!==partition('fixture'))suffix++;
  const x=await sourceExecutionWorkflowFixture('seeded',{extraDocumentId:'next-'+suffix});
  x.plan={...x.plan,version:2,maximumSources:1,maximumSteps:3};
  x.env.OPERATION_JSON=JSON.stringify(x.plan);return x;
}
test('a bounded child stops with a terminal held checkpoint and a distinct child completes the untouched remainder',async()=>{
  const x=await twoSourceFixture();try{
    const control=loadTs('src/services/restoration-source-execution-control.ts'),lineage=loadTs('src/services/restoration-held-execution-lineage.ts');
    assert.equal((await run(x.env,await event(x),step())).state,'source_checkpointed');
    const journal=await lineage.restorationExecutionJournalSnapshot(x.env.DB),outcome=JSON.parse(new TextDecoder().decode(
      await(await x.proofs.get('source-execution/'+x.execution.executionId+'/complete-outcome.json')).arrayBuffer()));
    assert.equal(outcome.result.sourceCheckpointed,true);assert.equal(outcome.result.remainingPlanned,1);
    assert.equal(outcome.result.journalSha256,await fingerprint(journal));
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'seeded');
    assert.equal(journal.beedle_restore_execution_runs[0].state,'held');assert.equal(journal.beedle_restore_execution_ticks[0].token,null);
    const id=crypto.randomUUID(),execution={...x.execution,executionId:id,executionPlanSha256:await fingerprint(id),initialProcessed:1,initialRemaining:1,
      priorJournalSha256:await fingerprint(await control.restorationPriorExecutionJournal(x.env.DB,id)),
      previousCurrentSha256:await fingerprint(journal.beedle_restore_execution_current[0])};
    x.plan={...x.plan,execution,beforeSchemaSha256:x.afterSchemaSha256};x.env.OPERATION_JSON=JSON.stringify(x.plan);
    assert.equal((await run(x.env,await event(x),step())).state,'source_complete');
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'indexing');
    for(const table of ['beedle_restore_execution_runs','beedle_restore_execution_ticks','beedle_restore_execution_history'])
      for(const row of journal[table])assert.deepEqual(x.sqlite.prepare('SELECT * FROM '+table+' WHERE '+(table.endsWith('history')?'token':'execution_id')+'=?')
        .get(row.token??row.execution_id),row);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,2);
  }finally{x.sqlite.close();}
});
test('an unacknowledged planned handoff retains its terminal journal and cannot replay any callback',async()=>{
  const x=await twoSourceFixture();try{
    const put=x.proofs.put;x.proofs.put=async(...args)=>{const value=await put(...args);if(args[0].endsWith('/complete-outcome.json'))throw Error('Lost handoff acknowledgment');return value;};
    await assert.rejects(run(x.env,await event(x),step()),/step_held/);
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_execution_runs').get().state,'held');
    await assert.rejects(run(x.env,await event(x),step()),/step_already_claimed/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
    assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='planned'").get().n,1);
  }finally{x.sqlite.close();}
});
test('bounded declarations reject oversized, empty-with-work and mismatched batches before a step',async()=>{
  const x=await twoSourceFixture();try{
    for(const changed of [{maximumSources:33,maximumSteps:35},{maximumSources:0,maximumSteps:2},{maximumSources:2,maximumSteps:3}])
      assert.throws(()=>parse(JSON.stringify({...x.plan,...changed})));
  }finally{x.sqlite.close();}
});
