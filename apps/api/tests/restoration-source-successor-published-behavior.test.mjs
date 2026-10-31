import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {successorJobFixture} from './helpers/restoration-source-successor-job-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';

const {initializeRestorationSourceSuccessor:initialize,claimRestorationSourceSuccessorTick:claim}=loadTs('src/services/restoration-source-successor-control.ts');
const {runSourceSuccessorWorkflow:run}=loadTs('src/services/restoration-source-successor-workflow.ts');
const {parseRestorationSourceSuccessorSpec:parse}=loadTs('src/restoration-source-successor-worker.ts');
const fixture=()=>successorJobFixture({initialize:false,partitionIndex:2,preservedGenerationStatus:'published'});
const preservedRows=x=>({document:x.sqlite.prepare('SELECT * FROM documents WHERE id=?').get(x.first),
  generations:x.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').all(x.first),
  tick:x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),history:x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history').all()});

test('a published predecessor stays published while a new execution processes only untouched records',async()=>{
  const x=await fixture();try{
    const before=preservedRows(x),now=Date.now(),plan={version:1,successorSpecJson:x.env.RESTORE_SPEC_JSON,beforeSchemaSha256:x.beforeSchemaSha256,
      notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+600000).toISOString(),completionDeadline:new Date(now+1200000).toISOString(),maximumTicks:2};
    const calls=[];const result=await run({...x.env,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(plan)},
      {instanceId:x.successor.successorId,payload:{operationPlanSha256:await fingerprint(plan)}},
      {async do(name,options,callback){calls.push({name,options});return callback();}});
    assert.equal(result.state,'source_complete');assert.equal(x.identity.partitionIndex,2);
    assert.deepEqual(preservedRows(x),before);assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM document_generations WHERE status='failed'").get().n,0);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,2);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE searchable_at IS NOT NULL OR approved_at IS NOT NULL').get().n,0);
    assert.equal(x.sqlite.prepare('SELECT preserved_generation_status FROM beedle_restore_source_successor').get().preserved_generation_status,'published');
    assert(calls.every(x=>x.options.retries.limit===0));
  }finally{x.sqlite.close();}
});

test('a published generation cannot be relabelled as failed to admit a successor',async()=>{
  const x=await fixture();try{
    await assert.rejects(initialize(x.db,{...x.successor,preservedGenerationStatus:'failed'},x.beforeSchemaSha256,x.afterSchemaSha256),/preserved_generation_changed/);
    assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM sqlite_schema WHERE name='beedle_restore_source_successor'").get().n,0);
  }finally{x.sqlite.close();}
});

test('document or sibling-generation drift cannot be adopted under the preserved generation hash',async()=>{
  for(const kind of ['document','retained']){const x=await fixture();try{
    if(kind==='document')x.sqlite.prepare('UPDATE documents SET title=? WHERE id=?').run('Changed fixture',x.first);
    else x.sqlite.prepare("UPDATE document_generations SET error=? WHERE document_id=? AND status='retained'").run('changed_fixture',x.first);
    await assert.rejects(initialize(x.db,x.successor,x.beforeSchemaSha256,x.afterSchemaSha256),/preserved_document_changed/);
    assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM sqlite_schema WHERE name='beedle_restore_source_successor'").get().n,0);
  }finally{x.sqlite.close();}}
});

test('drift after initialization stops a new tick before it can claim another source record',async()=>{
  const x=await fixture();try{
    await initialize(x.db,x.successor,x.beforeSchemaSha256,x.afterSchemaSha256);
    x.sqlite.prepare('UPDATE documents SET title=? WHERE id=?').run('Changed fixture',x.first);
    await assert.rejects(claim(x.db,x.successor),/preserved_document_changed/);
    assert.equal(x.sqlite.prepare('SELECT claim_token FROM beedle_restore_jobs WHERE document_id=?').get(x.next).claim_token,null);
  }finally{x.sqlite.close();}
});

test('the new explicit predecessor contract rejects a legacy successor declaration',async()=>{
  const x=await fixture();try{
    const old={...JSON.parse(x.env.RESTORE_SPEC_JSON),version:1};
    assert.throws(()=>parse(JSON.stringify(old)));
  }finally{x.sqlite.close();}
});
