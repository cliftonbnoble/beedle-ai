import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {restorationPublicationFixture} from './helpers/restoration-publication-workflow-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
const {runRestorationPublicationWorkflow:run,parseRestorationPublicationPlan:parse}=loadTs('src/services/restoration-publication-workflow.ts');
const event=x=>({instanceId:x.plan.operationId,payload:{operationPlanJson:JSON.stringify(x.plan)}});
const step=before=>({async do(name,options,callback){assert.equal(options.retries.limit,0);if(before)await before(name);return callback();}});
const rehash=async x=>x.env.OPERATION_JSON=JSON.stringify({version:1,operationId:x.plan.operationId,operationPlanSha256:await fingerprint(x.plan)});
const publicationCount=x=>x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE searchable_at IS NOT NULL').get().n;
const unchanged=x=>Object.fromEntries(['document_generations','beedle_restore_jobs','beedle_restore_source_results','beedle_restore_owner','beedle_restore_tick','beedle_restore_tick_history','beedle_restore_execution_runs','beedle_restore_execution_ticks','beedle_restore_execution_history','beedle_restore_execution_current'].map(table=>[table,x.sqlite.prepare('SELECT * FROM '+table+' ORDER BY 1').all()]));
test('bounded provisional publication preserves all held histories, sources and human review flags',async()=>{
  for(const mode of ['seeded','held-successor']){const x=await restorationPublicationFixture(mode);try{
    const before=unchanged(x),names=[],result=await run(x.env,event(x),step(name=>names.push(name)));
    assert.equal(result.published,x.plan.records.length);assert.equal(result.humanApproval,false);assert.equal(result.metadataConfirmation,false);
    assert.equal(result.maintenanceIntegrityAndReleaseApproval,false);assert.equal(publicationCount(x),x.plan.records.length);
    assert.equal(names.length,x.plan.maximumSteps);assert.deepEqual(unchanged(x),before);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL OR approved_revision IS NOT NULL OR qc_required_confirmed!=0').get().n,0);
    const key='provisional-publication/'+x.plan.operationId+'/complete-outcome.json';
    const outcome=JSON.parse(new TextDecoder().decode(await(await x.proofs.get(key)).arrayBuffer()));
    assert(outcome.capacity.samples>0);assert(outcome.bindingCalls<=700);assert.equal(outcome.capacity.unobservedTransientPeakCredited,false);
  }finally{x.sqlite.close();}}
});
test('wrong, expired or disabled invocations cannot issue publication SQL',async()=>{
  for(const mode of ['expired','disabled','instance','hash','payload']){const x=await restorationPublicationFixture();try{
    if(mode==='expired'){x.plan.notBefore=new Date(Date.now()-25*60000).toISOString();x.plan.notAfter=new Date(Date.now()-1000).toISOString();await rehash(x);}
    if(mode==='disabled')x.env.OPERATION_ENABLED='false';const e=event(x);if(mode==='instance')e.instanceId=crypto.randomUUID();
    if(mode==='hash')e.payload.operationPlanJson=JSON.stringify({...x.plan,documentsSha256:'0'.repeat(64)});
    if(mode==='payload')e.payload.extra=true;
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,e,step()));
    assert.equal(publicationCount(x),0);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
  }finally{x.sqlite.close();}}
});
test('owner, document, receipt, journal, catalog, revision and lease drift hold before publication',async()=>{
  for(const changed of ["UPDATE beedle_restore_owner SET last_outcome='changed'", "UPDATE documents SET title='Changed'",
    'receiptsSha256','journalsSha256','catalogVersion','UPDATE vector_lifecycle_epoch SET revision=revision+1',
    "UPDATE reviewer_queue_control SET lease_owner='different',lease_until=unixepoch()+60"]){const x=await restorationPublicationFixture();try{
    if(changed.startsWith('UPDATE '))x.sqlite.exec(changed);
    else{x.plan[changed]=changed==='catalogVersion'?'changed':'0'.repeat(64);await rehash(x);}
    await assert.rejects(run(x.env,event(x),step()));assert.equal(publicationCount(x),0);
  }finally{x.sqlite.close();}}
});
test('a changed source just before the one-statement update cannot partially publish a cohort',async()=>{
  const x=await restorationPublicationFixture();try{
    const prepare=x.env.DB.prepare;x.env.DB=new Proxy(x.env.DB,{get(target,key){if(key!=='prepare')return Reflect.get(target,key);return sql=>{
      if(sql.startsWith('UPDATE documents SET searchable_at='))x.sqlite.prepare("UPDATE documents SET source_r2_key='changed.md' WHERE id=?").run(x.plan.records[0].documentId);
      return prepare(sql);
    };}});
    await assert.rejects(run(x.env,event(x),step()));assert.equal(publicationCount(x),0);
  }finally{x.sqlite.close();}
});
test('an unacknowledged publication outcome leaves its claim and cannot replay SQL',async()=>{
  const x=await restorationPublicationFixture();try{
    const put=x.proofs.put;x.proofs.put=async function(key,...args){if(key.endsWith('publish-0000-outcome.json'))throw Error('Synthetic lost outcome');return put.call(this,key,...args);};
    await assert.rejects(run(x.env,event(x),step()));assert.equal(publicationCount(x),x.plan.records.length);
    const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,event(x),step()),/already_claimed/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);
  }finally{x.sqlite.close();}
});
test('a duplicate delivery and a conflicting immutable root cannot publish twice',async()=>{
  for(const conflict of [false,true]){const x=await restorationPublicationFixture();try{
    if(conflict){await x.proofs.put('provisional-publication/'+x.plan.operationId+'/plan.json','existing');await assert.rejects(run(x.env,event(x),step()));assert.equal(publicationCount(x),0);}
    else{await run(x.env,event(x),step());const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,event(x),step()),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);}
  }finally{x.sqlite.close();}}
});
test('the plan refuses altered policy, duplicate or unsorted identities and unbounded sizes or windows',async()=>{
  const x=await restorationPublicationFixture();try{
    assert.equal(parse(JSON.stringify(x.plan)).publicationPolicy,'unreviewed-provisional-v1');
    for(const changed of [{...x.plan,publicationPolicy:'trusted'}, {...x.plan,records:[x.plan.records[0],x.plan.records[0]]},
      {...x.plan,records:[x.plan.records[0],{...x.plan.records[0],documentId:'!earlier'}]}, {...x.plan,maximumSteps:42},
      {...x.plan,notAfter:new Date(Date.now()+31*60000).toISOString()},
      {...x.plan,records:x.plan.records.map(row=>({...row,sourceKey:'x\u0000.md'}))}])assert.throws(()=>parse(JSON.stringify(changed)));
    assert.throws(()=>parse(' '.repeat(768*1024+1)),/plan_size/);
  }finally{x.sqlite.close();}
});
