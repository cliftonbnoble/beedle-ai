import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {restorationReviewerFixture,provisionalRestorationReviewerFixture} from './helpers/restoration-reviewer-workflow-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
const {runRestorationReviewerWorkflow:run,parseRestorationReviewerPlan:parse}=loadTs('src/services/restoration-reviewer-workflow.ts');
const event=async x=>({instanceId:x.plan.operationId,payload:{operationPlanSha256:await fingerprint(x.plan)}});
const step=before=>({async do(name,options,callback){assert.equal(options.retries.limit,0);assert.equal(options.timeout,'10 minutes');if(before)await before(name);return callback();}});
const sourceSnapshot=x=>Object.fromEntries(['documents','document_generations','beedle_restore_jobs','beedle_restore_source_results','beedle_restore_owner','beedle_restore_tick','beedle_restore_tick_history','beedle_restore_execution_runs','beedle_restore_execution_ticks','beedle_restore_execution_history','beedle_restore_execution_current'].map(table=>[table,x.sqlite.prepare('SELECT * FROM '+table+' ORDER BY 1').all()]));
test('bounded reviewer maintenance preserves source data, all seeded or held journals and review holds',async()=>{
  for(const mode of ['seeded','held-successor']){const x=await restorationReviewerFixture(mode);try{
    const before=sourceSnapshot(x),names=[],result=await run(x.env,await event(x),step(name=>names.push(name)));
    assert.equal(result.state,'reviewer_ready');assert.equal(result.integrityPublicationAndHumanApproval,false);assert.equal(names.length,x.plan.maximumSteps);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,x.plan.documents);assert.deepEqual(sourceSnapshot(x),before);
    const outcome=JSON.parse(new TextDecoder().decode(await(await x.proofs.get('reviewer-maintenance/'+x.plan.operationId+'/complete-outcome.json')).arrayBuffer()));
    assert(outcome.capacity.samples>0);assert(outcome.bindingCalls<=700);assert.equal(outcome.capacity.unobservedTransientPeakCredited,false);
  }finally{x.sqlite.close();}}
});
test('expired, disabled and wrong-instance deliveries cannot materialize reviewer rows',async()=>{
  for(const mode of ['expired','disabled','instance']){const x=await restorationReviewerFixture();try{
    if(mode==='expired'){x.plan.notBefore=new Date(Date.now()-25*60000).toISOString();x.plan.notAfter=new Date(Date.now()-1000).toISOString();x.env.OPERATION_JSON=JSON.stringify(x.plan);}
    if(mode==='disabled')x.env.OPERATION_ENABLED='false';const e=await event(x);if(mode==='instance')e.instanceId=crypto.randomUUID();
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,e,step()));
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,0);
  }finally{x.sqlite.close();}}
});
test('source-owner drift or an unexpected existing maintenance lease holds before writes',async()=>{
  for(const mode of ['owner','lease']){const x=await restorationReviewerFixture();try{
    if(mode==='owner')x.sqlite.exec("UPDATE beedle_restore_owner SET last_outcome='changed'");
    else x.sqlite.exec("UPDATE reviewer_queue_control SET lease_owner='different',lease_until=unixepoch()+60");
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,await event(x),step()),/held/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,0);
  }finally{x.sqlite.close();}}
});
test('a lost immutable claim cannot repeat a SQL operation',async()=>{
  const x=await restorationReviewerFixture();try{
    const put=x.proofs.put;x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/initialize-claim.json'))throw Error('Synthetic lost claim acknowledgment');return result;};
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,await event(x),step()),/lost claim/);x.proofs.put=put;
    await assert.rejects(run(x.env,await event(x),step()),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
  }finally{x.sqlite.close();}
});
test('an unacknowledged reviewer outcome preserves its materialized rows and cannot be replayed',async()=>{
  const x=await restorationReviewerFixture();try{
    const put=x.proofs.put;x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/reviewer-0000-outcome.json'))throw Error('Synthetic lost outcome');return result;};
    await assert.rejects(run(x.env,await event(x),step()),/held/);x.proofs.put=put;const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,x.plan.documents);
    await assert.rejects(run(x.env,await event(x),step()),/already_claimed/);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
  }finally{x.sqlite.close();}
});
test('missing or threshold capacity samples hold before reviewer writes',async()=>{
  for(const mode of ['missing','limit']){const x=await restorationReviewerFixture();try{
    if(mode==='missing')x.capacityFixtureState.omitSize=true;else x.capacityFixtureState.sizeOverride=9000000000;
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,await event(x),step()),/held/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,0);
  }finally{x.sqlite.close();}}
});
test('step and time declarations remain bounded',async()=>{
  const x=await restorationReviewerFixture();try{
    for(const plan of [{...x.plan,maximumSteps:x.plan.maximumSteps+1},{...x.plan,notAfter:new Date(Date.parse(x.plan.notBefore)+31*60000).toISOString()},
      {...x.plan,completionDeadline:new Date(Date.parse(x.plan.notBefore)+5*60*60000).toISOString()}])assert.throws(()=>parse(JSON.stringify(plan)),/bounds/);
  }finally{x.sqlite.close();}
});

test('explicit provisional maintenance preserves already published unreviewed rows and every original held journal',async()=>{
 for(const mode of ['seeded','held-successor']){const x=await provisionalRestorationReviewerFixture(mode);try{
  const before=sourceSnapshot(x),result=await run(x.env,await event(x),step());assert.equal(result.state,'reviewer_ready');assert.equal(result.existingProvisionalPublications,x.plan.eligibleDocuments);assert.equal(result.publicationPolicy,'unreviewed-provisional-v1');assert.equal(result.integrityPublicationAndHumanApproval,false);assert.deepEqual(sourceSnapshot(x),before);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,x.plan.documents);
 }finally{x.sqlite.close();}}
});
test('an older zero-publication plan cannot silently accept published data and explicit counts or policy cannot drift',async()=>{
 for(const mode of ['legacy','count','policy','stale-publication']){const x=await provisionalRestorationReviewerFixture();try{
  if(mode==='legacy'){delete x.plan.publicationPolicy;delete x.plan.publishedDocuments;x.plan.version=1;}if(mode==='count')x.plan.publishedDocuments++;
  if(mode==='policy')x.plan.publicationPolicy='trusted';if(mode==='stale-publication')x.sqlite.exec('UPDATE documents SET published_revision=document_revision+1 WHERE searchable_at IS NOT NULL');x.env.OPERATION_JSON=JSON.stringify(x.plan);
  const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(run(x.env,await event(x),step()));assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,0);
 }finally{x.sqlite.close();}}
});
