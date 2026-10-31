import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {terminalDispositionFixture} from './helpers/restoration-terminal-disposition-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
const {terminalizeRestorationAttempt:terminalize}=loadTs('src/services/restoration-terminal-disposition.ts');
const {runSelectedRestorationReprocess:selected}=loadTs('src/services/restoration-selected-reprocess.ts');
const {completeRestorationSourceForward:forward}=loadTs('src/services/restoration-source-forward-completion.ts');
const {restorationSeedMetadataSha256:metadata}=loadTs('src/services/restoration-source-job.ts');
async function setup(){
  const x=await terminalDispositionFixture();
  // Retain a valid first staging batch to model an interruption between batches.
  const prior=x.sqlite.prepare('SELECT * FROM staged_document_sections LIMIT 1').get();assert(prior);
  const staged={...prior,generation_id:x.plan.abandonStagingGenerationId,id:'partial-stage-section',document_id:x.next};
  const fields=Object.keys(staged);x.sqlite.prepare('INSERT INTO staged_document_sections('+fields.join(',')+') VALUES('+fields.map(()=>'?').join(',')+')').run(...Object.values(staged));
  x.terminalBefore.stages.document_sections=x.sqlite.prepare('SELECT * FROM staged_document_sections WHERE generation_id=? ORDER BY id').all(x.plan.abandonStagingGenerationId);
  x.plan.stagedArtifactsSha256=await fingerprint(x.terminalBefore.stages);await terminalize(x.db,x.plan);
  const one=sql=>x.sqlite.prepare(sql).get(),failed=x.sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(x.plan.abandonStagingGenerationId),
    job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(x.next),before=await metadata(x.db,x.next),control=one('SELECT * FROM beedle_restore_source_successor'),tick=one('SELECT * FROM beedle_restore_source_successor_tick'),
    history=x.sqlite.prepare('SELECT * FROM beedle_restore_source_successor_history WHERE token=?').get(tick.token),root=one('SELECT * FROM beedle_restore_tick'),rootHistory=x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history WHERE token=?').get(root.token),copy=await x.target.get(job.source_copy_key),now=Date.now();
  const heldSuccessor={successorId:control.successor_id,successorPlanSha256:control.successor_plan_sha256,controlSha256:await fingerprint(control),tickSha256:await fingerprint(tick),historySha256:await fingerprint(history)},
    plan={version:2,operationId:'88888888-8888-4888-8888-888888888888',identity:x.identity,notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+20*60000).toISOString(),schemaSha256:x.afterSchemaSha256,documentId:x.next,
      expectedRevision:before.row.document_revision,expectedResultRevision:before.row.document_revision+3,documentSha256:before.sha256,jobSha256:await fingerprint(job),ownerSha256:await fingerprint(one('SELECT * FROM beedle_restore_owner')),tickSha256:await fingerprint(root),historySha256:await fingerprint(rootHistory),failedGenerationId:failed.id,failedGenerationSha256:await fingerprint(failed),
      source:{sourceSha256:x.job.sourceSha256,sourceBytes:x.job.sourceBytes,originalKey:x.job.originalKey,originalVersion:'original-v1',copyEtag:copy.etag},policy:x.job.extractionPolicy,profile:x.profile,heldSuccessor,failedStageSha256:x.plan.stagedArtifactsSha256,expectedSourceResults:1};
  assert(Object.values(x.terminalBefore.stages).some(rows=>rows.length>0));
  return {...x,reprocessPlan:plan,bindings:{db:x.db,originals:x.originals,target:x.target,proofs:x.proofs},failedBefore:failed,heldBefore:{control,tick,history},beforeMetadata:before};
}
async function forwardPlan(x){
  const readJson=async key=>JSON.parse(new TextDecoder().decode(await(await x.proofs.get(key)).arrayBuffer()));
  const p=x.reprocessPlan,current=await metadata(x.db,x.next),read=await readJson('selected-reprocess/'+p.operationId+'/source-read.json'),
    outcome=await readJson('selected-reprocess/'+p.operationId+'/outcome.json');
  const digest=async name=>fingerprint(await readJson('selected-reprocess/'+p.operationId+'/'+name+'.json'));
  return {version:2,operationId:'99999999-9999-4999-8999-999999999999',identity:p.identity,notBefore:p.notBefore,notAfter:p.notAfter,schemaSha256:p.schemaSha256,documentId:x.next,documentRevision:current.row.document_revision,generationId:current.row.active_text_generation,
    currentMetadataSha256:current.sha256,predecessorDocumentSha256:p.documentSha256,predecessorRevision:p.expectedRevision,jobSha256:p.jobSha256,ownerSha256:p.ownerSha256,tickSha256:p.tickSha256,historySha256:p.historySha256,failedGenerationId:p.failedGenerationId,failedGenerationSha256:p.failedGenerationSha256,
    selected:{operationId:p.operationId,planSha256:await fingerprint(p),outcomeSha256:await digest('outcome'),sourceReadSha256:await digest('source-read')},source:{...p.source,copyVersion:read.copyVersion,currentVersion:outcome.currentSourceVersion,currentEtag:outcome.currentSourceEtag},
    heldSuccessor:p.heldSuccessor,failedStageSha256:p.failedStageSha256,expectedSourceResults:p.expectedSourceResults};
}
function preserved(x){
  for(const[key,table]of [['control','beedle_restore_source_successor'],['tick','beedle_restore_source_successor_tick'],['history','beedle_restore_source_successor_history']])assert.deepEqual(x.sqlite.prepare('SELECT * FROM '+table).get(),x.heldBefore[key]);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(x.failedBefore.id),x.failedBefore);
  for(const[table,rows]of Object.entries(x.terminalBefore.stages))assert.deepEqual(x.sqlite.prepare('SELECT * FROM staged_'+table+' WHERE generation_id=? ORDER BY id').all(x.failedBefore.id),rows);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.old.tick);
}
test('partial staged evidence survives a distinct selected reprocess and forward receipt with both held journals',async()=>{
  const x=await setup();try{
    assert.equal((await selected(x.reprocessPlan,x.bindings)).state,'passed');preserved(x);
    assert.equal((await forward(await forwardPlan(x),x.bindings)).state,'completed');preserved(x);
    const job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(x.next);assert.equal(job.phase,'processed');assert.equal(job.source_verified,1);assert.equal(job.claim_token,x.terminalBefore.job.claim_token);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,2);assert.equal(x.sqlite.prepare('SELECT searchable_at FROM documents WHERE id=?').get(x.next).searchable_at,null);
  }finally{x.sqlite.close();}
});
test('changed partial stage, held lineage or source coverage refuses reprocessing',async()=>{
  for(const mode of ['stage','lineage','coverage','oldProtocol']){const x=await setup();try{
    const p=x.reprocessPlan;if(mode==='stage')p.failedStageSha256='0'.repeat(64);if(mode==='lineage')p.heldSuccessor.tickSha256='0'.repeat(64);if(mode==='coverage')p.expectedSourceResults=0;
    const plan=mode==='oldProtocol'?Object.fromEntries(Object.entries({...p,version:1}).filter(([key])=>!['heldSuccessor','failedStageSha256','expectedSourceResults'].includes(key))):p;
    const result=await selected(plan,x.bindings);assert.equal(result.state,'held');assert.equal(result.stage,'preflight');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM document_generations WHERE document_id=?').get(x.next).n,1);preserved(x);
  }finally{x.sqlite.close();}}
});
test('forward completion cannot waive a changed partial-stage or successor anchor',async()=>{
  for(const mode of ['stage','lineage','coverage']){const x=await setup();try{
    assert.equal((await selected(x.reprocessPlan,x.bindings)).state,'passed');const p=await forwardPlan(x);
    if(mode==='stage')p.failedStageSha256='0'.repeat(64);if(mode==='lineage')p.heldSuccessor.historySha256='0'.repeat(64);if(mode==='coverage')p.expectedSourceResults=0;
    const result=await forward(p,x.bindings);assert.equal(result.state,'held');assert.equal(result.stage,'preflight');assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,1);preserved(x);
  }finally{x.sqlite.close();}}
});
test('an unacknowledged forward SQL commit cannot be reissued and preserves every earlier hold',async()=>{
  const x=await setup();try{
    assert.equal((await selected(x.reprocessPlan,x.bindings)).state,'passed');const p=await forwardPlan(x),batch=x.db.batch;let calls=0;
    x.db.batch=async rows=>{calls++;const result=await batch(rows);throw Object.assign(Error('Lost final receipt acknowledgment'),{committed:!!result});};
    assert.equal((await forward(p,x.bindings)).state,'held');const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    assert.equal((await forward(p,x.bindings)).state,'already_claimed');assert.equal(calls,1);assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,2);preserved(x);
  }finally{x.sqlite.close();}
});
