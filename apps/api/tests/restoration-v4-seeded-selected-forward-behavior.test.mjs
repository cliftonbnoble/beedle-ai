import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {sourceExecutionWorkflowFixture} from './helpers/restoration-source-execution-workflow-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
const {runSourceExecutionWorkflow}=loadTs('src/services/restoration-source-execution-workflow.ts');
const {runSelectedRestorationReprocess}=loadTs('src/services/restoration-selected-reprocess.ts');
const {completeRestorationSourceForward}=loadTs('src/services/restoration-source-forward-completion.ts');
const {restorationExecutionJournalSnapshot}=loadTs('src/services/restoration-held-execution-lineage.ts');
const tables=['document_sections','section_paragraphs','document_chunks','document_reference_links','document_reference_issues'];
async function fixture(failureMessage='Network connection lost.'){
  const x=await sourceExecutionWorkflowFixture('seeded'),batch=x.db.batch.bind(x.db);
  x.db.batch=async statements=>{if(statements.some(row=>row.sql.startsWith('INSERT INTO staged_document_sections')))
    throw Error(failureMessage);return batch(statements);};
  await assert.rejects(runSourceExecutionWorkflow(x.env,{instanceId:x.execution.executionId,payload:{operationPlanSha256:await fingerprint(x.plan)}},
    {async do(_name,options,callback){assert.equal(options.retries.limit,0);return callback();}}),/step_held/);
  x.db.batch=batch;
  const one=sql=>x.sqlite.prepare(sql).get(),document=x.sqlite.prepare('SELECT * FROM documents WHERE id=?').get(x.job.documentId),
    job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(x.job.documentId),
    generation=x.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=?').get(x.job.documentId),
    stages=Object.fromEntries(tables.map(table=>[table,x.sqlite.prepare('SELECT * FROM staged_'+table+' WHERE generation_id=? ORDER BY id').all(generation.id)])),
    journal=await restorationExecutionJournalSnapshot(x.env.DB),run=journal.beedle_restore_execution_runs[0],tick=journal.beedle_restore_execution_ticks[0],
    current=journal.beedle_restore_execution_current[0],history=journal.beedle_restore_execution_history.find(row=>row.token===tick.token),
    copy=await x.target.get(job.source_copy_key),now=Date.now();
  const plan={version:4,operationId:crypto.randomUUID(),identity:x.identity,notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+600000).toISOString(),
    schemaSha256:x.afterSchemaSha256,documentId:document.id,expectedRevision:document.document_revision,expectedResultRevision:document.document_revision+3,
    documentSha256:await fingerprint(document),jobSha256:await fingerprint(job),ownerSha256:await fingerprint(one('SELECT * FROM beedle_restore_owner')),
    tickSha256:await fingerprint(one('SELECT * FROM beedle_restore_tick')),historySha256:null,
    failedGenerationId:generation.id,failedGenerationSha256:await fingerprint(generation),source:{sourceSha256:job.source_sha256,sourceBytes:job.source_bytes,
      originalKey:job.original_key,originalVersion:x.job.originalVersion??'original-v1',copyEtag:copy.etag},policy:job.extraction_policy,profile:x.profile,
    heldSuccessor:x.execution.parent.heldSuccessor,heldExecution:{executionId:run.execution_id,executionPlanSha256:run.execution_plan_sha256,
      controlSha256:await fingerprint(run),tickSha256:await fingerprint(tick),historySha256:await fingerprint(history),currentSha256:await fingerprint(current),journalSha256:await fingerprint(journal)},
    failedStageSha256:await fingerprint(stages),expectedSourceResults:one('SELECT COUNT(*) n FROM beedle_restore_source_results').n};
  return {...x,selectedPlan:plan,journal,job,failedGeneration:generation,stages,now,bindings:{db:x.env.DB,originals:x.originals,target:x.target,proofs:x.proofs}};
}

async function forward(x){
  const p=x.selectedPlan;
  assert.equal((await runSelectedRestorationReprocess(p,x.bindings,x.now)).state,'passed');
  const read=async name=>JSON.parse(new TextDecoder().decode(await(await x.proofs.get('selected-reprocess/'+p.operationId+'/'+name+'.json')).arrayBuffer()));
  const outcome=await read('outcome'),source=await read('source-read');
  assert.equal(outcome.seededRootPreserved,true);
  return {version:4,operationId:crypto.randomUUID(),identity:p.identity,notBefore:p.notBefore,notAfter:p.notAfter,schemaSha256:p.schemaSha256,
    documentId:p.documentId,documentRevision:outcome.documentRevision,generationId:outcome.generationId,currentMetadataSha256:outcome.currentMetadataSha256,
    predecessorDocumentSha256:p.documentSha256,predecessorRevision:p.expectedRevision,jobSha256:p.jobSha256,ownerSha256:p.ownerSha256,tickSha256:p.tickSha256,
    historySha256:null,failedGenerationId:p.failedGenerationId,failedGenerationSha256:p.failedGenerationSha256,
    selected:{operationId:p.operationId,planSha256:await fingerprint(p),outcomeSha256:await fingerprint(outcome),sourceReadSha256:await fingerprint(source)},
    source:{...p.source,copyVersion:source.copyVersion,currentVersion:outcome.currentSourceVersion,currentEtag:outcome.currentSourceEtag},
    heldSuccessor:null,heldExecution:p.heldExecution,failedStageSha256:p.failedStageSha256,expectedSourceResults:p.expectedSourceResults};
}
async function preserved(x){
  assert.deepEqual(await restorationExecutionJournalSnapshot(x.env.DB),x.journal);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_owner').get(),x.oldRows.owner);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.oldRows.tick);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history').all(),[]);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(x.selectedPlan.failedGenerationId),x.failedGeneration);
  const stages=Object.fromEntries(tables.map(table=>[table,x.sqlite.prepare('SELECT * FROM staged_'+table+' WHERE generation_id=? ORDER BY id').all(x.failedGeneration.id)]));
  assert.deepEqual(stages,x.stages);
}
test('V4 separately recovers a failed executor under an unchanged seeded root and appends a truthful receipt',async()=>{
  const x=await fixture();try{
    const plan=await forward(x);await preserved(x);
    assert.equal(x.sqlite.prepare('SELECT * FROM beedle_restore_jobs').get().phase,'seeded');
    assert.equal((await completeRestorationSourceForward(plan,x.bindings,x.now)).state,'completed');await preserved(x);
    const receipt=x.sqlite.prepare('SELECT * FROM beedle_restore_source_results').get(),proof=JSON.parse(receipt.proof_json);
    assert.equal(proof.version,4);assert.equal(proof.seededRootPreserved,true);
    assert.equal(proof.forwardCompletion.predecessorTickToken,null);assert.equal(proof.forwardCompletion.predecessorHistorySha256,null);
    assert.equal(proof.heldExecutionReconciliation.executionId,x.selectedPlan.heldExecution.executionId);
    assert.equal(proof.humanApproval,false);assert.equal(x.sqlite.prepare('SELECT searchable_at FROM documents').get().searchable_at,null);
    assert.equal(x.sqlite.prepare('SELECT * FROM beedle_restore_jobs').get().claim_token,x.job.claim_token);
    const deny=new Proxy({},{get(){throw Error('No repeated SQL authorized')}});
    assert.deepEqual(await runSelectedRestorationReprocess(x.selectedPlan,{...x.bindings,db:deny},x.now),{state:'already_claimed',sqlReissued:false});
    assert.deepEqual(await completeRestorationSourceForward(plan,{...x.bindings,db:deny},x.now),{state:'already_claimed',sqlReissued:false});
  }finally{x.sqlite.close();}
});
test('V4 requires null old history and no successor, and cannot relabel a held-root plan',async()=>{
  for(const fields of [{historySha256:'f'.repeat(64)},{heldSuccessor:{}},{version:3,historySha256:'f'.repeat(64)}]){
    const x=await fixture();try{const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
      if(fields.version===3){assert.equal((await runSelectedRestorationReprocess({...x.selectedPlan,...fields},x.bindings,x.now)).state,'held');}
      else await assert.rejects(runSelectedRestorationReprocess({...x.selectedPlan,...fields},x.bindings,x.now));
      assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
    }finally{x.sqlite.close();}
  }
});
test('V4 refuses every changed held-executor bookend before staging',async()=>{
  for(const key of ['controlSha256','tickSha256','historySha256','currentSha256','journalSha256']){
    const x=await fixture();try{const plan={...x.selectedPlan,heldExecution:{...x.selectedPlan.heldExecution,[key]:'f'.repeat(64)}},before=x.sqlite.prepare('SELECT total_changes() n').get().n;
      const result=await runSelectedRestorationReprocess(plan,x.bindings,x.now);assert.equal(result.state,'held');assert.equal(result.stage,'preflight');
      assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);await preserved(x);
    }finally{x.sqlite.close();}
  }
});
test('V4 cannot recover an unreviewed content, permission or SQL failure',async()=>{
  for(const message of ['D1_ERROR: synthetic interrupted staging','permission denied','source digest mismatch']){
    const x=await fixture(message);try{const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
      const result=await runSelectedRestorationReprocess(x.selectedPlan,x.bindings,x.now);
      assert.equal(result.state,'held');assert.equal(result.stage,'preflight');
      assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);await preserved(x);
    }finally{x.sqlite.close();}
  }
});
test('V4 atomic forward failure retains the seeded claim and both terminal journals',async()=>{
  const x=await fixture();try{const plan=await forward(x);
    x.sqlite.exec("CREATE TEMP TRIGGER reject_v4_finalization BEFORE UPDATE OF phase ON beedle_restore_jobs WHEN NEW.phase='processed' BEGIN SELECT RAISE(ABORT,'synthetic atomic checkpoint failure'); END");
    const result=await completeRestorationSourceForward(plan,x.bindings,x.now);assert.equal(result.state,'held');assert.equal(result.stage,'finalize');
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,0);
    assert.equal(x.sqlite.prepare('SELECT * FROM beedle_restore_jobs').get().phase,'seeded');await preserved(x);
    assert.equal((await completeRestorationSourceForward(plan,x.bindings,x.now)).state,'already_claimed');
  }finally{x.sqlite.close();}
});
test('V4 uncertain claim acknowledgment never reaches SQL or permits replay',async()=>{
  const x=await fixture();try{const put=x.proofs.put;let claims=0;
    x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].startsWith('selected-reprocess/')&&args[0].endsWith('/claim.json')){claims++;throw Error('lost claim acknowledgment')}return result};
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;await assert.rejects(runSelectedRestorationReprocess(x.selectedPlan,x.bindings,x.now),/lost claim/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
    assert.equal((await runSelectedRestorationReprocess(x.selectedPlan,x.bindings,x.now)).state,'already_claimed');assert.equal(claims,1);await preserved(x);
  }finally{x.sqlite.close();}
});
test('V4 finalization refuses root drift after the durable source proof is written',async()=>{
  const x=await fixture();try{const plan=await forward(x),put=x.proofs.put;
    x.proofs.put=async(...args)=>{const result=await put(...args);
      if(args[0].startsWith('restoration/'))x.sqlite.exec("UPDATE beedle_restore_owner SET phase='held' WHERE singleton=1");
      return result;
    };
    const result=await completeRestorationSourceForward(plan,x.bindings,x.now);
    assert.equal(result.state,'held');assert.equal(result.stage,'finalize');
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,0);
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_jobs').get().phase,'seeded');
    assert.deepEqual(await restorationExecutionJournalSnapshot(x.env.DB),x.journal);
  }finally{x.sqlite.close();}
});
