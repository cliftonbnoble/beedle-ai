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
async function fixture(){
  const x=await sourceExecutionWorkflowFixture('held-successor'),batch=x.db.batch.bind(x.db);
  x.db.batch=async statements=>{if(statements.some(row=>row.sql.startsWith('INSERT INTO staged_document_sections')))
    throw Error('D1_ERROR: synthetic interrupted staging');return batch(statements);};
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
  const plan={version:3,operationId:crypto.randomUUID(),identity:x.identity,notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+600000).toISOString(),
    schemaSha256:x.afterSchemaSha256,documentId:document.id,expectedRevision:document.document_revision,expectedResultRevision:document.document_revision+3,
    documentSha256:await fingerprint(document),jobSha256:await fingerprint(job),ownerSha256:await fingerprint(one('SELECT * FROM beedle_restore_owner')),
    tickSha256:await fingerprint(one('SELECT * FROM beedle_restore_tick')),historySha256:await fingerprint(x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history WHERE token=?').get(x.oldRows.tick.token)),
    failedGenerationId:generation.id,failedGenerationSha256:await fingerprint(generation),source:{sourceSha256:job.source_sha256,sourceBytes:job.source_bytes,
      originalKey:job.original_key,originalVersion:x.job.originalVersion??'original-v1',copyEtag:copy.etag},policy:job.extraction_policy,profile:x.profile,
    heldSuccessor:x.execution.parent.heldSuccessor,heldExecution:{executionId:run.execution_id,executionPlanSha256:run.execution_plan_sha256,
      controlSha256:await fingerprint(run),tickSha256:await fingerprint(tick),historySha256:await fingerprint(history),currentSha256:await fingerprint(current),journalSha256:await fingerprint(journal)},
    failedStageSha256:await fingerprint(stages),expectedSourceResults:one('SELECT COUNT(*) n FROM beedle_restore_source_results').n};
  return {...x,selectedPlan:plan,journal,job,failedGeneration:generation,stages,now,bindings:{db:x.env.DB,originals:x.originals,target:x.target,proofs:x.proofs}};
}
test('a V3 reprocess and forward receipt preserve the newer terminal journal as well as both older held owners',async()=>{
  const x=await fixture();try{
    assert.equal((await runSelectedRestorationReprocess(x.selectedPlan,x.bindings,x.now)).state,'passed');
    const read=async name=>JSON.parse(new TextDecoder().decode(await(await x.proofs.get('selected-reprocess/'+x.selectedPlan.operationId+'/'+name+'.json')).arrayBuffer())),
      outcome=await read('outcome'),source=await read('source-read'),p=x.selectedPlan;
    assert.equal(outcome.heldExecutionLineagePreserved,true);
    const plan={version:3,operationId:crypto.randomUUID(),identity:p.identity,notBefore:p.notBefore,notAfter:p.notAfter,schemaSha256:p.schemaSha256,
      documentId:p.documentId,documentRevision:outcome.documentRevision,generationId:outcome.generationId,currentMetadataSha256:outcome.currentMetadataSha256,
      predecessorDocumentSha256:p.documentSha256,predecessorRevision:p.expectedRevision,jobSha256:p.jobSha256,ownerSha256:p.ownerSha256,tickSha256:p.tickSha256,
      historySha256:p.historySha256,failedGenerationId:p.failedGenerationId,failedGenerationSha256:p.failedGenerationSha256,
      selected:{operationId:p.operationId,planSha256:await fingerprint(p),outcomeSha256:await fingerprint(outcome),sourceReadSha256:await fingerprint(source)},
      source:{...p.source,copyVersion:source.copyVersion,currentVersion:outcome.currentSourceVersion,currentEtag:outcome.currentSourceEtag},
      heldSuccessor:p.heldSuccessor,heldExecution:p.heldExecution,failedStageSha256:p.failedStageSha256,expectedSourceResults:p.expectedSourceResults};
    assert.equal((await completeRestorationSourceForward(plan,x.bindings,x.now)).state,'completed');
    assert.deepEqual(await restorationExecutionJournalSnapshot(x.env.DB),x.journal);
    assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(p.failedGenerationId),x.failedGeneration);
    assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_owner').get(),x.oldRows.owner);
    assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.oldRows.tick);
    const proof=JSON.parse(x.sqlite.prepare('SELECT proof_json FROM beedle_restore_source_results WHERE document_id=?').get(p.documentId).proof_json);
    assert.equal(proof.heldExecutionReconciliation.executionId,p.heldExecution.executionId);assert.equal(proof.humanApproval,false);
    assert.equal(x.sqlite.prepare('SELECT searchable_at FROM documents WHERE id=?').get(p.documentId).searchable_at,null);
  }finally{x.sqlite.close();}
});
test('a V2 selected plan cannot silently ignore the newer held execution',async()=>{
  const x=await fixture();try{
    const {heldExecution,...fields}=x.selectedPlan;void heldExecution;
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
    const result=await runSelectedRestorationReprocess({...fields,version:2},x.bindings,x.now);
    assert.equal(result.state,'held');assert.equal(result.stage,'preflight');
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
    assert.deepEqual(await restorationExecutionJournalSnapshot(x.env.DB),x.journal);
  }finally{x.sqlite.close();}
});
