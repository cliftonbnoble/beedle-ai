import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
import {completeMaintenanceJob} from '../scripts/lib/maintenance-job-client.mjs';
const {createBackfillJob,stepMaintenanceJob,inspectMaintenanceJob,controlMaintenanceJob,runScheduledMaintenance}=loadTs('src/services/maintenance-jobs.ts');
const {ingestDocumentBytes}=loadTs('src/services/ingest.ts');
const {handleBackfillRetrievalVectors}=loadTs('src/routes/admin-retrieval-vectors.ts');
const {handleBackfillReferenceValidation}=loadTs('src/routes/admin-references.ts');
async function setup() {
  const {DB,sqlite}=memoryD1();const objects=new Map();const vectors=new Map();const counts={embeds:0,upserts:0};let visible=true;
  const env={DB,VECTOR_NAMESPACE:'local',AI_EMBEDDING_MODEL:'local', AI_EMBEDDING_DIMENSIONS:"2",R2_PUBLIC_BASE_URL:'https://example.invalid',SOURCE_PROXY_BASE_URL:'https://api.example',
    SOURCE_BUCKET:{async put(key,bytes){objects.set(key,bytes);},async get(key){const bytes=objects.get(key);return bytes?{size:bytes.length,arrayBuffer:async()=>bytes.slice().buffer}:null;},async delete(key){objects.delete(key);}},
    AI:{async run(){counts.embeds++;return {data:[[0.3,0.7]]};}},VECTOR_INDEX:{async upsert(rows){counts.upserts++;for(const row of rows)vectors.set(row.id,row);},async getByIds(ids){return visible?ids.map(id=>vectors.get(id)).filter(Boolean):[];}}};
  const result=await ingestDocumentBytes(env,{title:'Maintenance fixture',citation:'TEST',jurisdiction:'Test',fileType:'decision_docx',performVectorUpsert:false,sourceFile:{filename:'sample.md',mimeType:'text/markdown'}},new TextEncoder().encode('# Findings of Fact\n\n'+Array.from({length:20},(_,i)=>('The tenant reported a persistent defect and the landlord disputed the evidence concerning the condition. '.repeat(12)+`Record ${i}.`)).join('\n\n')+'\n\n# Conclusions of Law\n\nThe Board examined the evidence and found notice was disputed.'));
  sqlite.prepare('UPDATE documents SET searchable_at=? WHERE id=?').run(new Date().toISOString(),result.documentId);
  return {env,sqlite,id:result.documentId,counts,vectors,setVisible:value=>{visible=value;}};
}
async function finish(env,job,max=30) {for(let i=0;i<max&&job.state!=='completed';i++) {job=await stepMaintenanceJob(env,job.jobId);if(job.state==='failed')throw Error(job.lastError);}assert.equal(job.state,'completed');return job;}

test('dry-run freezes a bounded immutable manifest and makes no provider calls',async()=>{
  const {env,sqlite,id,counts}=await setup();try {
    const before=sqlite.prepare('SELECT COUNT(*) AS n FROM document_chunks').get().n;
    const job=await createBackfillJob(env,'vector_backfill',{jobId:'dry',dryRun:true,limit:5});
    assert.equal(job.state,'completed');assert.equal(job.counts.total,5);assert.equal(job.counts.completed,0);
    await stepMaintenanceJob(env,job.jobId);assert.deepEqual(counts,{embeds:0,upserts:0});
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_chunks').get().n,before);
    assert.equal((await createBackfillJob(env,'vector_backfill',{jobId:'dry',dryRun:true,limit:5})).jobId,'dry');
    await assert.rejects(createBackfillJob(env,'vector_backfill',{jobId:'dry',limit:6}),/immutable plan/);
    assert.throws(()=>sqlite.prepare("UPDATE maintenance_job_items SET document_id='forged' WHERE job_id='dry'").run(),/immutable/);
    sqlite.prepare("DELETE FROM document_chunks WHERE id='maintenance-7'").run();
    assert.equal((await inspectMaintenanceJob(env,'dry')).counts.total,5);
    assert.ok(id);
  }finally{sqlite.close();}
});

test('four-item deliveries retain checkpoints; later ingestion cannot shift their selection',async()=>{
  const {env,sqlite,counts}=await setup();try {
    let job=await createBackfillJob(env,'vector_backfill',{jobId:'bounded',limit:50});const total=job.counts.total;
    job=await stepMaintenanceJob(env,job.jobId);assert.equal(counts.embeds,4);assert.equal(job.counts.completed,4);assert.equal(job.state,'queued');
    const concurrent=await ingestDocumentBytes(env,{title:'Concurrent ingestion',citation:'NEW',jurisdiction:'Test',fileType:'decision_docx',performVectorUpsert:false,sourceFile:{filename:'new.md',mimeType:'text/markdown'}},new TextEncoder().encode('# Findings of Fact\n\nNewly ingested evidence.'));
    sqlite.prepare('UPDATE documents SET searchable_at=? WHERE id=?').run(new Date().toISOString(),concurrent.documentId);
    job=await finish(env,job);
    assert.equal(job.counts.total,total);assert.equal(job.counts.completed,total);assert.equal(counts.embeds,total);
    const before={...counts};await stepMaintenanceJob(env,job.jobId);assert.deepEqual(counts,before);
  }finally{sqlite.close();}
});

test('acknowledged submissions wait for visibility without repeating embeddings',async()=>{
  const {env,sqlite,counts,setVisible}=await setup();try {
    setVisible(false);let job=await createBackfillJob(env,'vector_backfill',{limit:4});job=await stepMaintenanceJob(env,job.jobId);
    assert.equal(job.counts.submitted,4);assert.equal(counts.embeds,4);
    job=await stepMaintenanceJob(env,job.jobId);assert.equal(counts.embeds,4);assert.equal(counts.upserts,4);
    setVisible(true);job=await stepMaintenanceJob(env,job.jobId);assert.equal(job.state,'completed');assert.equal(counts.embeds,4);
  }finally{sqlite.close();}
});

test('competing workers, cancellation and stale source revisions do not start duplicate work',async()=>{
  const {env,sqlite,id,counts}=await setup();try {
    const job=await createBackfillJob(env,'vector_backfill',{limit:4});
    let release;const blocked=new Promise(resolve=>{release=resolve;});const run=env.AI.run;env.AI.run=async()=>{await blocked;return run();};
    const first=stepMaintenanceJob(env,job.jobId);await new Promise(resolve=>setImmediate(resolve));
    assert.equal((await stepMaintenanceJob(env,job.jobId)).state,'running');
    assert.equal((await controlMaintenanceJob(env,job.jobId,'cancel')).cancelRequested,true);release();
    const cancelled=await first;assert.equal(cancelled.state,'cancelled');assert.equal(counts.upserts,0);
    const stale=await createBackfillJob(env,'vector_backfill',{limit:4});
    sqlite.prepare('UPDATE documents SET document_revision=document_revision+1 WHERE id=?').run(id);
    const completed=await stepMaintenanceJob(env,stale.jobId);assert.equal(completed.state,'completed');assert.equal(completed.counts.skipped,4);assert.equal(counts.upserts,0);
  }finally{sqlite.close();}
});

test('faults retain completed work, safe errors and an explicit retry path',async()=>{
  const {env,sqlite,counts}=await setup();try {
    let calls=0;const original=env.AI.run;env.AI.run=async()=>{if(++calls===2)throw Error('private provider text');return original();};
    let job=await createBackfillJob(env,'vector_backfill',{limit:4});job=await stepMaintenanceJob(env,job.jobId);
    assert.equal(job.state,'retryable');assert.equal(job.counts.completed,3);assert.doesNotMatch(job.lastError,/private/);
    assert.equal(counts.embeds,3);await controlMaintenanceJob(env,job.jobId,'retry');env.AI.run=original;
    job=await finish(env,job);assert.equal(job.counts.completed,4);assert.equal(counts.embeds,4);
    // Crash recovery consumes a durable failure, and exhausts after five abandoned steps.
    const abandoned=await createBackfillJob(env,'vector_backfill',{limit:1});
    sqlite.prepare("UPDATE maintenance_jobs SET state='running',owner_token='lost',lease_expires_at='2000-01-01' WHERE id=?").run(abandoned.jobId);
      // An abandoned delivery with four recorded failures is held before any new provider work.
      sqlite.prepare('UPDATE maintenance_jobs SET failures=4 WHERE id=?').run(abandoned.jobId);
      assert.equal((await stepMaintenanceJob(env,abandoned.jobId)).state,'failed');
  }finally{sqlite.close();}
});

test('malformed maintenance mutations are rejected before a job can be created',async()=>{
  const {env,sqlite}=await setup();try {
    for(const handler of [handleBackfillRetrievalVectors,handleBackfillReferenceValidation]) {
      for(const body of ['{','',JSON.stringify({offset:10}),JSON.stringify({limit:-1}),JSON.stringify({limit:1,unexpected:true})]) {
        const response=await handler(new Request('https://api.example/backfill',{method:'POST',headers:{'content-type':'application/json'},body}),env);
        assert.equal(response.status,400);
      }
    }
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM maintenance_jobs').get().n,0);
  }finally{sqlite.close();}
});

test('compatibility polling preserves identity and terminates on failed/cancelled jobs',async()=>{
  let calls=0;const initial={jobId:'test-job',state:'queued',counts:{total:1,completed:0,skipped:0}};
  const result=await completeMaintenanceJob('https://api.example',initial,{pollMs:0,sleep:async()=>{},onProgress:()=>{},fetchImpl:async(url,init)=>{calls++;assert.equal(new URL(url).pathname,'/admin/maintenance/jobs/test-job/step');assert.equal(init.method,'POST');return Response.json({...initial,state:'completed',result:{state:'published'}});}});
  assert.equal(result.state,'published');assert.equal(calls,1);
  for(const state of ['failed','cancelled']) await assert.rejects(completeMaintenanceJob('https://api.example',{...initial,state},{onProgress:()=>{}}),new RegExp(state));
});

test('reference checkpoints share the source/owner transaction and pin the catalog',async()=>{
  const {env,sqlite}=await setup();try {
    let job=await createBackfillJob(env,'reference_backfill',{jobId:'reference-owner',limit:1});
    const before=sqlite.prepare('SELECT COUNT(*) AS n FROM document_reference_validation_history').get().n;
    const batch=env.DB.batch.bind(env.DB);let intercepted=false;
    env.DB.batch=async statements=>{
      if(!intercepted&&statements.some(statement=>statement.sql.includes('INSERT INTO maintenance_revision_guards'))) {
        intercepted=true;
        sqlite.prepare("UPDATE maintenance_jobs SET owner_token='new-owner' WHERE id=?").run(job.jobId);
      }
      return batch(statements);
    };
    job=await stepMaintenanceJob(env,job.jobId);
    assert.equal(job.state,'running');assert.equal(job.counts.completed,0);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_reference_validation_history').get().n,before);
    env.DB.batch=batch;
    const {rebuildLegalReferences}=loadTs('src/services/legal-references.ts');
    const {catalogPayload}=await import('./helpers/catalog-fixture.mjs');
    const pinned=await createBackfillJob(env,'reference_backfill',{jobId:'catalog-pin',limit:1});
    await rebuildLegalReferences(env,catalogPayload());
    const failed=await stepMaintenanceJob(env,pinned.jobId);
    assert.equal(failed.state,'failed');assert.match(failed.lastError,/catalog changed/);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_reference_validation_history').get().n,before);
  }finally{sqlite.close();}
});

test('shared-budget exhaustion defers maintenance without spending its failure budget',async()=>{
  const {env,sqlite,counts}=await setup();try {
    env.COMPUTE_BUDGETS_JSON=JSON.stringify({llm:{perMinute:20,perDay:500},embedding:{perMinute:1,perDay:1},vector_write:{perMinute:10,perDay:10}});
    const job=await createBackfillJob(env,'vector_backfill',{limit:4});
    const deferred=await stepMaintenanceJob(env,job.jobId);
    assert.equal(deferred.state,'retryable');assert.equal(deferred.failures,0);assert.equal(deferred.counts.completed,1);assert.equal(counts.embeds,1);
    assert.ok(Date.parse(deferred.nextAttemptAt)>Date.now());
    await runScheduledMaintenance(env);assert.equal(counts.embeds,1);
  }finally{sqlite.close();}
});

test('source keyset cursors advance across finite manifests without OFFSET shifts',async()=>{
  const {env,sqlite}=await setup();try {
    const first=await createBackfillJob(env,'vector_backfill',{limit:3,dryRun:true});
    const second=await createBackfillJob(env,'vector_backfill',{limit:3,dryRun:true,after:first.nextAfter});
    const firstKeys=sqlite.prepare('SELECT item_key FROM maintenance_job_items WHERE job_id=?').all(first.jobId).map(row=>row.item_key);
    const secondKeys=sqlite.prepare('SELECT item_key FROM maintenance_job_items WHERE job_id=?').all(second.jobId).map(row=>row.item_key);
    assert.equal(firstKeys.length,3);assert.equal(secondKeys.length,3);assert.ok(secondKeys.every(key=>key>first.nextAfter));assert.equal(new Set([...firstKeys,...secondKeys]).size,6);
  }finally{sqlite.close();}
});


test('cancellation survives an abandoned in-flight owner',async()=>{
  const {env,sqlite,counts}=await setup();try {
    const job=await createBackfillJob(env,'vector_backfill',{limit:1});
    sqlite.prepare("UPDATE maintenance_jobs SET state='running',owner_token='lost',lease_expires_at='2000-01-01',cancel_requested=1 WHERE id=?").run(job.jobId);
    await runScheduledMaintenance(env);
    assert.equal((await inspectMaintenanceJob(env,job.jobId)).state,'cancelled');
    assert.deepEqual(counts,{embeds:0,upserts:0});
  }finally{sqlite.close();}
});
