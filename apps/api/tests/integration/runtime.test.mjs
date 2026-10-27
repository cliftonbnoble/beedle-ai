import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import { createWorkerFixture } from './fixture.mjs';
let fixture;
before(async () => { fixture = await createWorkerFixture(); }, { timeout: 90000 });
after(async () => { await fixture?.stop(); });
test('real Worker starts from all migrations and holds incomplete indexes until explicit maintenance', async () => {
    const health = await fixture.request('/health');
    assert.equal(health.ok, true);
    await fixture.request('/search', { query: 'notice' }, { status: 503 });
    let state = await fixture.request('/admin/search-index');
    for (let step = 0; step < 30 && !state.readiness.ready; step++)
        state = await fixture.request('/admin/search-index/step', {});
    assert.equal(state.readiness.ready, true);
    const empty = await fixture.request('/search', { query: 'notice' });
    assert.deepEqual(empty.results, []);
});
let seedIds;
test('published search and reviewer low-confidence filters preserve all surrounding scope predicates', async () => {
    const { seedFixture } = await import('./seeds.mjs');
    seedIds = await seedFixture(fixture);
    const result = await fixture.request('/search', { query: 'repair notice', limit: 10 });
    assert.deepEqual(new Set(result.results.map(row => row.documentId)), new Set([seedIds.alpha, seedIds.beta]));
    const queue = await fixture.request('/admin/ingestion/documents?status=approved&fileType=decision_docx&realOnly=true&lowConfidenceTaxonomyOnly=true&limit=100');
    assert.deepEqual(new Set(queue.documents.map(row => row.id)), new Set([seedIds.alpha, seedIds.beta]));
    assert.ok(!queue.documents.some(row => row.id === seedIds.staged));
});
test('an R2 source-write failure cannot publish a document or enqueue vector work', async () => {
    const { ingestCase } = await import('./seeds.mjs');
    const before = await fixture.request('/admin/ingestion/documents?limit=100'), jobs = await fixture.request('/admin/vector-jobs');
    await fixture.request('/__test/control', { fault: 'source_put' });
    await ingestCase(fixture, 'SourceFailure', { vectors: true, status: 500 });
    const after = await fixture.request('/admin/ingestion/documents?limit=100');
    assert.deepEqual(after.documents.map(row => row.id).sort(), before.documents.map(row => row.id).sort());
    assert.deepEqual((await fixture.request('/admin/vector-jobs')).jobs, jobs.jobs);
});
test('native D1 rolls back a failed metadata transaction and converges after a lost acknowledgment', async () => {
    const path = '/admin/ingestion/documents/' + seedIds.alpha;
    let detail = await fixture.request(path);
    const revision = detail.documentRevision;
    await fixture.request('/__test/control', { fault: 'batch:' + path + '/metadata' });
    await fixture.request(path + '/metadata', { expectedRevision: revision, mutationId: crypto.randomUUID(), case_number: 'FAILED-CHANGE' }, { status: 500 });
    detail = await fixture.request(path);
    assert.equal(detail.documentRevision, revision);
    assert.equal(detail.caseNumber, 'T-ALPHA');
    await fixture.request('/__test/control', { fault: 'lost_ack:' + path + '/metadata' });
    const mutationId = crypto.randomUUID();
    const payload = { expectedRevision: revision, mutationId, case_number: 'RETRIED-CHANGE' };
    const applied = await fixture.request(path + '/metadata', payload), replayed = await fixture.request(path + '/metadata', payload);
    assert.equal(applied.documentRevision, replayed.documentRevision);
    assert.equal(replayed.caseNumber, 'RETRIED-CHANGE');
    assert.equal(replayed.mutationReceipt.replayed, true);
    await fixture.request(path + '/approve', { expectedRevision: revision }, { status: 409 });
    const fresh = await fixture.request(path);
    assert.equal(fresh.approvedAt, null);
});
test('a configured provider cannot inject unknown evidence IDs or force an oversized response to be consumed', async () => {
    await fixture.request('/__test/control', { mode: 'unknown_id' });
    const result = await fixture.request('/api/assistant/chat', { messages: [{ role: 'user', content: 'repair notice' }] });
    assert.equal(result.model, 'grounded-extractive-fallback');
    assert.equal(result.fallbackReason, 'unknown_evidence_id');
    assert.ok(result.claims.every(claim => claim.status === 'source_excerpt'));
    await fixture.request('/__test/control', { mode: 'oversized' });
    const oversized = await fixture.request('/api/assistant/chat', { messages: [{ role: 'user', content: 'repair notice' }] });
    assert.equal(oversized.model, 'grounded-extractive-fallback');
    assert.match(oversized.fallbackReason, /response_too_large/);
    await fixture.request('/__test/control', { mode: 'valid' });
});
test('populated native FTS recovery refuses reads during rebuild and restores exact published decision identities', async () => {
    const before = await fixture.request('/search', { query: 'repair notice', limit: 10 });
    assert.ok(before.results.length);
    await fixture.request('/__test/lose-postings', {});
    const current = await fixture.request('/admin/search-index/recovery');
    const rebuilding = await fixture.request('/admin/search-index/rebuild', { dryRun: false, generation: crypto.randomUUID(), expectedGeneration: current.generation });
    assert.equal(rebuilding.status, 'running');
    await fixture.request('/search', { query: 'repair notice' }, { status: 503 });
    let state = rebuilding;
    for (let n = 0; n < 40 && state.status !== 'completed'; n++)
        state = await fixture.request('/admin/search-index/rebuild/step', { generation: rebuilding.generation });
    assert.equal(state.status, 'completed');
    const { readyIndexes } = await import('./seeds.mjs');
    await readyIndexes(fixture);
    const after = await fixture.request('/search', { query: 'repair notice', limit: 10 });
    assert.deepEqual(new Set(after.results.map(row => row.documentId)), new Set(before.results.map(row => row.documentId)));
});
test('real local Queue retries a provider failure and resumes the same durable job without duplicating vectors', async () => {
    const { ingestCase } = await import('./seeds.mjs'), { eventually } = await import('./fixture.mjs');
    await fixture.request('/__test/control', { fault: 'vector_upsert' });
    const doc = await ingestCase(fixture, 'Queue', { vectors: true });
    // Provider failures are checkpointed into the durable outbox and the delivery is
    // acknowledged; transport/infrastructure exceptions use Queue retry instead.
    await eventually(async () => { const events = await fixture.request('/__test/events'); assert.ok(events.some(row => row.kind === 'queue_ack' && JSON.parse(row.payload_json).body.documentId === doc.documentId)); });
    const failed = (await fixture.request('/admin/vector-jobs')).jobs.find(row => row.documentId === doc.documentId);
    assert.equal(failed.state, 'queued');
    assert.equal(failed.failureCount, 1);
    assert.equal(failed.vectorCount, 0);
    assert.equal((await fixture.request('/__test/requeue-due', { documentId: doc.documentId })).sent, 1);
    await eventually(async () => { const row = (await fixture.request('/admin/vector-jobs')).jobs.find(row => row.documentId === doc.documentId); assert.equal(row.state, 'completed'); assert.equal(row.vectorCount, row.expectedChunkCount); assert.equal(row.attempts, 2); });
    const vectors = await fixture.request('/__test/vectors?documentId=' + doc.documentId);
    assert.equal(new Set(vectors.map(row => row.chunkId)).size, vectors.length);
    const delivery = await fixture.request('/__test/redeliver', { documentId: doc.documentId, generationId: 'initial' });
    assert.equal(delivery.ack, true);
    assert.equal(delivery.retry, false);
    const row = (await fixture.request('/admin/vector-jobs')).jobs.find(row => row.documentId === doc.documentId);
    assert.equal(row.state, 'completed');
    assert.equal(row.vectorCount, row.expectedChunkCount);
    assert.equal(row.attempts, 2);
    assert.equal(vectors.length, row.expectedChunkCount);
    assert.deepEqual(await fixture.request('/__test/vectors?documentId=' + doc.documentId), vectors);
    const jobKey=createHash('sha256').update(JSON.stringify(['vector',doc.documentId,'initial'])).digest('hex').slice(0,32);
    await eventually(()=>assert.ok(fixture.logs().split(jobKey).length-1>=3));
});

test('native ingress traces include request timing and exclude private debug query text',async()=>{
 const secret='PRIVATE_NATIVE_FACT_'+crypto.randomUUID();
 const response=await fetch(fixture.url+'/admin/retrieval/debug',{method:'POST',headers:{'content-type':'application/json','x-request-id':secret},body:JSON.stringify({query:secret,limit:5})});assert.equal(response.status,200);
 const id=response.headers.get('x-request-id');assert.match(id,/^[a-f0-9-]{36}$/);assert.notEqual(id,secret);assert.match(response.headers.get('server-timing'),/^app;dur=/);await response.json();
 const {eventually}=await import('./fixture.mjs');await eventually(()=>assert.ok(fixture.logs().includes(id)));assert.ok(!fixture.logs().includes(secret));
 const health=await fixture.request('/admin/operations/health');assert.equal(typeof health.healthy,'boolean');assert.equal(typeof health.metrics.vector_completed_incomplete,'number');
});

test('native scheduled maintenance publishes a private aggregate heartbeat',async()=>{
 const response=await fetch(fixture.url+'/__scheduled',{signal:AbortSignal.timeout(20000)});assert.equal(response.status,200);
 const {eventually}=await import('./fixture.mjs');await eventually(()=>assert.ok(fixture.logs().includes('job_health')));
 const health=await fixture.request('/admin/operations/health');assert.equal(health.metrics.maintenance_expired_leases,0);
});

test('native upload receipt converges after lost publication acknowledgment and rejects conflicting retries',async()=>{
 const {caseSourceText}=await import('./seeds.mjs');
 const key=crypto.randomUUID(),payload={idempotencyKey:key,fileType:'decision_docx',jurisdiction:'Test',title:'Native retry',citation:'T-RETRY',performVectorUpsert:false,sourceFile:{filename:'retry.md',mimeType:'text/markdown',bytesBase64:Buffer.from(caseSourceText).toString('base64')}};
 await fixture.request('/__test/control',{fault:'lost_publication_ack'});
 const first=await fixture.request('/ingest/decision',payload,{status:201});
 const cryptoHash=createHash('sha256').update(JSON.stringify(['ingestion/deployment/1','decision_docx',key])).digest('hex');
 const completed=await fixture.request('/ingest/requests/'+cryptoHash);assert.equal(completed.state,'complete');assert.deepEqual(completed.result,first);
 assert.deepEqual(await fixture.request('/ingest/decision',payload,{status:201}),first);
 await fixture.request('/ingest/decision',{...payload,title:'Conflicting bytes or metadata'},{status:409});
 const independent={...payload,idempotencyKey:crypto.randomUUID(),citation:'T-RETRY-2'};
 const done=await fixture.request('/ingest/decision',independent,{status:201});
 assert.deepEqual(await fixture.request('/ingest/decision',independent,{status:201}),done);
 const status=await fixture.request('/ingest/requests/'+done.ingestionRequestId);assert.equal(status.state,'complete');assert.deepEqual(status.result,done);
 assert.deepEqual(await fixture.request(`/ingest/requests/${independent.idempotencyKey}?operation=decision_docx`),status);
});
