import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
const { ingestDocumentBytes } = loadTs('src/services/ingest.ts');
const { processDocumentVectorJob, consumeVectorJobBatch, requeueStaleVectorJobs, listVectorJobs, vectorJobPolicy } = loadTs('src/services/vector-jobs.ts');
async function setup(paragraphs = 3) {
  const { DB, sqlite } = memoryD1();
  const sent = [];
  const upserts = [];
  let embeds = 0;
  const objects = new Map();
  const env = {
    DB, VECTOR_NAMESPACE: 'local', AI_EMBEDDING_MODEL: 'local', R2_PUBLIC_BASE_URL: 'https://example.invalid', SOURCE_PROXY_BASE_URL: 'https://api.example',
    SOURCE_BUCKET: { async put(key, bytes) { objects.set(key, bytes); }, async get(key) { const bytes=objects.get(key); return bytes ? {size:bytes.length,arrayBuffer:async()=>bytes.slice().buffer} : null; }, async delete(key) { objects.delete(key); } },
    VECTOR_JOBS_QUEUE: { async send(message) { sent.push(message); } },
    AI: { async run() { embeds++; return { data: [[0.4, 0.6]] }; } }, VECTOR_INDEX: { async upsert(rows) { upserts.push(rows); } }
  };
  const text = '# Findings of Fact\n\n' + Array.from({ length: paragraphs }, (_, i) => `The tenant presented evidence number ${i} about the dwelling and the repair request. The landlord received written notice. `.repeat(8)).join('\n\n');
  const result = await ingestDocumentBytes(env, { title: 'Lease concurrency case', citation: 'L-14', jurisdiction: 'Test', performVectorUpsert: true, fileType: 'decision_docx', sourceFile: { filename: 'lease.md', mimeType: 'text/markdown' } }, new TextEncoder().encode(text));
  const id = result.documentId;
  const job = () => sqlite.prepare('SELECT * FROM document_vector_jobs WHERE document_id=?').get(id);
  const count = sqlite.prepare('SELECT COUNT(*) AS n FROM document_chunks WHERE document_id=?').get(id).n;
  const due = () => sqlite.prepare("UPDATE document_vector_jobs SET next_attempt_at='2000-01-01T00:00:00.000Z',enqueued_at=NULL WHERE document_id=?").run(id);
  return { env, sqlite, id, sent, upserts, job, count, due, embeds: () => embeds };
}
function delivery(documentId, attempts = 1) { const state = { acked: 0, retried: 0 }; return { state, message: { body: { documentId, generationId: 'initial' }, attempts, ack() { state.acked++; }, retry() { state.retried++; } } }; }
test('concurrent duplicate deliveries claim once and completed replay never embeds again', async () => {
  const { env, sqlite, id, job, count, embeds } = await setup();
  try {
    let release;
    const blocked = new Promise(resolve => { release = resolve; });
    const run = env.AI.run;
    env.AI.run = async (...args) => { await blocked; return run(...args); };
    const first = processDocumentVectorJob(env, id);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(job().state, 'processing');
    assert.equal((await processDocumentVectorJob(env, id)).skipped, true);
    release();
    await first;
    assert.equal(job().state, 'completed');
    assert.equal(job().attempts, 1);
    assert.equal(job().vector_count, count);
    await processDocumentVectorJob(env, id);
    assert.equal(embeds(), count);
    assert.equal(job().attempts, 1);
  }
  finally {
    sqlite.close();
  }
});
test('durable failure budget survives fresh delivery histories and operators can inspect terminal jobs', async () => {
  const { env, sqlite, id, job, due, embeds } = await setup();
  try {
    env.AI.run = async () => { throw new Error('provider secret must not be stored'); };
    for (let i = 1;i <= vectorJobPolicy.maxFailures;i++) {
      due();
      const { message, state } = delivery(id, 1);
      await consumeVectorJobBatch({ messages: [message] }, env);
      assert.equal(state.acked, 1);
      assert.equal(state.retried, 0);
      assert.equal(job().failure_count, i);
      assert.equal(job().attempts, i);
      if (i < vectorJobPolicy.maxFailures) {
        assert.equal(job().state, 'queued');
        assert.ok(Date.parse(job().next_attempt_at) > Date.now());
      }
    }
    assert.equal(job().state, 'failed');
    assert.doesNotMatch(job().last_error, /secret/);
    due();
    await processDocumentVectorJob(env, id);
    assert.equal(job().attempts, 5);
    assert.equal(await requeueStaleVectorJobs(env), 0);
    const listing = await listVectorJobs(env, { state: 'failed' });
    assert.equal(listing.jobs.length, 1);
    assert.equal(listing.jobs[0].failureCount, 5);
    assert.equal('owner_token' in listing.jobs[0], false);
    assert.equal(embeds(), 0);
  }
  finally {
    sqlite.close();
  }
});
test('abandoned leases are redispatched and exhaustion is durable even without a consumer retry', async () => {
  const { env, sqlite, id, job, sent } = await setup();
  try {
    sqlite.prepare("UPDATE document_vector_jobs SET state='processing',owner_token='abandoned',attempts=1,lease_expires_at='2000-01-01T00:00:00.000Z' WHERE document_id=?").run(id);
    assert.equal(await requeueStaleVectorJobs(env), 1);
    assert.equal(sent.at(-1).documentId, id);
    await processDocumentVectorJob(env, id);
    assert.equal(job().state, 'completed');
    assert.equal(job().failure_count, 1);
    assert.equal(job().attempts, 2);
    sqlite.prepare("UPDATE document_vector_jobs SET state='processing',owner_token='last-abandoned',failure_count=4,lease_expires_at='2000-01-01T00:00:00.000Z' WHERE document_id=?").run(id);
    assert.equal(await requeueStaleVectorJobs(env), 0);
    assert.equal(job().state, 'failed');
    assert.equal(job().failure_count, 5);
  }
  finally {
    sqlite.close();
  }
});
test('stale completion and failure cannot overwrite a new owner or replacement generation', async () => {
  for (const replacement of ['owner', 'generation']) {
    const { env, sqlite, id, job } = await setup();
    try {
      env.VECTOR_INDEX.upsert = async () => {
        if (replacement === 'owner')
          sqlite.prepare("UPDATE document_vector_jobs SET owner_token='new-owner' WHERE document_id=?").run(id);
        else
          sqlite.prepare("UPDATE document_vector_jobs SET generation_id='new-generation',owner_token=NULL,state='queued',attempts=0 WHERE document_id=?").run(id);
      };
      await processDocumentVectorJob(env, id);
      assert.equal(job().vector_count, 0);
      assert.equal(job().failure_count, 0);
      assert.equal(job().state, replacement === 'owner' ? 'processing' : 'queued');
      if (replacement === 'owner')
        assert.equal(job().owner_token, 'new-owner');
      else
        assert.equal(job().generation_id, 'new-generation');
    }
    finally {
      sqlite.close();
    }
  }
});
test('slow embedding beyond the lease cannot submit vectors or complete; expired work recovers', async () => {
  const { env, sqlite, id, job, upserts } = await setup();
  try {
    const run = env.AI.run;
    env.AI.run = async (...args) => { const result = await run(...args); sqlite.prepare("UPDATE document_vector_jobs SET lease_expires_at='2000-01-01T00:00:00.000Z' WHERE document_id=?").run(id); return result; };
    await processDocumentVectorJob(env, id);
    assert.equal(upserts.length, 0);
    assert.equal(job().state, 'processing');
    env.AI.run = run;
    await processDocumentVectorJob(env, id);
    assert.equal(job().state, 'completed');
    assert.equal(job().failure_count, 1);
  }
  finally {
    sqlite.close();
  }
});
test('partial upserts resume acknowledged progress and long jobs yield without spending failure budget', async () => {
  const { env, sqlite, id, job, count, due, upserts } = await setup(95);
  try {
    assert.ok(count > 64);
    const upsert = env.VECTOR_INDEX.upsert;
    let calls = 0;
    env.VECTOR_INDEX.upsert = async (rows) => {
      calls++; if (calls === 2)
        throw new Error('second batch interrupted'); return upsert(rows);
    };
    await processDocumentVectorJob(env, id);
    assert.equal(job().state, 'queued');
    assert.equal(job().next_chunk_offset, 16);
    assert.equal(job().failure_count, 1);
    const firstIds = new Set(upserts.flat().map(row => row.id));
    env.VECTOR_INDEX.upsert = upsert;
    due();
    while (job().state !== 'completed')
      await processDocumentVectorJob(env, id);
    assert.equal(job().vector_count, count);
    assert.equal(job().failure_count, 1);
    assert.ok(job().attempts >= 3);
    for (const id of firstIds)
      assert.equal(upserts.flat().filter(row => row.id === id).length, 1);
  }
  finally {
    sqlite.close();
  }
});
test('lost database acknowledgment and transport failures cannot reset a completed job or the durable retry count', async () => {
  const { env, sqlite, id, job, embeds, count } = await setup();
  try {
    const prepare = env.DB.prepare;
    let lost = false;
    env.DB.prepare = sql => {
      const statement = prepare(sql); if (sql.includes('completed_at=?,owner_token=NULL')) {
        const bind = statement.bind;
        statement.bind = function(...args) {
          const bound = bind.apply(this, args); const run = bound.run; bound.run = async () => {
            const result = await run.call(bound); if (!lost) {
              lost = true;
              throw new Error('ack lost');
            } return result;
          }; return bound;
        };
      } return statement;
    };
    await processDocumentVectorJob(env, id);
    assert.equal(job().state, 'completed');
    assert.equal(job().failure_count, 0);
    await processDocumentVectorJob(env, id);
    assert.equal(embeds(), count);
    env.DB.prepare = () => { throw new Error('database unavailable'); };
    const { message, state } = delivery(id, 99);
    await consumeVectorJobBatch({ messages: [message] }, env);
    assert.equal(state.acked, 0);
    assert.equal(state.retried, 1);
  }
  finally {
    sqlite.close();
  }
});
test('early duplicate retries respect backoff and malformed messages never mutate jobs', async () => {
  const { env, sqlite, id, job } = await setup();
  try {
    env.AI.run = async () => { throw new Error('failed'); };
    await processDocumentVectorJob(env, id);
    const before = job();
    await processDocumentVectorJob(env, id);
    assert.deepEqual(job(), before);
    const { message, state } = delivery(id);
    message.body = { documentId: id, forgedOwner: 'owner' };
    await consumeVectorJobBatch({ messages: [message] }, env);
    assert.equal(state.acked, 1);
    assert.deepEqual(job(), before);
  }
  finally {
    sqlite.close();
  }
});
test('recent lease expiration applies backoff once, and healthy work renews before each provider group', async () => {
  const { env, sqlite, id, job, upserts } = await setup(20);
  try {
    sqlite.prepare("UPDATE document_vector_jobs SET state='processing',owner_token='expired',lease_expires_at=?,attempts=1 WHERE document_id=?").run(new Date(Date.now() - 1000).toISOString(), id);
    assert.equal(await requeueStaleVectorJobs(env), 0);
    assert.equal(job().state, 'queued');
    assert.equal(job().failure_count, 1);
    assert.ok(Date.parse(job().next_attempt_at) > Date.now());
    await requeueStaleVectorJobs(env);
    assert.equal(job().failure_count, 1);
    sqlite.prepare('UPDATE document_vector_jobs SET next_attempt_at=NULL WHERE document_id=?').run(id);
    const run = env.AI.run;
    env.AI.run = async (...args) => { assert.ok(Date.parse(job().lease_expires_at) > Date.now() + 60000); return run(...args); };
    await processDocumentVectorJob(env, id);
    assert.ok(upserts.length);
    assert.equal(job().state, 'completed');
  }
  finally {
    sqlite.close();
  }
});
test('provider upserts that never resolve time out and release to a durable retry', async () => {
  const { env, sqlite, id, job } = await setup();
  try {
    env.VECTOR_INDEX.upsert = () => new Promise(() => { });
    await processDocumentVectorJob(env, id);
    assert.equal(job().state, 'queued');
    assert.equal(job().owner_token, null);
    assert.equal(job().failure_count, 1);
    assert.equal(job().next_chunk_offset, 0);
  }
  finally {
    sqlite.close();
  }
});
