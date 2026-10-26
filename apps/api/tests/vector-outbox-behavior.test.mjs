import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
const { ingestDocumentBytes } = loadTs('src/services/ingest.ts');
const { processDocumentVectorJob, requeueStaleVectorJobs } = loadTs('src/services/vector-jobs.ts');
function setup() {
  const { DB, sqlite } = memoryD1();
  const objects = new Map();
  const sent = [];
  let embeds = 0;
  const env = {
    DB, VECTOR_NAMESPACE: 'outbox-local', AI_EMBEDDING_MODEL: 'local', R2_PUBLIC_BASE_URL: 'https://example.invalid', SOURCE_PROXY_BASE_URL: 'https://api.example',
    SOURCE_BUCKET: { async put(key, bytes) { objects.set(key, new Uint8Array(bytes)); }, async get(key) { const bytes = objects.get(key); return bytes ? { size: bytes.length, arrayBuffer: async () => bytes.slice().buffer } : null; }, async delete(key) { objects.delete(key); } },
    VECTOR_JOBS_QUEUE: { async send(message) { sent.push(message); } }, AI: { async run() { embeds++; return { data: [[0.2, 0.8]] }; } }, VECTOR_INDEX: { async upsert() { } }
  };
  const text = '# Findings of Fact\n\n' + Array.from({ length: 45 }, (_, i) => `The tenant presented evidence number ${i} about the dwelling and the repair request. The landlord received written notice. `.repeat(4)).join('\n\n');
  const ingest = () => ingestDocumentBytes(env, { title: 'Outbox import', citation: 'O-15', jurisdiction: 'Test', performVectorUpsert: true, fileType: 'decision_docx', sourceFile: { filename: 'outbox.md', mimeType: 'text/markdown' } }, new TextEncoder().encode(text));
  return { env, sqlite, objects, sent, ingest, embeds: () => embeds };
}
test('a sweeper and consumer interleaved after every text batch cannot publish an unfinished import', async () => {
  const { env, sqlite, ingest, sent, embeds } = setup();
  try {
    const batch = env.DB.batch.bind(env.DB);
    let checkpoints = 0;
    env.DB.batch = async (statements) => {
      const result = await batch(statements);
      if (!statements.some(row => row.sql.includes("SET ingestion_state = 'ready'"))) {
        const doc = sqlite.prepare('SELECT id,ingestion_state FROM documents').get();
        if (doc) {
          checkpoints++;
          assert.equal(doc.ingestion_state, 'preparing');
          assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_vector_jobs').get().n, 0);
          assert.equal(await requeueStaleVectorJobs(env), 0);
          assert.equal((await processDocumentVectorJob(env, doc.id)).skipped, true);
        }
      }
      return result;
    };
    const result = await ingest();
    assert.ok(checkpoints > 1);
    assert.equal(embeds(), 0);
    assert.equal(sent.length, 1);
    const job = sqlite.prepare('SELECT * FROM document_vector_jobs WHERE document_id=?').get(result.documentId);
    assert.equal(job.state, 'queued');
    assert.ok(job.outbox_ready_at);
    assert.equal(job.expected_chunk_count, result.chunkCount);
    assert.match(job.chunk_manifest_sha256, /^[a-f0-9]{64}$/);
    await processDocumentVectorJob(env, result.documentId);
    assert.equal(embeds(), result.chunkCount);
    assert.equal(sqlite.prepare('SELECT state FROM document_vector_jobs').get().state, 'completed');
  }
  finally {
    sqlite.close();
  }
});
test('storage and transaction failures compensate only unfinished imports and cannot leave dispatchable jobs', async () => {
  for (const fault of ['text', 'source', 'source-digest', 'publication']) {
    const { env, sqlite, ingest, objects, sent, embeds } = setup();
    try {
      const batch = env.DB.batch.bind(env.DB);
      let batches = 0;
      env.DB.batch = async (statements) => {
        batches++; if ((fault === 'text' && batches === 2) || (fault === 'publication' && statements.some(row => row.sql.includes("SET ingestion_state = 'ready'"))))
          throw new Error('injected batch failure'); return batch(statements);
      };
      if (fault === 'source')
        env.SOURCE_BUCKET.put = async () => { throw new Error('source failure'); };
      if (fault === 'source-digest')
        env.SOURCE_BUCKET.get = async () => ({ size: 3, arrayBuffer: async () => new Uint8Array([1, 2, 3]).buffer });
      await assert.rejects(ingest());
      assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM documents').get().n, 0, fault);
      assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_vector_jobs').get().n, 0);
      assert.equal(await requeueStaleVectorJobs(env), 0);
      assert.equal(sent.length, 0);
      assert.equal(embeds(), 0);
      assert.equal(objects.size, 0);
    }
    finally {
      sqlite.close();
    }
  }
});
test('lost final transaction acknowledgment retains the committed source, document, and outbox', async () => {
  const { env, sqlite, ingest, objects, sent } = setup();
  try {
    const batch = env.DB.batch.bind(env.DB);
    env.DB.batch = async (statements) => {
      const result = await batch(statements); if (statements.some(row => row.sql.includes("SET ingestion_state = 'ready'")))
        throw new Error('lost commit acknowledgment'); return result;
    };
    const result = await ingest();
    const doc = sqlite.prepare('SELECT * FROM documents').get();
    assert.equal(doc.ingestion_state, 'ready');
    assert.ok(objects.has(doc.source_r2_key));
    assert.equal(sent.length, 1);
    assert.equal(result.documentId, doc.id);
    assert.equal(sqlite.prepare('SELECT state FROM document_vector_jobs').get().state, 'queued');
  }
  finally {
    sqlite.close();
  }
});
test('unknown commit outcome retains evidence and cannot dispatch preparation state', async () => {
  const { env, sqlite, ingest, objects } = setup();
  try {
    const batch = env.DB.batch.bind(env.DB);
    const prepare = env.DB.prepare;
    env.DB.batch = async (statements) => {
      if (statements.some(row => row.sql.includes("SET ingestion_state = 'ready'")))
        throw new Error('database unavailable'); return batch(statements);
    };
    env.DB.prepare = sql => {
      if (sql.startsWith('SELECT ingestion_state,source_r2_key'))
        throw new Error('cannot inspect outcome'); return prepare(sql);
    };
    await assert.rejects(ingest(), /cannot inspect outcome/);
    assert.equal(sqlite.prepare('SELECT ingestion_state FROM documents').get().ingestion_state, 'preparing');
    assert.equal(objects.size, 1);
    assert.equal(await requeueStaleVectorJobs(env), 0);
  }
  finally {
    sqlite.close();
  }
});
test('consumer refuses missing, altered, incomplete, and superseded generation evidence', async () => {
  for (const fault of ['source-missing', 'source-changed', 'chunk-text', 'chunk-count', 'generation']) {
    const { env, sqlite, ingest, objects, embeds } = setup();
    try {
      const result = await ingest();
      const id = result.documentId;
      const doc = sqlite.prepare('SELECT * FROM documents').get();
      if (fault === 'source-missing')
        objects.delete(doc.source_r2_key);
      if (fault === 'source-changed')
        objects.set(doc.source_r2_key, new TextEncoder().encode('changed source'));
      if (fault === 'chunk-text')
        sqlite.prepare("UPDATE document_chunks SET chunk_text='corrupted text' WHERE id=(SELECT id FROM document_chunks LIMIT 1)").run();
      if (fault === 'chunk-count')
        sqlite.prepare('DELETE FROM document_chunks WHERE id=(SELECT id FROM document_chunks LIMIT 1)').run();
      if (fault.startsWith('chunk-'))
        sqlite.prepare("UPDATE documents SET ingestion_state='ready' WHERE id=?").run(id); // exercise digest gate even if readiness were restored incorrectly
      if (fault === 'generation')
        sqlite.prepare("UPDATE documents SET active_text_generation='replacement' WHERE id=?").run(id);
      await processDocumentVectorJob(env, id);
      assert.equal(embeds(), 0, fault);
      const job = sqlite.prepare('SELECT * FROM document_vector_jobs').get();
      assert.notEqual(job.state, 'completed');
      if (fault !== 'generation')
        assert.equal(job.state, 'failed');
    }
    finally {
      sqlite.close();
    }
  }
});
test('failed queue delivery leaves a verified outbox for sweep, while absent legacy manifests remain held', async () => {
  const { env, sqlite, ingest, sent } = setup();
  try {
    const send = env.VECTOR_JOBS_QUEUE.send;
    env.VECTOR_JOBS_QUEUE.send = async () => { throw new Error('queue unavailable'); };
    const result = await ingest();
    assert.equal(result.vectorJobStatus, 'queue_unavailable');
    env.VECTOR_JOBS_QUEUE.send = send;
    assert.equal(await requeueStaleVectorJobs(env), 1);
    assert.equal(sent.length, 1);
    sqlite.prepare('UPDATE document_vector_jobs SET outbox_ready_at=NULL,enqueued_at=NULL').run();
    assert.equal(await requeueStaleVectorJobs(env), 0);
    assert.equal((await processDocumentVectorJob(env, result.documentId)).skipped, true);
  }
  finally {
    sqlite.close();
  }
});

test('migration holds unfinished legacy jobs without inventing manifests or reopening completed history',async()=>{
  const {DatabaseSync}=await import('node:sqlite');const fs=await import('node:fs');
  const {sqlite,ingest}=setup();const old=new DatabaseSync(':memory:');
  try{
    await ingest();const original=sqlite.prepare('SELECT * FROM documents').get();
    const directory=new URL('../migrations/',import.meta.url);
    for(const name of fs.readdirSync(directory).filter(name=>name.endsWith('.sql')&&name<'0018').sort())old.exec(fs.readFileSync(new URL(name,directory),'utf8'));
    for(const state of ['queued','processing','completed','failed']){
      const doc={...original,id:`legacy-${state}`};const fields=Object.keys(doc);
      old.prepare(`INSERT INTO documents (${fields.join(',')}) VALUES (${fields.map(()=>'?').join(',')})`).run(...Object.values(doc));
      old.prepare('INSERT INTO document_vector_jobs (document_id,state,attempts,created_at,updated_at) VALUES (?,?,3,?,?)').run(doc.id,state,'now','now');
    }
    old.exec(fs.readFileSync(new URL('0018_vector_job_outbox.sql',directory),'utf8'));
    const rows=old.prepare('SELECT * FROM document_vector_jobs').all();
    for(const row of rows){assert.equal(row.state,row.document_id==='legacy-completed'?'completed':'failed');assert.equal(row.attempts,3);assert.equal(row.outbox_ready_at,null);assert.equal(row.chunk_manifest_sha256,null);}
  }finally{old.close();sqlite.close();}
});
