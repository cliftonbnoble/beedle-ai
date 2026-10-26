import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
const { ingestDocumentBytes, approveDecision } = loadTs('src/services/ingest.ts');
const { updateIngestionMetadata } = loadTs('src/services/admin-ingestion.ts');
const { prepareRetrievalActivation, signPlan } = loadTs('src/services/activation-plan.ts');
const { writeTrustedRetrievalActivation, rollbackTrustedRetrievalActivation } = loadTs('src/services/retrieval-activation.ts');
const { handleWriteRetrievalActivation } = loadTs('src/routes/admin-retrieval-activation.ts');
async function setup() {
  const { DB, sqlite } = memoryD1();
  const objects = new Map();
  const vectors = new Map();
  const counts = { embeds: 0, upserts: 0 };
  let visible = true;
  const env = {
    DB, R2_PUBLIC_BASE_URL: 'https://example.invalid', SOURCE_PROXY_BASE_URL: 'https://api.example', VECTOR_NAMESPACE: 'activation-local', AI_EMBEDDING_MODEL: 'local',
    SOURCE_BUCKET: { async put(key, bytes, options) { objects.set(key, { bytes: new Uint8Array(bytes), ...options }); }, async get(key) { const value = objects.get(key); return value ? { ...value, size: value.bytes.length, arrayBuffer: async () => value.bytes.slice().buffer } : null; }, async delete(key) { objects.delete(key); } },
    AI: { async run() { counts.embeds++; return { data: [[0.2, 0.8]] }; } },
    VECTOR_INDEX: {
      async upsert(rows) {
        counts.upserts++; for (const row of rows)
          vectors.set(row.id, row); return { mutationId: 'queued' };
      }, async getByIds(ids) { return visible ? ids.map(id => vectors.get(id)).filter(Boolean) : []; }
    }
  };
  const text = '# Index Codes\n\n100\n\n# Rules\n\nRule 1.2\n\n# Ordinance\n\nOrdinance 37.8\n\n# Findings of Fact\n\nThe tenant testified that the landlord received written notice of a repair needed to the dwelling. The landlord disputed whether the condition persisted after the repair.\n\n# Conclusions of Law\n\nThe tenant retained possession. The decision addresses the evidence of repair and the governing provisions in Rule 1.2 and Ordinance 37.8.';
  const result = await ingestDocumentBytes(env, { title: 'Tenant rent appeal', citation: 'T-481', jurisdiction: 'Test', fileType: 'decision_docx', performVectorUpsert: false, sourceFile: { filename: 'appeal.md', mimeType: 'text/markdown' } }, new TextEncoder().encode(text));
  const id = result.documentId;
  const detail = await updateIngestionMetadata(env, id, { index_codes: ['100'], rules_sections: ['1.2'], ordinance_sections: ['37.8'], confirm_required_metadata: true });
  const approved = await approveDecision(env, id, detail.documentRevision);
  assert.equal(approved.approved, true, JSON.stringify(approved));
  const prepare = (batchId, performVectorUpsert = false) => prepareRetrievalActivation(env, { schemaVersion: 2, batchId, documentIds: [id], performVectorUpsert });
  const active = () => sqlite.prepare('SELECT chunk_id,batch_id,chunk_text FROM retrieval_search_chunks WHERE active=1 ORDER BY chunk_id').all();
  return { env, sqlite, id, prepare, active, vectors, counts, objects, setVisible: value => { visible = value; } };
}
async function finishActivation(env,plan) {
  for(let step=0;step<64;step++) {
    const report=await writeTrustedRetrievalActivation(env,{plan});
    if(report.state==='published') return report;
  }
  throw new Error('Activation did not converge within its finite work units');
}
function rollback(plan) { return { schemaVersion: 2, batchId: plan.batchId, digest: plan.digest, rollbackDigest: plan.rollbackDigest }; }
async function resign(plan) { const { digest, rollbackDigest, ...value } = plan; return signPlan(value); }
test('strict plans reject forged evidence, bad digests, duplicates, mismatched rollback targets, and legacy payloads before writes', async () => {
  const { env, sqlite, prepare } = await setup();
  try {
    const original = await prepare('valid');
    const invalid = [];
    invalid.push({ ...original, digest: '0'.repeat(64) });
    let plan = structuredClone(original);
    plan.chunks[0].sourceText = 'Invented finding about an unrelated matter';
    invalid.push(await resign(plan));
    plan = structuredClone(original);
    plan.chunks[0].documentId = 'different-document';
    invalid.push(await resign(plan));
    plan = structuredClone(original);
    plan.chunks.push(plan.chunks[0]);
    invalid.push(await resign(plan));
    plan = structuredClone(original);
    plan.rollback.documents[0].previousChunkIds = ['unrelated-chunk'];
    invalid.push(await resign(plan));
    plan = structuredClone(original);
    plan.documents[0].sourceSha256 = 'f'.repeat(64);
    invalid.push(await resign(plan));
    plan = structuredClone(original);
    plan.documents[0].documentRevision++;
    invalid.push(await resign(plan));
    plan = structuredClone(original);
    plan.documents[0].retrievalRevision++;
    invalid.push(await resign(plan));
    for (const candidate of invalid)
      await assert.rejects(writeTrustedRetrievalActivation(env, { plan: candidate }));
    const response = await handleWriteRetrievalActivation(new Request('https://api.example/admin/retrieval/activation/write', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ activationManifest: { activationBatchIds: ['legacy'] } }) }), env);
    assert.equal(response.status, 400);
    for (const table of ['retrieval_activation_plans', 'retrieval_activation_staged_chunks', 'retrieval_activation_batches', 'retrieval_search_chunks'])
      assert.equal(sqlite.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, table);
    const dryRun = await writeTrustedRetrievalActivation(env, { plan: original, dryRun: true });
    assert.equal(dryRun.readOnly, true);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM retrieval_activation_plans').get().n, 0);
  }
  finally {
    sqlite.close();
  }
});
test('publication is immutable/idempotent and rollback restores only server-owned versions with monotonic retrieval revisions', async () => {
  const { env, sqlite, id, prepare, active } = await setup();
  try {
    const a = await prepare('batch-a');
    const first = await writeTrustedRetrievalActivation(env, { plan: a });
    assert.equal(first.state, 'published');
    const aRows = active();
    assert.equal(aRows.length, a.chunks.length);
    assert.ok(a.chunks.every(row => Buffer.byteLength(row.chunkId) <= 64));
    assert.equal((await writeTrustedRetrievalActivation(env, { plan: a })).replayed, true);
    assert.deepEqual(active(), aRows);
    const b = await prepare('batch-b');
    await writeTrustedRetrievalActivation(env, { plan: b });
    const bRows = active();
    assert.ok(bRows.every(row => row.batch_id === 'batch-b'));
    await writeTrustedRetrievalActivation(env, { plan: a });
    assert.deepEqual(active(), bRows, 'historical replay must not reactivate');
    const conflict = await resign({ ...a, chunks: a.chunks.slice(1) });
    await assert.rejects(writeTrustedRetrievalActivation(env, { plan: conflict }), /immutable/);
    assert.deepEqual(active(), bRows);
    await assert.rejects(rollbackTrustedRetrievalActivation(env, rollback(a)), /newer activation/);
    assert.equal((await rollbackTrustedRetrievalActivation(env, rollback(b))).state, 'rolled_back');
    assert.deepEqual(active(), aRows);
    assert.equal((await rollbackTrustedRetrievalActivation(env, rollback(b))).replayed, true);
    await rollbackTrustedRetrievalActivation(env, rollback(a));
    assert.equal(active().length, 0);
    assert.equal(sqlite.prepare('SELECT retrieval_revision FROM documents WHERE id=?').get(id).retrieval_revision, 4);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM retrieval_activation_batches').get().n, 2, 'batch parents must be retained');
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM retrieval_search_chunks').get().n, a.chunks.length + b.chunks.length);
    assert.equal((await writeTrustedRetrievalActivation(env, { plan: a })).state, 'rolled_back');
    assert.equal(active().length, 0);
  }
  finally {
    sqlite.close();
  }
});
test('asynchronous vector submission holds publication; identical retry verifies visibility without embedding again', async () => {
  const { env, sqlite, prepare, active, counts, setVisible } = await setup();
  try {
    const old = await prepare('previous');
    await writeTrustedRetrievalActivation(env, { plan: old });
    const before = active();
    const next = await prepare('pending', true);
    setVisible(false);
    const waiting = await writeTrustedRetrievalActivation(env, { plan: next });
    assert.equal(waiting.state, 'retryable');
    assert.deepEqual(active(), before);
    const embeds = counts.embeds;
    const upserts = counts.upserts;
    assert.equal(embeds, Math.min(4,next.chunks.length));
    setVisible(true);
    const completed = await writeTrustedRetrievalActivation(env, { plan: next });
    assert.equal(counts.embeds, embeds);
    assert.equal(counts.upserts, upserts);
    if(completed.state!=='published') await finishActivation(env,next);
    assert.equal(counts.embeds,next.chunks.length);
    assert.ok(active().every(row => row.batch_id === 'pending'));
  }
  finally {
    sqlite.close();
  }
});
test('failed staging and failed publication preserve the visible batch, and identical retry converges', async () => {
  for (const fault of ['stage', 'publish', 'vector']) {
    const { env, sqlite, prepare, active } = await setup();
    try {
      const old = await prepare('previous');
      await writeTrustedRetrievalActivation(env, { plan: old });
      const before = active();
      const next = await prepare('replacement', fault === 'vector');
      const batch = env.DB.batch.bind(env.DB);
      let injected = false;
      const upsert = env.VECTOR_INDEX.upsert;
      if (fault === 'vector')
        env.VECTOR_INDEX.upsert = async () => { throw new Error('provider failure'); };
      else
        env.DB.batch = statements => {
          const target = statements.some(row => row.sql.includes(fault === 'stage' ? 'INSERT INTO retrieval_activation_staged_chunks' : 'INSERT INTO retrieval_activation_guards')); if (target && !injected) {
            injected = true;
            return batch([...statements, env.DB.prepare('INSERT INTO missing_table VALUES (1)')]);
          } return batch(statements);
        };
      await assert.rejects(writeTrustedRetrievalActivation(env, { plan: next }));
      assert.deepEqual(active(), before, fault);
      env.DB.batch = batch;
      env.VECTOR_INDEX.upsert = upsert;
      assert.equal((await finishActivation(env,next)).state, 'published');
      assert.ok(active().every(row => row.batch_id === 'replacement'));
    }
    finally {
      sqlite.close();
    }
  }
});
test('concurrent duplicate activation claims do not publish twice and expired leases are recoverable', async () => {
  const { env, sqlite, id, prepare } = await setup();
  try {
    const plan = await prepare('concurrent');
    const results = await Promise.allSettled([writeTrustedRetrievalActivation(env, { plan }), writeTrustedRetrievalActivation(env, { plan })]);
    assert.ok(results.some(row => row.status === 'fulfilled'));
    assert.equal(sqlite.prepare('SELECT retrieval_revision FROM documents WHERE id=?').get(id).retrieval_revision, 1);
    const next = await prepare('expired');
    const batch = env.DB.batch.bind(env.DB);
    env.DB.batch = async () => { throw new Error('interrupted'); };
    await assert.rejects(writeTrustedRetrievalActivation(env, { plan: next }));
    env.DB.batch = batch;
    sqlite.prepare("UPDATE retrieval_activation_plans SET owner_token='abandoned',lease_expires_at='2000-01-01T00:00:00.000Z' WHERE batch_id=?").run(next.batchId);
    assert.equal((await finishActivation(env,next)).state, 'published');
  }
  finally {
    sqlite.close();
  }
});
test('source changes, stale document revisions, and legacy batch reuse are rejected', async () => {
  const { env, sqlite, id, prepare, objects } = await setup();
  try {
    const plan = await prepare('fresh');
    const key = plan.documents[0].sourceFileRef;
    const source = objects.get(key);
    objects.set(key, { ...source, bytes: new TextEncoder().encode('changed source') });
    await assert.rejects(writeTrustedRetrievalActivation(env, { plan }), /differs/);
    objects.set(key, source);
    sqlite.prepare("INSERT INTO retrieval_activation_batches VALUES ('legacy','now',1,1,'old','old')").run();
    const legacy = await prepare('legacy');
    await assert.rejects(writeTrustedRetrievalActivation(env, { plan: legacy }), /historical/);
    sqlite.prepare("UPDATE documents SET author_name='Concurrent reviewer' WHERE id=?").run(id);
    await assert.rejects(writeTrustedRetrievalActivation(env, { plan }), /not a completed/);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM retrieval_activation_plans').get().n, 0);
  }
  finally {
    sqlite.close();
  }
});
test('lost publication acknowledgment is replayable without deleting committed evidence', async () => {
  const { env, sqlite, prepare, active } = await setup();
  try {
    const plan = await prepare('lost-ack');
    const batch = env.DB.batch.bind(env.DB);
    let lost = false;
    env.DB.batch = async (statements) => {
      const result = await batch(statements); if (!lost && statements.some(row => row.sql.includes('INSERT INTO retrieval_activation_guards'))) {
        lost = true;
        throw new Error('response lost');
      } return result;
    };
    await assert.rejects(writeTrustedRetrievalActivation(env, { plan }), /response lost/);
    assert.equal(active().length, plan.chunks.length);
    const replay = await writeTrustedRetrievalActivation(env, { plan });
    assert.equal(replay.state, 'published');
    assert.equal(replay.replayed, true);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM retrieval_activation_chunks').get().n, plan.chunks.length);
  }
  finally {
    sqlite.close();
  }
});
test('missing or corrupt retained rollback rows and transaction failures preserve the current batch', async () => {
  for (const fault of ['missing', 'corrupt', 'transaction']) {
    const { env, sqlite, prepare, active } = await setup();
    try {
      const a = await prepare('retained');
      await writeTrustedRetrievalActivation(env, { plan: a });
      const b = await prepare('current');
      await writeTrustedRetrievalActivation(env, { plan: b });
      const before = active();
      if (fault === 'missing')
        sqlite.prepare('DELETE FROM retrieval_search_chunks WHERE chunk_id=?').run(a.chunks[0].chunkId);
      if (fault === 'corrupt')
        sqlite.prepare("UPDATE retrieval_search_chunks SET chunk_text='corruption' WHERE chunk_id=?").run(a.chunks[0].chunkId);
      const batch = env.DB.batch.bind(env.DB);
      if (fault === 'transaction')
        env.DB.batch = statements => batch([...statements, env.DB.prepare('INSERT INTO missing_table VALUES (1)')]);
      await assert.rejects(rollbackTrustedRetrievalActivation(env, rollback(b)));
      assert.deepEqual(active(), before, fault);
      assert.equal(sqlite.prepare('SELECT state FROM retrieval_activation_plans WHERE batch_id=?').get(b.batchId).state, 'published');
    }
    finally {
      sqlite.close();
    }
  }
});
test('source replacement, superseded ownership and reviewer edits during staging cannot publish', async () => {
  for (const fault of ['source', 'owner', 'revision']) {
    const { env, sqlite, id, prepare, objects, active } = await setup();
    try {
      const plan = await prepare('race', true);
      const upsert = env.VECTOR_INDEX.upsert;
      env.VECTOR_INDEX.upsert = async (rows) => {
        const result = await upsert(rows);
        if (fault === 'source') {
          const key = plan.documents[0].sourceFileRef;
          objects.set(key, { ...objects.get(key), bytes: new TextEncoder().encode('replaced') });
        }
        if (fault === 'owner')
          sqlite.prepare("UPDATE retrieval_activation_plans SET owner_token='replacement-owner' WHERE batch_id=?").run(plan.batchId);
        if (fault === 'revision')
          sqlite.prepare("UPDATE documents SET author_name='Concurrent edit' WHERE id=?").run(id);
        return result;
      };
      await assert.rejects(writeTrustedRetrievalActivation(env, { plan }));
      assert.equal(active().length, 0, fault);
      assert.notEqual(sqlite.prepare('SELECT state FROM retrieval_activation_plans WHERE batch_id=?').get(plan.batchId).state, 'published');
      if (fault === 'owner')
        assert.equal(sqlite.prepare('SELECT owner_token FROM retrieval_activation_plans WHERE batch_id=?').get(plan.batchId).owner_token, 'replacement-owner');
    }
    finally {
      sqlite.close();
    }
  }
});
test('legacy documents require captured source provenance before activation', async () => {
  const { env, sqlite, id, prepare } = await setup();
  try {
    sqlite.prepare("UPDATE documents SET metadata_json=json_remove(metadata_json,'$.sourceSha256') WHERE id=?").run(id);
    const detail = await updateIngestionMetadata(env, id, { confirm_required_metadata: true });
    assert.equal((await approveDecision(env, id, detail.documentRevision)).approved, true);
    await assert.rejects(prepare('missing-hash'), /captured digest/);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM retrieval_activation_plans').get().n, 0);
  }
  finally {
    sqlite.close();
  }
});
test('legacy conversion checks exact current evidence and only prepares a new plan', async () => {
  const { convertLegacySelection } = await import('../scripts/retrieval-activation-v2.mjs');
  const { env, sqlite, prepare } = await setup();
  try {
    const original = await prepare('legacy-convert');
    const rows = original.chunks.map(chunk => ({ ...chunk, chunkId: chunk.sourceChunkId }));
    let calls = 0;
    const convert = async (input) => { calls++; return prepareRetrievalActivation(env, input); };
    const result = await convertLegacySelection({ rows }, convert, 'converted', false);
    assert.equal(result.batchId, 'converted');
    const bad = structuredClone(rows);
    bad[0].sourceText = 'invented evidence';
    await assert.rejects(convertLegacySelection({ rows: bad }, convert, 'converted', false), /differs/);
    await assert.rejects(convertLegacySelection({ rows: [...rows, rows[0]] }, convert, 'duplicate', false), /duplicate/);
    assert.equal(calls, 2);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM retrieval_activation_plans').get().n, 0);
  }
  finally {
    sqlite.close();
  }
});

test('an embedding failure retains ownership until the other started operations settle', async () => {
  const {env,sqlite,prepare}=await setup();
  let release;
  try {
    const plan=await prepare('embedding-settlement',true);assert.ok(plan.chunks.length>=2);
    const gate=new Promise(resolve=>{release=resolve;});let started;const ready=new Promise(resolve=>{started=resolve;});let calls=0;
    env.AI.run=async()=>{if(++calls===1)throw new Error('first embedding failed');started();await gate;return {data:[[0.2,0.8]]};};
    let settled=false;const running=writeTrustedRetrievalActivation(env,{plan}).then(()=>{settled=true;return null;},error=>{settled=true;return error;});
    await ready;await new Promise(resolve=>setImmediate(resolve));
    assert.equal(settled,false,'the first failure must not release still-running peers');
    assert.ok(sqlite.prepare('SELECT owner_token FROM retrieval_activation_plans WHERE batch_id=?').get(plan.batchId).owner_token);
    await assert.rejects(writeTrustedRetrievalActivation(env,{plan}),/already running/);
    release();assert.match(String(await running),/first embedding failed/);
    assert.equal(sqlite.prepare('SELECT owner_token FROM retrieval_activation_plans WHERE batch_id=?').get(plan.batchId).owner_token,null);
    env.AI.run=async()=>({data:[[0.2,0.8]]});
    assert.equal((await finishActivation(env,plan)).state,'published');
  }finally{release?.();sqlite.close();}
});

test('rollback rechecks retained source labels and links inside the publication transaction', async () => {
  for(const field of ['title','citation','source_file_ref','source_link']) {
    const {env,sqlite,prepare,active}=await setup();
    try {
      const a=await prepare('retained');await writeTrustedRetrievalActivation(env,{plan:a});
      const b=await prepare('current');await writeTrustedRetrievalActivation(env,{plan:b});const before=active();
      const original=env.DB.prepare;env.DB.prepare=sql=>{
        const statement=original(sql);
        if(sql.includes('SET owner_token=?')&&sql.includes("= 'published'")){
          const run=statement.run;statement.run=async function(){const result=await run.call(this);sqlite.prepare(`UPDATE retrieval_search_chunks SET ${field}='Changed after validation' WHERE batch_id=?`).run(a.batchId);return result;};
        }
        return statement;
      };
      await assert.rejects(rollbackTrustedRetrievalActivation(env,rollback(b)),/changed|incomplete/);
      assert.deepEqual(active(),before,field);
    }finally{sqlite.close();}
  }
});

test('HTTP activation accepts a durable ID, then bounded deliveries publish the exact plan',async()=>{
  const {createActivationJob,stepMaintenanceJob,controlMaintenanceJob}=loadTs('src/services/maintenance-jobs.ts');
  const {env,sqlite,prepare,active,counts}=await setup();try {
    const plan=await prepare('durable-activation',true);
    const response=await handleWriteRetrievalActivation(new Request('https://api.example/admin/retrieval/activation/write',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({plan})}),env);
    assert.equal(response.status,202);let job=await response.json();assert.equal(job.counts.total,plan.chunks.length);assert.equal(counts.embeds,0);assert.equal(active().length,0);
    job=await stepMaintenanceJob(env,job.jobId);assert.equal(counts.embeds,Math.min(4,plan.chunks.length));
    for(let i=0;i<64&&job.state!=='completed';i++) job=await stepMaintenanceJob(env,job.jobId);
    assert.equal(job.state,'completed');
    assert.equal(job.result.state,'published');assert.equal(active().length,plan.chunks.length);assert.equal(counts.embeds,plan.chunks.length);
    const replay=await createActivationJob(env,{plan});assert.equal(replay.jobId,job.jobId);assert.equal(replay.state,'completed');
    const cancelledPlan=await prepare('cancelled-activation',true);const cancelled=await createActivationJob(env,{plan:cancelledPlan});
    await controlMaintenanceJob(env,cancelled.jobId,'cancel');assert.equal((await stepMaintenanceJob(env,cancelled.jobId)).state,'cancelled');
    assert.equal(sqlite.prepare('SELECT * FROM retrieval_activation_plans WHERE batch_id=?').get(cancelledPlan.batchId),undefined);
  }finally{sqlite.close();}
});
