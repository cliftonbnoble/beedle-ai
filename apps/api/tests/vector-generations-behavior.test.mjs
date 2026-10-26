import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
const {ingestDocumentBytes}=loadTs('src/services/ingest.ts');
const {ensureVersionedVectorBatch,ensureVersionedVector,synchronizeVectorLifecycle,vectorLifecycleReady,collectObsoleteVectors,reconcileVectorGenerations}=loadTs('src/services/vector-generations.ts');
const {vectorProfile}=loadTs('src/services/vector-profile.ts');
const {vectorSearch}=loadTs('src/services/search-fts.ts');
async function setup() {
  const {DB,sqlite}=memoryD1();const objects=new Map();const stored=new Map();const calls={embedCalls:0,texts:0,upserts:0,deletes:0,queries:0};let visible=true;
  const env={DB,VECTOR_NAMESPACE:'local',AI_EMBEDDING_MODEL:'synthetic-a',AI_EMBEDDING_DIMENSIONS:'2',VECTOR_LIFECYCLE_READY:'true',R2_PUBLIC_BASE_URL:'https://example.invalid',SOURCE_PROXY_BASE_URL:'https://api.example',
    SOURCE_BUCKET:{async put(key,bytes){objects.set(key,bytes);},async get(key){const bytes=objects.get(key);return bytes?{size:bytes.length,arrayBuffer:async()=>bytes.slice().buffer}:null;},async delete(key){objects.delete(key);}},
    AI:{async run(_model,{text}){calls.embedCalls++;calls.texts+=text.length;return {data:text.map(()=>[0.3,0.7])};}},VECTOR_INDEX:{
      async upsert(vectors){calls.upserts++;for(const vector of vectors)stored.set(vector.id,structuredClone(vector));},
      async getByIds(ids){return visible?ids.map(id=>stored.get(id)).filter(Boolean):[];},
      async deleteByIds(ids){calls.deletes++;for(const id of ids)stored.delete(id);},
      async query(_vector,options){calls.queries++;assert.ok(options.namespace.endsWith((await vectorProfile(env)).id.slice(0,16)));assert.deepEqual(options.filter,{live:true});return {matches:[...stored.values()].filter(vector=>vector.namespace===options.namespace&&vector.metadata.live).map(vector=>({id:vector.id,score:0.9})).slice(0,options.topK)};}
    }};
  const text='# Findings of Fact\n\n'+Array.from({length:6},(_,i)=>'The tenant gave written notice that repairs were needed. The landlord disputed the condition and the notice. '.repeat(10)+`Paragraph ${i}`).join('\n\n');
  const result=await ingestDocumentBytes(env,{title:'Vector lifecycle',citation:'V-1',jurisdiction:'Test',fileType:'decision_docx',performVectorUpsert:false,sourceFile:{filename:'vector.md',mimeType:'text/markdown'}},new TextEncoder().encode(text));
  const id=result.documentId;
  sqlite.prepare("UPDATE documents SET searchable_at=?,published_revision=document_revision WHERE id=?").run(new Date().toISOString(),id);
  const sources=sqlite.prepare('SELECT id AS chunkId,chunk_text AS text FROM document_chunks WHERE document_id=? ORDER BY id LIMIT 8').all(id).map(chunk=>({...chunk,documentId:id,sourceKind:'document',generation:'initial'}));
  async function sync(target=env){for(let i=0;i<20&&!await vectorLifecycleReady(target);i++)await synchronizeVectorLifecycle(target);assert.equal(await vectorLifecycleReady(target),true);}
  return {env,sqlite,id,sources,stored,calls,sync,setVisible:value=>{visible=value;}};
}

test('profile namespaces and vector identities isolate model, dimensions and source generations',async()=>{
  const {env,sqlite,sources,calls,sync}=await setup();try {
    const first=await ensureVersionedVectorBatch(env,sources.slice(0,4));
    assert.equal(calls.embedCalls,1);assert.equal(calls.texts,4);assert.equal(first.length,4);assert.ok(first.every(value=>value.entry.vector_id.length===64));
    const before=calls.texts;await ensureVersionedVectorBatch(env,sources.slice(0,4));assert.equal(calls.texts,before);
    const old=await vectorProfile(env);const nextEnv={...env,AI_EMBEDDING_MODEL:'synthetic-b'};
    const next=await vectorProfile(nextEnv);assert.notEqual(old.namespace,next.namespace);
    const changed=await ensureVersionedVector(nextEnv,sources[0]);assert.notEqual(first[0].entry.vector_id,changed.entry.vector_id);
    assert.equal(changed.entry.chunk_id,first[0].entry.chunk_id);
    assert.match(changed.entry.source_sha256,/^[a-f0-9]{64}$/);assert.ok(changed.entry.parser_version);assert.ok(changed.entry.chunker_version);
    await sync();await sync(nextEnv);assert.equal(await vectorLifecycleReady(env),true,'shadow synchronization does not invalidate an unchanged baseline');
    assert.throws(()=>sqlite.prepare("UPDATE vector_entries SET source_generation='forged'").run(),/immutable/);
  }finally{sqlite.close();}
});

test('successful embedding batches survive an upsert fault without repeated paid inference',async()=>{
  const {env,sqlite,sources,calls}=await setup();try {
    const upsert=env.VECTOR_INDEX.upsert;env.VECTOR_INDEX.upsert=async()=>{throw Error('synthetic failure');};
    await assert.rejects(ensureVersionedVectorBatch(env,sources.slice(0,6)),/failure/);
    const texts=calls.texts;assert.equal(texts,6);assert.equal(calls.embedCalls,2);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM vector_entries WHERE values_json IS NOT NULL').get().n,6);
    env.VECTOR_INDEX.upsert=upsert;await ensureVersionedVectorBatch(env,sources.slice(0,6));assert.equal(calls.texts,texts);
    assert.equal(sqlite.prepare("SELECT COUNT(*) AS n FROM vector_entries WHERE state='verified'").get().n,6);
  }finally{sqlite.close();}
});

test('dimension mismatch and delayed provider visibility cannot publish a verified entry',async()=>{
  const {env,sqlite,sources,calls,setVisible}=await setup();try {
    const run=env.AI.run;env.AI.run=async()=>({data:[[1,2,3]]});
    await assert.rejects(ensureVersionedVector(env,sources[0]),/invalid/);assert.equal(calls.upserts,0);
    env.AI.run=run;setVisible(false);await assert.rejects(ensureVersionedVector(env,sources[0]),/not yet visible/);
    assert.equal(sqlite.prepare('SELECT state FROM vector_entries').get().state,'submitted');const texts=calls.texts;
    setVisible(true);await ensureVersionedVector(env,sources[0]);assert.equal(calls.texts,texts);
  }finally{sqlite.close();}
});

test('retirement removes rejected generations before top-K and restores a fresh incarnation after GC',async()=>{
  const {env,sqlite,id,sources,stored,calls,sync}=await setup();try {
    const records=await ensureVersionedVectorBatch(env,sources.slice(0,4));await sync();
    let result=await vectorSearch(env,['repair notice'],50);assert.equal(result.scores.size,4);
    sqlite.prepare("UPDATE documents SET rejected_at=?,rejected_reason='test' WHERE id=?").run(new Date().toISOString(),id);
    const before=calls.queries;result=await vectorSearch(env,['repair notice'],50);assert.equal(result.errored,true);assert.equal(calls.queries,before);
    await sync();result=await vectorSearch(env,['repair notice'],50);assert.equal(result.scores.size,0);
    assert.ok(records.every(record=>stored.get(record.entry.vector_id).metadata.live===false));
    assert.equal((await collectObsoleteVectors(env,true)).candidateIds.length,0);
    sqlite.prepare("UPDATE vector_entries SET obsolete_at='2000-01-01T00:00:00.000Z'").run();
    assert.equal((await collectObsoleteVectors(env,true)).candidateIds.length,4);assert.equal(calls.deletes,0);
    await collectObsoleteVectors(env,false);assert.equal(stored.size,0);
    sqlite.prepare('UPDATE documents SET rejected_at=NULL,rejected_reason=NULL,searchable_at=?,published_revision=document_revision+1 WHERE id=?').run(new Date().toISOString(),id);
    // Evidence edits advance document_revision; republish the resulting current revision.
    sqlite.prepare('UPDATE documents SET published_revision=document_revision WHERE id=?').run(id);
    await sync();
    const current=sqlite.prepare("SELECT vector_id FROM vector_entries WHERE state='verified'").all().map(row=>row.vector_id);
    assert.equal(current.length,4);assert.ok(current.every(id=>!records.some(record=>record.entry.vector_id===id)));
    result=await vectorSearch(env,['repair notice'],50);assert.equal(result.scores.size,4);
  }finally{sqlite.close();}
});

test('a publication change and completed sync during an in-flight query invalidates that recall set',async()=>{
  const {env,sqlite,id,sources,calls,sync}=await setup();try {
    await ensureVersionedVectorBatch(env,sources.slice(0,4));await sync();
    const query=env.VECTOR_INDEX.query;
    env.VECTOR_INDEX.query=async(...args)=>{
      const result=await query(...args);
      sqlite.prepare("UPDATE documents SET rejected_at=?,rejected_reason='concurrent' WHERE id=?").run(new Date().toISOString(),id);
      await sync();return result;
    };
    const result=await vectorSearch(env,['repair notice'],50);assert.equal(result.scores.size,0);assert.equal(result.errored,true);assert.match(result.firstErrorMessage,/changed_during_query/);assert.equal(calls.queries,1);
  }finally{sqlite.close();}
});

test('unknown initial acknowledgements reuse cached values with a new physical ID',async()=>{
  const {env,sqlite,sources,stored,calls,sync}=await setup();try {
    const upsert=env.VECTOR_INDEX.upsert;let old;
    env.VECTOR_INDEX.upsert=async rows=>{old=structuredClone(rows[0]);await upsert(rows);throw Error('acknowledgement lost');};
    await assert.rejects(ensureVersionedVector(env,sources[0]),/lost/);
    assert.equal(sqlite.prepare('SELECT state FROM vector_entries').get().state,'uncertain');const texts=calls.texts;
    env.VECTOR_INDEX.upsert=upsert;const fresh=await ensureVersionedVector(env,sources[0]);assert.notEqual(fresh.entry.vector_id,old.id);assert.equal(calls.texts,texts);
    await sync();await upsert([old]); // A very late initial write stays outside live recall.
    const result=await vectorSearch(env,['repair notice'],50);assert.equal(result.scores.size,1);
    sqlite.prepare("UPDATE vector_entries SET obsolete_at='2000-01-01' WHERE state='uncertain'").run();
    await collectObsoleteVectors(env,false);assert.ok(stored.has(fresh.entry.vector_id));assert.ok(!stored.has(old.id));
  }finally{sqlite.close();}
});

test('concurrent source jobs share one embedding lease',async()=>{
  const {env,sqlite,sources,calls}=await setup();let release;try {
    const run=env.AI.run;let started;const entered=new Promise(resolve=>{started=resolve;});const gate=new Promise(resolve=>{release=resolve;});
    env.AI.run=async(...args)=>{started();await gate;return run(...args);};
    const first=ensureVersionedVector(env,sources[0]);await entered;
    await assert.rejects(ensureVersionedVector(env,sources[0]),/concurrent embedding/);
    release();await first;assert.equal(calls.texts,1);assert.equal(calls.upserts,1);
  }finally{release?.();sqlite.close();}
});

test('lifecycle ownership serializes promotion against a concurrent rejection',async()=>{
  const {env,sqlite,id,sources,stored,sync}=await setup();let release;try {
    await ensureVersionedVector(env,sources[0]);
    const upsert=env.VECTOR_INDEX.upsert;let entered;const started=new Promise(resolve=>{entered=resolve;});const gate=new Promise(resolve=>{release=resolve;});
    env.VECTOR_INDEX.upsert=async rows=>{if(rows.some(row=>row.metadata.live)){entered();await gate;}return upsert(rows);};
    const promoting=synchronizeVectorLifecycle(env);await started;
    sqlite.prepare("UPDATE documents SET rejected_at=?,rejected_reason='concurrent' WHERE id=?").run(new Date().toISOString(),id);
    assert.equal((await synchronizeVectorLifecycle(env)).processed,0);assert.equal(await vectorLifecycleReady(env),false);
    release();await promoting;env.VECTOR_INDEX.upsert=upsert;await sync();
    assert.ok([...stored.values()].every(vector=>vector.metadata.live===false));
    assert.equal((await vectorSearch(env,['repair'],50)).scores.size,0);
  }finally{release?.();sqlite.close();}
});

test('an unknown metadata acknowledgement holds recall until explicit operator reconciliation',async()=>{
  const {env,sqlite,sources,calls}=await setup();try {
    await ensureVersionedVector(env,sources[0]);const upsert=env.VECTOR_INDEX.upsert;
    env.VECTOR_INDEX.upsert=async rows=>{await upsert(rows);throw Error('metadata acknowledgement lost');};
    await assert.rejects(synchronizeVectorLifecycle(env),/lost/);const writes=calls.upserts;
    assert.equal(await vectorLifecycleReady(env),false);await synchronizeVectorLifecycle(env);assert.equal(calls.upserts,writes);
    assert.equal(sqlite.prepare('SELECT uncertain FROM vector_sync_locks').get().uncertain,1);
    env.VECTOR_INDEX.upsert=upsert;
    // The mock has fully settled its mutation queue, matching the documented
    // operator precondition for this explicit recovery action.
    await synchronizeVectorLifecycle(env,undefined,true);assert.equal(await vectorLifecycleReady(env),true);
  }finally{sqlite.close();}
});

test('reconciliation compares eligible D1 sources and a bounded provider page',async()=>{
  const {env,sqlite,sources,stored,sync}=await setup();try {
    const records=await ensureVersionedVectorBatch(env,sources.slice(0,4));await sync();
    const baseline=await reconcileVectorGenerations(env,{probe:true});
    assert.equal(baseline.coverage.verifiedLiveSources,4);assert.ok(baseline.coverage.eligibleSources>=4);assert.equal(baseline.providerProbe.checked,4);assert.deepEqual(baseline.providerProbe.missingIds,[]);
    stored.delete(records[0].entry.vector_id);
    const report=await reconcileVectorGenerations(env,{probe:true});assert.deepEqual(report.providerProbe.missingIds,[records[0].entry.vector_id]);
  }finally{sqlite.close();}
});

test('bulk publication counts target rows rather than lifecycle trigger writes',async()=>{
  const {env,sqlite,id}=await setup();try {
    const {bulkEnableSearchability}=loadTs('src/services/admin-ingestion.ts');
    sqlite.prepare('UPDATE documents SET qc_passed=1,qc_has_index_codes=1,qc_has_rules_section=1,qc_has_ordinance_section=1,searchable_at=NULL WHERE id=?').run(id);
    const result=await bulkEnableSearchability(env,{realOnly:false,limit:10});
    assert.equal(result.summary.enabledCount,1);
    assert.equal((await bulkEnableSearchability(env,{realOnly:false,limit:10})).summary.enabledCount,0);
  }finally{sqlite.close();}
});

test('lifecycle promotion, retirement and cleanup honor a one-vector configured batch budget',async()=>{
 const {env,sqlite,id,sources,stored}=await setup();
 try{
  await ensureVersionedVectorBatch(env,sources.slice(0,2));
  const {defaultComputeBudgets}=loadTs('src/services/compute-budget.ts');
  const small={...env,COMPUTE_BUDGETS_JSON:JSON.stringify({...defaultComputeBudgets,vector_write:{perMinute:1,perDay:100}})};
  const nextWindow=()=>sqlite.prepare("UPDATE compute_budget_counters SET minute_bucket=minute_bucket-1 WHERE category='vector_write'").run();
  nextWindow();await synchronizeVectorLifecycle(small);assert.equal([...stored.values()].filter(row=>row.metadata.live).length,1);assert.equal(await vectorLifecycleReady(small),false);
  nextWindow();await synchronizeVectorLifecycle(small);assert.equal(await vectorLifecycleReady(small),true);
  sqlite.prepare("UPDATE documents SET rejected_at='now',rejected_reason='fixture' WHERE id=?").run(id);
  for(let n=0;n<2;n++){nextWindow();await synchronizeVectorLifecycle(small);}
  assert.equal(await vectorLifecycleReady(small),true);assert.ok([...stored.values()].every(row=>!row.metadata.live));
  sqlite.prepare("UPDATE vector_entries SET obsolete_at='2000-01-01T00:00:00.000Z'").run();
  assert.equal((await collectObsoleteVectors(small,true)).candidateIds.length,1);
  for(let n=0;n<2;n++){nextWindow();await collectObsoleteVectors(small,false);}
  assert.equal(stored.size,0);
 }finally{sqlite.close();}
});
