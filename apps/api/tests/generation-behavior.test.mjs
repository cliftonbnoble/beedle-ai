import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
const { ingestDocumentBytes, buildDocumentTextArtifactStatements } = loadTs('src/services/ingest.ts');
const { reprocessIngestionDocument, updateIngestionMetadata } = loadTs('src/services/admin-ingestion.ts');
const { restoreDocumentGeneration } = loadTs('src/services/document-generations.ts');
const { processDocumentVectorJob } = loadTs('src/services/vector-jobs.ts');

async function setup(paragraphCount = 4) {
  const { DB, sqlite } = memoryD1(); const objects = new Map(); const messages = [];
  const env = { DB, R2_PUBLIC_BASE_URL:'https://example.invalid', SOURCE_PROXY_BASE_URL:'https://api.example', VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'@cf/baai/bge-base-en-v1.5',AI_EMBEDDING_DIMENSIONS:'768',
    SOURCE_BUCKET:{async put(key,bytes,options){objects.set(key,{bytes:new Uint8Array(bytes),...options});},async get(key){const value=objects.get(key);return value?{...value,size:value.bytes.length,arrayBuffer:async()=>value.bytes.slice().buffer}:null;},async delete(key){objects.delete(key);}},
    VECTOR_JOBS_QUEUE:{async send(message){messages.push(message);}} };
  const text = '# Findings of Fact\n\n'+Array.from({length:paragraphCount},(_,i)=>`The tenant presented new evidence number ${i} about required repairs to the dwelling. The landlord received written notice and reviewed the testimony. `.repeat(3)).join('\n\n')+'\n\n# Conclusions of Law\n\nThe new remedy follows the evidence presented at the hearing and requires review of the governing law.';
  const result = await ingestDocumentBytes(env,{title:'Generation case',citation:'T-1',jurisdiction:'Test',fileType:'decision_docx',performVectorUpsert:false,sourceFile:{filename:'case.md',mimeType:'text/markdown'}},new TextEncoder().encode(text));
  const id = result.documentId;
  // Model a previous extractor that retained only a fragment of this same source.
  const old = buildDocumentTextArtifactStatements(env,{documentId:id,citation:'T-1',sections:[{canonicalKey:'conclusions_of_law',heading:'Conclusions of Law',order:0,paragraphs:[{anchor:'p1',order:0,text:'Obsolete fragment from the previous extractor. The source contains additional evidence and a new remedy.'}]}]});
  await DB.batch(old.statements);
  sqlite.prepare("UPDATE documents SET ingestion_state='ready', ingestion_completed_at='now', expected_chunk_count=1 WHERE id=?").run(id);
  const detail = await updateIngestionMetadata(env,id,{index_codes:['100'],rules_sections:['1.2(a)'],ordinance_sections:['37.8(b)'],case_number:'Reviewed-7',author_name:'Reviewed author',outcome_label:'partial',confirm_required_metadata:true});
  sqlite.prepare("UPDATE documents SET approved_at='now', approved_revision=document_revision, approval_policy_version='reviewer-policy-1', searchable_at='now', published_revision=document_revision WHERE id=?").run(id);
  const visible = () => JSON.stringify({doc:sqlite.prepare('SELECT * FROM documents WHERE id=?').get(id),sections:sqlite.prepare('SELECT * FROM document_sections WHERE document_id=? ORDER BY id').all(id),chunks:sqlite.prepare('SELECT * FROM document_chunks WHERE document_id=? ORDER BY id').all(id),fts:sqlite.prepare('SELECT * FROM search_chunks_fts WHERE document_id=? ORDER BY chunk_id').all(id)});
  return {env,sqlite,objects,messages,id,visible,detail};
}

test('reprocessing rebuilds complete artifacts, preserves reviewed values, fences old retrieval, and restores a retained generation', async () => {
  const {env,sqlite,objects,messages,id} = await setup();
  try {
    sqlite.exec(`INSERT INTO retrieval_activation_batches VALUES ('old','now',1,1,'digest','rollback');`);
    sqlite.prepare(`INSERT INTO retrieval_search_chunks (chunk_id,batch_id,document_id,title,citation,source_file_ref,source_link,section_label,paragraph_anchor,citation_anchor,chunk_text,chunk_type,retrieval_priority,created_at) VALUES ('old-active','old',?,'Title','T1','source','https://example.invalid','Conclusion','p1','T1#p1','Obsolete activation','analysis','high','now')`).run(id);
    const oldChunks = sqlite.prepare('SELECT id,chunk_text FROM document_chunks WHERE document_id=? ORDER BY id').all(id);
    const before = sqlite.prepare('SELECT document_revision FROM documents WHERE id=?').get(id).document_revision;
    const result = await reprocessIngestionDocument(env,id);
    assert.equal(result.authorName,'Reviewed author'); assert.equal(result.caseNumber,'Reviewed-7'); assert.equal(result.outcomeLabel,'partial');
    assert.deepEqual(result.rulesSections,['1.2(a)']); assert.deepEqual(result.ordinanceSections,['37.8(b)']);
    assert.equal(result.reviewerOverrides.author.provenance,'reviewer');
    assert.ok(result.reprocessArtifacts.rebuilt); assert.ok(result.reprocessArtifacts.chunkCountAfter > 1); assert.ok(result.documentRevision > before);
    assert.equal(result.approvedAt,null); assert.equal(result.searchableAt,null); assert.equal(result.qcRequiredConfirmed,0);
    const current = sqlite.prepare('SELECT * FROM documents WHERE id=?').get(id);
    assert.equal(current.ingestion_state,'ready'); assert.equal(current.active_text_generation,result.reprocessArtifacts.generationId);
    assert.equal(sqlite.prepare('SELECT active FROM retrieval_search_chunks').get().active,0);
    const readerText = result.sections.flatMap(section=>section.paragraphs.map(paragraph=>paragraph.text)).join('\n');
    assert.match(readerText,/new evidence number 3/); assert.doesNotMatch(readerText,/Obsolete fragment/);
    const fts = sqlite.prepare("SELECT chunk_text FROM search_chunks_fts WHERE document_id=? AND active=1").all(id).map(row=>row.chunk_text).join('\n');
    assert.match(fts,/new evidence number 3/); assert.doesNotMatch(fts,/Obsolete/);
    assert.equal(messages.at(-1).generationId,current.active_text_generation);
    assert.equal(sqlite.prepare('SELECT generation_id FROM document_vector_jobs WHERE document_id=?').get(id).generation_id,current.active_text_generation);
    assert.equal((await processDocumentVectorJob(env,id,'initial')).skipped,true);
    const upserts=[];const stored=new Map();env.AI_EMBEDDING_MODEL='synthetic';env.AI_EMBEDDING_DIMENSIONS='2';env.AI={async run(_model,{text}){return {data:text.map(()=>[0.2,0.4])};}};env.VECTOR_INDEX={async upsert(rows){upserts.push(...rows);for(const row of rows)stored.set(row.id,row);},async getByIds(ids){return ids.map(id=>stored.get(id)).filter(Boolean);}};
    assert.equal((await processDocumentVectorJob(env,id,current.active_text_generation)).skipped,false);
    assert.deepEqual(upserts.map(row=>row.metadata.chunkId).sort(),sqlite.prepare('SELECT id FROM document_chunks WHERE document_id=?').all(id).map(row=>row.id).sort());
    assert.ok(objects.has(current.source_r2_key));
    const restored = await restoreDocumentGeneration(env,id,result.reprocessArtifacts.previousGenerationId,result.documentRevision);
    assert.notEqual(restored.generationId,current.active_text_generation);
    assert.deepEqual(sqlite.prepare('SELECT id,chunk_text FROM document_chunks WHERE document_id=? ORDER BY id').all(id),oldChunks);
    const after = sqlite.prepare('SELECT * FROM documents WHERE id=?').get(id);
    assert.equal(after.author_name,'Reviewed author'); assert.equal(after.approved_at,null); assert.ok(after.document_revision>result.documentRevision);
    assert.equal(after.qc_required_confirmed,0); assert.equal(after.active_text_generation,restored.generationId);
    assert.equal((await processDocumentVectorJob(env,id,current.active_text_generation)).skipped,true);
    assert.equal(sqlite.prepare('SELECT state FROM document_vector_jobs WHERE document_id=?').get(id).state,'queued');
    assert.equal(sqlite.prepare('SELECT last_error FROM document_vector_jobs WHERE document_id=?').get(id).last_error,null);
  } finally {sqlite.close();}
});

test('failure inside every staging/publication batch leaves the previous document and FTS projection unchanged', async () => {
  let observedBatches = 0;
  { const {env,sqlite,id} = await setup(40); const batch=env.DB.batch.bind(env.DB);env.DB.batch=async statements=>{observedBatches++;return batch(statements);};try{await reprocessIngestionDocument(env,id);}finally{sqlite.close();} }
  assert.ok(observedBatches>=3);
  for(let failure=1;failure<=observedBatches;failure++) {
    const {env,sqlite,id,visible} = await setup(40); const before=visible();const batch=env.DB.batch.bind(env.DB);let call=0;
    env.DB.batch=statements=>batch(++call===failure?[...statements,env.DB.prepare('INSERT INTO missing_table VALUES (1)')]:statements);
    try {await assert.rejects(reprocessIngestionDocument(env,id));assert.equal(visible(),before,`batch ${failure}`);assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_revision_guards').get().n,0);}
    finally{sqlite.close();}
  }
});

test('concurrent reprocesses publish one coherent generation; the stale publisher cannot overwrite it', async () => {
  const {env,sqlite,id} = await setup(12);
  try {
    const outcomes=await Promise.allSettled([reprocessIngestionDocument(env,id),reprocessIngestionDocument(env,id)]);
    assert.equal(outcomes.filter(row=>row.status==='fulfilled').length,1);
    assert.match(String(outcomes.find(row=>row.status==='rejected').reason),/Document changed/);
    const current=sqlite.prepare('SELECT * FROM documents WHERE id=?').get(id);
    const generation=sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(current.active_text_generation);
    assert.equal(generation.status,'published');assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_chunks WHERE document_id=?').get(id).n,JSON.parse(generation.manifest_json).document_chunks.count);
  }finally{sqlite.close();}
});

test('source failure, corrupt staged text, and cross-document rollback preserve the current generation', async () => {
  for(const failure of ['put','digest','staging']) {
    const {env,sqlite,id,visible}=await setup();const before=visible();
    if(failure==='put')env.SOURCE_BUCKET.put=async()=>{throw new Error('source unavailable');};
    if(failure==='digest'){const get=env.SOURCE_BUCKET.get;env.SOURCE_BUCKET.get=async key=>key.startsWith('revisions/')?{size:3,arrayBuffer:async()=>new TextEncoder().encode('bad').buffer}:get(key);}
    if(failure==='staging'){const prepare=env.DB.prepare.bind(env.DB);env.DB.prepare=sql=>{const statement=prepare(sql);if(sql.includes("SET status='ready', manifest_json")){const run=statement.run;statement.run=async function(){const result=await run.call(this);sqlite.exec("UPDATE staged_document_chunks SET chunk_text='Tampered'");return result;};}return statement;};}
    try{await assert.rejects(reprocessIngestionDocument(env,id));assert.equal(visible(),before,failure);}finally{sqlite.close();}
  }
  const {env,sqlite,id}=await setup();
  try{const result=await reprocessIngestionDocument(env,id);await assert.rejects(restoreDocumentGeneration(env,'another-document',result.reprocessArtifacts.previousGenerationId,result.documentRevision),/not found/);}
  finally{sqlite.close();}
});


test('legacy edits without provenance survive reprocessing as explicitly unverified preserved values', async () => {
  const {env,sqlite,id}=await setup();
  try {
    sqlite.prepare("UPDATE documents SET reviewer_overrides_json='{}', author_name='Legacy correction', rules_sections_json='[\"2.5(c)\"]' WHERE id=?").run(id);
    const result=await reprocessIngestionDocument(env,id);
    assert.equal(result.authorName,'Legacy correction');assert.deepEqual(result.rulesSections,['2.5(c)']);
    assert.equal(result.reviewerOverrides.author.provenance,'legacy_preserved');assert.equal(result.reviewerOverrides.rulesSections.provenance,'legacy_preserved');
  }finally{sqlite.close();}
});

test('a reviewer edit during staging wins over the stale publisher', async () => {
  const {env,sqlite,id}=await setup(12);const batch=env.DB.batch.bind(env.DB);let edited=false;
  env.DB.batch=statements=>{if(!edited&&statements.some(statement=>statement.sql.includes('staged_document_sections'))){edited=true;sqlite.prepare("UPDATE documents SET author_name='Concurrent reviewer' WHERE id=?").run(id);}return batch(statements);};
  try{await assert.rejects(reprocessIngestionDocument(env,id),/Document changed/);assert.equal(sqlite.prepare('SELECT author_name FROM documents WHERE id=?').get(id).author_name,'Concurrent reviewer');assert.match(sqlite.prepare('SELECT chunk_text FROM document_chunks WHERE document_id=?').get(id).chunk_text,/Obsolete fragment/);}
  finally{sqlite.close();}
});

test('lost publication acknowledgment retains both generations and never cleans up the committed source', async () => {
  const {env,sqlite,id,objects}=await setup();const batch=env.DB.batch.bind(env.DB);
  env.DB.batch=async statements=>{const result=await batch(statements);if(statements.some(statement=>statement.sql.includes('INSERT INTO document_revision_guards')))throw new Error('lost commit acknowledgment');return result;};
  try{
    await assert.rejects(reprocessIngestionDocument(env,id),/lost commit/);
    const current=sqlite.prepare('SELECT * FROM documents WHERE id=?').get(id);
    assert.equal(sqlite.prepare('SELECT status FROM document_generations WHERE id=?').get(current.active_text_generation).status,'published');
    assert.ok(objects.has(current.source_r2_key));assert.equal(sqlite.prepare("SELECT COUNT(*) AS n FROM document_generations WHERE status='retained'").get().n,1);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_chunks WHERE document_id=?').get(id).n,current.expected_chunk_count);
  }finally{sqlite.close();}
});

test('extraction failure leaves the previously visible document intact', async () => {
  const {env,sqlite,id,visible}=await setup();const before=visible();env.SOURCE_BUCKET.get=async()=>({size:3,arrayBuffer:async()=>new TextEncoder().encode('bad').buffer});
  try{await assert.rejects(reprocessIngestionDocument(env,id),/current revision was preserved/);assert.equal(visible(),before);}
  finally{sqlite.close();}
});

test('readers reject mixed revisions and search drops evidence captured before a concurrent edit', async () => {
  const {getDecisionRetrievalPreview}=loadTs('src/services/retrieval-foundation.ts');
  const {getIngestionDocumentDetail}=loadTs('src/services/admin-ingestion.ts');
  const {search}=loadTs('src/services/search.ts');
  for(const reader of [getDecisionRetrievalPreview,getIngestionDocumentDetail]) {
    const {env,sqlite,id}=await setup();const prepare=env.DB.prepare.bind(env.DB);let changed=false;
    env.DB.prepare=sql=>{if(!changed&&sql.includes('FROM document_sections')){changed=true;sqlite.prepare("UPDATE documents SET author_name='New revision' WHERE id=?").run(id);}return prepare(sql);};
    try{await assert.rejects(reader(env,id),/Document changed while loading/);}finally{sqlite.close();}
  }
  const {env,sqlite,id}=await setup();const prepare=env.DB.prepare.bind(env.DB);let changed=false;
  env.DB.prepare=sql=>{if(!changed&&sql.includes('SELECT d.id, d.document_revision as revision')){changed=true;sqlite.prepare("UPDATE documents SET author_name='New revision' WHERE id=?").run(id);sqlite.prepare('UPDATE documents SET qc_required_confirmed=1 WHERE id=?').run(id);sqlite.prepare("UPDATE documents SET approved_at='now',approved_revision=document_revision,approval_policy_version='reviewer-policy-1',published_revision=document_revision,searchable_at='now' WHERE id=?").run(id);}return prepare(sql);};
  try{const response=await search(env,{query:'obsolete',corpusMode:'trusted_only',filters:{approvedOnly:true}});assert.equal(changed,true);assert.equal(response.results.length,0);}finally{sqlite.close();}
});

test('moving a section, paragraph or chunk invalidates both source and destination document revisions', async () => {
  for (const kind of ['section','paragraph','chunk']) {
    const {env,sqlite,id}=await setup();
    try {
      const second=await ingestDocumentBytes(env,{title:'Destination',citation:'T-2',jurisdiction:'Test',fileType:'decision_docx',performVectorUpsert:false,sourceFile:{filename:'destination.md',mimeType:'text/markdown'}},new TextEncoder().encode('# Findings\n\nThe second decision concerns a different property and a separate hearing on disputed repairs. '.repeat(6)));
      sqlite.prepare("UPDATE documents SET ingestion_state='ready',approved_at='now',approved_revision=document_revision,approval_policy_version='reviewer-policy-1',searchable_at='now',published_revision=document_revision WHERE id=?").run(second.documentId);
      const before=sqlite.prepare('SELECT document_revision FROM documents WHERE id=?').get(id).document_revision;
      const section=sqlite.prepare('SELECT id FROM document_sections WHERE document_id=? LIMIT 1').get(id).id;
      const targetSection=sqlite.prepare('SELECT id FROM document_sections WHERE document_id=? LIMIT 1').get(second.documentId).id;
      if(kind==='section')sqlite.prepare('UPDATE document_sections SET document_id=? WHERE id=?').run(second.documentId,section);
      if(kind==='paragraph')sqlite.prepare('UPDATE section_paragraphs SET section_id=? WHERE section_id=?').run(targetSection,section);
      if(kind==='chunk')sqlite.prepare('UPDATE document_chunks SET document_id=? WHERE document_id=?').run(second.documentId,id);
      for(const documentId of [id,second.documentId]) {
        const row=sqlite.prepare('SELECT ingestion_state,approved_at FROM documents WHERE id=?').get(documentId);
        assert.equal(row.ingestion_state,'legacy',`${kind}: ${documentId}`);
        assert.equal(row.approved_at,null,`${kind}: ${documentId}`);
      }
      assert.ok(sqlite.prepare('SELECT document_revision FROM documents WHERE id=?').get(id).document_revision>before);
    }finally{sqlite.close();}
  }
});
