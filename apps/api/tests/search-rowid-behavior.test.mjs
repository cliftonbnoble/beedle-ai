import test from 'node:test';
import assert from 'node:assert/strict';
import {readdirSync,readFileSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {advancePhraseIndex,phraseIndexStatus}=loadTs('src/services/search-phrase-build.ts');
const {buildDocumentTextArtifactStatements}=loadTs('src/services/ingest.ts');
const {search}=loadTs('src/services/search.ts');
async function doc(env,id,text='The tenant gave the landlord written notice of necessary repairs.'){
 await env.DB.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,ingestion_state,ingestion_completed_at,expected_chunk_count) VALUES (?,'decision_docx','Test',?,?,'source','https://example.invalid','now','now','preparing','now',1)`).bind(id,id,id).run();
 await env.DB.batch(buildDocumentTextArtifactStatements(env,{documentId:id,citation:id,sections:[{canonicalKey:'conclusions_of_law',heading:'Conclusions of Law',order:0,paragraphs:[{anchor:'p1',order:0,text}]}]}).statements);
 await env.DB.prepare("UPDATE documents SET ingestion_state='ready',searchable_at='now',published_revision=document_revision WHERE id=?").bind(id).run();
 return (await env.DB.prepare('SELECT id FROM document_chunks WHERE document_id=?').bind(id).first()).id;
}
async function finish(env){for(let i=0;i<30;i++){if((await phraseIndexStatus(env)).phase==='ready')return;await advancePhraseIndex(env);}assert.fail('builder did not converge');}
function matched(sqlite,term){return sqlite.prepare('SELECT chunk_id FROM search_chunks_fts WHERE search_chunks_fts MATCH ? ORDER BY chunk_id').all(term).map(row=>row.chunk_id);}
test('empty install cutover is atomic/idempotent and rowid maintenance survives edits, active toggles and cascades',async()=>{
 const {DB,sqlite}=memoryD1({finalizeEmptyPhraseIndex:false});try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};await finish(env);assert.equal((await phraseIndexStatus(env)).legacy_frozen,1);
  const id=await doc(env,'primary');const row=sqlite.prepare("SELECT row_id FROM search_body_identity WHERE source_kind='document' AND chunk_id=?").get(id).row_id;
  assert.deepEqual(matched(sqlite,'"written notice"'),[id]);
  sqlite.prepare('UPDATE document_chunks SET chunk_text=? WHERE id=?').run('The adjudicator considered a replacement remedy.',id);
  assert.deepEqual(matched(sqlite,'written'),[]);assert.deepEqual(matched(sqlite,'replacement'),[id]);assert.equal(sqlite.prepare('SELECT row_id FROM search_body_identity WHERE chunk_id=?').get(id).row_id,row);
  sqlite.exec("INSERT INTO retrieval_activation_batches VALUES('batch','now',1,1,'digest','rollback')");
  sqlite.prepare(`INSERT INTO retrieval_search_chunks(chunk_id,batch_id,document_id,title,citation,source_file_ref,source_link,section_label,paragraph_anchor,citation_anchor,chunk_text,chunk_type,retrieval_priority,active,created_at) VALUES(?,'batch','primary','primary','primary','source','https://example.invalid','Conclusions','p1','c1','A separate activated remedy.','conclusions_of_law','high',1,'now')`).run(id);
  assert.equal(sqlite.prepare('SELECT count(*) n FROM search_body_identity WHERE chunk_id=?').get(id).n,2,'source kinds disambiguate shared IDs');
  assert.throws(()=>sqlite.prepare("UPDATE search_body_identity SET row_id=row_id+10000 WHERE source_kind='document' AND chunk_id=?").run(id),/identity_immutable/);
  assert.throws(()=>sqlite.prepare("UPDATE search_body_identity SET source_kind='retrieval' WHERE source_kind='document' AND chunk_id=?").run(id),/identity_immutable/);
  sqlite.prepare('UPDATE document_chunks SET id=? WHERE id=?').run(id+'-renamed',id);
  assert.equal(sqlite.prepare("SELECT row_id FROM search_body_identity WHERE source_kind='document' AND chunk_id=?").get(id+'-renamed').row_id,row);
  assert.deepEqual(matched(sqlite,'replacement'),[id+'-renamed']);

  sqlite.prepare('UPDATE retrieval_search_chunks SET active=0 WHERE chunk_id=?').run(id);assert.equal(sqlite.prepare("SELECT active FROM search_chunks_fts WHERE source_kind='retrieval' AND chunk_id=?").get(id).active,0);
  sqlite.prepare('DELETE FROM documents WHERE id=?').run('primary');assert.deepEqual(matched(sqlite,'remedy'),[]);assert.equal(sqlite.prepare('SELECT count(*) n FROM search_body_identity').get().n,0);
  sqlite.exec("INSERT INTO search_chunks_fts(search_chunks_fts,rank) VALUES('integrity-check',1)");
  await advancePhraseIndex(env);assert.equal((await phraseIndexStatus(env)).phase,'ready');
 }finally{sqlite.close();}
});
test('populated upgrade builds beside live readers, resumes failures and swaps only complete postings',async()=>{
 const {DB,sqlite}=memoryD1({migrationThrough:24});try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};const ids=[];for(let i=0;i<105;i++)ids.push(await doc(env,`upgrade-${String(i).padStart(3,'0')}`));
  for(const migration of readdirSync(new URL('../migrations/',import.meta.url)).filter(name=>name.endsWith('.sql') && Number(name.split('_')[0])>24).sort())sqlite.exec(readFileSync(new URL('../migrations/'+migration,import.meta.url),'utf8'));
  assert.equal(matched(sqlite,'written').length,105);
  await advancePhraseIndex(env);assert.equal((await phraseIndexStatus(env)).visited,100);assert.equal(matched(sqlite,'written').length,105,'old readers remain complete');
  sqlite.prepare('UPDATE document_chunks SET chunk_text=? WHERE id=?').run('Changed before cutover evidence.',ids[0]);
  const inserted=await doc(env,'aaa-late','A low-sort concurrent addition.');
  const batch=DB.batch;DB.batch=async()=>{throw new Error('injected build transaction failure');};await assert.rejects(advancePhraseIndex(env),/injected/);DB.batch=batch;
  await finish(env);assert.deepEqual(matched(sqlite,'Changed'),[ids[0]]);assert.deepEqual(matched(sqlite,'addition'),[inserted]);assert.equal(matched(sqlite,'written').length,104);
  sqlite.exec("INSERT INTO search_chunks_fts(search_chunks_fts,rank) VALUES('integrity-check',1)");
  const archived=sqlite.prepare('SELECT count(*) n FROM search_chunks_fts_legacy').get().n;
  await doc(env,'after-swap');assert.equal(sqlite.prepare('SELECT count(*) n FROM search_chunks_fts_legacy').get().n,archived,'archive writes are frozen');assert.equal(matched(sqlite,'written').length,105);
  const {advanceFacetBuild}=loadTs('src/services/search-facet-build.ts');for(let step=0;step<4;step++)await advanceFacetBuild(env);
  const result=await search(env,{query:'"written notice"',limit:20,corpusMode:'trusted_plus_provisional',filters:{approvedOnly:false}},{session:false});assert.ok(result.results.length>0);assert.ok(result.results.every(row=>row.documentId!=='upgrade-000'));
 }finally{sqlite.close();}
});
test('rowid-based plans and bulk maintenance scale with changed identities',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};const ids=[];for(let i=0;i<400;i++)ids.push(await doc(env,`bulk-${i}`));
  const plan=sqlite.prepare('EXPLAIN QUERY PLAN SELECT row_id FROM search_body_identity WHERE source_kind=? AND chunk_id=?').all('document',ids[0]);assert.ok(plan.some(row=>/SEARCH.*USING.*INDEX.*source_kind=\? AND chunk_id=\?/.test(row.detail)),JSON.stringify(plan));
  const remove=sqlite.prepare('EXPLAIN QUERY PLAN DELETE FROM search_chunks_fts WHERE rowid=?').all(1);assert.ok(remove.some(row=>/VIRTUAL TABLE INDEX.*=/.test(row.detail)),JSON.stringify(remove));
  const times=[];for(const amount of [10,100]){const start=performance.now();for(const id of ids.splice(0,amount))sqlite.prepare('DELETE FROM document_chunks WHERE id=?').run(id);times.push({changed:amount,ms:performance.now()-start});}
  assert.equal(matched(sqlite,'written').length,290);sqlite.exec("INSERT INTO search_chunks_fts(search_chunks_fts,rank) VALUES('integrity-check',1)");console.info(JSON.stringify({fixture:'400 synthetic chunks, Node SQLite; diagnostic only',times}));
 }finally{sqlite.close();}
});
test('failed cutover rolls back its schema pointer, and lost acknowledgment replays once',async()=>{
 for(const failure of ['rollback','lost-ack']){
  const {DB,sqlite}=memoryD1({finalizeEmptyPhraseIndex:false});try{
   const env={DB};await doc(env,'cutover');await advancePhraseIndex(env);await advancePhraseIndex(env);assert.equal((await phraseIndexStatus(env)).phase,'retrieval');
   const batch=DB.batch;DB.batch=async statements=>{
    if(failure==='rollback')return batch([...statements,DB.prepare('INSERT INTO missing_table VALUES(1)')]);
    await batch(statements);throw new Error('lost cutover acknowledgment');
   };
   await assert.rejects(advancePhraseIndex(env));DB.batch=batch;
   if(failure==='rollback'){
    assert.equal((await phraseIndexStatus(env)).phase,'retrieval');assert.ok(sqlite.prepare("SELECT 1 FROM sqlite_master WHERE name='search_phrase_fts'").get());assert.equal(sqlite.prepare("SELECT 1 FROM sqlite_master WHERE name='search_chunks_fts_legacy'").get(),undefined);
   }else assert.equal((await phraseIndexStatus(env)).phase,'ready');
   await advancePhraseIndex(env);assert.equal((await phraseIndexStatus(env)).phase,'ready');assert.equal(matched(sqlite,'written').length,1);
   sqlite.exec("INSERT INTO search_chunks_fts(search_chunks_fts,rank) VALUES('integrity-check',1)");
  }finally{sqlite.close();}
 }
});
