import {readFileSync} from 'node:fs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {performance} from 'node:perf_hooks';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {indexedLexicalSearch}=loadTs('src/services/search-indexed-lexical.ts');
const {advanceSearchIndex}=loadTs('src/services/search-index-build.ts');
const {searchBudgetEnvironment}=loadTs('src/services/search-work-budget.ts');
const {buildDocumentTextArtifactStatements}=loadTs('src/services/ingest.ts');
const {publishedDocumentSql}=loadTs('src/services/publication-policy.ts');
const scope=`WHERE ${publishedDocumentSql()}`;
async function doc(env,id,title='Unrelated proceeding',text='The parent reported the defect.',author='Other judge'){
  await env.DB.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,ingestion_state,expected_chunk_count) VALUES (?,'decision_docx','Test',?,?,'source','https://example.invalid','now','now','preparing',1)`).bind(id,title,id).run();
  await env.DB.batch(buildDocumentTextArtifactStatements(env,{documentId:id,citation:id,sections:[{canonicalKey:'conclusions_of_law',heading:'Conclusions of Law',order:0,paragraphs:[{anchor:'p1',order:0,text}]}]}).statements);
  await env.DB.prepare("UPDATE documents SET author_name=?,ingestion_state='ready',ingestion_completed_at='now' WHERE id=?").bind(author,id).run();
  await env.DB.prepare("UPDATE documents SET searchable_at='now',published_revision=document_revision WHERE id=?").bind(id).run();
}
test('indexed body substrings, independent metadata and scoped recall preserve unique scan cases',async()=>{
  const {DB,sqlite}=memoryD1();try{
    const env={DB};await doc(env,'body');await doc(env,'title','Rent increase');await doc(env,'author','Other','Unrelated text','Florence Quill');
    assert.ok((await indexedLexicalSearch(env,scope,[],['rent'],100)).some(row=>row.documentId==='body'),'inside parent');
    assert.ok((await indexedLexicalSearch(env,scope,[],['rent'],100,[],{metadataOnly:true})).some(row=>row.documentId==='title'));
    assert.deepEqual((await indexedLexicalSearch(env,scope,[],['quill'],100)).map(row=>row.documentId),['author']);
    assert.deepEqual((await indexedLexicalSearch(env,scope+' AND d.author_name=?',['Florence Quill'],['rent','quill'],100,['body','title','author'])).map(row=>row.documentId),['author']);
    assert.equal((await indexedLexicalSearch(env,scope,[],['rent'],100,[],{wholeWord:true})).some(row=>row.documentId==='body'),false);
    assert.equal((await indexedLexicalSearch(env,scope,[],['nonexistent'],100)).length,0);
    await assert.rejects(indexedLexicalSearch(env,scope,[],['ab'],100),/three characters/);
    const plan=sqlite.prepare('EXPLAIN QUERY PLAN SELECT rowid FROM search_body_substrings WHERE search_body_substrings MATCH ? ORDER BY rowid LIMIT 1600').all('"rent"');
    assert.ok(plan.some(row=>/VIRTUAL TABLE INDEX.*M/.test(row.detail)),JSON.stringify(plan));
    sqlite.prepare("UPDATE documents SET author_name='Different' WHERE id='author'").run();assert.equal((await indexedLexicalSearch(env,scope,[],['quill'],100)).length,0);
  }finally{sqlite.close();}
});
test('populated upgrade backfill is bounded, replayable and keeps live source edits',async()=>{
  const {DB,sqlite}=memoryD1({migrationThrough:24});try{
    // Current publication scopes require M22 lineage; keep the M06 builder fixture.
    sqlite.exec(readFileSync(new URL("../migrations/0032_ingestion_receipts.sql",import.meta.url),"utf8"));
    const env={DB};for(let i=0;i<105;i++)await doc(env,`doc-${String(i).padStart(3,'0')}`);
    // Simulate a populated upgrade by deleting postings correctly, then maps.
    sqlite.exec("INSERT INTO search_body_substrings(search_body_substrings) VALUES('delete-all');INSERT INTO search_metadata_substrings(search_metadata_substrings) VALUES('delete-all');DELETE FROM search_body_identity;DELETE FROM search_document_identity;UPDATE search_index_build SET phase='documents',after_id='',visited=0");
    await assert.rejects(indexedLexicalSearch(env,scope,[],['rent'],10),/backfill/);
    const first=await advanceSearchIndex(env);assert.equal(first.visited,100);assert.equal(first.phase,'documents');
    await doc(env,'aaa-new','New title','A newly repaired tenancy.');
    sqlite.prepare("UPDATE documents SET title='Updated rent' WHERE id='doc-000'").run();
    for(let i=0;i<20;i++){const state=await advanceSearchIndex(env);if(state.phase==='ready')break;}
    assert.equal(sqlite.prepare('SELECT phase FROM search_index_build').get().phase,'ready');
    assert.equal(sqlite.prepare('SELECT count(*) n FROM search_body_identity WHERE indexed=1').get().n,106);
    assert.ok((await indexedLexicalSearch(env,scope,[],['repaired'],100)).some(row=>row.documentId==='aaa-new'));
    const before=sqlite.prepare('SELECT count(*) n FROM search_body_identity').get().n;await advanceSearchIndex(env);assert.equal(sqlite.prepare('SELECT count(*) n FROM search_body_identity').get().n,before);
  }finally{sqlite.close();}
});
test('bounded indexed recall measures common, rare, zero-hit and metadata query classes',async()=>{
  const {DB,sqlite}=memoryD1();try{
    for(let i=0;i<250;i++)await doc({DB},`perf-${i}`,i===249?'Unique metadata title':'Ordinary decision',i===248?'A rareword exception to common tenancy.':'A common tenancy dispute about rent.');
    const timings=[];
    for(const term of ['common','rareword','nonexistent','metadata'])for(let pass=0;pass<3;pass++){
      const env=searchBudgetEnvironment({DB});const start=performance.now();await indexedLexicalSearch(env,scope,[],[term],120);timings.push(performance.now()-start);assert.ok(env.SEARCH_WORK_BUDGET.snapshot().statements<=2);assert.ok(env.SEARCH_WORK_BUDGET.snapshot().returnedRows<=2000);
    }
    timings.sort((a,b)=>a-b);console.info(JSON.stringify({fixture:'250 synthetic documents; Node SQLite, not production SLO',p50:timings[6],p95:timings[11],p99:timings[11]}));
  }finally{sqlite.close();}
});
test('index builder rejects a live lease and resumes after an atomic batch failure',async()=>{
  const {DB,sqlite}=memoryD1();try{
    await doc({DB},'resume');sqlite.exec("UPDATE search_index_build SET phase='documents',after_id='',visited=0,owner_token='other',lease_expires_at='9999-01-01T00:00:00.000Z'");
    await assert.rejects(advanceSearchIndex({DB}),/already running/);
    sqlite.exec('UPDATE search_index_build SET lease_expires_at=\'2000-01-01T00:00:00.000Z\'');
    const batch=DB.batch;DB.batch=async()=>{throw new Error('injected transaction failure');};
    await assert.rejects(advanceSearchIndex({DB}),/injected transaction failure/);assert.equal(sqlite.prepare('SELECT visited FROM search_index_build').get().visited,0);assert.equal(sqlite.prepare('SELECT owner_token FROM search_index_build').get().owner_token,null);
    DB.batch=batch;assert.equal((await advanceSearchIndex({DB})).visited,1);
  }finally{sqlite.close();}
});
