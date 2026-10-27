import test from 'node:test';
import assert from 'node:assert/strict';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {search,searchDebug}=loadTs('src/services/search.ts');
const {buildDocumentTextArtifactStatements}=loadTs('src/services/ingest.ts');
const {createSearchSession,readSearchSession,searchSnapshotPage,expireSearchSessions,searchCorpusRevision,searchSessionPolicy}=loadTs('src/services/search-sessions.ts');
const {searchRequestSchema}=loadTs('../../packages/shared/src/index.ts');
async function doc(env,id){
 await env.DB.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,ingestion_state,ingestion_completed_at,expected_chunk_count) VALUES (?,'decision_docx','Test',?,'SHARED-CITATION','source','https://example.invalid','now','now','preparing','now',1)`).bind(id,'Related decision '+id).run();
 await env.DB.batch(buildDocumentTextArtifactStatements(env,{documentId:id,citation:'SHARED-CITATION',sections:[{canonicalKey:'conclusions_of_law',heading:'Conclusions of Law',order:0,paragraphs:[{anchor:'p1',order:0,text:'The tenant retained possession of the dwelling after a disputed notice. This identical passage prefix belongs to a distinct related decision and its own source.'}]}]}).statements);
 await env.DB.prepare("UPDATE documents SET ingestion_state='ready',searchable_at='now',published_revision=document_revision WHERE id=?").bind(id).run();
}
const input={query:'possession',limit:2,corpusMode:'trusted_plus_provisional',filters:{approvedOnly:false}};
test('sessions page a fixed decision order once, preserve related identities and allow page-size changes',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};for(let i=0;i<18;i++)await doc(env,`decision-${i}`);
  let candidateQueries=0;const prepare=DB.prepare;DB.prepare=sql=>{if(sql.includes('search_chunks_fts MATCH'))candidateQueries++;return prepare(sql);};
  const first=await search(env,input,{session:true});assert.ok(first.total>2);assert.equal(first.results.length,2);assert.ok(first.nextCursor);assert.equal(first.countKind,'bounded_candidates');assert.equal(first.corpusExhaustive,false);const initialQueries=candidateQueries;assert.ok(initialQueries>0);
  const ids=first.results.map(row=>row.documentId);let page=first;let size=3;
  while(page.nextCursor){page=await search(env,{...input,limit:size,cursor:page.nextCursor});ids.push(...page.results.map(row=>row.documentId));size=size===3?1:3;assert.equal(page.total,first.total);}
  assert.equal(candidateQueries,initialQueries,'cursor pages do not rerun candidate search');assert.equal(new Set(ids).size,ids.length);assert.equal(ids.length,first.total);assert.equal(page.hasMore,false);assert.equal(page.candidateExhausted,true);
  const whole=await search(env,{...input,limit:100},{session:true});assert.deepEqual(ids,whole.results.map(row=>row.documentId),'page size does not change the candidate universe or ranking');assert.equal(ids.length,18,'same citation/snippet prefix does not merge distinct decisions');
  const stored=sqlite.prepare('SELECT payload_json FROM search_sessions LIMIT 1').get().payload_json;assert.equal(Object.hasOwn(JSON.parse(stored),'query'),false);assert.equal(Object.hasOwn(JSON.parse(stored),'filters'),false);
  await assert.rejects(search(env,{...input,offset:2}),/use nextCursor/);
 }finally{sqlite.close();}
});
test('cursor capability rejects changed query, filters, format, positions, epoch and expiry',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};for(let i=0;i<5;i++)await doc(env,`cursor-${i}`);const first=await search(env,input,{session:true});assert.ok(first.nextCursor);
  for(const override of [{query:'different'},{filters:{approvedOnly:true}},{filters:{approvedOnly:false,documentIds:['cursor-1']}},{snippetMaxLength:120},{corpusMode:'trusted_only'}])await assert.rejects(search(env,{...input,...override,cursor:first.nextCursor}),/does not match/);
  await assert.rejects(searchDebug(env,{...input,queryType:'keyword',cursor:first.nextCursor}),/does not match/);
  await assert.rejects(search({...env,AI_EMBEDDING_MODEL:'changed-model'},{...input,cursor:first.nextCursor}),/does not match/);
  const parts=first.nextCursor.split('.');parts[2]='3';await assert.rejects(search(env,{...input,cursor:parts.join('.')}),/Invalid search cursor/);
  const debug=await searchDebug(env,{...input,queryType:'keyword'},{session:true});const debugPage=await searchDebug(env,{...input,queryType:'keyword',cursor:debug.nextCursor});assert.deepEqual(debugPage.filters,{approvedOnly:false});assert.ok(debugPage.results.every(row=>row.diagnostics));
  const now=Date.now;Date.now=()=>now()+searchSessionPolicy.ttlMs+1;try{await assert.rejects(search(env,{...input,cursor:first.nextCursor}),/expired/);}finally{Date.now=now;}
  sqlite.prepare("UPDATE documents SET title='Changed decision' WHERE id='cursor-0'").run();await assert.rejects(search(env,{...input,cursor:first.nextCursor}),/corpus changed/);
 }finally{sqlite.close();}
});
test('snapshot capacity and TTL cleanup remain atomic and bounded',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};const parsed=searchRequestSchema.parse(input);const revision=await searchCorpusRevision(env);const snapshot={query:input.query,results:[{documentId:'x',corpusTier:'provisional'}]};
  await assert.rejects(createSearchSession(env,parsed,'search',{...snapshot,tooBig:'x'.repeat(searchSessionPolicy.maxBytes)},revision),/too large/);
  sqlite.exec('UPDATE search_session_capacity SET reserved_count=255');
  const attempts=await Promise.allSettled([createSearchSession(env,parsed,'search',snapshot,revision),createSearchSession(env,parsed,'search',snapshot,revision)]);assert.equal(attempts.filter(row=>row.status==='fulfilled').length,1);assert.match(String(attempts.find(row=>row.status==='rejected').reason),/capacity/);
  sqlite.exec('DELETE FROM search_sessions;UPDATE search_session_capacity SET reserved_count=0,reserved_bytes=0');
  const json=JSON.stringify({results:[]});for(let i=0;i<33;i++)sqlite.prepare("INSERT INTO search_sessions VALUES(?,'secret','hash','scope','rank',1,?,?,1,2)").run('expired-'+i,json,Buffer.byteLength(json));
  await expireSearchSessions(env,3);assert.equal(sqlite.prepare('SELECT count(*) n FROM search_sessions').get().n,1);const capacity=sqlite.prepare('SELECT * FROM search_session_capacity').get();assert.equal(capacity.reserved_count,1);assert.equal(capacity.reserved_bytes,Buffer.byteLength(json));
 }finally{sqlite.close();}
});
