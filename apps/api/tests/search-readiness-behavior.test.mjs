import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,readdirSync} from 'node:fs';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {search,searchDebug}=loadTs('src/services/search.ts');
const {inspectSearchReadiness,searchReadyEnvironment}=loadTs('src/services/search-readiness.ts');
const {advanceFacetBuild,facetBuildStatus}=loadTs('src/services/search-facet-build.ts');
const {fetchFtsTokenDocFrequencies}=loadTs('src/services/search-fts.ts');
const {toErrorResponse}=loadTs('src/lib/http.ts');
const migration=readdirSync(new URL('../migrations/',import.meta.url)).filter(name=>name.endsWith('.sql') && Number(name.split('_')[0])>26).sort().map(name=>readFileSync(new URL('../migrations/'+name,import.meta.url),'utf8')).join('\n');
const query={query:'unmatchedquery',filters:{approvedOnly:false},corpusMode:'trusted_plus_provisional'};
test('held corpora return empty public/debug pages without FTS or provider work, retaining cursor metadata',async()=>{
 const {DB,sqlite}=memoryD1();try{
  sqlite.exec("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES('held','decision_docx','Test','rent','rent','source','https://example.invalid','now','now')");
  const prepare=DB.prepare;DB.prepare=sql=>{assert.doesNotMatch(sql,/FROM search_chunks_fts\b|FROM search_body_substrings\b|FROM search_chunks_fts_vocab\b/i);return prepare(sql);};
  const env={DB,VECTOR_NAMESPACE:'test',AI:{run(){assert.fail('no inference');}},VECTOR_INDEX:{query(){assert.fail('no vectors');}}};
  for(const corpusMode of ['trusted_only','trusted_plus_provisional']){
   const request={query:'rent',corpusMode,filters:{approvedOnly:false}};
   const result=await search(env,request);assert.equal(result.total,0);assert.equal(result.nextCursor,null);assert.equal(result.candidateExhausted,true);assert.equal(result.countKind,'bounded_candidates');
   const debug=await searchDebug(env,{...request,query:'"quiet enjoyment"',queryType:'keyword'});assert.equal(debug.queryType,'exact_phrase');assert.equal(debug.runtimeDiagnostics.vectorQueryAttempted,false);assert.equal(debug.total,0);
  }
 }finally{sqlite.close();}
});
test('publication inspection errors propagate instead of being treated as an empty corpus',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const prepare=DB.prepare;DB.prepare=sql=>{if(sql.startsWith('SELECT 1 AS present'))throw Error('transient publication read');return prepare(sql);};
  await assert.rejects(search({DB,VECTOR_NAMESPACE:'test'},query),/transient publication read/);
 }finally{sqlite.close();}
});
test('normal cold/concurrent searches perform read-only readiness and no schema/bootstrap writes',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};const writes=[];const prepare=DB.prepare;DB.prepare=sql=>{if(/^\s*(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE)/i.test(sql))writes.push(sql);return prepare(sql);};
  await Promise.all([search(env,query,{session:false}),search(env,query,{session:false})]);assert.deepEqual(writes,[]);assert.equal((await inspectSearchReadiness(env)).ready,true);
 }finally{sqlite.close();}
});
test('transient readiness/vocabulary errors affect only their operation, without isolate poison',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};const prepare=DB.prepare;let failed=false;
  DB.prepare=sql=>{if(!failed&&sql.includes('FROM sqlite_master')){failed=true;throw new Error('error code: 1031 injected transient');}return prepare(sql);};
  let error;try{await search(env,query,{session:false});}catch(value){error=value;}assert.equal(toErrorResponse(error).status,503);assert.match(error.message,/temporarily unavailable/);
  assert.equal((await search(env,query,{session:false})).total,0);
  const ready=await searchReadyEnvironment(env);let probes=0;DB.prepare=sql=>{if(sql.includes('SELECT term, doc FROM search_chunks_fts_vocab')&&++probes===1)throw new Error('error code: 1031 injected vocabulary failure');return prepare(sql);};
  assert.equal((await fetchFtsTokenDocFrequencies(ready,['tenant'])).size,0);await fetchFtsTokenDocFrequencies(ready,['tenant']);assert.equal(probes,2,'next operation retries the optional query');
 }finally{sqlite.close();}
});
test('missing schema/index is explicit, is never provisioned by search, and recovers after operator migration',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};
  const indexSql=sqlite.prepare("SELECT sql FROM sqlite_master WHERE name='idx_document_chunks_doc'").get().sql;sqlite.exec('DROP INDEX idx_document_chunks_doc');
  const report=await inspectSearchReadiness(env);assert.equal(report.ready,false);assert.ok(report.missing.includes('idx_document_chunks_doc'));
  await assert.rejects(search(env,query,{session:false}),/schema is incomplete/);assert.equal(sqlite.prepare("SELECT 1 FROM sqlite_master WHERE name='idx_document_chunks_doc'").get(),undefined);
  sqlite.exec(indexSql);assert.equal((await search(env,query,{session:false})).total,0);
  sqlite.exec('DELETE FROM search_index_rebuild');assert.equal((await inspectSearchReadiness(env)).ready,false);sqlite.exec('INSERT INTO search_index_rebuild(singleton) VALUES(1)');assert.equal((await inspectSearchReadiness(env)).ready,true);
  sqlite.exec('DROP TABLE document_rules_sections');await assert.rejects(search(env,query,{session:false}),/schema is incomplete/);
 }finally{sqlite.close();}
});
test('partial facet population is reconciled independently in bounded, replayable batches',async()=>{
 const {DB,sqlite}=memoryD1({migrationThrough:26});try{
  const env={DB};for(let i=0;i<105;i++)sqlite.prepare("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,index_codes_json,rules_sections_json,ordinance_sections_json) VALUES(?,'decision_docx','Test','Facet','F','source','https://example.invalid','now','now','[\"IC-100\"]','[\"Rule 1.2\"]','[\"Ordinance 37.8\"]')").run(`facet-${String(i).padStart(3,'0')}`);
  sqlite.exec('DELETE FROM document_rules_sections');assert.equal(sqlite.prepare('SELECT count(*) n FROM document_index_codes').get().n,105);
  sqlite.exec(migration);assert.equal((await inspectSearchReadiness(env)).ready,false);assert.equal((await facetBuildStatus(env)).phase,'pending');
  sqlite.exec("UPDATE search_facet_build SET owner_token='other',lease_expires_at='9999-01-01T00:00:00.000Z'");await assert.rejects(advanceFacetBuild(env),/already running/);sqlite.exec("UPDATE search_facet_build SET lease_expires_at='2000-01-01T00:00:00.000Z'");
  await advanceFacetBuild(env);assert.equal((await facetBuildStatus(env)).visited,100);assert.equal(sqlite.prepare('SELECT count(*) n FROM document_rules_sections').get().n,100);
  sqlite.prepare("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,index_codes_json,rules_sections_json,ordinance_sections_json) SELECT 'aaa-late',file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,index_codes_json,rules_sections_json,ordinance_sections_json FROM documents LIMIT 1").run();
  sqlite.prepare('UPDATE documents SET rules_sections_json=? WHERE id=?').run('["Rule 2.3"]','facet-000');
  const before=(await facetBuildStatus(env)).after_id;const batch=DB.batch;DB.batch=statements=>batch([...statements,DB.prepare('INSERT INTO missing_table VALUES(1)')]);await assert.rejects(advanceFacetBuild(env));assert.equal((await facetBuildStatus(env)).after_id,before);DB.batch=batch;
  await advanceFacetBuild(env);await advanceFacetBuild(env);assert.equal((await facetBuildStatus(env)).phase,'ready');assert.equal((await inspectSearchReadiness(env)).ready,true);
  for(const [table,column,value]of[['document_index_codes','normalized_code','100'],['document_rules_sections','normalized_section','1.2'],['document_ordinance_sections','normalized_section','37.8']])assert.equal(sqlite.prepare(`SELECT count(*) n FROM ${table} WHERE ${column}=?`).get(value).n,table==='document_rules_sections'?105:106);
  await advanceFacetBuild(env);assert.equal(sqlite.prepare('SELECT count(*) n FROM document_rules_sections').get().n,106);assert.equal(sqlite.prepare("SELECT normalized_section FROM document_rules_sections WHERE document_id='facet-000'").get().normalized_section,'2.3');
 }finally{sqlite.close();}
});
