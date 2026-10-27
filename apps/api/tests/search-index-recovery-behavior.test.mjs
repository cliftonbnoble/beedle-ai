import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,readdirSync} from 'node:fs';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {startIndexRecovery,advanceIndexRecovery,indexRecoveryStatus,indexCoveragePage}=loadTs('src/services/search-index-recovery.ts');
const {advanceFacetBuild}=loadTs('src/services/search-facet-build.ts');
const {inspectSearchReadiness}=loadTs('src/services/search-readiness.ts');
const {buildDocumentTextArtifactStatements}=loadTs('src/services/ingest.ts');
const {search}=loadTs('src/services/search.ts');
const fresh=()=>({dryRun:false,generation:crypto.randomUUID(),expectedGeneration:'legacy',replaceArchive:false,restart:false});
async function doc(env,id,text='The tenant gave written notice about the legacyneedle repairs.'){
 await env.DB.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,ingestion_state,ingestion_completed_at,expected_chunk_count) VALUES(?,'decision_docx','Test',? ,?,'source','https://example.invalid','now','now','preparing','now',1)`).bind(id,'Metadata '+id,id).run();
 await env.DB.batch(buildDocumentTextArtifactStatements(env,{documentId:id,citation:id,sections:[{canonicalKey:'conclusions_of_law',heading:'Conclusions of Law',order:0,paragraphs:[{anchor:'p1',order:0,text}]}]}).statements);
 await env.DB.prepare("UPDATE documents SET ingestion_state='ready',searchable_at='now',published_revision=document_revision WHERE id=?").bind(id).run();return (await env.DB.prepare('SELECT id FROM document_chunks WHERE document_id=? LIMIT 1').bind(id).first()).id;
}
async function finish(env,max=100){for(let step=0;step<max;step++){const state=await indexRecoveryStatus(env);if(state.status==='completed'){for(let i=0;i<20;i++){const readiness=await inspectSearchReadiness(env);if(readiness.ready)return state;await advanceFacetBuild(env);}assert.fail('facets did not finish');}await advanceIndexRecovery(env,state.generation);}assert.fail('recovery did not finish');}
function integrity(sqlite){for(const table of ['search_body_substrings','search_metadata_substrings','search_chunks_fts'])sqlite.exec(`INSERT INTO ${table}(${table},rank) VALUES('integrity-check',1)`);}

test('genuine populated 0007 upgrade remains untrusted and gets complete primary/retrieval/metadata postings',async()=>{
 const {DB,sqlite}=memoryD1({migrationThrough:7,finalizeEmptyPhraseIndex:false});try{
  sqlite.exec(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES('historic','decision_docx','Test','Historic authority','OLD-1','source','https://example.invalid','now','now');
   INSERT INTO document_sections(id,document_id,canonical_key,heading,section_order) VALUES('s','historic','conclusions_of_law','Conclusions of Law',0);
   INSERT INTO section_paragraphs(id,section_id,anchor,paragraph_order,text) VALUES('p','s','p1',0,'A unique legacyneedle in the historical source.');
   INSERT INTO document_chunks(id,document_id,section_id,paragraph_id,paragraph_anchor,citation_anchor,section_label,chunk_order,chunk_text,token_estimate,created_at) VALUES('c','historic','s','p','p1','OLD-1:p1','Conclusions of Law',0,'A unique legacyneedle in the historical source.',10,'now');
   INSERT INTO retrieval_activation_batches VALUES('old','now',1,1,'digest','rollback');
   INSERT INTO retrieval_search_chunks(chunk_id,batch_id,document_id,title,citation,source_file_ref,source_link,section_label,paragraph_anchor,citation_anchor,chunk_text,chunk_type,retrieval_priority,active,created_at) VALUES('r','old','historic','Historic','OLD-1','source','https://example.invalid','Findings of Fact','p2','OLD-1:p2','The activatedneedle remains available.','findings_of_fact','high',1,'now');`);
  const files=readdirSync(new URL('../migrations/',import.meta.url)).filter(name=>name.endsWith('.sql')&&Number(name.split('_')[0])>7).sort();
  for(const name of files){sqlite.exec(readFileSync(new URL('../migrations/'+name,import.meta.url),'utf8'));if(name.startsWith('0010_'))assert.equal(sqlite.prepare('SELECT count(*) n FROM search_chunks_fts').get().n,0,'original upgrade hole reproduced');}
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};assert.equal((await inspectSearchReadiness(env)).ready,false);
  const input=fresh();await startIndexRecovery(env,input);await finish(env);integrity(sqlite);
  assert.equal(sqlite.prepare("SELECT count(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'legacyneedle'").get().n,1);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'activatedneedle'").get().n,1);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_metadata_substrings WHERE search_metadata_substrings MATCH 'historic'").get().n,1);
  for(const kind of ['metadata','document','retrieval']){const page=await indexCoveragePage(env,kind);assert.equal(page.issueCount,0);assert.equal(page.checked,1);assert.equal(page.revisionVerified,true);}
  assert.equal(sqlite.prepare("SELECT approved_at FROM documents WHERE id='historic'").get().approved_at,null);assert.equal((await search(env,{query:'legacyneedle',filters:{approvedOnly:true}})).total,0,'index repair cannot confer trust');
  const visited=(await indexRecoveryStatus(env)).visited;await startIndexRecovery(env,input);await advanceIndexRecovery(env,input.generation);assert.equal((await indexRecoveryStatus(env)).visited,visited,'completed replay does not rebuild');
 }finally{sqlite.close();}
});
test('restored ready markers cannot hide missing postings; bounded generation rebuild removes duplicates/orphans',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};for(let i=0;i<105;i++)await doc(env,`restore-${String(i).padStart(3,'0')}`);
  // A copied external-content view still reports all rows after postings are lost.
  sqlite.exec("INSERT INTO search_body_substrings(search_body_substrings) VALUES('delete-all');INSERT INTO search_chunks_fts(search_chunks_fts) VALUES('delete-all')");
  sqlite.exec("INSERT INTO search_chunks_fts(rowid,section_label,chunk_text) VALUES(999999,'Ghost','staleghost index data')");assert.equal(sqlite.prepare('SELECT count(*) n FROM search_chunks_fts').get().n,105);assert.ok((await indexCoveragePage(env,'document')).issueCount>0);
  sqlite.exec("DROP TABLE search_metadata_substrings;INSERT INTO search_body_identity(source_kind,chunk_id,document_id) VALUES('document','orphan','missing-document');INSERT INTO search_document_identity(document_id) VALUES('orphan-document')");
  const input=fresh();await startIndexRecovery(env,input);await assert.rejects(search(env,{query:'legacyneedle',filters:{approvedOnly:false},corpusMode:'trusted_plus_provisional'}),/maintenance is incomplete/);
  await advanceIndexRecovery(env,input.generation);assert.equal((await indexRecoveryStatus(env)).visited,100);
  const first=sqlite.prepare("SELECT id FROM document_chunks WHERE document_id='restore-000'").get().id;sqlite.prepare('UPDATE document_chunks SET chunk_text=? WHERE id=?').run('A revisedneedle replaces the prior source text.',first);await doc(env,'aaa-live','A liveaddition below the captured cursor.');
  await finish(env);integrity(sqlite);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_body_identity WHERE chunk_id='orphan'").get().n,0);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_document_identity WHERE document_id='orphan-document'").get().n,0);
  assert.equal(sqlite.prepare("SELECT count(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'legacyneedle'").get().n,104);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'revisedneedle'").get().n,1);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'liveaddition'").get().n,1);
  assert.equal(sqlite.prepare('SELECT count(*) n FROM search_chunks_fts_docsize').get().n,106);assert.equal(sqlite.prepare('SELECT count(*) n FROM search_body_identity').get().n,106);
  const page=await indexCoveragePage(env,'document');assert.equal(page.checked,100);assert.equal(page.done,false);assert.equal(page.issueCount,0);assert.equal((await indexCoveragePage(env,'document',page.nextAfter)).checked,6);
 }finally{sqlite.close();}
});
test('start/checkpoint failures and lost acknowledgments are atomic and replayable',async()=>{
 for(const failure of ['start-rollback','start-lost-ack','checkpoint-rollback','checkpoint-lost-ack']){
  const {DB,sqlite}=memoryD1();try{
   const env={DB};await doc(env,'atomic');const input=fresh();const batch=DB.batch;
   const inject=()=>{DB.batch=async statements=>{if(failure.endsWith('rollback'))return batch([...statements,DB.prepare('INSERT INTO missing_table VALUES(1)')]);await batch(statements);throw new Error('lost acknowledgment');};};
   if(failure.startsWith('start')){inject();if(failure.endsWith('rollback'))await assert.rejects(startIndexRecovery(env,input));else await startIndexRecovery(env,input);DB.batch=batch;if(failure.endsWith('rollback'))assert.equal((await indexRecoveryStatus(env)).generation,'legacy');await startIndexRecovery(env,input);}
   else{await startIndexRecovery(env,input);inject();if(failure.endsWith('rollback'))await assert.rejects(advanceIndexRecovery(env,input.generation));else await advanceIndexRecovery(env,input.generation);DB.batch=batch;}
   await finish(env);integrity(sqlite);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'legacyneedle'").get().n,1);
  }finally{sqlite.close();}
 }
});
test('generation-aware maintenance keeps revisions, source kinds, active flags and cascades coherent',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};const id=await doc(env,'maintenance');sqlite.exec("INSERT INTO retrieval_activation_batches VALUES('b','now',1,1,'digest','rollback')");
  sqlite.prepare("INSERT INTO retrieval_search_chunks(chunk_id,batch_id,document_id,title,citation,source_file_ref,source_link,section_label,paragraph_anchor,citation_anchor,chunk_text,chunk_type,retrieval_priority,active,created_at) VALUES(?,'b','maintenance','Metadata maintenance','maintenance','source','https://example.invalid','Findings of Fact','p2','c2','The activatedneedle is distinct.','findings_of_fact','high',1,'now')").run(id);
  await startIndexRecovery(env,fresh());await finish(env);const generation=(await indexRecoveryStatus(env)).generation;
  sqlite.prepare('UPDATE document_chunks SET chunk_text=? WHERE id=?').run('The changedneedle is current.',id);sqlite.prepare("UPDATE documents SET author_name='Unique Metadata Judge' WHERE id='maintenance'").run();sqlite.prepare('UPDATE retrieval_search_chunks SET active=0 WHERE chunk_id=?').run(id);
  for(const kind of ['metadata','document','retrieval'])assert.equal((await indexCoveragePage(env,kind)).issueCount,0);
  assert.equal(sqlite.prepare('SELECT count(*) n FROM search_body_identity WHERE chunk_id=?').get(id).n,2);assert.ok(sqlite.prepare("SELECT content_revision>1 AS advanced,indexed_revision=content_revision AS current FROM search_body_identity WHERE source_kind='document' AND chunk_id=?").get(id).current);
  assert.equal(sqlite.prepare("SELECT count(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'legacyneedle'").get().n,0);assert.equal(sqlite.prepare("SELECT count(*) n FROM search_metadata_substrings WHERE search_metadata_substrings MATCH 'unique'").get().n,1);assert.equal(sqlite.prepare("SELECT active FROM search_chunks_fts WHERE source_kind='retrieval' AND chunk_id=?").get(id).active,0);
  sqlite.prepare('UPDATE document_chunks SET id=? WHERE id=?').run(id+'-renamed',id);assert.equal((await indexCoveragePage(env,'document')).issueCount,0);integrity(sqlite);
  sqlite.prepare("DELETE FROM documents WHERE id='maintenance'").run();integrity(sqlite);assert.equal(sqlite.prepare('SELECT count(*) n FROM search_chunks_fts_docsize').get().n,0);assert.equal((await indexRecoveryStatus(env)).generation,generation);
 }finally{sqlite.close();}
});
test('unknown receipt corruption holds readiness; explicit restart preserves archives and forbids generation reuse',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};await doc(env,'restart');const a=fresh();await startIndexRecovery(env,a);await finish(env);
  const b={...fresh(),expectedGeneration:a.generation};await assert.rejects(startIndexRecovery(env,b),/previous index archive/);await startIndexRecovery(env,{...b,replaceArchive:true});
  const archivedCount=sqlite.prepare('SELECT count(*) n FROM search_previous_phrase_fts_docsize').get().n;
  while(!(await indexRecoveryStatus(env)).phase.startsWith('verify_'))await advanceIndexRecovery(env,b.generation);
  sqlite.exec("INSERT INTO search_chunks_fts(search_chunks_fts) VALUES('delete-all')");
  await advanceIndexRecovery(env,b.generation);await assert.rejects(advanceIndexRecovery(env,b.generation),/coverage verification failed/);assert.equal((await indexRecoveryStatus(env)).status,'running');assert.equal((await inspectSearchReadiness(env)).ready,false);
  const c={...fresh(),expectedGeneration:b.generation,restart:true};await startIndexRecovery(env,c);assert.equal(sqlite.prepare('SELECT count(*) n FROM search_previous_phrase_fts_docsize').get().n,archivedCount);assert.equal((await indexRecoveryStatus(env)).archived_generation,a.generation);await assert.rejects(advanceIndexRecovery(env,b.generation),/generation changed/);
  await finish(env);integrity(sqlite);await assert.rejects(startIndexRecovery(env,{...fresh(),generation:a.generation,expectedGeneration:c.generation,replaceArchive:true}),/historical index generation/);
  assert.equal(sqlite.prepare('SELECT status FROM search_index_generations WHERE generation=?').get(b.generation).status,'superseded');
  sqlite.prepare('DELETE FROM search_index_generations WHERE generation=?').run(c.generation);assert.equal((await inspectSearchReadiness(env)).ready,false,'partial restored generation history cannot claim readiness');
 }finally{sqlite.close();}
});
test('concurrent duplicate starts converge and stale deliveries cannot write into a replacement generation',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};await doc(env,'fenced');const input=fresh();const started=await Promise.all([startIndexRecovery(env,input),startIndexRecovery(env,input)]);assert.ok(started.every(row=>row.generation===input.generation));
  const {advanceSearchIndex}=loadTs('src/services/search-index-build.ts');await assert.rejects(advanceSearchIndex(env),/rebuild_in_progress/);
  const batch=DB.batch;let release;const gate=new Promise(resolve=>{release=resolve;});let entered;const entry=new Promise(resolve=>{entered=resolve;});let held=false;
  DB.batch=async statements=>{if(!held&&statements[0].sql.includes('INSERT INTO search_index_rebuild_guards')){held=true;entered();await gate;}return batch(statements);};
  const old=advanceIndexRecovery(env,input.generation).then(value=>({value}),error=>({error}));await entry;
  sqlite.exec("UPDATE search_index_rebuild SET lease_expires_at='2000-01-01T00:00:00.000Z'");const replacement={...fresh(),expectedGeneration:input.generation,restart:true};await startIndexRecovery(env,replacement);release();const outcome=await old;assert.match(String(outcome.error),/owner or generation changed/);DB.batch=batch;
  assert.equal((await indexRecoveryStatus(env)).generation,replacement.generation);assert.equal((await indexRecoveryStatus(env)).visited,0);await finish(env);integrity(sqlite);
 }finally{sqlite.close();}
});
test('mapping pruning and receipt validation preserve integer rowids beyond JavaScript precision',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};await doc(env,'large-map');sqlite.exec("INSERT INTO search_body_identity(row_id,source_kind,chunk_id,document_id) VALUES(9007199254740993,'document','orphan-big','none')");
  const input=fresh();await startIndexRecovery(env,input);assert.equal((await indexRecoveryStatus(env)).upper_body_mapping,'9007199254740993');await doc(env,'new-large-row');await finish(env);integrity(sqlite);
  assert.equal(sqlite.prepare("SELECT count(*) n FROM search_body_identity WHERE chunk_id='orphan-big'").get().n,0);assert.equal((await indexCoveragePage(env,'document')).issueCount,0);
 }finally{sqlite.close();}
});
test('dry runs and rejected archive rotation leave schema, source and recovery state unchanged',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};await doc(env,'dryrun');const before=JSON.stringify(sqlite.prepare('SELECT * FROM search_index_rebuild').get());const source=JSON.stringify(sqlite.prepare('SELECT * FROM documents').get());const writes=[];const prepare=DB.prepare;DB.prepare=sql=>{if(/^\s*(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)/i.test(sql))writes.push(sql);return prepare(sql);};
  const report=await startIndexRecovery(env,{dryRun:true,replaceArchive:true,restart:true});assert.equal(report.dryRun,true);assert.deepEqual(writes,[]);assert.equal(JSON.stringify(sqlite.prepare('SELECT * FROM search_index_rebuild').get()),before);assert.equal(JSON.stringify(sqlite.prepare('SELECT * FROM documents').get()),source);
 }finally{sqlite.close();}
});
test('diagnostic coverage cannot claim one stable corpus revision across a concurrent edit',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};await doc(env,'coverage-race');const prepare=DB.prepare;let edited=false;
  DB.prepare=sql=>{if(!edited&&sql.includes(' AS receipt_one')){edited=true;sqlite.prepare("UPDATE documents SET title='Concurrent title' WHERE id='coverage-race'").run();}return prepare(sql);};
  const page=await indexCoveragePage(env,'document');assert.equal(page.issueCount,0);assert.equal(page.consistent,false);assert.equal(page.sourceChangedDuringCheck,true);DB.prepare=prepare;assert.equal((await indexCoveragePage(env,'document')).consistent,true);
 }finally{sqlite.close();}
});
