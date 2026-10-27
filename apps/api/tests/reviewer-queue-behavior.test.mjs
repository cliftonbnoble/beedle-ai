import test from 'node:test';
import assert from 'node:assert/strict';
import {performance} from 'node:perf_hooks';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
import {readyReviewerQueue} from './helpers/reviewer-queue.mjs';
const {listIngestionDocuments,buildReviewerRows,advanceReviewerQueueIndex}=loadTs('src/services/admin-ingestion.ts');
const {inspectReviewerQueue}=loadTs('src/services/reviewer-queue-index.ts');
const {handleReviewerPeers}=loadTs('src/routes/admin-ingestion.ts');
function setup(count=30){
 const {DB,sqlite}=memoryD1();
 const insert=sqlite.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,
  ingestion_state,ingestion_completed_at,expected_chunk_count,extraction_confidence,index_codes_json,rules_sections_json,ordinance_sections_json,
  qc_has_index_codes,qc_has_rules_section,qc_has_ordinance_section,qc_passed,qc_required_confirmed,reference_catalog_version)
  VALUES(?,'decision_docx','Test',?,?,'source','https://example.invalid',?,'now','ready','now',1,?,'["100"]','["1.2"]','["37.8"]',1,1,1,1,0,'legacy')`);
 sqlite.exec('BEGIN');
 for(let i=0;i<count;i++){const id=String(i).padStart(6,'0');insert.run(id,`Case ${id}`,`C-${id}`,i%2?'2026-01-01':'2026-01-02',0.7+(i%3)*0.1);}
 sqlite.exec('COMMIT');
 return {env:{DB,SOURCE_PROXY_BASE_URL:'https://api.example'},sqlite};
}
async function collect(env,options={}){let cursor;const documents=[];let first;let pages=0;do{const page=await listIngestionDocuments(env,{...options,cursor});first??=page;documents.push(...page.documents);cursor=page.nextCursor;pages++;assert.ok(pages<1000);}while(cursor);return {documents,first,pages};}

test('more than 4000 same-group rows traverse completely with exact global counts and linear peer metadata',async()=>{
 const {env,sqlite}=setup(4207);
 try{
  const start=performance.now();await readyReviewerQueue(env);const buildMs=performance.now()-start;
  const first=await listIngestionDocuments(env,{limit:100,sort:'batchabilityDesc'});
  assert.equal(first.summary.total,4207);assert.equal(first.pageSummary.total,100);assert.ok(first.nextCursor);
  assert.ok(first.documents.every(row=>row.peerCount===4206 && /^[a-f0-9]{64}$/.test(row.triageGroupKey) && !('canBatchReviewWith' in row)));
  const result=await collect(env,{limit:97,sort:'batchabilityDesc'});
  assert.equal(result.documents.length,4207);assert.equal(new Set(result.documents.map(row=>row.id)).size,4207);
  assert.ok(result.documents.some(row=>row.id==='004206')); assert.ok(result.pages>40);
  const bytes=Buffer.byteLength(JSON.stringify(first));assert.ok(bytes<600_000,`response was ${bytes} bytes`);
  assert.equal(sqlite.prepare('SELECT SUM(member_count) AS n FROM reviewer_queue_groups').get().n,4207);
  assert.equal((await inspectReviewerQueue(env)).ready,true);
  console.log(JSON.stringify({fixture:4207,buildMs:Math.round(buildMs),pageBytes:bytes,pageSize:100,pages:result.pages,heapMiB:Math.round(process.memoryUsage().heapUsed/1048576)}));
 }finally{sqlite.close();}
});

test('every derived sort traverses ties once with deterministic ordering and variable page sizes',async()=>{
 const {env,sqlite}=setup(43);
 try{
  await readyReviewerQueue(env);
  for(const sort of ['createdAtDesc','createdAtAsc','titleAsc','titleDesc','confidenceDesc','confidenceAsc','warningCountDesc','unresolvedReferenceDesc','criticalExceptionDesc','approvalReadinessDesc','reviewerReadinessDesc','reviewerEffortAsc','batchabilityDesc','unresolvedLeverageDesc','blocked37xBatchKeyAsc']){
   const expected=await listIngestionDocuments(env,{sort,limit:100});const traversed=await collect(env,{sort,limit:7});
   assert.deepEqual(traversed.documents.map(row=>row.id),expected.documents.map(row=>row.id),sort);
   const first=await listIngestionDocuments(env,{sort,limit:3});const second=await listIngestionDocuments(env,{sort,limit:11,cursor:first.nextCursor});
   assert.deepEqual([...first.documents,...second.documents].map(row=>row.id),expected.documents.slice(0,14).map(row=>row.id));
  }
  const peers=await handleReviewerPeers(new Request(`https://api.example/admin/ingestion/reviewer-peers?groupKey=${(await listIngestionDocuments(env)).documents[0].triageGroupKey}`),env);
  assert.equal(peers.status,200);const body=await peers.json();assert.equal(body.ids.length,43);assert.equal(body.total,43);
  const self=(await listIngestionDocuments(env)).documents[0].id;
  const response=await handleReviewerPeers(new Request(`https://api.example/admin/ingestion/reviewer-peers?groupKey=${body.ids.length?(await listIngestionDocuments(env)).documents[0].triageGroupKey:''}&documentId=${self}`),env);
  const excluded=await response.json();assert.equal(excluded.total,42);assert.ok(!excluded.ids.includes(self));

 }finally{sqlite.close();}
});

test('source, validation, approval, catalog and deletion changes invalidate cursors and hold stale lists until refreshed',async()=>{
 for(const change of ["UPDATE documents SET title='Changed case' WHERE id='000000'", "UPDATE documents SET approved_at='now' WHERE id='000000'",
  "INSERT INTO document_reference_issues(id,document_id,reference_type,raw_value,normalized_value,message,severity,created_at) VALUES('issue','000000','rules_section','bad','bad','Unknown','error','now')",
  "UPDATE legal_catalog_state SET active_version='Changed catalog' WHERE singleton=1", "DELETE FROM documents WHERE id='000000'"]){
  const {env,sqlite}=setup();try{
   await readyReviewerQueue(env);const first=await listIngestionDocuments(env,{limit:3});
   if(change.includes("Changed catalog"))sqlite.exec("INSERT INTO legal_catalog_versions(id,base_epoch,state,created_at) VALUES('Changed catalog',1,'published','now')");
   sqlite.exec(change);
   if(!change.startsWith('DELETE'))await assert.rejects(listIngestionDocuments(env),/refresh is pending/);
   await readyReviewerQueue(env);
   await assert.rejects(listIngestionDocuments(env,{limit:3,cursor:first.nextCursor}),/changed/);
   const latest=await listIngestionDocuments(env,{limit:100});assert.equal(latest.summary.total,change.startsWith('DELETE')?29:30);
   assert.equal(sqlite.prepare('SELECT SUM(member_count) AS n FROM reviewer_queue_groups').get().n,latest.summary.total);
  }finally{sqlite.close();}
 }
});

test('materialization shares canonical detail policy and filters the complete derived set',async()=>{
 const {env,sqlite}=setup(80);try{
  for(const id of ['000001','000002','000003'])sqlite.prepare("UPDATE documents SET title='Fixture row',qc_has_rules_section=0 WHERE id=?").run(id);
  sqlite.prepare("UPDATE documents SET qc_required_confirmed=1 WHERE id='000004'").run();
  for(const id of ['000005','000006'])sqlite.prepare(`INSERT INTO document_reference_issues(id,document_id,reference_type,raw_value,normalized_value,message,severity,created_at)
    VALUES(?,?,'ordinance_section','Ordinance 37.3','ordinance37.3','Unknown ordinance','error','now')`).run('issue'+id,id);
  await readyReviewerQueue(env);
  const canonical=[];for(let i=0;i<80;i+=25)canonical.push(...await buildReviewerRows(env,Array.from({length:Math.min(25,80-i)},(_,j)=>String(i+j).padStart(6,'0'))));
  const optionCases=[{realOnly:true},{approvalReadyOnly:true},{reviewerReadyOnly:true},{reviewerRiskLevel:'high'},
   {blocked37xOnly:true},{blocked37xFamily:'37.3'},{runtimeManualCandidatesOnly:true},{estimatedReviewerEffort:'low'},
   {blocker:'metadata_not_confirmed'},{missingRulesOnly:true},{unresolvedReferencesOnly:true},{query:'C-000006'}];
  for(const options of optionCases){
   const expected=canonical.filter(row=>(!options.realOnly||!row.isLikelyFixture)&&(!options.approvalReadyOnly||row.approvalReadiness.eligible)&&(!options.reviewerReadyOnly||row.reviewerReady)&&(!options.reviewerRiskLevel||row.reviewerRiskLevel===options.reviewerRiskLevel)&&(!options.blocked37xOnly||row.blocked37xReferences.length>0)&&(!options.blocked37xFamily||row.blocked37xReferences.some(ref=>ref.family===options.blocked37xFamily))&&(!options.runtimeManualCandidatesOnly||row.runtimeSurfaceForManualReview)&&(!options.estimatedReviewerEffort||row.estimatedReviewerEffort===options.estimatedReviewerEffort)&&(!options.blocker||row.approvalReadiness.blockers.includes(options.blocker))&&(!options.missingRulesOnly||row.missingRulesDetection)&&(!options.unresolvedReferencesOnly||row.unresolvedReferenceCount>0)&&(!options.query||row.citation===options.query));
   const actual=await collect(env,{...options,limit:9,sort:'reviewerEffortAsc'});
   assert.deepEqual(actual.documents.map(row=>row.id).sort(),expected.map(row=>row.id).sort(),JSON.stringify(options));assert.equal(actual.first.summary.total,expected.length);
  }
  assert.equal((await listIngestionDocuments(env,{query:'C-000006'})).documents[0].unresolvedReferenceCount,1);
  assert.equal((await listIngestionDocuments(env,{blocked37xOnly:true})).summary.unsafeRuntimeManualSuppressedCount,2);
 }finally{sqlite.close();}
});

test('failed, concurrent and lost-ack steps preserve checkpoints; source changes during materialization cannot mark stale state ready',async()=>{
 for(const mode of ['failure','lost-ack','source-race','concurrent']){
  const {env,sqlite}=setup(27);try{
   const batch=env.DB.batch.bind(env.DB);let injected=false;
   env.DB.batch=async statements=>{
    if(!injected){injected=true;
     if(mode==='failure')return batch([...statements,env.DB.prepare('INSERT INTO absent_table VALUES(1)')]);
     if(mode==='source-race')sqlite.exec("UPDATE documents SET title='Winning edit' WHERE id='000000'");
     if(mode==='lost-ack'){await batch(statements);throw new Error('Lost step acknowledgment');}
    }
    return batch(statements);
   };
   if(mode==='failure'||mode==='source-race'){
    await assert.rejects(advanceReviewerQueueIndex(env));assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM reviewer_queue_state').get().n,0);assert.equal((await inspectReviewerQueue(env)).after_id,'');
   }else if(mode==='lost-ack'){assert.equal((await advanceReviewerQueueIndex(env)).reconciled,true);}
   else{const results=await Promise.all([advanceReviewerQueueIndex(env),advanceReviewerQueueIndex(env)]);assert.ok(results.some(result=>result.busy));}
   await readyReviewerQueue(env);assert.equal((await listIngestionDocuments(env)).summary.total,27);
   assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM reviewer_queue_guards').get().n,0);
   if(mode==='source-race')assert.equal((await listIngestionDocuments(env,{query:'Winning edit'})).documents.length,1);
  }finally{sqlite.close();}
 }
});

test('cursors reject filter/position corruption and a mid-read epoch change; list reads do no source policy work',async()=>{
 const {env,sqlite}=setup(35);try{
  await readyReviewerQueue(env);const first=await listIngestionDocuments(env,{limit:3});
  await assert.rejects(listIngestionDocuments(env,{cursor:first.nextCursor,query:'different'}),/changed/);
  await assert.rejects(listIngestionDocuments(env,{cursor:'invalid'}),/Invalid reviewer/);
  const prepare=env.DB.prepare.bind(env.DB);const queries=[];let injected=false;
  env.DB.prepare=sql=>{queries.push(sql);if(!injected&&sql.includes('SELECT q.payload_json AS payload')){injected=true;sqlite.exec("UPDATE documents SET author_name='Intervening edit' WHERE id='000000'");}return prepare(sql);};
  await assert.rejects(listIngestionDocuments(env,{limit:3}),/changed while loading/);
  assert.ok(queries.every(sql=>!sql.includes('FROM document_reference_issues')&&!sql.includes('FROM document_reference_links')&&!/^\s*(UPDATE|INSERT|CREATE|DELETE)/.test(sql)));
 }finally{sqlite.close();}
});

test('independent reviewer policy fixtures retain low/medium/high and approval decisions after indexing',async()=>{
 const {env,sqlite}=setup(12);try{
  sqlite.exec(`INSERT INTO document_reference_links(id,document_id,reference_type,raw_value,normalized_value,canonical_value,is_valid,created_at)
    SELECT d.id||'-'||r.kind,d.id,r.kind,r.value,r.value,r.value,1,'now' FROM documents d
    CROSS JOIN (SELECT 'index_code' AS kind,'100' AS value UNION ALL SELECT 'rules_section','1.2' UNION ALL SELECT 'ordinance_section','37.8') r;
    UPDATE documents SET title='Fixture case' WHERE id='000001';
    UPDATE documents SET extraction_confidence=0.7 WHERE id='000002';
    UPDATE documents SET extraction_confidence=0.8 WHERE id='000003';
    UPDATE documents SET rejected_at='now' WHERE id='000004';
    UPDATE documents SET qc_has_rules_section=0 WHERE id='000005';
    UPDATE documents SET extraction_confidence=0.2 WHERE id='000006';
    UPDATE documents SET approved_at='now' WHERE id='000007';
    UPDATE documents SET qc_required_confirmed=1 WHERE id='000008';
    UPDATE documents SET rules_sections_json='["10.10(c)(3)"]' WHERE id='000009';
    UPDATE documents SET extraction_warnings_json='["w1","w2","w3","w4","w5","w6","w7","w8","w9"]' WHERE id='000010';
    UPDATE documents SET reference_catalog_version='unverified' WHERE id='000011';`);
  for(const [id,count] of [['000002',2],['000003',3]])for(let i=0;i<count;i++)sqlite.prepare(`INSERT INTO document_reference_issues(id,document_id,reference_type,raw_value,normalized_value,message,severity,created_at)
    VALUES(?,?,'rules_section','Unknown citation','unknown','Not found','error','now')`).run(`${id}-${i}`,id);
  await readyReviewerQueue(env);
  assert.deepEqual((await listIngestionDocuments(env,{reviewerRiskLevel:'low'})).documents.map(row=>row.id),['000000']);
  assert.deepEqual((await listIngestionDocuments(env,{reviewerRiskLevel:'medium',sort:'titleAsc'})).documents.map(row=>row.id),['000002','000003']);
  assert.equal((await listIngestionDocuments(env,{reviewerRiskLevel:'high'})).summary.total,9);
  assert.deepEqual((await listIngestionDocuments(env,{approvalReadyOnly:true})).documents.map(row=>row.id),['000008']);
  assert.deepEqual((await listIngestionDocuments(env,{reviewerReadyOnly:true,sort:'titleAsc'})).documents.map(row=>row.id),['000000','000002','000003']);
 }finally{sqlite.close();}
});

test('lost derived rows cannot masquerade as an empty complete queue and recover with bounded rebuild steps',async()=>{
 const {env,sqlite}=setup(31);try{
  await readyReviewerQueue(env);sqlite.exec('DELETE FROM reviewer_queue_state');
  assert.equal((await inspectReviewerQueue(env)).ready,false);
  await assert.rejects(listIngestionDocuments(env),/refresh is pending/);
  await readyReviewerQueue(env);assert.equal((await listIngestionDocuments(env)).summary.total,31);
  assert.equal(sqlite.prepare('SELECT SUM(member_count) AS n FROM reviewer_queue_groups').get().n,31);
 }finally{sqlite.close();}
});

test('live inserts on both sides of a finite build cursor and moved reference issues remain reachable',async()=>{
 const {env,sqlite}=setup(27);try{
  const first=await advanceReviewerQueueIndex(env);assert.equal(first.phase,'backfill');assert.equal(first.after_id,'000024');
  for(const id of ['000001a','zz-new'])sqlite.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at)
    VALUES(?,'decision_docx','Test',?,'NEW','source','https://example.invalid','now','now')`).run(id,id);
  await readyReviewerQueue(env);const all=await collect(env,{limit:7});assert.equal(all.documents.length,29);assert.ok(all.documents.some(row=>row.id==='000001a'));assert.ok(all.documents.some(row=>row.id==='zz-new'));
  sqlite.exec(`INSERT INTO document_reference_issues(id,document_id,reference_type,raw_value,normalized_value,message,severity,created_at)
    VALUES('move','000000','rules_section','bad','bad','Not found','error','now');`);
  await readyReviewerQueue(env);sqlite.exec("UPDATE document_reference_issues SET document_id='000001' WHERE id='move'");await readyReviewerQueue(env);
  assert.deepEqual((await listIngestionDocuments(env,{unresolvedReferencesOnly:true})).documents.map(row=>row.id),['000001']);
 }finally{sqlite.close();}
});

test('materialization projects only policy metadata and rejects oversized policy/reference input before storing rows',async()=>{
 const {env,sqlite}=setup(2);try{
  sqlite.prepare('UPDATE documents SET metadata_json=? WHERE id=?').run(JSON.stringify({unusedParserPayload:'x'.repeat(180000),taxonomy:{caseTypeId:'repairs',confidence:0.2,fallback:false}}),'000000');
  await readyReviewerQueue(env);const projected=(await listIngestionDocuments(env,{query:'C-000000'})).documents[0];assert.equal(projected.taxonomySuggestion.caseTypeId,'repairs');assert.equal(projected.lowConfidenceTaxonomy,true);
  sqlite.prepare('UPDATE documents SET title=? WHERE id=?').run('x'.repeat(70000),'000001');
  await assert.rejects(advanceReviewerQueueIndex(env),/64 KiB policy input/);assert.equal((await inspectReviewerQueue(env)).ready,false);
  sqlite.prepare("UPDATE documents SET title='Case 000001' WHERE id=?").run('000001');
  const insert=sqlite.prepare(`INSERT INTO document_reference_issues(id,document_id,reference_type,raw_value,normalized_value,message,severity,created_at)
    VALUES(?,'000001','rules_section',?,'bad','Not found','error','now')`);
  for(let i=0;i<12;i++)insert.run('large-'+i,'x'.repeat(100000));
  await assert.rejects(advanceReviewerQueueIndex(env),/1 MiB step limit/);assert.equal((await inspectReviewerQueue(env)).ready,false);
 }finally{sqlite.close();}
});

test('nonfinite imported confidence cannot publish an unusable reviewer cursor',async()=>{
 const {env,sqlite}=setup(3);
 try{
  sqlite.prepare("UPDATE documents SET extraction_confidence=? WHERE id='000000'").run(Infinity);
  await assert.rejects(readyReviewerQueue(env),/confidence.*invalid|invalid.*confidence/i);
  assert.equal((await inspectReviewerQueue(env)).ready,false);
  assert.equal(sqlite.prepare('SELECT COUNT(*) n FROM reviewer_queue_state').get().n,0);
 }finally{sqlite.close();}
 const clean=setup(3);
 try{
  await readyReviewerQueue(clean.env);
  clean.sqlite.prepare("UPDATE reviewer_queue_state SET confidence=? WHERE id='000000'").run(Infinity);
  await assert.rejects(listIngestionDocuments(clean.env,{sort:'confidenceDesc',limit:1}),/invalid sort|invalid.*queue/i);
 }finally{clean.sqlite.close();}
});
