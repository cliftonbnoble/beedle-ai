import test from 'node:test';
import assert from 'node:assert/strict';
import { createWorkerFixture } from './fixture.mjs';
import { query } from '../../scripts/recovery.mjs';

test('DB-only backfill uses native D1 checkpoints without publishing held documents', {timeout:180000},async()=>{
 const f=await createWorkerFixture({lexicalOnly:true,consumers:false,autoStart:false});
 try{
  const ep={mode:'local',config:f.configPath,persistTo:f.persistTo};
  const identity={runId:crypto.randomUUID(),target:'a'.repeat(64),bundle:'b'.repeat(64)};
  await query(ep,`CREATE TABLE beedle_recovery_receipt(singleton INTEGER PRIMARY KEY,run_id TEXT,bundle_sha256 TEXT,target_sha256 TEXT); INSERT INTO beedle_recovery_receipt VALUES(1,'${identity.runId}','${identity.bundle}','${identity.target}'); INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES('held','decision_docx','Test','Held case','Test citation','held.docx','https://example.invalid','now','now'); INSERT INTO document_sections(id,document_id,canonical_key,heading,section_order) VALUES('s','held','conclusions_of_law','Conclusions',0); INSERT INTO section_paragraphs(id,section_id,anchor,paragraph_order,text) VALUES('p','s','p1',0,'nativelegacyneedle rent'); INSERT INTO document_chunks(id,document_id,section_id,paragraph_id,paragraph_anchor,citation_anchor,section_label,chunk_order,chunk_text,token_estimate,created_at) VALUES('c','held','s','p','p1','L:p1','Conclusions',0,'nativelegacyneedle rent',5,'now');`,f.dir);
  const before=await query(ep,'SELECT * FROM documents',f.dir);
  await f.start();
  await f.request('/__test/migration-tick',{...identity,bundle:'c'.repeat(64)},{status:503});
  let state;for(let tick=0;tick<10;tick++){state=await f.request('/__test/migration-tick',identity);if(state.state==='indexes_ready')break;}
  assert.equal(state.state,'indexes_ready');
  const replay=await f.request('/__test/migration-tick',identity);assert.equal(replay.deliveries,state.deliveries);
  await f.pause();
  assert.deepEqual(await query(ep,'SELECT * FROM documents',f.dir),before);
  assert.deepEqual(await query(ep,"SELECT COUNT(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'nativelegacyneedle'",f.dir),[{n:1}]);
  const control=await query(ep,'SELECT generation,status FROM search_index_rebuild',f.dir);
  assert.deepEqual(control,[{generation:identity.runId,status:'completed'}]);
  assert.deepEqual(await query(ep,"SELECT COUNT(*) n FROM reviewer_queue_sources WHERE dirty=1",f.dir),[{n:0}]);
 }finally{await f.stop();}
});
