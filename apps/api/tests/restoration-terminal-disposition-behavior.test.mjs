import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {terminalDispositionFixture as fixture} from './helpers/restoration-terminal-disposition-fixture.mjs';
const {terminalizeRestorationAttempt:terminalize}=loadTs('src/services/restoration-terminal-disposition.ts');
const changes=x=>x.sqlite.prepare('SELECT total_changes() n').get().n;
function unchangedCase(x){
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM documents WHERE id=?').get(x.next),x.terminalBefore.document);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(x.next),x.terminalBefore.job);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.old.tick);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history').get(),x.old.history);
}
test('a proven abandoned staging generation receives a technical failure while claims, stage rows and original holds remain',async()=>{
  const x=await fixture();try{
    const result=await terminalize(x.db,x.plan);assert.equal(result.state,'terminal_disposition_acknowledged');
    assert.equal(result.stagingGenerationTechnicallyFailed,true);assert.equal(result.sourceReprocessed,false);assert.equal(result.publicationOrIntegrityApproval,false);
    assert.equal(x.sqlite.prepare('SELECT status FROM document_generations WHERE id=?').get(x.plan.abandonStagingGenerationId).status,'failed');
    for(const table of Object.keys(x.terminalBefore.stages))assert.deepEqual(x.sqlite.prepare('SELECT * FROM staged_'+table+' WHERE generation_id IN (SELECT id FROM document_generations WHERE document_id=?) ORDER BY generation_id,id').all(x.next),x.terminalBefore.stages[table]);
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor').get().state,'held');
    assert.equal(x.sqlite.prepare('SELECT token FROM beedle_restore_source_successor_tick').get().token,x.terminalBefore.currentTick.token);unchangedCase(x);
    const before=changes(x);await assert.rejects(terminalize(x.db,x.plan),/parent_changed/);assert.equal(changes(x),before);
  }finally{x.sqlite.close();}
});
test('a published attempt is preserved exactly while its abandoned executor is held',async()=>{
  const x=await fixture('published');try{
    const result=await terminalize(x.db,x.plan);assert.equal(result.stagingGenerationTechnicallyFailed,false);
    assert.equal(result.publishedGenerationChanged,false);unchangedCase(x);
    assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').all(x.next),x.terminalBefore.generations);
  }finally{x.sqlite.close();}
});
test('a published generation cannot be relabelled as an abandoned staging generation',async()=>{
  const x=await fixture('published');try{
    const before=changes(x);await assert.rejects(terminalize(x.db,{...x.plan,abandonStagingGenerationId:x.terminalBefore.document.active_text_generation}),/not_abandoned_staging/);
    assert.equal(changes(x),before);unchangedCase(x);
  }finally{x.sqlite.close();}
});
test('changed identity, checkpoint, metadata, generation or stage hashes authorize no SQL mutation',async()=>{
  for(const key of ['controlSha256','tickSha256','historySha256','jobSha256','documentSha256','generationsSha256','stagedArtifactsSha256']){
    const x=await fixture();try{const before=changes(x);await assert.rejects(terminalize(x.db,{...x.plan,[key]:'0'.repeat(64)}));assert.equal(changes(x),before);unchangedCase(x);}finally{x.sqlite.close();}
  }
});
test('a parent without the planned quiescence interval is rejected before database access',async()=>{
  const x=await fixture();try{await assert.rejects(terminalize({get prepare(){throw Error('Unexpected database access');}},
    {...x.plan,parentEndedAt:new Date().toISOString()}),/quiescence/);}finally{x.sqlite.close();}
});
test('a failing statement rolls the entire terminal disposition batch back',async()=>{
  const x=await fixture();try{
    const prepare=x.db.prepare;x.db.prepare=sql=>prepare(sql.startsWith('UPDATE document_generations SET status=')?'SELECT * FROM missing_terminal_fixture_table':sql);
    await assert.rejects(terminalize(x.db,x.plan),/missing_terminal_fixture_table/);
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor').get().state,'building');
    assert.equal(x.sqlite.prepare('SELECT status FROM document_generations WHERE id=?').get(x.plan.abandonStagingGenerationId).status,'staging');unchangedCase(x);
  }finally{x.sqlite.close();}
});
test('a lost batch acknowledgment retains the terminal state and cannot replay the mutation',async()=>{
  const x=await fixture();try{
    const batch=x.db.batch;let calls=0;x.db.batch=async rows=>{calls++;await batch(rows);throw Error('Lost terminal batch acknowledgment');};
    await assert.rejects(terminalize(x.db,x.plan),/Lost terminal batch/);await assert.rejects(terminalize(x.db,x.plan),/parent_changed/);
    assert.equal(calls,1);assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor_tick').get().state,'held');unchangedCase(x);
  }finally{x.sqlite.close();}
});
