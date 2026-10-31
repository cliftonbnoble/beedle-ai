import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {successorJobFixture as fixture} from './helpers/restoration-source-successor-job-fixture.mjs';
const {advanceRestorationSourceSuccessorTick:advance,default:worker}=loadTs('src/restoration-source-successor-worker.ts');
function preserved(x){
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),x.old.tick);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history').get(),x.old.history);
  assert.deepEqual(x.sqlite.prepare('SELECT * FROM document_generations WHERE id=?').get(x.old.preserved.id),x.old.preserved);
}
test('a separate successor processes only untouched source and completes without rewriting held history',async()=>{
  const x=await fixture();try{
    const result=await advance(x.env);assert.equal(result.state,'source_complete');assert.equal(result.deliveries,1);assert(result.bindingCalls<700);
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'indexing');
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,2);
    const proof=JSON.parse(x.sqlite.prepare('SELECT proof_json FROM beedle_restore_source_results WHERE document_id=?').get(x.next).proof_json);
    assert.equal(proof.version,2);assert.equal(proof.successorExecution.successorId,x.successor.successorId);assert.equal(proof.humanApproval,false);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL OR searchable_at IS NOT NULL OR qc_required_confirmed!=0').get().n,0);
    preserved(x);const changes=x.sqlite.prepare('SELECT total_changes() n').get().n;
    assert.deepEqual(await advance(x.env),{state:'source_complete',deliveries:0});assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,changes);
  }finally{x.sqlite.close();}
});
test('an uncertain source receipt holds the new execution and never replays generation',async()=>{
  const x=await fixture();try{
    const put=x.proofs.put,initial=x.proofPuts();x.proofs.put=async(...args)=>{const value=await put(...args);if(args[0].startsWith('restoration/'))throw Error('Lost proof response');return value;};
    await assert.rejects(advance(x.env),/tick_held/);const count=x.sqlite.prepare('SELECT COUNT(*) n FROM document_generations').get().n;
    assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_jobs WHERE document_id=?').get(x.next).phase,'seeded');
    assert.deepEqual(await advance(x.env),{state:'held',deliveries:0});assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM document_generations').get().n,count);
    assert.equal(x.proofPuts()-initial,2);assert.deepEqual(x.sqlite.prepare('SELECT * FROM beedle_restore_owner').get(),x.old.owner);preserved(x);
  }finally{x.sqlite.close();}
});
test('source plan or schema drift holds the successor before claiming a source job',async()=>{
  for(const mode of ['schema','plan']){const x=await fixture();try{
    if(mode==='schema')x.sqlite.exec('CREATE TABLE unknown_writer(id TEXT)');
    else x.proofs.get=async()=>null;
    await assert.rejects(advance(x.env),/preflight_held/);
    assert.equal(x.sqlite.prepare('SELECT state FROM beedle_restore_source_successor').get().state,'held');
    assert.equal(x.sqlite.prepare('SELECT claim_token FROM beedle_restore_jobs WHERE document_id=?').get(x.next).claim_token,null);preserved(x);
  }finally{x.sqlite.close();}}
});
test('disabled and duplicate successor deliveries cannot touch source objects; HTTP stays private',async()=>{
  assert.equal(worker.fetch().status,503);assert.deepEqual(await advance({RESTORE_ENABLED:'false',get DB(){throw Error('No binding access');}}),{state:'disabled',deliveries:0});
  const x=await fixture();try{x.sqlite.exec("UPDATE beedle_restore_source_successor_tick SET state='running',token='existing'");x.proofs.get=async()=>{throw Error('No source access');};
    assert.deepEqual(await advance(x.env),{state:'busy',deliveries:0});preserved(x);
  }finally{x.sqlite.close();}
});
