import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
const {restorationCapacityObserver:observer}=loadTs('src/services/restoration-capacity-observer.ts');
const {restorationMeasuredDatabase:measured}=loadTs('src/services/restoration-measured-database.ts');
function fixture(){
  const calls=[],state={size:16384,rows:[{value:7,nullable:null}],fail:false};
  const result=()=>({success:true,meta:{size_after:state.size},results:state.rows});
  const raw={prepare(sql){return {sql,args:[],bind(...args){return {...this,args};},async all(){calls.push({method:'all',sql,args:this.args});if(state.fail)throw Error('Native lost response');return result();},async run(){calls.push({method:'run',sql,args:this.args});if(state.fail)throw Error('Native lost response');return result();},async first(){throw Error('first metadata unavailable');}};},
    async batch(statements){calls.push({method:'batch',statements});if(state.fail)throw Error('Native lost response');return statements.map(()=>result());}};
  const capacity=observer(),db=measured(raw,capacity,'staging');return {raw,db,capacity,calls,state};
}
test('first observes one real result while preserving row, named-column and empty semantics',async()=>{
  const x=fixture();assert.deepEqual(await x.db.prepare('SELECT ? value').bind(7).first(),{value:7,nullable:null});assert.equal(x.calls.length,1);assert.deepEqual(x.calls[0].args,[7]);
  assert.equal(await x.db.prepare('SELECT value').first('value'),7);assert.equal(await x.db.prepare('SELECT nullable').first('nullable'),null);
  x.state.rows=[];assert.equal(await x.db.prepare('SELECT value WHERE 0').first(),null);assert.equal(x.capacity.snapshot().samples,4);assert.equal(x.calls.length,4);
});
test('all, run and batch retain their response values and use only one binding request each',async()=>{
  const x=fixture();assert.equal((await x.db.prepare('SELECT value').all()).results,x.state.rows);assert.equal((await x.db.prepare('UPDATE synthetic SET value=?').bind(8).run()).success,true);
  const results=await x.db.batch([x.db.prepare('UPDATE first SET value=?').bind(9),x.db.prepare('UPDATE second SET value=?').bind(10)]);assert.equal(results.length,2);assert.equal(x.calls.length,3);assert.deepEqual(x.calls[2].statements.map(s=>s.args),[[9],[10]]);assert.equal(x.capacity.snapshot().samples,4);
});
test('an unknown statement or unsupported metadata-free API is refused before SQL',async()=>{
  const x=fixture();await assert.rejects(x.db.batch([x.raw.prepare('SELECT 1')]),/unmeasured_batch/);assert.throws(()=>x.db.exec('SELECT 1'),/unmeasured_database/);assert.throws(()=>x.db.prepare('SELECT 1').raw(),/unmeasured_raw/);assert.equal(x.calls.length,0);
});
test('a lost response never receives capacity credit or a second SQL attempt',async()=>{
  const x=fixture();x.state.fail=true;await assert.rejects(x.db.prepare('UPDATE synthetic SET value=1').run(),/lost response/);assert.equal(x.calls.length,1);assert.equal(x.capacity.snapshot().samples,0);
});
test('capacity threshold and missing-column errors retain the acknowledged query without reissue',async()=>{
  const x=fixture();x.state.size=9000000000;await assert.rejects(x.db.prepare('UPDATE synthetic SET value=1').run(),e=>e.reason==='limit_reached'&&e.sqlResponseAcknowledged);assert.equal(x.calls.length,1);assert.equal(x.capacity.snapshot().peakBytes,9000000000);
  const y=fixture();await assert.rejects(y.db.prepare('SELECT value').first('absent'),/column_missing/);assert.equal(y.calls.length,1);assert.equal(y.capacity.snapshot().samples,1);
});
