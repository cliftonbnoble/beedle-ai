import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from '../../api/tests/helpers/load-ts.mjs';
const { ReviewerSession }=loadTs('../web/src/lib/reviewer-session.ts');
function load(session,id,revision=1){session.select(id);const ticket=session.startLoad(id);session.acceptLoad(ticket,{id,documentRevision:revision});return {documentId:id,revision};}
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
test('out-of-order detail responses and failed selection never admit the previous form',async()=>{
  const session=new ReviewerSession();const a=load(session,'A');
  session.select('B');assert.equal(session.canMutate(a),false);
  const b=session.startLoad('B');const response=deferred();
  const late=response.promise.then(value=>session.acceptLoad(b,value));
  session.select('C');const c=session.startLoad('C');assert.equal(session.failLoad(c),true);
  response.resolve({id:'B',documentRevision:1});assert.equal(await late,false);
  assert.equal(session.loading,false);assert.equal(session.canMutate(a),false);
  assert.equal(session.startLoad('A'),null);
});
test('mutation A completing during B load, after B load, after B failure, or after A-B-A cannot refresh the pane',async()=>{
  for(const phase of ['loading','loaded','failed','returned']){
    const session=new ReviewerSession();const form=load(session,'A',7);const mutation=session.beginMutation(form);
    assert.ok(mutation);assert.equal(session.beginMutation(form),null);
    const completion=deferred();const result=completion.promise.then(()=>session.finishMutation(mutation));
    session.select('B');const b=session.startLoad('B');
    if(phase==='loaded')session.acceptLoad(b,{id:'B',documentRevision:2});
    if(phase==='failed')session.failLoad(b);
    if(phase==='returned')load(session,'A',7);
    completion.resolve();assert.equal(await result,false,phase);
    assert.equal(session.beginMutation({documentId:'wrong',revision:7}),null);
  }
});
test('form revisions, forged response identities and repeated submissions are fenced',()=>{
  const session=new ReviewerSession();const form=load(session,'A',4);
  assert.equal(session.beginMutation({...form,revision:3}),null);
  const mutation=session.beginMutation(form);assert.ok(mutation);
  for(let i=0;i<10;i++)assert.equal(session.beginMutation(form),null);
  assert.equal(session.finishMutation(mutation),true);
  const reload=session.startLoad('A');assert.equal(session.canMutate(form),false);
  assert.throws(()=>session.acceptLoad(reload,{id:'B',documentRevision:4}),/identity/);
  session.failLoad(reload);assert.equal(session.canMutate(form),false);
  for(const revision of [0,NaN,1.5,undefined])assert.throws(()=>session.acceptLoad(session.startLoad('A'),{id:'A',documentRevision:revision}),/identity/);
});
test('an older completion cannot release a newer mutation and a failed request preserves retry eligibility',()=>{
  const session=new ReviewerSession();const form=load(session,'A',3);
  const first=session.beginMutation(form);assert.equal(session.finishMutation(first),true);
  const retry=session.beginMutation(form);assert.ok(retry);
  assert.equal(session.finishMutation(first),false);assert.equal(session.canMutate(form),false);
  assert.equal(session.finishMutation(retry),true);assert.equal(session.canMutate(form),true);
});
