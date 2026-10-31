import test from'node:test';import assert from'node:assert/strict';
import{loadTs}from'./helpers/load-ts.mjs';import{fingerprint}from'../scripts/lib/release-contract.mjs';
const{recordRestorationFailure,restorationErrorEvidence}=loadTs('src/services/restoration-failure-proof.ts');
const{restorationBindingBudget}=loadTs('src/services/restoration-binding-budget.ts');
const identity={ownerId:'11111111-1111-4111-8111-111111111111',resourceId:'22222222-2222-4222-8222-222222222222',partitionIndex:0,
  planSha256:'a'.repeat(64),resourcePlanSha256:'b'.repeat(64),sourceManifestSha256:'c'.repeat(64),catalogSha256:'d'.repeat(64)};
const attemptId='33333333-3333-4333-8333-333333333333';
function bucket(){const objects=new Map();let puts=0,gets=0;return{objects,get puts(){return puts;},get gets(){return gets;},
  async put(key,value,options){puts++;assert.deepEqual(options.onlyIf,{etagDoesNotMatch:'*'});if(objects.has(key))return null;
    const bytes=new Uint8Array(value),row={bytes,size:bytes.length,version:'v1',etag:'etag1',customMetadata:options.customMetadata};objects.set(key,row);return row;},
  async get(key){gets++;const row=objects.get(key);return row?{...row,arrayBuffer:async()=>row.bytes.buffer.slice(0)}:null;}};}
const input=proofs=>({proofs,identity,attemptId,stage:'source',bindingCalls:17,error:new Error('D1_ERROR: private SQL and source object key; SQLITE_BUSY')});

test('failure evidence hashes private messages and projects only known fields',async()=>{const proofs=bucket(),error=new Error('D1_ERROR: private-sentinel time limit');error.cause=new Error('private-original-key SQLITE_BUSY');
  await recordRestorationFailure({...input(proofs),identity:{...identity,secret:'private-sentinel'},error,
    bindingFailure:{method:'batch',statements:50,sqlSha256:['f'.repeat(64)],boundValues:'private-sentinel'}});
  const text=new TextDecoder().decode([...proofs.objects.values()][0].bytes),proof=JSON.parse(text);assert(!text.includes('private-sentinel'));assert(!text.includes('private-original-key'));assert(!text.includes('boundValues'));
  assert.equal(proof.errorChain[0].messageSha256,await fingerprint(error.message));assert.deepEqual(proof.errorChain[0].codes,['D1_ERROR']);assert.deepEqual(proof.errorChain[0].markers,['time_limit']);
  assert.deepEqual(proof.errorChain[1].codes,['SQLITE_BUSY']);assert.equal(proof.engineSuccessCredited,false);assert.equal(proof.holdPreserved,true);assert.equal(proofs.puts,1);});
test('an immutable failure result refuses overwrite and never retries an uncertain put',async()=>{const proofs=bucket();await recordRestorationFailure(input(proofs));const original=[...proofs.objects.values()][0].bytes.slice();
  await assert.rejects(recordRestorationFailure(input(proofs)),/conditional_conflict/);assert.equal(proofs.objects.size,1);assert.deepEqual([...proofs.objects.values()][0].bytes,original);
  const lost=bucket(),put=lost.put.bind(lost);lost.put=async(...args)=>{await put(...args);throw Error('lost acknowledgment');};await assert.rejects(recordRestorationFailure(input(lost)),/lost acknowledgment/);assert.equal(lost.puts,1);assert.equal(lost.gets,0);assert.equal(lost.objects.size,1);});
test('changed readback remains unconfirmed without another put',async()=>{const proofs=bucket(),get=proofs.get.bind(proofs);proofs.get=async key=>({...await get(key),version:'different'});
  await assert.rejects(recordRestorationFailure(input(proofs)),/readback_unconfirmed/);assert.equal(proofs.puts,1);});
test('error chains are bounded, cycle-safe and never copy arbitrary error names',async()=>{const error=new Error('x'.repeat(70000));error.name='private-name';error.cause=error;
  const evidence=await restorationErrorEvidence(error);assert.equal(evidence.length,1);assert.equal(evidence[0].name,'other');assert.equal(evidence[0].messageTruncated,true);assert.equal(evidence[0].messageSha256,await fingerprint('x'.repeat(65536)));});
test('invalid failure identities and raw SQL cannot reach proof storage',async()=>{const proofs=bucket();await assert.rejects(recordRestorationFailure({...input(proofs),attemptId:'invalid'}),/identity/);
  await assert.rejects(recordRestorationFailure({...input(proofs),bindingFailure:{method:'batch',statements:1,sqlSha256:['INSERT private value']}}),/binding_evidence/);assert.equal(proofs.puts,0);});
test('binding diagnostics preserve the original batch failure and never reissue it',async()=>{const failure=new Error('D1_ERROR: private bound value SQLITE_BUSY');let executions=0;
  const raw={prepare(sql){return{sql,bind(...values){return{sql,values}}}},async batch(){executions++;throw failure;}};
  const meter=restorationBindingBudget(raw),statement=meter.db.prepare('INSERT INTO private_table VALUES (?)').bind('private bound value');
  await assert.rejects(meter.db.batch([statement]),error=>error===failure);assert.equal(executions,1);assert.equal(meter.calls,1);
  assert.deepEqual(await meter.failureEvidence(),{method:'batch',statements:1,sqlSha256:[await fingerprint('INSERT INTO private_table VALUES (?)')]});
  assert(!JSON.stringify(await meter.failureEvidence()).includes('private bound value'));});
