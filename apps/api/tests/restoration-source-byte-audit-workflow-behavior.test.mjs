import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {loadTs} from './helpers/load-ts.mjs';

const {runRestorationByteAuditWorkflow,parseRestorationByteAuditPlan}=loadTs('src/services/restoration-source-byte-audit-workflow.ts');
const {canonical,fingerprint}=await import('../scripts/lib/release-contract.mjs');
const {copyVerifiedRestorationSource} = loadTs('src/services/restoration-source.ts');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const source = {ownerId:'11111111-1111-4111-8111-111111111111',
  resourceId:'22222222-2222-4222-8222-222222222222', partitionIndex:3,
  planSha256:'a'.repeat(64), resourcePlanSha256:'b'.repeat(64),
  sourceManifestSha256:'c'.repeat(64), catalogSha256:'d'.repeat(64)};

async function fixture() {
  const text = 'Synthetic source for a byte audit.\n', bytes = new TextEncoder().encode(text);
  function bucket() {
    const rows = new Map(), counts = {get:0, put:0, read:0};
    return {rows, counts,
      async get(key) {
        counts.get++;
        const row = rows.get(key);
        return row ? {...row, size:row.reportedSize ?? row.bytes.length,
          async arrayBuffer() {counts.read++;return Uint8Array.from(row.bytes).buffer;}} : null;
      },
      async put(key, value, options) {
        counts.put++;
        if (rows.has(key) && options?.onlyIf?.etagDoesNotMatch === '*') return null;
        const data = typeof value === 'string' ? new TextEncoder().encode(value) : Uint8Array.from(value);
        const row = {bytes:data, version:'version-'+counts.put, etag:sha(data), customMetadata:options?.customMetadata};
        rows.set(key,row);return {...row,size:data.length};
      }
    };
  }
  const originals = bucket(), target = bucket(), proofs = bucket();
  const descriptor = {ownerId:source.ownerId, planSha256:source.planSha256, partitionIndex:source.partitionIndex,
    originalKey:'private/synthetic-source.md', originalVersion:'original-version', sourceSha256:sha(bytes), sourceBytes:bytes.length};
  originals.rows.set(descriptor.originalKey,{bytes,version:descriptor.originalVersion,etag:sha(bytes)});
  const copy = await copyVerifiedRestorationSource(originals,target,descriptor);
  const currentKey = 'current/synthetic.md';
  target.rows.set(currentKey,{bytes:Uint8Array.from(bytes),version:'current-version',etag:sha(bytes)});
  const receipt = {version:3,...source,documentId:'synthetic-document',claimToken:'33333333-3333-4333-8333-333333333333',
    copy,sourceSha256:descriptor.sourceSha256,sourceBytes:bytes.length,generationId:'synthetic-generation',
    documentRevision:2,humanApproval:false,disposition:'processed_unpublished_unreviewed'};
  const receiptBytes = new TextEncoder().encode(JSON.stringify(receipt));
  const receiptKey = `restoration/${source.ownerId}/${source.planSha256}/${source.partitionIndex}/${sha(receipt.documentId)}/${receipt.generationId}.json`;
  proofs.rows.set(receiptKey,{bytes:receiptBytes,version:'receipt-version',etag:sha(receiptBytes),
    customMetadata:{ownerid:source.ownerId,plansha256:source.planSha256,proofsha256:sha(receiptBytes)}});
  const record = {documentId:receipt.documentId,claimToken:receipt.claimToken,sourceSha256:descriptor.sourceSha256,
    sourceBytes:bytes.length,originalKey:descriptor.originalKey,originalVersion:descriptor.originalVersion,
    copyKey:copy.targetKey,copyVersion:copy.targetVersion,copyEtag:copy.targetEtag,
    receiptGenerationId:receipt.generationId,receiptDocumentRevision:2,receiptSha256:sha(receiptBytes),
    currentKey,currentVersion:'current-version',currentEtag:sha(bytes)};
  // Audit capabilities expose GET only. Any attempted write is an immediate
  // failure, even though test setup needed the existing copy service once.
  const getOnly = b => ({get:b.get.bind(b)});
  const input = {source:{...source},record,originals:getOnly(originals),target:getOnly(target),proofs:getOnly(proofs)};
  return {input,originals,target,proofs,receiptKey,descriptor,copy};
}

async function workflowFixture(count=1) {
  const f=await fixture(),records=[],jobs=[];
  for(let i=0;i<count;i++) {
    const row={...f.input.record,documentId:'synthetic-document-'+String(i).padStart(3,'0')};
    const proof=JSON.parse(new TextDecoder().decode(f.proofs.rows.get(f.receiptKey).bytes));proof.documentId=row.documentId;
    const bytes=new TextEncoder().encode(JSON.stringify(proof));row.receiptSha256=sha(bytes);
    f.proofs.rows.set(`restoration/${source.ownerId}/${source.planSha256}/${source.partitionIndex}/${sha(row.documentId)}/${row.receiptGenerationId}.json`,
      {bytes,version:'receipt-'+i,etag:sha(bytes),customMetadata:{ownerid:source.ownerId,plansha256:source.planSha256,proofsha256:sha(bytes)}});
    records.push(row);jobs.push({documentId:row.documentId,originalKey:row.originalKey,originalVersion:row.originalVersion,
      sourceBytes:row.sourceBytes,sourceSha256:row.sourceSha256});
  }
  const descriptor={version:1,ownerId:source.ownerId,resourceId:source.resourceId,partitionIndex:3,
    resourcePlanSha256:source.resourcePlanSha256,sourceManifestSha256:source.sourceManifestSha256,catalogSha256:source.catalogSha256,jobs};
  const descriptorText=canonical(descriptor),partitionPlanSha256=sha(descriptorText),auditRootId='55555555-5555-4555-8555-555555555555',operationId='66666666-6666-4666-8666-666666666666';
  const now=Date.now(),root={version:1,auditRootId,partitions:Array.from({length:32},(_,i)=>({
    source:i===3?{...source}:{...source,partitionIndex:i,resourceId:'77777777-7777-4777-8777-'+String(i).padStart(12,'0')},
    partitionPlanSha256:i===3?partitionPlanSha256:sha('partition-'+i)})),
    notBefore:new Date(now-2000).toISOString(),completionDeadline:new Date(now+3*3600000).toISOString()};
  const plan={version:1,auditRootId,operationId,partitionIndex:3,databaseBookendSha256:'e'.repeat(64),records,
    notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+29*60000).toISOString(),completionDeadline:new Date(now+2*3600000).toISOString()};
  const env={originals:f.input.originals,target:f.input.target,proofs:f.proofs,operationEnabled:'true',operationJson:''},steps=[];
  function store(key,text,customMetadata={}) {f.proofs.rows.set(key,{bytes:new TextEncoder().encode(text),version:'prepared-'+sha(text),etag:sha(text),customMetadata});}
  function refreshRoot() {const text=canonical(root),rootPlanSha256=sha(text),metadata={auditrootid:auditRootId,rootsha256:rootPlanSha256},header={version:root.version,auditRootId,rootPlanSha256};
    store(`source-byte-audit/${auditRootId}/root.json`,text,metadata);
    store(`source-byte-audit/${auditRootId}/root-bootstrap-claim.json`,canonical(header),metadata);
    store(`source-byte-audit/${auditRootId}/root-bootstrap-outcome.json`,canonical({state:'source_byte_audit_root_ready',auditRootId,rootPlanSha256,rootVersion:'prepared-'+sha(text),rootEtag:sha(text),noDatabaseSourceMutationOrIntegrityCredit:true}),metadata);
    env.operationJson=JSON.stringify(header);
  }
  store('plans/'+partitionPlanSha256+'.json',descriptorText);refreshRoot();
  const event={instanceId:operationId,payload:{operationPlanJson:canonical(plan)}},step={async do(name,options,callback){assert.equal(options.retries.limit,0);steps.push(name);return callback();}};
  return{...f,env,root,plan,event,step,steps,refreshRoot,descriptor,partitionPlanSha256,
    prefix:`source-byte-audit/${auditRootId}/${operationId}/`,run(){event.payload.operationPlanJson=canonical(plan);return runRestorationByteAuditWorkflow(env,event,step);}};
}
async function heldWorkflowFixture() {
  const f=await workflowFixture(3);f.root.version=2;f.plan.version=2;f.root.heldSources=[];
  f.plan.records=f.plan.records.map((row,index)=>{
    const held=index!==1;f.descriptor.jobs[index].extractionPolicy=held?'held-unreadable':'verified-source-restoration-v1';
    if(!held)return{kind:'processed',...row};
    const oldKey=`restoration/${source.ownerId}/${source.planSha256}/${source.partitionIndex}/${sha(row.documentId)}/${row.receiptGenerationId}.json`,
      stored=f.proofs.rows.get(oldKey),proof=JSON.parse(new TextDecoder().decode(stored.bytes));
    proof.generationId='initial';proof.disposition='held_unreadable_original_preserved';
    proof.currentSourceKey=row.copyKey;proof.currentSourceVersion=row.copyVersion;proof.currentSourceEtag=row.copyEtag;
    stored.bytes=new TextEncoder().encode(canonical(proof));stored.customMetadata.proofsha256=sha(stored.bytes);
    f.proofs.rows.delete(oldKey);f.proofs.rows.set(oldKey.replace('/synthetic-generation.json','/initial.json'),stored);
    f.root.heldSources.push({partitionIndex:3,documentSha256:sha(row.documentId),sourceSha256:row.sourceSha256,sourceBytes:row.sourceBytes});
    const{currentKey,currentVersion,currentEtag,...record}=row;
    return{...record,kind:'held_unreadable',receiptGenerationId:'initial',receiptSha256:sha(stored.bytes)};
  });
  f.root.heldSources.sort((a,b)=>a.documentSha256.localeCompare(b.documentSha256));
  function refreshDescriptor(){const text=canonical(f.descriptor),hash=sha(text);
    f.proofs.rows.set('plans/'+hash+'.json',{bytes:new TextEncoder().encode(text),version:'descriptor-'+hash,etag:hash,customMetadata:{}});
    f.root.partitions[3].partitionPlanSha256=hash;f.refreshRoot();}
  refreshDescriptor();return{...f,refreshDescriptor};
}

test('v2 durable audit verifies processed text and only the two root-declared held originals without publication credit',async()=>{
  const f=await heldWorkflowFixture(),puts=[f.originals.counts.put,f.target.counts.put],result=await f.run();
  assert.equal(result.records,3);assert.equal(result.fullIntegrityOrPublicationApproval,false);
  const saved=JSON.parse(new TextDecoder().decode(f.proofs.rows.get(f.prefix+'bytes-0000-outcome.json').bytes));
  assert.deepEqual(saved.result.results.map(row=>row.state),[
    'held_original_copy_bytes_verified','original_copy_current_bytes_verified','held_original_copy_bytes_verified']);
  assert.deepEqual([f.originals.counts.put,f.target.counts.put],puts);
  const reads=f.originals.counts.get;await assert.rejects(()=>f.run());assert.equal(f.originals.counts.get,reads);
});

for(const scenario of ['missing_held','extra_held','duplicate_held','unsorted_held','held_partition','held_hash','held_size',
  'undeclared_held','wrong_policy','missing_policy','processed_as_held','held_as_processed','v1_root','v1_plan','v1_header',
  'fabricated_current','processed_initial_receipt']){
  test('v2 audit rejects '+scenario+' before reading originals',async()=>{
    const f=await heldWorkflowFixture();
    if(scenario==='missing_held')f.root.heldSources.pop();
    if(scenario==='extra_held')f.root.heldSources.push({...f.root.heldSources[0],documentSha256:'f'.repeat(64)});
    if(scenario==='duplicate_held')f.root.heldSources[1]={...f.root.heldSources[0]};
    if(scenario==='unsorted_held')f.root.heldSources.reverse();
    if(scenario==='held_partition')f.root.heldSources.forEach(row=>row.partitionIndex=4);
    if(scenario==='held_hash')f.root.heldSources.forEach(row=>row.sourceSha256='0'.repeat(64));
    if(scenario==='held_size')f.root.heldSources.forEach(row=>row.sourceBytes++);
    if(scenario==='undeclared_held'){f.root.heldSources[0].documentSha256='0'.repeat(64);f.root.heldSources.sort((a,b)=>a.documentSha256.localeCompare(b.documentSha256));}
    if(scenario==='wrong_policy')f.descriptor.jobs[0].extractionPolicy='recovered-markdown-v1';
    if(scenario==='missing_policy')delete f.descriptor.jobs[0].extractionPolicy;
    if(scenario==='processed_as_held')f.plan.records[1]={...f.plan.records[0],documentId:f.plan.records[1].documentId};
    if(scenario==='held_as_processed'){f.plan.records[0]={...f.plan.records[0],kind:'processed',currentKey:'invented',currentVersion:'invented',currentEtag:'invented'};}
    if(scenario==='v1_root'){f.root.version=1;delete f.root.heldSources;}
    if(scenario==='v1_plan')f.plan.version=1;
    if(scenario==='fabricated_current')f.plan.records[0].currentKey='invented';
    if(scenario==='processed_initial_receipt')f.plan.records[0].receiptGenerationId='synthetic-generation';
    f.refreshDescriptor();
    if(scenario==='v1_header')f.env.operationJson=JSON.stringify({...JSON.parse(f.env.operationJson),version:1});
    const reads=f.originals.counts.get,puts=[f.originals.counts.put,f.target.counts.put];
    await assert.rejects(()=>f.run());assert.equal(f.originals.counts.get,reads);
    assert.deepEqual([f.originals.counts.put,f.target.counts.put],puts);
  });
}

for(const name of ['initialize-claim.json','initialize-outcome.json','bytes-0000-outcome.json']){
  test('v2 held audit preserves lost '+name+' acknowledgment and does not repeat original reads',async()=>{
    const f=await heldWorkflowFixture(),put=f.env.proofs.put.bind(f.env.proofs);let lost=false;
    f.env.proofs.put=async(key,value,options)=>{const stored=await put(key,value,options);
      if(key===f.prefix+name&&!lost){lost=true;throw Error('synthetic unknown acknowledgment');}return stored;};
    await assert.rejects(()=>f.run());assert.ok(f.proofs.rows.has(f.prefix+name));
    const reads=f.originals.counts.get;await assert.rejects(()=>f.run());assert.equal(f.originals.counts.get,reads);
  });
}
test('durable audit groups nine sources into two zero-retry callbacks, with no DB or source writes',async()=>{
  const f=await workflowFixture(9),writes=[f.originals.counts.put,f.target.counts.put],result=await f.run();
  assert.equal(result.records,9);assert.equal(f.steps.length,4);assert.equal(result.databaseBookendsVerified,false);
  assert.equal(result.fullIntegrityOrPublicationApproval,false);assert.equal(result.recordsSha256,await fingerprint(f.plan.records));
  assert.deepEqual([f.originals.counts.put,f.target.counts.put],writes);
  const results=[...f.proofs.rows].filter(([key])=>key.startsWith(f.prefix)&&key.endsWith('-outcome.json'));
  assert.equal(results.length,4);assert.equal(results.some(([,row])=>new TextDecoder().decode(row.bytes).includes(f.plan.records[0].originalKey)),false);
  const readCount=f.originals.counts.get;await assert.rejects(()=>f.run());assert.equal(f.originals.counts.get,readCount);
});
for(const scenario of ['disabled','binding_alias','wrong_instance','unknown_payload','root_hash','root_metadata','root_indices',
  'root_resource_alias','root_owner','root_window','partition_hash','partition_membership','plan_extra','plan_duplicate','plan_order','plan_window','expired']) {
  test('durable byte audit refuses '+scenario+' before reading case bytes',async()=>{
    const f=await workflowFixture(2);
    if(scenario==='disabled')f.env.operationEnabled='false';
    if(scenario==='binding_alias')f.env.target=f.env.originals;
    if(scenario==='wrong_instance')f.event.instanceId='77777777-7777-4777-8777-777777777777';
    if(scenario==='unknown_payload')f.event.payload.extra=true;
    if(scenario==='root_hash')f.env.operationJson=JSON.stringify({...JSON.parse(f.env.operationJson),rootPlanSha256:'0'.repeat(64)});
    if(scenario==='root_metadata')f.proofs.rows.get(`source-byte-audit/${f.root.auditRootId}/root.json`).customMetadata.rootsha256='0'.repeat(64);
    if(scenario==='root_indices'){f.root.partitions[1].source.partitionIndex=0;f.refreshRoot();}
    if(scenario==='root_resource_alias'){f.root.partitions[1].source.resourceId=f.root.partitions[0].source.resourceId;f.refreshRoot();}
    if(scenario==='root_owner'){f.root.partitions[1].source.ownerId='88888888-8888-4888-8888-888888888888';f.refreshRoot();}
    if(scenario==='root_window'){f.root.completionDeadline=new Date(Date.now()+5*3600000).toISOString();f.refreshRoot();}
    if(scenario==='partition_hash')f.proofs.rows.get('plans/'+f.partitionPlanSha256+'.json').bytes=new TextEncoder().encode('{}');
    if(scenario==='partition_membership')f.plan.records[0].originalVersion='unknown';
    if(scenario==='plan_extra')f.plan.unexpected=true;
    if(scenario==='plan_duplicate')f.plan.records[1]={...f.plan.records[0]};
    if(scenario==='plan_order')f.plan.records.reverse();
    if(scenario==='plan_window')f.plan.notAfter=f.plan.notBefore;
    if(scenario==='expired'){f.plan.notBefore=new Date(Date.now()-25*60000).toISOString();f.plan.notAfter=new Date(Date.now()-60000).toISOString();f.root.notBefore=new Date(Date.now()-26*60000).toISOString();f.refreshRoot();}
    const reads=f.originals.counts.get,puts=[f.originals.counts.put,f.target.counts.put];
    await assert.rejects(()=>f.run());assert.equal(f.originals.counts.get,reads);assert.deepEqual([f.originals.counts.put,f.target.counts.put],puts);
  });
}
for(const name of ['initialize-claim.json','initialize-outcome.json','bytes-0000-outcome.json']) {
  test('lost acknowledgment of '+name+' preserves claim and never replays case reads',async()=>{
    const f=await workflowFixture(),put=f.env.proofs.put.bind(f.env.proofs);let lost=false;
    f.env.proofs.put=async(key,value,options)=>{const saved=await put(key,value,options);if(key===f.prefix+name&&!lost){lost=true;throw Error('private '+f.input.record.originalKey);}return saved;};
    await assert.rejects(()=>f.run(),error=>!error.message.includes(f.input.record.originalKey));
    assert.ok(f.proofs.rows.has(f.prefix+name));const reads=f.originals.counts.get;
    await assert.rejects(()=>f.run());assert.equal(f.originals.counts.get,reads);assert.ok(f.proofs.rows.has(f.prefix+'initialize-claim.json'));
  });
}
test('changed source holds without repair and stores only redacted failure',async()=>{
  const f=await workflowFixture();f.originals.rows.get(f.input.record.originalKey).bytes=new TextEncoder().encode('bad');
  await assert.rejects(()=>f.run(),/byte_audit_held/);
  assert.ok(f.proofs.rows.has(f.prefix+'bytes-0000-claim.json'));assert.ok(f.proofs.rows.has(f.prefix+'bytes-0000-failure.json'));
  assert.equal(f.proofs.rows.has(f.prefix+'bytes-0000-outcome.json'),false);
  const bytes=f.proofs.rows.get(f.prefix+'bytes-0000-failure.json').bytes;
  assert.equal(new TextDecoder().decode(bytes).includes(f.input.record.originalKey),false);
});
test('plan parser refuses oversized input',()=>assert.throws(()=>parseRestorationByteAuditPlan(' '.repeat(768*1024+1))));
function rootUpload(f) {
  const prefix=`source-byte-audit/${f.root.auditRootId}/`;
  for(const name of ['root.json','root-bootstrap-claim.json','root-bootstrap-outcome.json'])f.proofs.rows.delete(prefix+name);
  const event={instanceId:f.root.auditRootId,payload:{rootPlanJson:canonical(f.root)}};
  return{prefix,event,run:()=>runRestorationByteAuditWorkflow(f.env,event,f.step)};
}
test('root uploads once through conditional proof-only writes before ordinary audits',async()=>{
  const f=await workflowFixture(),upload=rootUpload(f),before=f.originals.counts.get,puts=[f.originals.counts.put,f.target.counts.put];
  const result=await upload.run();assert.equal(result.state,'source_byte_audit_root_ready');
  assert.equal(f.originals.counts.get,before);assert.deepEqual([f.originals.counts.put,f.target.counts.put],puts);
  assert.equal(f.proofs.rows.get(upload.prefix+'root.json').version,result.rootVersion);
  await assert.rejects(upload.run);assert.equal(f.originals.counts.get,before);
  assert.equal((await f.run()).records,1);
});
for(const scenario of ['claim_lost_ack','root_lost_ack','outcome_lost_ack','existing_root','wrong_instance','expired','wrong_hash']) {
  test('root upload holds '+scenario+' without overwriting or repeating',async()=>{
    const f=await workflowFixture(),upload=rootUpload(f),put=f.env.proofs.put.bind(f.env.proofs);let lost=false;
    const name={claim_lost_ack:'root-bootstrap-claim.json',root_lost_ack:'root.json',outcome_lost_ack:'root-bootstrap-outcome.json'}[scenario];
    f.env.proofs.put=async(key,value,options)=>{const saved=await put(key,value,options);if(key===upload.prefix+name&&!lost){lost=true;throw Error('synthetic unknown acknowledgment');}return saved;};
    if(scenario==='existing_root')f.proofs.rows.set(upload.prefix+'root.json',{bytes:new TextEncoder().encode('existing immutable proof'),version:'old',etag:'old'});
    if(scenario==='wrong_instance')upload.event.instanceId=f.plan.operationId;
    if(scenario==='expired'){f.root.notBefore=new Date(Date.now()-31*60000).toISOString();upload.event.payload.rootPlanJson=canonical(f.root);f.env.operationJson=JSON.stringify({...JSON.parse(f.env.operationJson),rootPlanSha256:sha(canonical(f.root))});}
    if(scenario==='wrong_hash')f.env.operationJson=JSON.stringify({...JSON.parse(f.env.operationJson),rootPlanSha256:'0'.repeat(64)});
    const reads=f.originals.counts.get;await assert.rejects(upload.run);assert.equal(f.originals.counts.get,reads);
    if(scenario==='existing_root')assert.equal(f.proofs.rows.get(upload.prefix+'root.json').version,'old');
    const count=f.proofs.counts.put;await assert.rejects(upload.run);assert.equal(f.proofs.counts.put,count);
  });
}
for(const scenario of ['missing_claim','missing_outcome','root_version','root_receipt_metadata']) {
  test('ordinary audit refuses '+scenario+' before source reads',async()=>{
    const f=await workflowFixture(),prefix=`source-byte-audit/${f.root.auditRootId}/`;
    if(scenario==='missing_claim')f.proofs.rows.delete(prefix+'root-bootstrap-claim.json');
    if(scenario==='missing_outcome')f.proofs.rows.delete(prefix+'root-bootstrap-outcome.json');
    if(scenario==='root_version')f.proofs.rows.get(prefix+'root.json').version='changed';
    if(scenario==='root_receipt_metadata')f.proofs.rows.get(prefix+'root-bootstrap-outcome.json').customMetadata.rootsha256='0'.repeat(64);
    const reads=f.originals.counts.get;await assert.rejects(()=>f.run());assert.equal(f.originals.counts.get,reads);
  });
}
