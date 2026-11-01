import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {loadTs} from './helpers/load-ts.mjs';

const {auditRestorationSourceBytes,auditHeldRestorationSourceBytes} = loadTs('src/services/restoration-source-byte-audit.ts');
const {copyVerifiedRestorationSource} = loadTs('src/services/restoration-source.ts');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const source = {ownerId:'11111111-1111-4111-8111-111111111111',
  resourceId:'22222222-2222-4222-8222-222222222222', partitionIndex:3,
  planSha256:'a'.repeat(64), resourcePlanSha256:'b'.repeat(64),
  sourceManifestSha256:'c'.repeat(64), catalogSha256:'d'.repeat(64)};

async function fixture(bytes = new TextEncoder().encode('Synthetic source for a byte audit.\n')) {
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

async function heldFixture(bytes=new Uint8Array([0xff,0xc0,0x80,0x00,0x41])) {
  const f=await fixture(bytes),row=f.input.record,stored=f.proofs.rows.get(f.receiptKey),
    proof=JSON.parse(new TextDecoder().decode(stored.bytes));
  proof.generationId='initial';proof.disposition='held_unreadable_original_preserved';
  proof.currentSourceKey=row.copyKey;proof.currentSourceVersion=row.copyVersion;proof.currentSourceEtag=row.copyEtag;
  stored.bytes=new TextEncoder().encode(JSON.stringify(proof));stored.etag=sha(stored.bytes);
  stored.customMetadata.proofsha256=sha(stored.bytes);row.receiptGenerationId='initial';row.receiptSha256=sha(stored.bytes);
  f.proofs.rows.delete(f.receiptKey);f.receiptKey=`restoration/${source.ownerId}/${source.planSha256}/${source.partitionIndex}/${sha(row.documentId)}/initial.json`;
  f.proofs.rows.set(f.receiptKey,stored);f.target.rows.delete(row.currentKey);
  delete row.currentKey;delete row.currentVersion;delete row.currentEtag;
  return f;
}

test('held byte audit preserves invalid UTF-8 without decoding, parsed text, writes or publication credit',async()=>{
  const f=await heldFixture(),before=[f.originals.counts.put,f.target.counts.put,f.proofs.counts.put],
    bytes=Uint8Array.from(f.originals.rows.get(f.input.record.originalKey).bytes),result=await auditHeldRestorationSourceBytes(f.input);
  assert.equal(result.state,'held_original_copy_bytes_verified');
  for(const name of ['parsedTextVerified','searchPublication','humanApproval','databaseBookendsVerified','fullIntegrityOrPublicationApproval'])
    assert.equal(result[name],false);
  assert.deepEqual(f.originals.rows.get(f.input.record.originalKey).bytes,bytes);
  assert.deepEqual([f.originals.counts.put,f.target.counts.put,f.proofs.counts.put],before);
  assert.equal(JSON.stringify(result).includes(f.input.record.documentId),false);
  assert.equal(JSON.stringify(result).includes(f.input.record.originalKey),false);
});

for(const scenario of ['processed_receipt','approved','generation','current_key','current_version','current_etag',
  'copy_owner','copy_version','original_version','original_bytes','receipt_metadata','receipt_hash','receipt_revision',
  'unknown_owner','fabricated_current','empty_original','permission','binding_alias']){
  test('held byte audit rejects '+scenario+' without repair or retry',async()=>{
    const f=await heldFixture(),row=f.input.record,stored=f.proofs.rows.get(f.receiptKey),
      proof=JSON.parse(new TextDecoder().decode(stored.bytes));
    if(scenario==='processed_receipt')proof.disposition='processed_unpublished_unreviewed';
    if(scenario==='approved')proof.humanApproval=true;
    if(scenario==='generation')proof.generationId='invented-generation';
    if(scenario==='current_key')proof.currentSourceKey='fabricated/parsed.md';
    if(scenario==='current_version')proof.currentSourceVersion='unknown';
    if(scenario==='current_etag')proof.currentSourceEtag='unknown';
    if(scenario==='copy_owner')f.target.rows.get(row.copyKey).customMetadata.ownerid='unknown';
    if(scenario==='copy_version')f.target.rows.get(row.copyKey).version='unknown';
    if(scenario==='original_version')f.originals.rows.get(row.originalKey).version='unknown';
    if(scenario==='original_bytes')f.originals.rows.get(row.originalKey).bytes=new Uint8Array([1,2,3,4,5]);
    if(scenario==='receipt_revision')proof.documentRevision++;
    stored.bytes=new TextEncoder().encode(JSON.stringify(proof));row.receiptSha256=sha(stored.bytes);
    stored.customMetadata.proofsha256=row.receiptSha256;
    if(scenario==='receipt_metadata')stored.customMetadata.ownerid='unknown';
    if(scenario==='receipt_hash')row.receiptSha256='0'.repeat(64);
    if(scenario==='unknown_owner')f.input.source.ownerId='44444444-4444-4444-8444-444444444444';
    if(scenario==='fabricated_current')row.currentKey='fabricated/parsed.md';
    if(scenario==='empty_original')f.originals.rows.get(row.originalKey).bytes=new Uint8Array();
    if(scenario==='permission')f.input.originals={async get(){throw Error('403 '+row.originalKey);}};
    if(scenario==='binding_alias')f.input.target=f.input.originals;
    const before=[f.originals.counts.put,f.target.counts.put,f.proofs.counts.put];
    await assert.rejects(()=>auditHeldRestorationSourceBytes(f.input),error=>!error.message.includes(row.originalKey));
    assert.deepEqual([f.originals.counts.put,f.target.counts.put,f.proofs.counts.put],before);
    assert.ok(f.originals.counts.get<=2);assert.ok(f.target.counts.get<=3);assert.ok(f.proofs.counts.get<=1);
  });
}

test('independent byte audit reads original, owned copy, current source and exact receipt without writes',async()=>{
  const f=await fixture(),before=[f.originals.counts.put,f.target.counts.put,f.proofs.counts.put];
  const result=await auditRestorationSourceBytes(f.input);
  assert.equal(result.state,'original_copy_current_bytes_verified');
  assert.equal(result.documentSha256,sha(f.input.record.documentId));
  assert.equal(result.databaseBookendsVerified,false);
  assert.equal(result.fullIntegrityOrPublicationApproval,false);
  assert.deepEqual([f.originals.counts.put,f.target.counts.put,f.proofs.counts.put],before);
  assert.equal(JSON.stringify(result).includes(f.input.record.documentId),false);
  assert.equal(JSON.stringify(result).includes(f.descriptor.originalKey),false);
});

for (const scenario of ['original_bytes','original_version','copy_bytes','copy_owner','copy_version',
  'receipt_bytes','receipt_owner','receipt_identity','current_bytes','current_version','current_etag',
  'missing_current','oversized_current','invalid_record','binding_alias','permission']) {
  test('byte audit holds '+scenario+' without repair, write or retry',async()=>{
    const f=await fixture(),r=f.input.record;
    const changed = new TextEncoder().encode('different bytes');
    if(scenario==='original_bytes')f.originals.rows.get(r.originalKey).bytes=changed;
    if(scenario==='original_version')f.originals.rows.get(r.originalKey).version='changed';
    if(scenario==='copy_bytes')f.target.rows.get(r.copyKey).bytes=changed;
    if(scenario==='copy_owner')f.target.rows.get(r.copyKey).customMetadata.ownerid='unknown';
    if(scenario==='copy_version')f.target.rows.get(r.copyKey).version='changed';
    if(scenario==='receipt_bytes')f.proofs.rows.get(f.receiptKey).bytes=changed;
    if(scenario==='receipt_owner')f.proofs.rows.get(f.receiptKey).customMetadata.ownerid='unknown';
    if(scenario==='receipt_identity')r.claimToken='44444444-4444-4444-8444-444444444444';
    if(scenario==='current_bytes')f.target.rows.get(r.currentKey).bytes=changed;
    if(scenario==='current_version')f.target.rows.get(r.currentKey).version='changed';
    if(scenario==='current_etag')f.target.rows.get(r.currentKey).etag='changed';
    if(scenario==='missing_current')f.target.rows.delete(r.currentKey);
    if(scenario==='oversized_current')f.target.rows.get(r.currentKey).reportedSize=1000001;
    if(scenario==='invalid_record')r.receiptGenerationId='../unexpected';
    if(scenario==='binding_alias')f.input.target=f.input.originals;
    if(scenario==='permission')f.input.originals={async get(){throw Error('403 '+r.originalKey);}};
    const puts=[f.originals.counts.put,f.target.counts.put,f.proofs.counts.put];
    await assert.rejects(()=>auditRestorationSourceBytes(f.input),error=>{
      assert.equal(error.message.includes(r.originalKey),false);return true;
    });
    assert.deepEqual([f.originals.counts.put,f.target.counts.put,f.proofs.counts.put],puts);
    // Each role is read at most once, except target which has copy and current.
    // Setup already read original once and target once for its initial copy.
    assert.ok(f.originals.counts.get<=2);
    assert.ok(f.target.counts.get<=4);
    assert.ok(f.proofs.counts.get<=1);
  });
}
