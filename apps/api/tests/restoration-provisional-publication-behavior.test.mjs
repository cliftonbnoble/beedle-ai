import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {restorationSourceJobFixture} from './helpers/restoration-source-job-fixture.mjs';
import {successorJobFixture} from './helpers/restoration-source-successor-job-fixture.mjs';
const {advanceRestorationSourceTick}=loadTs('src/restoration-source-worker.ts'),{restorationSeedMetadataSha256}=loadTs('src/services/restoration-source-job.ts');
const {publishRestorationProvisionalBatch:publish}=loadTs('src/services/restoration-provisional-publication.ts');
const {advanceRestorationSourceSuccessorTick}=loadTs('src/restoration-source-successor-worker.ts');
const {restorationSchemaFingerprint}=await import('../scripts/lib/owned-restoration-resources.mjs');
async function setup(policy){
  const x=await restorationSourceJobFixture(policy,{idle:true,partitionPlan:true});
  const spec={version:1,identity:x.identity,partitionPlanSha256:x.partitionPlanSha256,
    schemaSha256:await restorationSchemaFingerprint(x.sqlite.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all()),
    originalBucket:'beedle-sources-migration-20261027',targetBucket:'beedle-restored-11111111',proofBucket:'beedle-restoration-proof-11111111',profile:x.profile};
  await advanceRestorationSourceTick({DB:x.db,ORIGINALS:x.originals,TARGET_ORIGINALS:x.target,PROOFS:x.proofs,RESTORE_ENABLED:'true',RESTORE_SPEC_JSON:JSON.stringify(spec)});
  const current=await restorationSeedMetadataSha256(x.db,x.job.documentId),record={documentId:x.job.documentId,documentRevision:current.row.document_revision,
    generationId:current.row.active_text_generation,metadataSha256:current.sha256,sourceKey:current.row.source_r2_key,sourceSha256:x.job.sourceSha256,sourceBytes:x.job.sourceBytes};
  return {...x,record};
}
test('verified ready sources can be explicitly published as provisional without human approval or metadata confirmation',async()=>{
  const x=await setup();try{
    const revision=x.record.documentRevision,result=await publish(x.db,x.identity,'unreviewed-provisional-v1',[x.record]);
    assert.equal(result.published,1);assert.equal(result.humanApproval,false);assert.equal(result.metadataConfirmation,false);
    const row=x.sqlite.prepare('SELECT * FROM documents').get();assert(row.searchable_at);assert.equal(row.published_revision,revision);assert.equal(row.document_revision,revision);
    assert.equal(row.approved_at,null);assert.equal(row.approved_revision,null);assert.equal(row.qc_required_confirmed,0);
    assert.equal(row.approval_policy_version,null);assert.equal(result.provenanceOrLegalEditionConfirmed,false);
    await assert.rejects(publish(x.db,x.identity,'unreviewed-provisional-v1',[x.record]),/document_changed/);
  }finally{x.sqlite.close();}
});
test('unreadable, wrong-source and changed-revision plans remain unpublished',async()=>{
  for(const mode of ['unreadable','source','revision']){const x=await setup(mode==='unreadable'?'held-unreadable':undefined);try{
    if(mode==='source')x.record.sourceSha256='0'.repeat(64);if(mode==='revision')x.record.documentRevision++;
    await assert.rejects(publish(x.db,x.identity,'unreviewed-provisional-v1',[x.record]));
    assert.equal(x.sqlite.prepare('SELECT searchable_at FROM documents').get().searchable_at,null);
    assert.equal(x.sqlite.prepare('SELECT approved_at FROM documents').get().approved_at,null);
  }finally{x.sqlite.close();}}
});
test('a changed row immediately before the one-statement update prevents publication',async()=>{
  const x=await setup();try{
    const prepare=x.db.prepare;x.db.prepare=sql=>{if(sql.startsWith('UPDATE documents SET searchable_at='))x.sqlite.exec("UPDATE documents SET ingestion_state='legacy'");return prepare(sql);};
    await assert.rejects(publish(x.db,x.identity,'unreviewed-provisional-v1',[x.record]),/batch_unacknowledged/);
    assert.equal(x.sqlite.prepare('SELECT searchable_at FROM documents').get().searchable_at,null);
  }finally{x.sqlite.close();}
});
test('a lost publication acknowledgment cannot be retried as a fresh publication',async()=>{
  const x=await setup();try{
    const prepare=x.db.prepare;let updates=0;x.db.prepare=sql=>{const row=prepare(sql);if(sql.startsWith('UPDATE documents SET searchable_at=')){const all=row.all;row.all=async function(){updates++;await all.call(this);throw Error('Lost publication response');};}return row;};
    await assert.rejects(publish(x.db,x.identity,'unreviewed-provisional-v1',[x.record]),/Lost publication/);
    await assert.rejects(publish(x.db,x.identity,'unreviewed-provisional-v1',[x.record]),/document_changed/);assert.equal(updates,1);
    assert.equal(x.sqlite.prepare('SELECT approved_at FROM documents').get().approved_at,null);
  }finally{x.sqlite.close();}
});
test('one changed document blocks the entire multi-document publication statement',async()=>{
  const x=await successorJobFixture();try{
    await advanceRestorationSourceSuccessorTick(x.env);const records=[];
    for(const id of [x.first,x.next]){const current=await restorationSeedMetadataSha256(x.db,id),job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(id);
      records.push({documentId:id,documentRevision:current.row.document_revision,generationId:current.row.active_text_generation,
        metadataSha256:current.sha256,sourceKey:current.row.source_r2_key,sourceSha256:job.source_sha256,sourceBytes:job.source_bytes});}
    const prepare=x.db.prepare;x.db.prepare=sql=>{if(sql.startsWith('UPDATE documents SET searchable_at='))x.sqlite.prepare("UPDATE documents SET source_r2_key='changed-source.md' WHERE id=?").run(x.next);return prepare(sql);};
    await assert.rejects(publish(x.db,x.identity,'unreviewed-provisional-v1',records),/batch_unacknowledged/);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE searchable_at IS NOT NULL').get().n,0);
    assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM documents WHERE approved_at IS NOT NULL').get().n,0);
  }finally{x.sqlite.close();}
});
