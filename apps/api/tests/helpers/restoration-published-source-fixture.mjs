import assert from 'node:assert/strict';
import {loadTs} from './load-ts.mjs';
import {restorationSourceJobFixture} from './restoration-source-job-fixture.mjs';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
const {runOwnedRestorationSourceJob,restorationSeedMetadataSha256}=loadTs('src/services/restoration-source-job.ts');
const {holdRestorationTick}=loadTs('src/services/restoration-job-control.ts');
export async function publishedSourceReceiptFixture(){
  const x=await restorationSourceJobFixture(),put=x.proofs.put;
  x.proofs.put=async()=>{throw Error('Native planned interruption before source receipt put');};
  await assert.rejects(runOwnedRestorationSourceJob(x),/Native planned interruption/);x.proofs.put=put;
  await holdRestorationTick(x.db,x.identity,x.tickToken,await fingerprint('native interrupted receipt'));
  const old={owner:x.sqlite.prepare('SELECT * FROM beedle_restore_owner').get(),tick:x.sqlite.prepare('SELECT * FROM beedle_restore_tick').get(),
    history:x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history').get(),job:x.sqlite.prepare('SELECT * FROM beedle_restore_jobs').get(),
    generations:x.sqlite.prepare('SELECT * FROM document_generations ORDER BY id').all(),document:x.sqlite.prepare('SELECT * FROM documents').get()};
  const current=await restorationSeedMetadataSha256(x.db,x.job.documentId),copy=await x.target.get(old.job.source_copy_key),source=await x.target.get(current.row.source_r2_key),now=Date.now();
  const plan={version:1,operationId:'33333333-3333-4333-8333-333333333333',identity:x.identity,
    notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+20*60000).toISOString(),
    schemaSha256:await restorationSchemaFingerprint(x.sqlite.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all()),
    documentId:x.job.documentId,documentRevision:current.row.document_revision,generationId:current.row.active_text_generation,
    ownerSha256:await fingerprint(old.owner),tickSha256:await fingerprint(old.tick),historySha256:await fingerprint(old.history),jobSha256:await fingerprint(old.job),
    currentMetadataSha256:current.sha256,generationsSha256:await fingerprint(old.generations),expectedSourceResults:0,
    source:{sourceSha256:x.job.sourceSha256,sourceBytes:x.job.sourceBytes,originalKey:x.job.originalKey,originalVersion:'original-v1',copyEtag:copy.etag,currentEtag:source.etag}};
  return {...x,old,plan,bindings:{db:x.db,originals:x.originals,target:x.target,proofs:x.proofs}};
}
