import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {sha256Bytes} from '../lib/digests';
import {copyVerifiedRestorationSource} from './restoration-source';
import {restorationReprocessEnv} from './restoration-capabilities';
import {reprocessIngestionDocument} from './admin-ingestion';
import {claimRestorationJob,RestorationControlError,type RestorationIdentity,type RestorationPlannedJob} from './restoration-job-control';

export interface RestorationSourceJob extends RestorationPlannedJob {
  initialMetadataSha256:string;initialRevision:number;originalVersion?:string;
}
export const restorationSourceResultSql=[
  `CREATE TABLE beedle_restore_source_results(document_id TEXT NOT NULL,generation_id TEXT NOT NULL,owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,
    claim_token TEXT NOT NULL,document_revision INTEGER NOT NULL,source_sha256 TEXT NOT NULL,source_bytes INTEGER NOT NULL,
    proof_key TEXT NOT NULL,proof_sha256 TEXT NOT NULL,proof_json TEXT NOT NULL,recorded_at TEXT NOT NULL,PRIMARY KEY(document_id,generation_id))`,
  `CREATE TRIGGER beedle_restore_source_result_update BEFORE UPDATE ON beedle_restore_source_results BEGIN SELECT RAISE(ABORT,'restoration_source_proof_immutable'); END`,
  `CREATE TRIGGER beedle_restore_source_result_delete BEFORE DELETE ON beedle_restore_source_results BEGIN SELECT RAISE(ABORT,'restoration_source_proof_immutable'); END`
] as const;
interface SeedDocument extends Record<string,unknown> {
  id:string;document_revision:number;source_r2_key:string;ingestion_state:string;active_text_generation:string;
  approved_at:string|null;approved_revision:number|null;qc_required_confirmed:number;searchable_at:string|null;
}
export async function restorationSeedMetadataSha256(db:D1Database,documentId:string){
  const row=await db.prepare('SELECT * FROM documents WHERE id=?').bind(documentId).first<SeedDocument>();
  if(!row)throw new RestorationControlError('source_job_document_missing');
  return{row,sha256:await fingerprint(row)};
}
// One previously unclaimed job only. A partial/unknown outcome is deliberately
// left at its actual durable phase for the controller to hold and reconcile.
// Neither this function nor a later tick resets a claim or reprocesses it.
export async function runOwnedRestorationSourceJob(input:{
  db:D1Database;originals:Pick<R2Bucket,'get'>;target:R2Bucket;proofs:Pick<R2Bucket,'get'|'put'>;
  identity:RestorationIdentity;tickToken:string;job:RestorationSourceJob;
  profile:Parameters<typeof restorationReprocessEnv>[2];
}){
  const{db,originals,target,proofs,identity,tickToken,job}=input;
  if(originals===target||originals===proofs||target===proofs||!/^[a-f0-9]{64}$/.test(job.initialMetadataSha256)||
    !Number.isSafeInteger(job.initialRevision)||job.initialRevision<1)throw new RestorationControlError('invalid_source_job_plan');
  const initial=await restorationSeedMetadataSha256(db,job.documentId),row=initial.row;
  if(initial.sha256!==job.initialMetadataSha256||row.document_revision!==job.initialRevision||row.source_r2_key!==job.originalKey||row.ingestion_state!=='legacy'||
    row.active_text_generation!=='initial'||row.approved_at!==null||row.approved_revision!==null||row.qc_required_confirmed!==0||row.searchable_at!==null)
    throw new RestorationControlError('source_job_seed_changed');
  const artifacts=await db.prepare(`SELECT (SELECT COUNT(*) FROM document_chunks WHERE document_id=?) chunks,
    (SELECT COUNT(*) FROM section_paragraphs p JOIN document_sections s ON s.id=p.section_id WHERE s.document_id=?) paragraphs,(SELECT COUNT(*) FROM document_generations WHERE document_id=?) generations`)
    .bind(job.documentId,job.documentId,job.documentId).first<{chunks:number;paragraphs:number;generations:number}>();
  if(!artifacts||artifacts.chunks!==0||artifacts.paragraphs!==0||artifacts.generations!==0)throw new RestorationControlError('source_job_initial_artifacts_changed');
  const claim=await claimRestorationJob(db,identity,tickToken,job);if(!claim)return{state:'not_claimed'};
  const copy=await copyVerifiedRestorationSource(originals,target,{ownerId:identity.ownerId,planSha256:identity.planSha256,
    partitionIndex:identity.partitionIndex,originalKey:job.originalKey,sourceSha256:job.sourceSha256,sourceBytes:job.sourceBytes,originalVersion:job.originalVersion});
  const copied=await db.prepare(`UPDATE beedle_restore_jobs SET phase='source_copied',source_copy_key=? WHERE document_id=? AND claim_token=? AND phase='claimed' RETURNING document_id`)
    .bind(copy.targetKey,job.documentId,claim.token).all<{document_id:string}>();
  if(copied.results.length!==1)throw new RestorationControlError('source_copy_checkpoint_unacknowledged');
  const pointer=await db.prepare(`UPDATE documents SET source_r2_key=? WHERE id=? AND document_revision=? AND source_r2_key=? AND approved_at IS NULL AND approved_revision IS NULL AND qc_required_confirmed=0 AND active_text_generation='initial' RETURNING id`)
    .bind(copy.targetKey,job.documentId,job.initialRevision,job.originalKey).all<{id:string}>();
  if(pointer.results.length!==1)throw new RestorationControlError('source_pointer_checkpoint_unacknowledged');
  const seeded=await db.prepare("UPDATE beedle_restore_jobs SET phase='seeded' WHERE document_id=? AND claim_token=? AND phase='source_copied' RETURNING document_id")
    .bind(job.documentId,claim.token).all<{document_id:string}>();
  if(seeded.results.length!==1)throw new RestorationControlError('source_seed_checkpoint_unacknowledged');
  const before=await restorationSeedMetadataSha256(db,job.documentId);
  if(job.extractionPolicy!=='held-unreadable'){
    const result=await reprocessIngestionDocument(restorationReprocessEnv(db,target,input.profile),job.documentId,{policy:job.extractionPolicy,
      documentId:job.documentId,expectedRevision:before.row.document_revision,sourceSha256:job.sourceSha256,planSha256:identity.planSha256});
    if(!result?.reprocessArtifacts.rebuilt)throw new RestorationControlError('source_generation_not_rebuilt');
  }
  const current=(await restorationSeedMetadataSha256(db,job.documentId)).row;
  if(current.approved_at!==null||current.approved_revision!==null||current.qc_required_confirmed!==0||current.searchable_at!==null||
    (job.extractionPolicy==='held-unreadable'?(current.ingestion_state!=='legacy'||current.active_text_generation!=='initial'):
      (current.ingestion_state!=='ready'||current.active_text_generation==='initial')))throw new RestorationControlError('source_generation_or_review_state_changed');
  const original=await target.get(current.source_r2_key);
  if(!original||original.size!==job.sourceBytes||!original.version||!original.etag||await sha256Bytes(new Uint8Array(await original.arrayBuffer()))!==job.sourceSha256)
    throw new RestorationControlError('current_original_source_verification_failed');
  const generationId=current.active_text_generation;
  const outcome={version:1,ownerId:identity.ownerId,planSha256:identity.planSha256,partitionIndex:identity.partitionIndex,resourceId:identity.resourceId,
    documentId:job.documentId,claimToken:claim.token,copy,sourceSha256:job.sourceSha256,sourceBytes:job.sourceBytes,generationId,
    documentRevision:current.document_revision,currentSourceKey:current.source_r2_key,currentSourceVersion:original.version,currentSourceEtag:original.etag,
    disposition:job.extractionPolicy==='held-unreadable'?'held_unreadable_original_preserved':'processed_unpublished_unreviewed',humanApproval:false};
  const text=canonical(outcome),proofSha256=await sha256Bytes(new TextEncoder().encode(text)),documentHash=await sha256Bytes(new TextEncoder().encode(job.documentId));
  const proofKey=`restoration/${identity.ownerId}/${identity.planSha256}/${identity.partitionIndex}/${documentHash}/${generationId}.json`;
  const written=await proofs.put(proofKey,text,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'},
    customMetadata:{ownerid:identity.ownerId,plansha256:identity.planSha256,proofsha256:proofSha256}});
  if(!written)throw new RestorationControlError('source_result_conditional_conflict');
  const stored=await proofs.get(proofKey);
  if(!stored||stored.size!==new TextEncoder().encode(text).byteLength||stored.version!==written.version||stored.etag!==written.etag||
    stored.customMetadata?.ownerid!==identity.ownerId||stored.customMetadata.plansha256!==identity.planSha256||stored.customMetadata.proofsha256!==proofSha256||
    await sha256Bytes(new Uint8Array(await stored.arrayBuffer()))!==proofSha256)throw new RestorationControlError('source_result_readback_unconfirmed');
  const now=new Date().toISOString(),phase=job.extractionPolicy==='held-unreadable'?'held':'processed';
  const final=await db.batch([
    db.prepare(`INSERT INTO beedle_restore_source_results(document_id,generation_id,owner_id,plan_sha256,claim_token,document_revision,source_sha256,source_bytes,proof_key,proof_sha256,proof_json,recorded_at)
      SELECT ?,?,?,?,?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE document_id=? AND claim_token=? AND phase='seeded')`)
      .bind(job.documentId,generationId,identity.ownerId,identity.planSha256,claim.token,current.document_revision,job.sourceSha256,job.sourceBytes,proofKey,proofSha256,text,now,job.documentId,claim.token),
    db.prepare(`UPDATE beedle_restore_jobs SET phase=?,generation_id=?,source_verified=1,completed_at=? WHERE document_id=? AND claim_token=? AND phase='seeded'
      AND EXISTS(SELECT 1 FROM beedle_restore_source_results WHERE document_id=? AND generation_id=? AND proof_sha256=?)`)
      .bind(phase,job.extractionPolicy==='held-unreadable'?null:current.active_text_generation,now,job.documentId,claim.token,job.documentId,generationId,proofSha256)
  ]);
  if(final.length!==2||final.some(result=>result.success!==true||Number(result.meta.changes)!==1))throw new RestorationControlError('source_job_finalization_unacknowledged');
  return{state:phase,proofSha256,generationId};
}
