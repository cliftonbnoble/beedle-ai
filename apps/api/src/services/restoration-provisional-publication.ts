import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {inspectRestorationOwner,RestorationControlError,type RestorationIdentity} from './restoration-job-control';
import {restorationSeedMetadataSha256} from './restoration-source-job';
import {readyDocumentSql} from './publication-policy';

export interface RestorationPublicationRecord {
  documentId:string;documentRevision:number;generationId:string;metadataSha256:string;sourceKey:string;sourceSha256:string;sourceBytes:number;
}
// A controller must own a distinct immutable attempt before calling this
// non-idempotent batch. A lost acknowledgment permits observation, never replay.
export async function publishRestorationProvisionalBatch(db:D1Database,identity:RestorationIdentity,
  policy:'unreviewed-provisional-v1',records:RestorationPublicationRecord[]){
  const hash=/^[a-f0-9]{64}$/;
  if(policy!=='unreviewed-provisional-v1'||!Array.isArray(records)||records.length<1||records.length>25||
    new Set(records.map(row=>row.documentId)).size!==records.length||records.some(row=>!row.documentId||row.documentId.length>200||
      !Number.isSafeInteger(row.documentRevision)||row.documentRevision<1||!row.generationId||row.generationId==='initial'||
      !row.sourceKey||new TextEncoder().encode(row.sourceKey).length>1024||/[\u0000-\u001f\u007f]/.test(row.sourceKey)||
      !hash.test(row.metadataSha256)||!hash.test(row.sourceSha256)||!Number.isSafeInteger(row.sourceBytes)||row.sourceBytes<1||row.sourceBytes>1000000))
    throw new RestorationControlError('restoration_provisional_batch_plan');
  const owner=await inspectRestorationOwner(db,identity);
  if(owner.phase!=='indexing')throw new RestorationControlError('restoration_provisional_owner_phase');
  for(const record of records){
    const current=await restorationSeedMetadataSha256(db,record.documentId),row=current.row;
    if(current.sha256!==record.metadataSha256||row.document_revision!==record.documentRevision||row.active_text_generation!==record.generationId||
      row.source_r2_key!==record.sourceKey||row.ingestion_state!=='ready'||row.searchable_at!==null||row.published_revision!==null||row.approved_at!==null||
      row.approved_revision!==null||row.qc_required_confirmed!==0)
      throw new RestorationControlError('restoration_provisional_document_changed');
    const source=await db.prepare("SELECT j.source_verified,j.phase,j.extraction_policy,j.source_sha256,j.source_bytes,(SELECT COUNT(*) FROM beedle_restore_source_results s WHERE s.document_id=j.document_id AND s.claim_token=j.claim_token AND s.owner_id=? AND s.plan_sha256=? AND s.source_sha256=j.source_sha256 AND s.source_bytes=j.source_bytes) receipts,(SELECT COUNT(*) FROM document_generations g WHERE g.id=? AND g.document_id=j.document_id AND g.status='published' AND g.source_sha256=j.source_sha256 AND g.source_key=?) currentGeneration FROM beedle_restore_jobs j WHERE j.document_id=?")
      .bind(identity.ownerId,identity.planSha256,record.generationId,record.sourceKey,record.documentId)
      .first<{source_verified:number;phase:string;extraction_policy:string;source_sha256:string;source_bytes:number;receipts:number;currentGeneration:number}>();
    if(!source||source.phase!=='processed'||source.source_verified!==1||source.extraction_policy==='held-unreadable'||
      source.source_sha256!==record.sourceSha256||source.source_bytes!==record.sourceBytes||source.receipts!==1||source.currentGeneration!==1)
      throw new RestorationControlError('restoration_provisional_source_not_verified');
  }
  const planned=JSON.stringify(records),now=new Date().toISOString();
  // The count guard is evaluated for the entire plan by one UPDATE. A changed
  // record prevents every publication, rather than partially passing a batch.
  const eligible=(alias:string)=>`${readyDocumentSql(alias)} AND ${alias}.searchable_at IS NULL AND ${alias}.published_revision IS NULL
    AND ${alias}.approved_at IS NULL AND ${alias}.approved_revision IS NULL AND ${alias}.qc_required_confirmed=0
    AND ${alias}.document_revision=json_extract(p.value,'$.documentRevision') AND ${alias}.active_text_generation=json_extract(p.value,'$.generationId')
    AND ${alias}.source_r2_key=json_extract(p.value,'$.sourceKey')`;
  const result=await db.prepare(`UPDATE documents SET searchable_at=?,published_revision=document_revision,updated_at=?
    WHERE id IN (SELECT json_extract(value,'$.documentId') FROM json_each(?))
    AND (SELECT COUNT(*) FROM json_each(?) p JOIN documents d ON d.id=json_extract(p.value,'$.documentId') WHERE ${eligible('d')})=?
    AND EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='indexing')
    AND NOT EXISTS(SELECT 1 FROM documents WHERE approved_at IS NOT NULL OR approved_revision IS NOT NULL OR qc_required_confirmed!=0)
    RETURNING id,document_revision,active_text_generation,approved_at,approved_revision,qc_required_confirmed`)
    .bind(now,now,planned,planned,records.length,identity.ownerId,identity.planSha256)
    .all<{id:string;document_revision:number;active_text_generation:string;approved_at:null;approved_revision:null;qc_required_confirmed:number}>();
  if(result.success!==true||result.results.length!==records.length||result.results.some(row=>row.approved_at!==null||row.approved_revision!==null||row.qc_required_confirmed!==0)||
    new Set(result.results.map(row=>row.id)).size!==records.length)
    throw new RestorationControlError('restoration_provisional_batch_unacknowledged');
  return {state:'provisional_published' as const,policy,published:result.results.length,recordPlanSha256:await fingerprint(records),
    resultSha256:await fingerprint(result.results.slice().sort((a,b)=>a.id.localeCompare(b.id))),humanApproval:false,
    metadataConfirmation:false,provenanceOrLegalEditionConfirmed:false,releaseOrIntegrityApproval:false};
}
