import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {sha256Bytes} from '../lib/digests';
import {reprocessIngestionDocument} from './admin-ingestion';
import {restorationReprocessEnv} from './restoration-capabilities';
import {inspectRestorationOwner} from './restoration-job-control';
import {restorationSeedMetadataSha256} from './restoration-source-job';
import {observeVerifiedRestorationSource,restorationSourceObjectKey} from './restoration-source';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationErrorEvidence} from './restoration-failure-proof';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid();
const identitySchema=z.object({ownerId:uuid,resourceId:uuid,partitionIndex:z.number().int().min(0).max(31),
  planSha256:hash,resourcePlanSha256:hash,sourceManifestSha256:hash,catalogSha256:hash}).strict();
const privateKey=z.string().min(1).max(1024).refine(value=>new TextEncoder().encode(value).length<=1024&&!/[\u0000-\u001f\u007f]/.test(value));
const planSchema=z.object({version:z.literal(1),operationId:uuid,identity:identitySchema,
  notBefore:z.string().datetime(),notAfter:z.string().datetime(),schemaSha256:hash,
  documentId:z.string().min(1).max(200),expectedRevision:z.number().int().min(1),expectedResultRevision:z.number().int().min(2),
  documentSha256:hash,jobSha256:hash,ownerSha256:hash,tickSha256:hash,historySha256:hash,
  failedGenerationId:uuid,failedGenerationSha256:hash,
  source:z.object({sourceSha256:hash,sourceBytes:z.number().int().min(1).max(1000000),originalKey:privateKey,
    originalVersion:z.string().min(1).max(1024),copyEtag:z.string().min(1).max(1024)}).strict(),
  policy:z.enum(['verified-source-restoration-v1','recovered-markdown-v1']),
  profile:z.object({AI_EMBEDDING_MODEL:z.string().min(1).max(200),VECTOR_NAMESPACE:z.string().regex(/^[A-Za-z0-9_-]{1,128}$/),AI_EMBEDDING_DIMENSIONS:z.string().regex(/^[1-9]\d{0,3}$/).refine(value=>Number(value)<=4096)}).strict()
}).strict();
export type SelectedRestorationReprocessPlan=z.infer<typeof planSchema>;
type Buckets={originals:Pick<R2Bucket,'get'>;target:R2Bucket;proofs:Pick<R2Bucket,'get'|'put'>};

// A separate, explicitly planned single-record operation. It never calls the
// old source-job runner, advances its receipts, releases its tick or resumes it.
export async function runSelectedRestorationReprocess(planInput:unknown,bindings:{db:D1Database}&Buckets,now=Date.now()){
  const plan=planSchema.parse(planInput),begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter);
  if(end<=begin||end-begin>30*60000||now<begin||now>=end)throw Error('selected_reprocess_admission_window');
  if(plan.expectedResultRevision<=plan.expectedRevision)throw Error('selected_reprocess_revision_plan');
  if(bindings.originals===bindings.target||bindings.originals===bindings.proofs||bindings.target===bindings.proofs)throw Error('selected_reprocess_binding_alias');
  const planSha256=await fingerprint(plan),prefix=`selected-reprocess/${plan.operationId}/`,metadata={operationid:plan.operationId,plansha256:planSha256};
  async function exactRead(key:string,text:string,written?:R2Object){
    const object=await bindings.proofs.get(key);
    if(!object||!object.version||!object.etag||object.size!==new TextEncoder().encode(text).length||
      object.customMetadata?.operationid!==plan.operationId||object.customMetadata.plansha256!==planSha256||
      written&&(written.version!==object.version||written.etag!==object.etag)||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await object.arrayBuffer())!==text)throw Error('selected_reprocess_proof_readback');
    return {version:object.version,etag:object.etag};
  }
  async function put(key:string,value:unknown){
    const text=canonical(value),written=await bindings.proofs.put(key,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('selected_reprocess_conditional_conflict');
    return exactRead(key,text,written);
  }
  const claim={version:1,operationId:plan.operationId,planSha256,identity:plan.identity,purpose:'single_selected_reprocess_old_source_execution_stays_held'};
  const existing=await bindings.proofs.get(prefix+'claim.json');
  if(existing){await exactRead(prefix+'claim.json',canonical(claim));return{state:'already_claimed' as const,sqlReissued:false};}
  // A lost claim acknowledgment is terminal for automatic execution: a later
  // delivery only observes this immutable claim and cannot reissue any SQL.
  await put(prefix+'claim.json',claim);
  const meter=restorationBindingBudget(bindings.db),db=meter.db;
  let stage:'preflight'|'source'|'bookend'|'outcome'='preflight';
  const one=async(sql:string,values:unknown[]=[])=>{const rows=(await db.prepare(sql).bind(...values).all<Record<string,unknown>>()).results;const row=rows[0];if(rows.length!==1||!row)throw Error('selected_reprocess_expected_one_row');return row;};
  async function oldExecution(){
    const owner=await inspectRestorationOwner(db,plan.identity);
    if(owner.phase!=='held'||await fingerprint(owner)!==plan.ownerSha256)throw Error('selected_reprocess_old_owner_changed');
    const tick=await one('SELECT * FROM beedle_restore_tick WHERE singleton=1');
    if(tick.state!=='held'||!tick.token||await fingerprint(tick)!==plan.tickSha256)throw Error('selected_reprocess_old_tick_changed');
    const history=await one('SELECT * FROM beedle_restore_tick_history WHERE token=?',[tick.token]);
    if(history.state!=='held'||!history.completed_at||!history.error_sha256||await fingerprint(history)!==plan.historySha256)throw Error('selected_reprocess_old_history_changed');
    const job=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.documentId]);
    if(job.phase!=='seeded'||!job.claim_token||job.source_verified!==0||job.source_sha256!==plan.source.sourceSha256||
      job.source_bytes!==plan.source.sourceBytes||job.original_key!==plan.source.originalKey||
      job.extraction_policy!==plan.policy||await fingerprint(job)!==plan.jobSha256)throw Error('selected_reprocess_old_job_changed');
    const failed=await one('SELECT * FROM document_generations WHERE id=? AND document_id=?',[plan.failedGenerationId,plan.documentId]);
    if(failed.status!=='failed'||failed.published_at!==null||failed.base_revision!==plan.expectedRevision||failed.source_sha256!==plan.source.sourceSha256||
      failed.manifest_json!=='{}'||await fingerprint(failed)!==plan.failedGenerationSha256)throw Error('selected_reprocess_old_failed_generation_changed');
    for(const table of ['staged_document_sections','staged_section_paragraphs','staged_document_chunks','staged_document_reference_links','staged_document_reference_issues']){
      const count=await one('SELECT COUNT(*) n FROM '+table+' WHERE generation_id=?',[plan.failedGenerationId]);if(count.n!==0)throw Error('selected_reprocess_old_stage_changed');
    }
    const controls=await one('SELECT (SELECT owner_token FROM search_index_rebuild WHERE singleton=1) indexOwner,(SELECT owner_token FROM search_facet_build WHERE singleton=1) facetOwner,(SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner');
    if(Object.values(controls).some(value=>value!==null))throw Error('selected_reprocess_other_lease_active');
    return job;
  }
  try{
    const schema=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
    if(await restorationSchemaFingerprint(schema)!==plan.schemaSha256)throw Error('selected_reprocess_schema_changed');
    const job=await oldExecution(),before=await restorationSeedMetadataSha256(db,plan.documentId);
    const descriptor={ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,partitionIndex:plan.identity.partitionIndex,...plan.source};
    const copyKey=restorationSourceObjectKey(descriptor);
    if(before.sha256!==plan.documentSha256||before.row.document_revision!==plan.expectedRevision||before.row.active_text_generation!=='initial'||before.row.ingestion_state!=='legacy'||
      before.row.source_r2_key!==copyKey||job.source_copy_key!==copyKey||before.row.approved_at!==null||before.row.approved_revision!==null||before.row.qc_required_confirmed!==0||before.row.searchable_at!==null)
      throw Error('selected_reprocess_current_document_changed');
    const generations=await one('SELECT COUNT(*) n FROM document_generations WHERE document_id=?',[plan.documentId]);
    if(generations.n!==1)throw Error('selected_reprocess_unexpected_generation');
    const copy=await observeVerifiedRestorationSource(bindings.originals,bindings.target,descriptor);
    if(copy.targetEtag!==plan.source.copyEtag)throw Error('selected_reprocess_copy_etag_changed');
    // The binding supplies the copy's actual opaque version. Preserve that
    // acknowledged read before staging; never invent it from a REST ETag.
    await put(prefix+'source-read.json',{version:1,operationId:plan.operationId,planSha256,
      sourceSha256:plan.source.sourceSha256,sourceBytes:plan.source.sourceBytes,
      originalVersion:copy.originalVersion,copyVersion:copy.targetVersion,copyEtag:copy.targetEtag});
    stage='source';
    const result=await reprocessIngestionDocument(restorationReprocessEnv(db,bindings.target,plan.profile),plan.documentId,
      {policy:plan.policy,documentId:plan.documentId,expectedRevision:plan.expectedRevision,sourceSha256:plan.source.sourceSha256,planSha256:plan.identity.planSha256});
    if(!result?.reprocessArtifacts.rebuilt)throw Error('selected_reprocess_not_rebuilt');
    stage='bookend';await oldExecution();
    const current=await restorationSeedMetadataSha256(db,plan.documentId);
    // Metadata and edition triggers can advance more than one revision. Pin
    // the rehearsed result for this exact source rather than assuming +1.
    if(current.row.document_revision!==plan.expectedResultRevision||current.row.document_revision!==result.documentRevision||current.row.ingestion_state!=='ready'||current.row.active_text_generation==='initial'||
      current.row.approved_at!==null||current.row.approved_revision!==null||current.row.qc_required_confirmed!==0||current.row.searchable_at!==null)
      throw Error('selected_reprocess_publication_or_revision_changed');
    const generation=await one('SELECT status,source_sha256 FROM document_generations WHERE id=? AND document_id=?',[current.row.active_text_generation,plan.documentId]);
    if(generation.status!=='published'||generation.source_sha256!==plan.source.sourceSha256)throw Error('selected_reprocess_generation_not_published');
    const original=await bindings.target.get(current.row.source_r2_key);
    if(!original||!original.version||!original.etag||original.size!==plan.source.sourceBytes||await sha256Bytes(new Uint8Array(await original.arrayBuffer()))!==plan.source.sourceSha256)
      throw Error('selected_reprocess_current_source_changed');
    const preservedCopy=await observeVerifiedRestorationSource(bindings.originals,bindings.target,descriptor);
    if(preservedCopy.targetVersion!==copy.targetVersion||preservedCopy.targetEtag!==copy.targetEtag)throw Error('selected_reprocess_copy_bookend_changed');
    stage='outcome';
    const outcome={version:1,operationId:plan.operationId,planSha256,state:'selected_reprocess_passed_unpublished_unreviewed',
      documentSha256:await fingerprint(plan.documentId),currentMetadataSha256:current.sha256,documentRevision:current.row.document_revision,
      generationId:current.row.active_text_generation,sourceSha256:plan.source.sourceSha256,sourceBytes:plan.source.sourceBytes,
      currentSourceVersion:original.version,currentSourceEtag:original.etag,bindingCalls:meter.calls,
      oldJobTickHistoryOwnerAndFailedGenerationUnchanged:true,oldSourceExecutionStillHeld:true,humanApproval:false,
      fullIntegrityCredited:false,at:new Date().toISOString()};
    await put(prefix+'outcome.json',outcome);
    return {state:'passed' as const,planSha256,sourceExecutionStillHeld:true};
  }catch(error){
    try{await put(prefix+'failure.json',{version:1,operationId:plan.operationId,planSha256,stage,errorChain:await restorationErrorEvidence(error),
      bindingFailure:await meter.failureEvidence()??null,bindingCalls:meter.calls,oldSourceExecutionStillHeld:true,
      noRetryNoReset:true,at:new Date().toISOString()});}catch{/* Retain the immutable claim and every uncertain outcome. */}
    return {state:'held' as const,stage,planSha256,sqlRetryAuthorized:false};
  }
}
