import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {sha256Bytes} from '../lib/digests';
import {inspectRestorationOwner} from './restoration-job-control';
import {restorationSeedMetadataSha256} from './restoration-source-job';
import {observeVerifiedRestorationSource} from './restoration-source';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationErrorEvidence} from './restoration-failure-proof';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid(),opaque=z.string().min(1).max(1024);
const identity=z.object({ownerId:uuid,resourceId:uuid,partitionIndex:z.number().int().min(0).max(31),
  planSha256:hash,resourcePlanSha256:hash,sourceManifestSha256:hash,catalogSha256:hash}).strict();
const schema=z.object({version:z.literal(1),operationId:uuid,identity,notBefore:z.string().datetime(),notAfter:z.string().datetime(),
  schemaSha256:hash,documentId:z.string().min(1).max(200),documentRevision:z.number().int().min(2),generationId:uuid,
  currentMetadataSha256:hash,predecessorDocumentSha256:hash,predecessorRevision:z.number().int().min(1),jobSha256:hash,ownerSha256:hash,tickSha256:hash,historySha256:hash,
  failedGenerationId:uuid,failedGenerationSha256:hash,
  selected:z.object({operationId:uuid,planSha256:hash,outcomeSha256:hash,sourceReadSha256:hash}).strict(),
  source:z.object({sourceSha256:hash,sourceBytes:z.number().int().min(1).max(1000000),originalKey:opaque,
    originalVersion:opaque,copyVersion:opaque,copyEtag:opaque,currentVersion:opaque,currentEtag:opaque}).strict()
}).strict();
export type RestorationSourceForwardPlan=z.infer<typeof schema>;
type Bindings={db:D1Database;originals:Pick<R2Bucket,'get'>;target:Pick<R2Bucket,'get'>;proofs:Pick<R2Bucket,'get'|'put'>};

// Append a new source receipt after a separately acknowledged reprocess. The
// predecessor stays held; its claim, tick, history and failed generation remain.
// This action neither reruns source staging nor admits subsequent source jobs.
export async function completeRestorationSourceForward(input:unknown,bindings:Bindings,now=Date.now()){
  const plan=schema.parse(input),begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter);
  if(end<=begin||end-begin>30*60000||now<begin||now>=end)throw Error('source_forward_admission_window');
  if(plan.operationId===plan.selected.operationId||plan.generationId===plan.failedGenerationId||
    bindings.originals===bindings.target||bindings.originals===bindings.proofs||bindings.target===bindings.proofs)
    throw Error('source_forward_identity_or_binding_alias');
  const planSha256=await fingerprint(plan),prefix='source-forward/'+plan.operationId+'/',meta={
    operationid:plan.operationId,operationplansha256:planSha256,ownerid:plan.identity.ownerId,plansha256:plan.identity.planSha256};
  async function put(key:string,value:unknown){
    const text=canonical(value),proofSha256=await sha256Bytes(new TextEncoder().encode(text)),object=await bindings.proofs.put(key,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:{...meta,proofsha256:proofSha256},
      httpMetadata:{contentType:'application/json'}});
    if(!object)throw Error('source_forward_proof_conflict');
    const read=await bindings.proofs.get(key);
    if(!read||!read.version||!read.etag||read.version!==object.version||read.etag!==object.etag||
      read.customMetadata?.operationid!==plan.operationId||read.customMetadata.operationplansha256!==planSha256||read.customMetadata.proofsha256!==proofSha256||
      read.customMetadata.ownerid!==plan.identity.ownerId||read.customMetadata.plansha256!==plan.identity.planSha256||
      read.size!==new TextEncoder().encode(text).length||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await read.arrayBuffer())!==text)throw Error('source_forward_proof_unacknowledged');
    return {text,sha256:proofSha256};
  }
  const claim={version:1,operationId:plan.operationId,planSha256,purpose:'append_forward_source_receipt_preserve_failed_predecessor'};
  const prior=await bindings.proofs.get(prefix+'claim.json');
  if(prior){
    if(!prior.version||!prior.etag||prior.size!==new TextEncoder().encode(canonical(claim)).length||
      prior.customMetadata?.proofsha256!==await sha256Bytes(new TextEncoder().encode(canonical(claim)))||
      prior.customMetadata.ownerid!==plan.identity.ownerId||prior.customMetadata.plansha256!==plan.identity.planSha256||
      prior.customMetadata.operationid!==plan.operationId||prior.customMetadata.operationplansha256!==planSha256||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await prior.arrayBuffer())!==canonical(claim))
      throw Error('source_forward_prior_claim_differs');
    return {state:'already_claimed' as const,sqlReissued:false};
  }
  // An unknown claim acknowledgment is not caught or retried. Its immutable
  // presence prevents a later delivery from issuing any SQL.
  await put(prefix+'claim.json',claim);
  const meter=restorationBindingBudget(bindings.db),db=meter.db;
  let stage:'preflight'|'receipt'|'finalize'|'outcome'='preflight';
  const one=async(sql:string,args:unknown[]=[])=>{
    const rows=(await db.prepare(sql).bind(...args).all<Record<string,unknown>>()).results;
    if(rows.length!==1||!rows[0])throw Error('source_forward_expected_one_row');return rows[0];
  };
  async function selectedProof(name:'outcome'|'source-read',expectedSha:string){
    const key='selected-reprocess/'+plan.selected.operationId+'/'+name+'.json',object=await bindings.proofs.get(key);
    if(!object||!object.version||!object.etag||object.size>32768||
      object.customMetadata?.operationid!==plan.selected.operationId||object.customMetadata.plansha256!==plan.selected.planSha256)
      throw Error('source_forward_selected_proof_identity');
    const bytes=new Uint8Array(await object.arrayBuffer());
    if(await sha256Bytes(bytes)!==expectedSha)throw Error('source_forward_selected_proof_digest');
    const value:Record<string,unknown>=JSON.parse(new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(bytes));
    if(value.operationId!==plan.selected.operationId||value.planSha256!==plan.selected.planSha256)
      throw Error('source_forward_selected_proof_plan');
    return value;
  }
  async function preservedPredecessor(){
    const owner=await inspectRestorationOwner(db,plan.identity),tick=await one('SELECT * FROM beedle_restore_tick WHERE singleton=1');
    if(owner.phase!=='held'||await fingerprint(owner)!==plan.ownerSha256||tick.state!=='held'||!tick.token||
      await fingerprint(tick)!==plan.tickSha256)throw Error('source_forward_predecessor_changed');
    const history=await one('SELECT * FROM beedle_restore_tick_history WHERE token=?',[tick.token]);
    const failed=await one('SELECT * FROM document_generations WHERE id=? AND document_id=?',[plan.failedGenerationId,plan.documentId]);
    if(history.state!=='held'||await fingerprint(history)!==plan.historySha256||failed.status!=='failed'||
      failed.published_at!==null||failed.base_revision!==plan.predecessorRevision||await fingerprint(failed)!==plan.failedGenerationSha256)
      throw Error('source_forward_failed_evidence_changed');
    for(const table of ['staged_document_sections','staged_section_paragraphs','staged_document_chunks','staged_document_reference_links','staged_document_reference_issues']){
      const count=await one('SELECT COUNT(*) n FROM '+table+' WHERE generation_id=?',[plan.failedGenerationId]);if(count.n!==0)throw Error('source_forward_failed_stage_changed');
    }
    const controls=await one('SELECT (SELECT owner_token FROM search_index_rebuild WHERE singleton=1) indexOwner,(SELECT owner_token FROM search_facet_build WHERE singleton=1) facetOwner,(SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner');
    if(Object.values(controls).some(x=>x!==null))throw Error('source_forward_other_lease_active');
    return tick;
  }
  try{
    const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
    if(await restorationSchemaFingerprint(objects)!==plan.schemaSha256)throw Error('source_forward_schema_changed');
    const tick=await preservedPredecessor(),job=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.documentId]);
    if(await fingerprint(job)!==plan.jobSha256||job.phase!=='seeded'||!job.claim_token||job.source_verified!==0||
      job.source_sha256!==plan.source.sourceSha256||job.source_bytes!==plan.source.sourceBytes||job.original_key!==plan.source.originalKey)
      throw Error('source_forward_seeded_job_changed');
    const current=await restorationSeedMetadataSha256(db,plan.documentId),row=current.row;
    if(current.sha256!==plan.currentMetadataSha256||row.document_revision!==plan.documentRevision||
      row.active_text_generation!==plan.generationId||row.ingestion_state!=='ready'||row.searchable_at!==null||
      row.approved_at!==null||row.approved_revision!==null||row.qc_required_confirmed!==0)
      throw Error('source_forward_current_revision_or_review_changed');
    const generation=await one('SELECT status,source_sha256,source_key,base_revision,previous_generation FROM document_generations WHERE id=? AND document_id=?',[plan.generationId,plan.documentId]);
    if(generation.status!=='published'||generation.base_revision!==plan.predecessorRevision||generation.source_sha256!==plan.source.sourceSha256||generation.source_key!==row.source_r2_key)
      throw Error('source_forward_current_generation_changed');
    const generations=await one('SELECT COUNT(*) n FROM document_generations WHERE document_id=?',[plan.documentId]);
    if(generations.n!==3||!generation.previous_generation)throw Error('source_forward_unexpected_generation');
    const retained=await one('SELECT * FROM document_generations WHERE id=? AND document_id=?',[generation.previous_generation,plan.documentId]);
    if(retained.status!=='retained'||retained.base_revision!==plan.predecessorRevision||retained.published_at!==null||
      retained.source_sha256!==plan.source.sourceSha256||retained.source_key!==generation.source_key||
      typeof retained.snapshot_json!=='string'||await fingerprint(JSON.parse(retained.snapshot_json))!==plan.predecessorDocumentSha256)
      throw Error('source_forward_retained_predecessor_changed');
    for(const table of ['staged_document_sections','staged_section_paragraphs','staged_document_chunks','staged_document_reference_links','staged_document_reference_issues']){
      const count=await one('SELECT COUNT(*) n FROM '+table+' WHERE generation_id=?',[retained.id]);if(count.n!==0)throw Error('source_forward_retained_initial_artifacts_changed');
    }
    if(retained.manifest_version!==3||typeof retained.manifest_json!=='string')throw Error('source_forward_retained_manifest_changed');
    const empty=z.object({count:z.literal(0),sha256:z.literal(await sha256Bytes(new TextEncoder().encode('[]')))}).strict();
    z.object({document_sections:empty,section_paragraphs:empty,document_chunks:empty,document_reference_links:empty,document_reference_issues:empty}).strict().parse(JSON.parse(retained.manifest_json));
    const count=await one('SELECT COUNT(*) n FROM beedle_restore_source_results WHERE document_id=?',[plan.documentId]);
    if(count.n!==0)throw Error('source_forward_receipt_already_exists');
    const selected=await selectedProof('outcome',plan.selected.outcomeSha256),read=await selectedProof('source-read',plan.selected.sourceReadSha256);
    if(selected.state!=='selected_reprocess_passed_unpublished_unreviewed'||selected.humanApproval!==false||
      selected.fullIntegrityCredited!==false||selected.oldSourceExecutionStillHeld!==true||selected.oldJobTickHistoryOwnerAndFailedGenerationUnchanged!==true||
      selected.documentSha256!==await fingerprint(plan.documentId)||selected.currentMetadataSha256!==current.sha256||
      selected.generationId!==plan.generationId||selected.documentRevision!==plan.documentRevision||
      selected.sourceSha256!==plan.source.sourceSha256||selected.sourceBytes!==plan.source.sourceBytes||
      selected.currentSourceVersion!==plan.source.currentVersion||selected.currentSourceEtag!==plan.source.currentEtag||
      read.sourceSha256!==plan.source.sourceSha256||read.sourceBytes!==plan.source.sourceBytes||read.copyVersion!==plan.source.copyVersion||read.copyEtag!==plan.source.copyEtag||read.originalVersion!==plan.source.originalVersion)
      throw Error('source_forward_selected_outcome_changed');
    const copy=await observeVerifiedRestorationSource(bindings.originals,bindings.target,{
      ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,partitionIndex:plan.identity.partitionIndex,...plan.source});
    if(copy.targetKey!==job.source_copy_key||copy.targetVersion!==plan.source.copyVersion||copy.targetEtag!==plan.source.copyEtag)
      throw Error('source_forward_copy_changed');
    const original=await bindings.target.get(row.source_r2_key);
    if(!original||original.version!==plan.source.currentVersion||original.etag!==plan.source.currentEtag||
      original.size!==plan.source.sourceBytes||await sha256Bytes(new Uint8Array(await original.arrayBuffer()))!==plan.source.sourceSha256)
      throw Error('source_forward_current_original_changed');
    stage='receipt';
    const result={version:2,ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,partitionIndex:plan.identity.partitionIndex,
      resourceId:plan.identity.resourceId,documentId:plan.documentId,claimToken:job.claim_token,copy,
      sourceSha256:plan.source.sourceSha256,sourceBytes:plan.source.sourceBytes,generationId:plan.generationId,
      documentRevision:plan.documentRevision,currentSourceKey:row.source_r2_key,currentSourceVersion:original.version,
      currentSourceEtag:original.etag,disposition:'processed_unpublished_unreviewed',humanApproval:false,
      forwardCompletion:{operationId:plan.operationId,planSha256,selectedOperationId:plan.selected.operationId,
        selectedOutcomeSha256:plan.selected.outcomeSha256,predecessorTickToken:tick.token,
        predecessorHistorySha256:plan.historySha256,preservedFailedGenerationId:plan.failedGenerationId}};
    const proofKey='restoration/'+plan.identity.ownerId+'/'+plan.identity.planSha256+'/'+plan.identity.partitionIndex+'/'+await sha256Bytes(new TextEncoder().encode(plan.documentId))+'/'+plan.generationId+'.json';
    const proof=await put(proofKey,result),completedAt=new Date().toISOString();stage='finalize';
    const finalized=await db.batch([
      db.prepare("INSERT INTO beedle_restore_source_results(document_id,generation_id,owner_id,plan_sha256,claim_token,document_revision,source_sha256,source_bytes,proof_key,proof_sha256,proof_json,recorded_at) SELECT ?,?,?,?,?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE document_id=? AND claim_token=? AND phase='seeded' AND source_verified=0) AND EXISTS(SELECT 1 FROM documents WHERE id=? AND document_revision=? AND active_text_generation=? AND searchable_at IS NULL AND approved_at IS NULL AND approved_revision IS NULL AND qc_required_confirmed=0) AND EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='held') AND EXISTS(SELECT 1 FROM beedle_restore_tick WHERE singleton=1 AND token=? AND state='held')")
        .bind(plan.documentId,plan.generationId,plan.identity.ownerId,plan.identity.planSha256,job.claim_token,plan.documentRevision,
          plan.source.sourceSha256,plan.source.sourceBytes,proofKey,proof.sha256,proof.text,completedAt,plan.documentId,job.claim_token,
          plan.documentId,plan.documentRevision,plan.generationId,plan.identity.ownerId,plan.identity.planSha256,tick.token),
      db.prepare("UPDATE beedle_restore_jobs SET phase='processed',generation_id=?,source_verified=1,completed_at=? WHERE document_id=? AND claim_token=? AND phase='seeded' AND EXISTS(SELECT 1 FROM beedle_restore_source_results WHERE document_id=? AND generation_id=? AND proof_sha256=?)")
        .bind(plan.generationId,completedAt,plan.documentId,job.claim_token,plan.documentId,plan.generationId,proof.sha256)
    ]);
    if(finalized.length!==2||finalized.some(x=>x.success!==true||Number(x.meta.changes)!==1))
      throw Error('source_forward_finalization_unacknowledged');
    await preservedPredecessor();
    const after=await restorationSeedMetadataSha256(db,plan.documentId),finalJob=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.documentId]);
    if(after.sha256!==plan.currentMetadataSha256||finalJob.phase!=='processed'||finalJob.source_verified!==1||
      finalJob.claim_token!==job.claim_token||finalJob.generation_id!==plan.generationId||
      await fingerprint(finalJob)!==await fingerprint({...job,phase:'processed',generation_id:plan.generationId,source_verified:1,completed_at:completedAt}))
      throw Error('source_forward_final_bookend_changed');
    stage='outcome';await put(prefix+'outcome.json',{version:1,operationId:plan.operationId,planSha256,
      state:'source_forward_completed_predecessor_still_held',proofKey,proofSha256:proof.sha256,
      oldClaimTokenPreserved:true,predecessorTickHistoryOwnerAndFailedGenerationUnchanged:true,
      sourceSqlReprocessed:false,humanApproval:false,publicationAndFullIntegrityCredited:false,at:new Date().toISOString()});
    return {state:'completed' as const,proofSha256:proof.sha256,predecessorStillHeld:true};
  }catch(error){
    try{await put(prefix+'failure.json',{version:1,operationId:plan.operationId,planSha256,stage,
      errorChain:await restorationErrorEvidence(error),bindingFailure:await meter.failureEvidence()??null,
      noSqlRetryOrReset:true,at:new Date().toISOString()});}catch{/* Preserve the uncertain claim and every receipt. */}
    return {state:'held' as const,stage,sqlRetryAuthorized:false};
  }
}
