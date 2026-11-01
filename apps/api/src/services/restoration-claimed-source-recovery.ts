import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {sha256Bytes} from '../lib/digests';
import {corpusPartitionIndex} from './corpus-partitions';
import {inspectRestorationOwner} from './restoration-job-control';
import {heldRestorationExecutionSchema,verifyHeldRestorationExecution} from './restoration-held-execution-lineage';
import {heldSourceSuccessorSchema,verifyHeldSourceLineage} from './restoration-held-source-lineage';
import {restorationSeedMetadataSha256} from './restoration-source-job';
import {observeVerifiedRestorationSource,RestorationSourceError} from './restoration-source';
import {restorationReprocessEnv} from './restoration-capabilities';
import {reprocessIngestionDocument} from './admin-ingestion';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationCapacityObserver} from './restoration-capacity-observer';
import {restorationMeasuredDatabase} from './restoration-measured-database';
import {restorationErrorEvidence} from './restoration-failure-proof';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid();
const opaque=z.string().min(1).max(1024).refine(value=>!/[\u0000-\u001f\u007f]/.test(value));
const schema=z.object({version:z.literal(1),operationId:uuid,notBefore:z.string().datetime(),notAfter:z.string().datetime(),
  identity:z.object({ownerId:uuid,resourceId:uuid,partitionIndex:z.number().int().min(0).max(31),planSha256:hash,
    resourcePlanSha256:hash,sourceManifestSha256:hash,catalogSha256:hash}).strict(),
  schemaSha256:hash,ownerSha256:hash,rootTickSha256:hash,rootHistorySha256:hash.nullable(),
  heldSuccessor:heldSourceSuccessorSchema.nullable(),heldExecution:heldRestorationExecutionSchema,
  expectedSourceResults:z.number().int().min(0).max(1000),jobSha256:hash,
  otherJobsSha256:hash,otherDocumentsSha256:hash,sourceResultsSha256:hash,
  job:z.object({documentId:z.string().min(1).max(200),sourceSha256:hash,sourceBytes:z.number().int().min(1).max(1000000),
    originalKey:opaque,planObjectKey:opaque,extractionPolicy:z.enum(['verified-source-restoration-v1','recovered-markdown-v1']),
    initialMetadataSha256:hash,initialRevision:z.number().int().min(1),originalVersion:opaque}).strict(),
  copy:z.object({version:opaque,etag:opaque}).strict(),
  profile:z.object({AI_EMBEDDING_MODEL:z.string().min(1).max(200),VECTOR_NAMESPACE:z.string().regex(/^[A-Za-z0-9_-]{1,128}$/),
    AI_EMBEDDING_DIMENSIONS:z.string().regex(/^[1-9]\d{0,3}$/).refine(value=>Number(value)<=4096)}).strict()
}).strict();
export type ClaimedRestorationSourceRecoveryPlan=z.infer<typeof schema>;
type Bindings={db:D1Database;originals:Pick<R2Bucket,'get'>;target:R2Bucket;proofs:Pick<R2Bucket,'get'|'put'>};

// A new forward operation for an independently reconciled target-read failure.
// It preserves the original job claim and every failed executor row. It never
// invokes the old source callback, reclaims a job, or changes failure to success.
export async function recoverClaimedRestorationSource(input:unknown,bindings:Bindings,now=Date.now()){
  const plan=schema.parse(input),begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter);
  if(end<=begin||end-begin>30*60000||now<begin||now>=end)throw Error('claimed_source_recovery_window');
  if(plan.operationId===plan.heldExecution.executionId||bindings.originals===bindings.target||
    bindings.originals===bindings.proofs||bindings.target===bindings.proofs||
    await corpusPartitionIndex(plan.job.documentId)!==plan.identity.partitionIndex)
    throw Error('claimed_source_recovery_identity_or_binding_alias');
  const planSha256=await fingerprint(plan),prefix='claimed-source-recovery/'+plan.operationId+'/',metadata={
    operationid:plan.operationId,operationplansha256:planSha256,ownerid:plan.identity.ownerId,plansha256:plan.identity.planSha256};
  async function put(name:string,value:unknown){
    const text=canonical(value),bytes=new TextEncoder().encode(text),sha256=await sha256Bytes(bytes);
    const written=await bindings.proofs.put(prefix+name+'.json',text,{onlyIf:{etagDoesNotMatch:'*'},
      customMetadata:{...metadata,proofsha256:sha256},httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('claimed_source_recovery_proof_conflict');
    const read=await bindings.proofs.get(prefix+name+'.json');
    if(!read||!read.version||!read.etag||read.version!==written.version||read.etag!==written.etag||read.size!==bytes.length||
      Object.entries({...metadata,proofsha256:sha256}).some(([key,value])=>read.customMetadata?.[key]!==value)||
      await sha256Bytes(new Uint8Array(await read.arrayBuffer()))!==sha256)
      throw Error('claimed_source_recovery_proof_unacknowledged');
    return{text,sha256};
  }
  const claim={version:1,operationId:plan.operationId,planSha256,purpose:'forward_original_claim_after_reconciled_target_read_failure'};
  const existing=await bindings.proofs.get(prefix+'claim.json');
  if(existing){
    const text=canonical(claim);
    if(!existing.version||!existing.etag||existing.size!==new TextEncoder().encode(text).length||
      Object.entries({...metadata,proofsha256:await sha256Bytes(new TextEncoder().encode(text))}).some(([key,value])=>existing.customMetadata?.[key]!==value)||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await existing.arrayBuffer())!==text)
      throw Error('claimed_source_recovery_existing_claim_differs');
    return{state:'already_claimed' as const,sqlReissued:false};
  }
  // No SQL or generation work follows a lost claim acknowledgment. Its durable
  // existence also prevents a later delivery from repeating the operation.
  await put('claim',claim);
  const capacity=restorationCapacityObserver(),meter=restorationBindingBudget(restorationMeasuredDatabase(bindings.db,capacity,'reprocess')),db=meter.db;
  let stage:'preflight'|'source_read'|'source_pointer'|'reprocess'|'receipt'|'finalize'|'outcome'='preflight';
  const rows=async(sql:string,args:unknown[]=[])=>{
    const result=await db.prepare(sql).bind(...args).all<Record<string,unknown>>();
    if(result.success!==true)throw Error('claimed_source_recovery_read_unacknowledged');return result.results;
  };
  const one=async(sql:string,args:unknown[]=[])=>{
    const result=await rows(sql,args);if(result.length!==1||!result[0])throw Error('claimed_source_recovery_expected_one');return result[0];
  };
  async function preserved(){
    await inspectRestorationOwner(db,plan.identity);
    const owner=await one('SELECT * FROM beedle_restore_owner WHERE singleton=1'),tick=await one('SELECT * FROM beedle_restore_tick WHERE singleton=1');
    if(!['seeded','held'].includes(String(owner.phase))||await fingerprint(owner)!==plan.ownerSha256||await fingerprint(tick)!==plan.rootTickSha256)
      throw Error('claimed_source_recovery_root_changed');
    const history=tick.token===null?null:await one('SELECT * FROM beedle_restore_tick_history WHERE token=?',[tick.token]);
    if(tick.state==='running'||(await one('SELECT COUNT(*) n FROM beedle_restore_tick_history')).n!==(history===null?0:1)||
      (history===null?plan.rootHistorySha256!==null:history.state!=='held'||await fingerprint(history)!==plan.rootHistorySha256))
      throw Error('claimed_source_recovery_root_history_changed');
    await verifyHeldSourceLineage(db,plan.identity,plan.heldSuccessor);
    await verifyHeldRestorationExecution(db,plan.identity,plan.heldExecution);
    const held=await one('SELECT * FROM beedle_restore_execution_runs WHERE execution_id=?',[plan.heldExecution.executionId]);
    const heldTick=await one('SELECT * FROM beedle_restore_execution_ticks WHERE execution_id=?',[plan.heldExecution.executionId]);
    const heldHistory=await one('SELECT * FROM beedle_restore_execution_history WHERE token=?',[heldTick.token]);
    if(held.error_sha256!==await fingerprint('target_access_failed')||heldTick.start_receipts!==plan.expectedSourceResults||
      heldHistory.deliveries!==0||!heldHistory.completed_at||heldHistory.error_sha256!==held.error_sha256)
      throw Error('claimed_source_recovery_failure_boundary_changed');
    const leases=await one('SELECT (SELECT owner_token FROM search_index_rebuild WHERE singleton=1) indexOwner,(SELECT owner_token FROM search_facet_build WHERE singleton=1) facetOwner,(SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner');
    if(Object.values(leases).some(value=>value!==null)||(await one('SELECT COUNT(*) n FROM documents WHERE searchable_at IS NOT NULL OR published_revision IS NOT NULL')).n!==0)
      throw Error('claimed_source_recovery_active_lease_or_publication');
    if(await fingerprint(await rows('SELECT * FROM beedle_restore_jobs WHERE document_id!=? ORDER BY document_id',[plan.job.documentId]))!==plan.otherJobsSha256||
      await fingerprint(await rows('SELECT * FROM documents WHERE id!=? ORDER BY id',[plan.job.documentId]))!==plan.otherDocumentsSha256||
      await fingerprint(await rows('SELECT * FROM beedle_restore_source_results WHERE document_id!=? ORDER BY document_id,generation_id',[plan.job.documentId]))!==plan.sourceResultsSha256)
      throw Error('claimed_source_recovery_other_records_changed');
    const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
    if(await restorationSchemaFingerprint(objects)!==plan.schemaSha256)throw Error('claimed_source_recovery_schema_changed');
  }
  try{
    await preserved();
    const job=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.job.documentId]);
    if(await fingerprint(job)!==plan.jobSha256||job.phase!=='claimed'||typeof job.claim_token!=='string'||!job.claim_token||
      job.source_copy_key!==null||job.generation_id!==null||job.source_verified!==0||job.source_sha256!==plan.job.sourceSha256||
      job.source_bytes!==plan.job.sourceBytes||job.original_key!==plan.job.originalKey||job.plan_object_key!==plan.job.planObjectKey||
      job.extraction_policy!==plan.job.extractionPolicy)
      throw Error('claimed_source_recovery_original_claim_changed');
    const counts=await one("SELECT (SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN('claimed','source_copied','seeded')) unresolved");
    if(counts.receipts!==plan.expectedSourceResults||counts.unresolved!==1)throw Error('claimed_source_recovery_coverage_changed');
    const initial=await restorationSeedMetadataSha256(db,plan.job.documentId),document=initial.row;
    if(initial.sha256!==plan.job.initialMetadataSha256||document.document_revision!==plan.job.initialRevision||
      document.source_r2_key!==plan.job.originalKey||document.ingestion_state!=='legacy'||document.active_text_generation!=='initial'||
      document.approved_at!==null||document.approved_revision!==null||document.qc_required_confirmed!==0||
      document.searchable_at!==null||document.published_revision!==null)
      throw Error('claimed_source_recovery_initial_document_changed');
    const artifacts=await one('SELECT (SELECT COUNT(*) FROM document_generations WHERE document_id=?) generations,(SELECT COUNT(*) FROM document_chunks WHERE document_id=?) chunks,(SELECT COUNT(*) FROM document_sections WHERE document_id=?) sections,(SELECT COUNT(*) FROM beedle_restore_source_results WHERE document_id=?) receipts',Array(4).fill(plan.job.documentId));
    if(Object.values(artifacts).some(value=>value!==0))throw Error('claimed_source_recovery_unexpected_artifacts');
    stage='source_read';
    // This forward operation can only observe an existing, separately pinned
    // owned copy. It has no copy creation/overwrite fallback.
    const copy=await observeVerifiedRestorationSource(bindings.originals,bindings.target,{
      ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,partitionIndex:plan.identity.partitionIndex,...plan.job});
    if(copy.targetVersion!==plan.copy.version||copy.targetEtag!==plan.copy.etag)throw Error('claimed_source_recovery_copy_version_changed');
    await put('source-read',{version:1,operationId:plan.operationId,planSha256,copy,oldExecutionStillHeld:true});
    await preserved();
    if(await fingerprint(await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.job.documentId]))!==plan.jobSha256||
      (await restorationSeedMetadataSha256(db,plan.job.documentId)).sha256!==plan.job.initialMetadataSha256)
      throw Error('claimed_source_recovery_prewrite_state_changed');
    stage='source_pointer';
    const copied=await db.prepare("UPDATE beedle_restore_jobs SET phase='source_copied',source_copy_key=? WHERE document_id=? AND claim_token=? AND phase='claimed' AND source_copy_key IS NULL AND generation_id IS NULL AND source_verified=0 AND EXISTS(SELECT 1 FROM beedle_restore_execution_runs WHERE execution_id=? AND execution_plan_sha256=? AND state='held' AND error_sha256=?) RETURNING document_id")
      .bind(copy.targetKey,plan.job.documentId,job.claim_token,plan.heldExecution.executionId,plan.heldExecution.executionPlanSha256,await fingerprint('target_access_failed')).all<{document_id:string}>();
    if(copied.success!==true||copied.meta.changes!==1||copied.results.length!==1||copied.results[0]?.document_id!==plan.job.documentId)
      throw Error('claimed_source_recovery_copy_checkpoint_unacknowledged');
    const pointer=await db.prepare("UPDATE documents SET source_r2_key=? WHERE id=? AND document_revision=? AND source_r2_key=? AND active_text_generation='initial' AND ingestion_state='legacy' AND approved_at IS NULL AND approved_revision IS NULL AND qc_required_confirmed=0 AND searchable_at IS NULL AND published_revision IS NULL RETURNING id")
      .bind(copy.targetKey,plan.job.documentId,plan.job.initialRevision,plan.job.originalKey).all<{id:string}>();
    // The schema-pinned document triggers also invalidate dependent state.
    // D1 changes includes those writes; RETURNING identifies the one document.
    if(pointer.success!==true||!Number.isSafeInteger(pointer.meta.changes)||pointer.meta.changes<1||pointer.results.length!==1||pointer.results[0]?.id!==plan.job.documentId)
      throw Error('claimed_source_recovery_pointer_unacknowledged');
    const beforeReprocess=await restorationSeedMetadataSha256(db,plan.job.documentId);
    if(beforeReprocess.sha256!==await fingerprint({...document,source_r2_key:copy.targetKey,document_revision:plan.job.initialRevision+1})||
      beforeReprocess.row.source_r2_key!==copy.targetKey||beforeReprocess.row.document_revision!==plan.job.initialRevision+1||
      beforeReprocess.row.active_text_generation!=='initial'||beforeReprocess.row.ingestion_state!=='legacy'||
      beforeReprocess.row.approved_at!==null||beforeReprocess.row.approved_revision!==null||beforeReprocess.row.qc_required_confirmed!==0||
      beforeReprocess.row.searchable_at!==null||beforeReprocess.row.published_revision!==null)
      throw Error('claimed_source_recovery_pointer_readback_changed');
    const seeded=await db.prepare("UPDATE beedle_restore_jobs SET phase='seeded' WHERE document_id=? AND claim_token=? AND phase='source_copied' AND source_copy_key=? RETURNING document_id")
      .bind(plan.job.documentId,job.claim_token,copy.targetKey).all<{document_id:string}>();
    if(seeded.success!==true||seeded.meta.changes!==1||seeded.results.length!==1||seeded.results[0]?.document_id!==plan.job.documentId)
      throw Error('claimed_source_recovery_seed_unacknowledged');
    stage='reprocess';
    const rebuilt=await reprocessIngestionDocument(restorationReprocessEnv(db,bindings.target,plan.profile),plan.job.documentId,{
      policy:plan.job.extractionPolicy,documentId:plan.job.documentId,expectedRevision:beforeReprocess.row.document_revision,
      sourceSha256:plan.job.sourceSha256,planSha256:plan.identity.planSha256});
    if(!rebuilt?.reprocessArtifacts.rebuilt)throw Error('claimed_source_recovery_generation_not_rebuilt');
    await preserved();
    const current=(await restorationSeedMetadataSha256(db,plan.job.documentId)).row;
    if(current.ingestion_state!=='ready'||current.active_text_generation!==rebuilt.reprocessArtifacts.generationId||
      current.document_revision!==rebuilt.documentRevision||current.document_revision<=beforeReprocess.row.document_revision||
      current.approved_at!==null||current.approved_revision!==null||current.qc_required_confirmed!==0||
      current.searchable_at!==null||current.published_revision!==null)
      throw Error('claimed_source_recovery_generation_or_review_changed');
    const generation=await one('SELECT * FROM document_generations WHERE id=? AND document_id=?',[current.active_text_generation,plan.job.documentId]);
    const retained=await one('SELECT * FROM document_generations WHERE id=? AND document_id=?',[rebuilt.reprocessArtifacts.previousGenerationId,plan.job.documentId]);
    if(generation.status!=='published'||generation.base_revision!==beforeReprocess.row.document_revision||generation.source_sha256!==plan.job.sourceSha256||
      generation.source_key!==current.source_r2_key||generation.previous_generation!==retained.id||retained.status!=='retained'||
      retained.base_revision!==beforeReprocess.row.document_revision||retained.source_sha256!==plan.job.sourceSha256||retained.source_key!==generation.source_key||
      typeof retained.snapshot_json!=='string'||await fingerprint(JSON.parse(retained.snapshot_json))!==beforeReprocess.sha256||
      (await one('SELECT COUNT(*) n FROM document_generations WHERE document_id=?',[plan.job.documentId])).n!==2)
      throw Error('claimed_source_recovery_generation_provenance_changed');
    const freshCopy=await observeVerifiedRestorationSource(bindings.originals,bindings.target,{
      ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,partitionIndex:plan.identity.partitionIndex,...plan.job});
    if(canonical(freshCopy)!==canonical(copy))throw Error('claimed_source_recovery_original_or_copy_changed');
    const original=await bindings.target.get(current.source_r2_key);
    if(!original||!original.version||!original.etag||original.size!==plan.job.sourceBytes||
      await sha256Bytes(new Uint8Array(await original.arrayBuffer()))!==plan.job.sourceSha256)
      throw Error('claimed_source_recovery_current_original_changed');
    stage='receipt';
    const result={version:4,claimedSourceRecovery:{operationId:plan.operationId,operationPlanSha256:planSha256,
      heldExecutionId:plan.heldExecution.executionId,heldExecutionPlanSha256:plan.heldExecution.executionPlanSha256,
      heldJournalSha256:plan.heldExecution.journalSha256,originalClaimedJobSha256:plan.jobSha256},
      ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,partitionIndex:plan.identity.partitionIndex,resourceId:plan.identity.resourceId,
      documentId:plan.job.documentId,claimToken:job.claim_token,copy,sourceSha256:plan.job.sourceSha256,sourceBytes:plan.job.sourceBytes,
      generationId:current.active_text_generation,documentRevision:current.document_revision,currentSourceKey:current.source_r2_key,
      currentSourceVersion:original.version,currentSourceEtag:original.etag,disposition:'processed_unpublished_unreviewed',
      humanApproval:false,oldExecutionStillHeld:true,oldTargetReadFailureCauseStillUnknown:true};
    const proof=await put('source-result',result),proofKey=prefix+'source-result.json',at=new Date().toISOString();
    stage='finalize';
    const finalized=await db.batch([
      db.prepare("INSERT INTO beedle_restore_source_results(document_id,generation_id,owner_id,plan_sha256,claim_token,document_revision,source_sha256,source_bytes,proof_key,proof_sha256,proof_json,recorded_at) SELECT ?,?,?,?,?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE document_id=? AND claim_token=? AND phase='seeded' AND source_verified=0) AND EXISTS(SELECT 1 FROM documents WHERE id=? AND active_text_generation=? AND document_revision=? AND approved_at IS NULL AND approved_revision IS NULL AND qc_required_confirmed=0 AND searchable_at IS NULL AND published_revision IS NULL)")
        .bind(plan.job.documentId,current.active_text_generation,plan.identity.ownerId,plan.identity.planSha256,job.claim_token,current.document_revision,
          plan.job.sourceSha256,plan.job.sourceBytes,proofKey,proof.sha256,proof.text,at,plan.job.documentId,job.claim_token,
          plan.job.documentId,current.active_text_generation,current.document_revision),
      db.prepare("UPDATE beedle_restore_jobs SET phase='processed',generation_id=?,source_verified=1,completed_at=? WHERE document_id=? AND claim_token=? AND phase='seeded' AND EXISTS(SELECT 1 FROM beedle_restore_source_results WHERE document_id=? AND generation_id=? AND proof_sha256=?)")
        .bind(current.active_text_generation,at,plan.job.documentId,job.claim_token,plan.job.documentId,current.active_text_generation,proof.sha256)
    ]);
    if(finalized.length!==2||finalized.some(value=>value.success!==true||value.meta.changes!==1))
      throw Error('claimed_source_recovery_finalization_unacknowledged');
    await preserved();
    const final=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.job.documentId]),coverage=await one("SELECT (SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN('claimed','source_copied','seeded')) unresolved");
    if(final.phase!=='processed'||final.claim_token!==job.claim_token||final.claimed_at!==job.claimed_at||final.generation_id!==current.active_text_generation||
      final.source_verified!==1||coverage.receipts!==plan.expectedSourceResults+1||coverage.unresolved!==0)
      throw Error('claimed_source_recovery_final_coverage_changed');
    stage='outcome';
    const outcome={version:1,state:'claimed_source_recovered_forward_unpublished_unreviewed',operationId:plan.operationId,planSha256,
      sourceResults:coverage.receipts,generationId:current.active_text_generation,documentRevision:current.document_revision,
      originalJobClaimPreserved:true,allFailedExecutorRowsUnchanged:true,oldExecutionStillHeld:true,
      oldTargetReadFailureCauseStillUnknown:true,humanApproval:false,fullIntegrityCredited:false,sqlBindingCalls:meter.calls,capacity:capacity.snapshot()};
    await put('outcome',outcome);return outcome;
  }catch(error){
    await put('failure',{version:1,operationId:plan.operationId,planSha256,stage,noRetry:true,
      oldExecutorNotClearedOrCredited:true,error:await restorationErrorEvidence(error),
      sourceError:error instanceof RestorationSourceError?{codeSha256:await fingerprint(error.code),outcomeUnknown:error.outcomeUnknown,
        causeSha256:error.causeSha256??null,causeKind:error.causeKind??null,causeCode:error.causeCode??null}:null,
      binding:await meter.failureEvidence()??null,sqlBindingCalls:meter.calls,capacity:capacity.snapshot()});
    throw error;
  }
}
