import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {sha256Bytes} from '../lib/digests';
import {generationTables} from './generation-tables';
import {inspectRestorationOwner} from './restoration-job-control';
import {restorationSeedMetadataSha256} from './restoration-source-job';
import {observeVerifiedRestorationSource} from './restoration-source';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationErrorEvidence} from './restoration-failure-proof';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid(),opaque=z.string().min(1).max(1024);
const schema=z.object({version:z.literal(1),operationId:uuid,
  identity:z.object({ownerId:uuid,resourceId:uuid,partitionIndex:z.number().int().min(0).max(31),
    planSha256:hash,resourcePlanSha256:hash,sourceManifestSha256:hash,catalogSha256:hash}).strict(),
  notBefore:z.string().datetime(),notAfter:z.string().datetime(),schemaSha256:hash,
  documentId:z.string().min(1).max(200),documentRevision:z.number().int().min(2),generationId:uuid,
  ownerSha256:hash,tickSha256:hash,historySha256:hash,jobSha256:hash,currentMetadataSha256:hash,generationsSha256:hash,
  expectedSourceResults:z.number().int().min(0),
  source:z.object({sourceSha256:hash,sourceBytes:z.number().int().min(1).max(1000000),originalKey:opaque,
    originalVersion:opaque,copyEtag:opaque,currentEtag:opaque}).strict()}).strict();
type Bindings={db:D1Database;originals:Pick<R2Bucket,'get'>;target:Pick<R2Bucket,'get'>;proofs:Pick<R2Bucket,'get'|'put'>};

// Reconcile an ALREADY published internal text generation. No parser, source
// write, generation staging, approval or predecessor hold reset is performed.
export async function reconcilePublishedRestorationSource(input:unknown,bindings:Bindings,now=Date.now()){
  const plan=schema.parse(input),begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter);
  if(end<=begin||end-begin>30*60000||now<begin||now>=end)throw Error('published_source_receipt_admission');
  if(bindings.originals===bindings.target||bindings.originals===bindings.proofs||bindings.target===bindings.proofs)
    throw Error('published_source_receipt_binding_alias');
  const planSha256=await fingerprint(plan),prefix=`published-source-receipt/${plan.operationId}/`,metadata={
    operationid:plan.operationId,operationplansha256:planSha256,ownerid:plan.identity.ownerId,plansha256:plan.identity.planSha256};
  async function exact(key:string,text:string,written?:R2Object){
    const read=await bindings.proofs.get(key);
    if(!read||!read.version||!read.etag||read.size!==new TextEncoder().encode(text).length||
      written&&(read.version!==written.version||read.etag!==written.etag)||
      Object.entries(metadata).some(([name,value])=>read.customMetadata?.[name]!==value)||
      read.customMetadata?.proofsha256!==await sha256Bytes(new TextEncoder().encode(text))||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await read.arrayBuffer())!==text)
      throw Error('published_source_receipt_proof_unacknowledged');
  }
  async function put(name:string,value:unknown){
    const text=canonical(value),sha256=await sha256Bytes(new TextEncoder().encode(text)),key=prefix+name;
    const written=await bindings.proofs.put(key,text,{onlyIf:{etagDoesNotMatch:'*'},
      customMetadata:{...metadata,proofsha256:sha256},httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('published_source_receipt_conditional_conflict');await exact(key,text,written);return {key,text,sha256};
  }
  const claim={version:1,operationId:plan.operationId,planSha256,purpose:'observe_published_source_append_receipt_no_reprocess'};
  if(await bindings.proofs.get(prefix+'claim.json')){await exact(prefix+'claim.json',canonical(claim));return {state:'already_claimed' as const,sqlReissued:false};}
  await put('claim.json',claim);
  const meter=restorationBindingBudget(bindings.db),db=meter.db;
  let stage:'preflight'|'source'|'manifest'|'receipt'|'finalize'|'outcome'='preflight';
  const one=async(sql:string,args:unknown[]=[])=>{
    const rows=(await db.prepare(sql).bind(...args).all<Record<string,unknown>>()).results;
    if(rows.length!==1||!rows[0])throw Error('published_source_receipt_expected_one');return rows[0];
  };
  async function bookend(){
    const owner=await inspectRestorationOwner(db,plan.identity),tick=await one('SELECT * FROM beedle_restore_tick WHERE singleton=1');
    const history=await one('SELECT * FROM beedle_restore_tick_history WHERE token=?',[tick.token]);
    if(owner.phase!=='held'||await fingerprint(owner)!==plan.ownerSha256||tick.state!=='held'||await fingerprint(tick)!==plan.tickSha256||
      history.state!=='held'||await fingerprint(history)!==plan.historySha256)throw Error('published_source_receipt_old_hold_changed');
    const current=await restorationSeedMetadataSha256(db,plan.documentId),row=current.row;
    const generations=(await db.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').bind(plan.documentId).all<Record<string,unknown>>()).results;
    if(current.sha256!==plan.currentMetadataSha256||row.document_revision!==plan.documentRevision||row.active_text_generation!==plan.generationId||
      row.ingestion_state!=='ready'||row.approved_at!==null||row.approved_revision!==null||row.qc_required_confirmed!==0||row.searchable_at!==null||row.published_revision!==null||
      generations.length!==2||await fingerprint(generations)!==plan.generationsSha256)throw Error('published_source_receipt_current_generation_changed');
    const generation=generations.find(value=>value.id===plan.generationId),retained=generations.find(value=>value.id!==plan.generationId);
    if(!generation||!retained||generation.status!=='published'||generation.error!==null||generation.source_key!==row.source_r2_key||
      generation.source_sha256!==plan.source.sourceSha256||generation.previous_generation!==retained.id||generation.manifest_version!==3||
      retained.status!=='retained'||retained.error!==null||retained.source_sha256!==plan.source.sourceSha256||retained.source_key!==generation.source_key||
      retained.base_revision!==generation.base_revision||retained.published_at!==null||retained.manifest_version!==3)
      throw Error('published_source_receipt_retained_initial_changed');
    const leases=await one('SELECT (SELECT owner_token FROM search_index_rebuild WHERE singleton=1) indexOwner,(SELECT owner_token FROM search_facet_build WHERE singleton=1) facetOwner,(SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner');
    if(Object.values(leases).some(value=>value!==null))throw Error('published_source_receipt_other_lease_active');
    return {tick,current:row,generation,retained};
  }
  async function verifyManifests(generation:Record<string,unknown>,retained:Record<string,unknown>){
    const entry=z.object({count:z.number().int().min(0).max(10000),sha256:hash}).strict();
    const manifestSchema=z.object({document_sections:entry,section_paragraphs:entry,document_chunks:entry,document_reference_links:entry,document_reference_issues:entry}).strict();
    const manifest=manifestSchema.parse(JSON.parse(String(generation.manifest_json))),old=manifestSchema.parse(JSON.parse(String(retained.manifest_json)));
    for(const table of Object.keys(generationTables) as (keyof typeof generationTables)[]){
      if(old[table].count!==0||old[table].sha256!==await sha256Bytes(new TextEncoder().encode('[]')))throw Error('published_source_receipt_retained_manifest_changed');
      const count=await one('SELECT COUNT(*) n FROM staged_'+table+' WHERE generation_id=?',[retained.id]);if(count.n!==0)throw Error('published_source_receipt_retained_stage_changed');
      for(const staged of [false,true]){
        const where=staged?'generation_id=?':table==='section_paragraphs'?'section_id IN (SELECT id FROM document_sections WHERE document_id=?)':'document_id=?';
        const rows=(await db.prepare('SELECT '+generationTables[table].join(',')+' FROM '+(staged?'staged_':'')+table+' WHERE '+where+' ORDER BY id')
          .bind(staged?generation.id:plan.documentId).all()).results;
        const encoded=JSON.stringify(rows);if(encoded.length>8*1024*1024||rows.length!==manifest[table].count||
          await sha256Bytes(new TextEncoder().encode(encoded))!==manifest[table].sha256)throw Error('published_source_receipt_artifact_manifest_changed');
      }
    }
  }
  const oldProofKey=`restoration/${plan.identity.ownerId}/${plan.identity.planSha256}/${plan.identity.partitionIndex}/${await sha256Bytes(new TextEncoder().encode(plan.documentId))}/${plan.generationId}.json`;
  try{
    const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
    if(await restorationSchemaFingerprint(objects)!==plan.schemaSha256)throw Error('published_source_receipt_schema_changed');
    const before=await bookend(),job=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.documentId]);
    if(await fingerprint(job)!==plan.jobSha256||job.phase!=='seeded'||job.source_verified!==0||!job.claim_token||job.original_key!==plan.source.originalKey||
      job.source_sha256!==plan.source.sourceSha256||job.source_bytes!==plan.source.sourceBytes)throw Error('published_source_receipt_seeded_job_changed');
    const counts=await one('SELECT (SELECT COUNT(*) FROM beedle_restore_source_results) total,(SELECT COUNT(*) FROM beedle_restore_source_results WHERE document_id=?) existing',[plan.documentId]);
    if(counts.total!==plan.expectedSourceResults||counts.existing!==0||await bindings.proofs.get(oldProofKey))throw Error('published_source_receipt_original_outcome_changed');
    stage='source';const descriptor={...plan.source,ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,partitionIndex:plan.identity.partitionIndex};
    const copy=await observeVerifiedRestorationSource(bindings.originals,bindings.target,descriptor);
    const current=await bindings.target.get(String(before.current.source_r2_key));
    if(copy.targetKey!==job.source_copy_key||copy.targetEtag!==plan.source.copyEtag||!current||!current.version||current.etag!==plan.source.currentEtag||
      current.size!==plan.source.sourceBytes||await sha256Bytes(new Uint8Array(await current.arrayBuffer()))!==plan.source.sourceSha256)
      throw Error('published_source_receipt_source_changed');
    await put('source-read.json',{version:1,operationId:plan.operationId,planSha256,sourceSha256:plan.source.sourceSha256,
      sourceBytes:plan.source.sourceBytes,originalVersion:copy.originalVersion,copyVersion:copy.targetVersion,copyEtag:copy.targetEtag,
      currentVersion:current.version,currentEtag:current.etag});
    stage='manifest';await verifyManifests(before.generation,before.retained);await bookend();
    const again=await observeVerifiedRestorationSource(bindings.originals,bindings.target,descriptor),againCurrent=await bindings.target.get(String(before.current.source_r2_key));
    if(again.targetVersion!==copy.targetVersion||again.targetEtag!==copy.targetEtag||!againCurrent||againCurrent.version!==current.version||againCurrent.etag!==current.etag||
      againCurrent.size!==plan.source.sourceBytes||await sha256Bytes(new Uint8Array(await againCurrent.arrayBuffer()))!==plan.source.sourceSha256||
      await bindings.proofs.get(oldProofKey))throw Error('published_source_receipt_source_bookend_changed');
    stage='receipt';const receipt=await put('source-result.json',{version:3,ownerId:plan.identity.ownerId,planSha256:plan.identity.planSha256,
      partitionIndex:plan.identity.partitionIndex,resourceId:plan.identity.resourceId,documentId:plan.documentId,claimToken:job.claim_token,copy,
      sourceSha256:plan.source.sourceSha256,sourceBytes:plan.source.sourceBytes,generationId:plan.generationId,documentRevision:plan.documentRevision,
      currentSourceKey:before.current.source_r2_key,currentSourceVersion:current.version,currentSourceEtag:current.etag,
      disposition:'processed_unpublished_unreviewed',humanApproval:false,publishedAttemptReconciliation:{operationId:plan.operationId,planSha256,
        originalTickToken:before.tick.token,originalHeldHistorySha256:plan.historySha256,bothOriginalGenerationsSha256:plan.generationsSha256,
        originalSourceProofAbsent:true,noSourceSqlReprocessed:true,oldExecutionStillHeld:true}});
    stage='finalize';const at=new Date().toISOString();const results=await db.batch([
      db.prepare('INSERT INTO beedle_restore_source_results(document_id,generation_id,owner_id,plan_sha256,claim_token,document_revision,source_sha256,source_bytes,proof_key,proof_sha256,proof_json,recorded_at) SELECT ?,?,?,?,?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM beedle_restore_jobs WHERE document_id=? AND claim_token=? AND phase=\'seeded\' AND source_verified=0)')
        .bind(plan.documentId,plan.generationId,plan.identity.ownerId,plan.identity.planSha256,job.claim_token,plan.documentRevision,plan.source.sourceSha256,plan.source.sourceBytes,
          receipt.key,receipt.sha256,receipt.text,at,plan.documentId,job.claim_token),
      db.prepare("UPDATE beedle_restore_jobs SET phase='processed',generation_id=?,source_verified=1,completed_at=? WHERE document_id=? AND claim_token=? AND phase='seeded' AND source_verified=0 AND EXISTS(SELECT 1 FROM beedle_restore_source_results WHERE document_id=? AND generation_id=? AND proof_sha256=?)")
        .bind(plan.generationId,at,plan.documentId,job.claim_token,plan.documentId,plan.generationId,receipt.sha256)]);
    if(results.length!==2||results.some(result=>result.success!==true||Number(result.meta.changes)!==1))throw Error('published_source_receipt_finalization_unacknowledged');
    await bookend();const after=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.documentId]);
    const omit=(value:Record<string,unknown>)=>Object.fromEntries(Object.entries(value).filter(([key])=>!['phase','generation_id','source_verified','completed_at'].includes(key)));
    if(after.phase!=='processed'||after.generation_id!==plan.generationId||after.source_verified!==1||await fingerprint(omit(after))!==await fingerprint(omit(job)))
      throw Error('published_source_receipt_job_bookend_changed');
    stage='outcome';await put('outcome.json',{version:1,operationId:plan.operationId,planSha256,state:'published_source_receipt_reconciled',
      sourceReceiptSha256:receipt.sha256,sourceResults:plan.expectedSourceResults+1,documentRevision:plan.documentRevision,generationId:plan.generationId,
      oldOwnerTickHistoryAndBothGenerationsUnchanged:true,originalJobClaimUnchanged:true,noSourceSqlReprocessed:true,
      sourceShaBytesAndVersionsPassed:true,bindingCalls:meter.calls,oldExecutionStillHeld:true,humanApproval:false,fullIntegrityCredited:false,at});
    return {state:'passed' as const,planSha256,oldExecutionStillHeld:true,sourceSqlReprocessed:false};
  }catch(error){
    try{await put('failure.json',{version:1,operationId:plan.operationId,planSha256,stage,errorChain:await restorationErrorEvidence(error),
      bindingFailure:await meter.failureEvidence()??null,oldExecutionStillHeld:true,noRetry:true,at:new Date().toISOString()});}catch{/* Retain claim and every uncertain receipt. */}
    return {state:'held' as const,stage,planSha256,sqlRetryAuthorized:false};
  }
}
