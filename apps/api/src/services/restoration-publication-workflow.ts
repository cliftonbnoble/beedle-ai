import type {WorkflowEvent, WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical, fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import releaseContract from '../../release/contract.json';
import {inspectRestorationOwner} from './restoration-job-control';
import {inspectSearchReadiness} from './search-readiness';
import {restorationSourceJournalSnapshot, restorationIndexControlSnapshot} from './restoration-reviewer-workflow';
import {publishRestorationProvisionalBatch} from './restoration-provisional-publication';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationMeasuredDatabase} from './restoration-measured-database';
import {restorationCapacityObserver} from './restoration-capacity-observer';
import {restorationErrorEvidence} from './restoration-failure-proof';

const hash = z.string().regex(/^[a-f0-9]{64}$/), uuid = z.string().uuid();
const recordSchema = z.object({documentId:z.string().min(1).max(200), documentRevision:z.number().int().positive(),
  generationId:z.string().min(1).max(200).refine(value=>value!=='initial'), metadataSha256:hash,
  sourceKey:z.string().min(1).max(1024).refine(value=>new TextEncoder().encode(value).length<=1024&&!/[\u0000-\u001f\u007f]/.test(value)),
  sourceSha256:hash, sourceBytes:z.number().int().min(1).max(1000000)}).strict();
const planSchema = z.object({version:z.literal(1), operationId:uuid, publicationPolicy:z.literal('unreviewed-provisional-v1'),
  source:z.object({ownerId:uuid, resourceId:uuid, partitionIndex:z.number().int().min(0).max(31),
    planSha256:hash, resourcePlanSha256:hash, sourceManifestSha256:hash, catalogSha256:hash}).strict(),
  schemaSha256:hash, ownerSha256:hash, journalsSha256:hash, jobsSha256:hash, receiptsSha256:hash,
  indexControlsSha256:hash, documentsSha256:hash, initialCorpusRevision:z.number().int().positive(),
  catalogVersion:z.string().min(1).max(200), documents:z.number().int().min(1).max(1000),
  records:z.array(recordSchema).min(1).max(1000), notBefore:z.string().datetime(), notAfter:z.string().datetime(),
  completionDeadline:z.string().datetime(), maximumSteps:z.number().int().min(3).max(42)}).strict();
export interface RestorationPublicationWorkflowBindings {
  DB:D1Database; PROOFS:R2Bucket; OPERATION_ENABLED:string; OPERATION_JSON:string;
}
export interface RestorationPublicationWorkflowParams {operationPlanJson:string}
export function parseRestorationPublicationPlan(text:string) {
  if(new TextEncoder().encode(text).length>768*1024)throw Error('restoration_publication_plan_size');
  const plan=planSchema.parse(JSON.parse(text)), begin=Date.parse(plan.notBefore), end=Date.parse(plan.notAfter);
  if(end<=begin||end-begin>30*60000||Date.parse(plan.completionDeadline)<=end||Date.parse(plan.completionDeadline)-begin>4*60*60000||
    plan.records.length>plan.documents||plan.maximumSteps!==Math.ceil(plan.records.length/25)+2||
    plan.records.some((row,index)=>{const previous=plan.records[index-1];return previous!==undefined&&previous.documentId>=row.documentId;}))
    throw Error('restoration_publication_plan_bounds');
  return plan;
}
// Publication may change these three fields only. All other document values,
// including review/metadata flags and source/generation revisions, stay pinned.
export function restorationPublicationStableDocuments(rows:Record<string,unknown>[]) {
  return rows.map(row=>Object.fromEntries(Object.entries(row).filter(([key])=>!['updated_at','searchable_at','published_revision'].includes(key))));
}
async function inspect(db:D1Database,plan:z.infer<typeof planSchema>,published:number) {
  await inspectRestorationOwner(db,plan.source);
  const owners=(await db.prepare('SELECT * FROM beedle_restore_owner WHERE singleton=1').all()).results;
  if(owners.length!==1||owners[0]?.phase!=='indexing'||await fingerprint(owners[0])!==plan.ownerSha256)
    throw Error('restoration_publication_owner');
  const schema=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
  const ledger=(await db.prepare('SELECT name FROM d1_migrations ORDER BY id').all<{name:string}>()).results.map(row=>row.name);
  if(await restorationSchemaFingerprint(schema)!==plan.schemaSha256||canonical(ledger)!==canonical(releaseContract.requiredMigrations)||
    await fingerprint(await restorationSourceJournalSnapshot(db))!==plan.journalsSha256||
    await fingerprint(await restorationIndexControlSnapshot(db))!==plan.indexControlsSha256)
    throw Error('restoration_publication_schema_or_journal');
  const jobs=(await db.prepare('SELECT * FROM beedle_restore_jobs ORDER BY document_id').all<Record<string,unknown>>()).results,
    receipts=(await db.prepare('SELECT * FROM beedle_restore_source_results ORDER BY document_id').all()).results,
    docs=(await db.prepare('SELECT * FROM documents ORDER BY id').all<Record<string,unknown>>()).results;
  if(jobs.length!==plan.documents||receipts.length!==plan.documents||docs.length!==plan.documents||
    jobs.some(job=>!['processed','held'].includes(String(job.phase))||job.source_verified!==1)||
    await fingerprint(jobs)!==plan.jobsSha256||await fingerprint(receipts)!==plan.receiptsSha256||
    await fingerprint(restorationPublicationStableDocuments(docs))!==plan.documentsSha256)
    throw Error('restoration_publication_source_coverage');
  const eligible=jobs.filter(job=>job.phase==='processed'&&job.extraction_policy!=='held-unreadable').map(job=>String(job.document_id));
  if(canonical(eligible)!==canonical(plan.records.map(row=>row.documentId)))throw Error('restoration_publication_eligible_set');
  const expected=new Set(plan.records.slice(0,published).map(row=>row.documentId));
  for(const doc of docs) {
    if(doc.approved_at!==null||doc.approved_revision!==null||doc.qc_required_confirmed!==0)
      throw Error('restoration_publication_review_changed');
    if(expected.has(String(doc.id)) ? typeof doc.searchable_at!=='string'||!Number.isFinite(Date.parse(doc.searchable_at))||
      doc.updated_at!==doc.searchable_at||doc.published_revision!==doc.document_revision : doc.searchable_at!==null||doc.published_revision!==null)
      throw Error('restoration_publication_unexpected_publication');
  }
  const state=await db.prepare(`SELECT (SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1) revision,
    (SELECT active_version FROM legal_catalog_state WHERE singleton=1) catalog,
    (SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner`).first<{revision:number;catalog:string;reviewerOwner:string|null}>();
  if(!state||state.revision!==plan.initialCorpusRevision+published||state.catalog!==plan.catalogVersion||state.reviewerOwner!==null)
    throw Error('restoration_publication_revision_or_lease');
}
export async function runRestorationPublicationWorkflow(env:RestorationPublicationWorkflowBindings,
  event:Pick<WorkflowEvent<RestorationPublicationWorkflowParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>) {
  if(env.OPERATION_ENABLED!=='true'||new TextEncoder().encode(env.OPERATION_JSON).length>5000)
    throw Error('restoration_publication_disabled');
  const header=z.object({version:z.literal(1),operationId:uuid,operationPlanSha256:hash}).strict().parse(JSON.parse(env.OPERATION_JSON));
  if(!event.payload||Object.keys(event.payload).join('|')!=='operationPlanJson')throw Error('restoration_publication_payload');
  const plan=parseRestorationPublicationPlan(event.payload.operationPlanJson), planSha256=await fingerprint(plan);
  if(event.instanceId!==plan.operationId||header.operationId!==plan.operationId||header.operationPlanSha256!==planSha256)
    throw Error('restoration_publication_instance');
  const prefix='provisional-publication/'+plan.operationId+'/', metadata={operationid:plan.operationId,plansha256:planSha256};
  async function put(name:string,value:unknown) {
    const text=canonical(value), written=await env.PROOFS.put(prefix+name,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('restoration_publication_proof_conflict');const read=await env.PROOFS.get(prefix+name);
    if(!read||!read.version||!read.etag||read.version!==written.version||read.etag!==written.etag||read.size!==new TextEncoder().encode(text).length||
      read.customMetadata?.operationid!==plan.operationId||read.customMetadata.plansha256!==planSha256||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await read.arrayBuffer())!==text)
      throw Error('restoration_publication_proof_unacknowledged');
  }
  const options={retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const};
  async function once<T>(name:string,run:(db:D1Database)=>Promise<T>) {
    if(await env.PROOFS.get(prefix+name+'-claim.json'))throw Error('restoration_publication_already_claimed');
    await put(name+'-claim.json',{version:1,operationId:plan.operationId,planSha256,step:name});
    const capacity=restorationCapacityObserver(), meter=restorationBindingBudget(restorationMeasuredDatabase(env.DB,capacity,'staging'));
    try {
      const now=Date.now();if(name==='initialize'?(now<Date.parse(plan.notBefore)||now>=Date.parse(plan.notAfter)):now>=Date.parse(plan.completionDeadline))
        throw Error('restoration_publication_window');
      if(name==='initialize')await put('plan.json',plan);
      else {
        const stored=await env.PROOFS.get(prefix+'plan.json');
        if(!stored||stored.customMetadata?.operationid!==plan.operationId||stored.customMetadata.plansha256!==planSha256||
          new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await stored.arrayBuffer())!==canonical(plan))throw Error('restoration_publication_stored_plan');
      }
      const result=await run(meter.db);
      await put(name+'-outcome.json',{version:1,operationId:plan.operationId,planSha256,step:name,result,capacity:capacity.snapshot(),bindingCalls:meter.calls,at:new Date().toISOString()});
      return result;
    }catch(error) {
      try {await put(name+'-failure.json',{version:1,operationId:plan.operationId,planSha256,step:name,
        errorChain:await restorationErrorEvidence(error),bindingFailure:await meter.failureEvidence()??null,capacity:capacity.snapshot(),noRetry:true,at:new Date().toISOString()});}catch{/* Preserve the immutable claim and uncertain outcome. */}
      throw Error('restoration_publication_held');
    }
  }
  await step.do('provisional-publication-initialize-once',options,()=>once('initialize',async db=>{
    await inspect(db,plan,0);if(!(await inspectSearchReadiness({DB:db})).ready)throw Error('restoration_publication_index_not_ready');
    return{state:'initialized',records:plan.records.length};
  }));
  for(let offset=0;offset<plan.records.length;offset+=25) {
    const name='publish-'+String(offset/25).padStart(4,'0');
    await step.do('provisional-publication-'+name,options,()=>once(name,async db=>{
      await inspect(db,plan,offset);
      const result=await publishRestorationProvisionalBatch(db,plan.source,plan.publicationPolicy,plan.records.slice(offset,offset+25));
      await inspect(db,plan,Math.min(offset+25,plan.records.length));return result;
    }));
  }
  await step.do('provisional-publication-complete-once',options,()=>once('complete',async db=>{
    await inspect(db,plan,plan.records.length);return{state:'provisional_publication_complete',published:plan.records.length};
  }));
  return{state:'provisional_publication_complete',operationId:plan.operationId,published:plan.records.length,steps:plan.maximumSteps,
    publicationPolicy:plan.publicationPolicy,humanApproval:false,metadataConfirmation:false,provenanceOrLegalEditionConfirmed:false,
    maintenanceIntegrityAndReleaseApproval:false};
}
