import type {WorkflowEvent,WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {partitionIntegrityCommands} from '../../scripts/lib/corpus-partition-contract.mjs';
import releaseContract from '../../release/contract.json';
import {restorationByteAuditSourceSchema} from './restoration-source-byte-audit';
import {inspectRestorationOwner} from './restoration-job-control';
import {restorationSourceJournalSnapshot,restorationIndexControlSnapshot} from './restoration-reviewer-workflow';
import {inspectSearchReadiness} from './search-readiness';
import {inspectReviewerQueue} from './reviewer-queue-index';
import {restorationCapacityObserver} from './restoration-capacity-observer';
import {restorationMeasuredDatabase} from './restoration-measured-database';
import {restorationBindingBudget} from './restoration-binding-budget';
import {restorationErrorEvidence} from './restoration-failure-proof';

const hash=z.string().regex(/^[a-f0-9]{64}$/),uuid=z.string().uuid();
const kinds=['metadata','body','phrase','physical','foreign_keys'] as const;
type Kind=typeof kinds[number];
const planSchema=z.object({version:z.literal(1),operationId:uuid,source:restorationByteAuditSourceSchema,
  publicationPolicy:z.literal('unreviewed-provisional-v1'),schemaSha256:hash,ownerSha256:hash,
  journalsSha256:hash,jobsSha256:hash,receiptsSha256:hash,documentsSha256:hash,indexControlsSha256:hash,
  materializationsSha256:hash,sourceShaReceiptSha256:hash,corpusRevision:z.number().int().positive(),
  catalogVersion:z.string().min(1).max(200),documents:z.number().int().min(1).max(1000),
  publishedDocuments:z.number().int().min(1).max(1000),notBefore:z.string().datetime(),
  notAfter:z.string().datetime(),completionDeadline:z.string().datetime()}).strict();
export interface RestorationFinalIntegrityCapabilities {
  DB:D1Database;PROOFS:Pick<R2Bucket,'get'|'put'>;OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export interface RestorationFinalIntegrityParams {operationPlanSha256:string}
export function parseRestorationFinalIntegrityPlan(text:string){
  if(new TextEncoder().encode(text).length>5000)throw Error('final_integrity_plan_size');
  const plan=planSchema.parse(JSON.parse(text)),begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter),deadline=Date.parse(plan.completionDeadline);
  if(end<=begin||end-begin>30*60000||deadline<=end||deadline-begin>4*60*60000||plan.publishedDocuments>plan.documents)
    throw Error('final_integrity_plan_bounds');
  return plan;
}
type Plan=z.infer<typeof planSchema>;
const receiptSchema=z.object({kind:z.enum(['source_sha',...kinds]),owner_id:uuid,plan_sha256:hash,
  corpus_revision:z.number().int().positive(),claim_token:uuid,command_sha256:hash,
  status:z.enum(['claimed','passed','held','uncertain']),engine_acknowledged:z.number().int().min(0).max(1),
  result_sha256:hash.nullable(),claimed_at:z.string().min(1),completed_at:z.string().min(1).nullable()}).strict();
type Receipt=z.infer<typeof receiptSchema>;
export async function restorationFinalMaterializations(db:D1Database){
  const rows:Record<string,unknown[]>={};
  for(const table of ['document_index_codes','document_rules_sections','document_ordinance_sections','reviewer_queue_control','reviewer_queue_state']){
    rows[table]=(await db.prepare('SELECT * FROM '+table+' ORDER BY 1,2').all()).results;
    if(rows[table].length>20000)throw Error('final_integrity_materialization_rows');
  }
  if(new TextEncoder().encode(canonical(rows)).length>8*1024*1024)throw Error('final_integrity_materialization_bytes');
  return rows;
}
async function inspectFrozen(db:D1Database,plan:Plan){
  await inspectRestorationOwner(db,plan.source);
  const owners=(await db.prepare('SELECT * FROM beedle_restore_owner WHERE singleton=1').all()).results;
  if(owners.length!==1||owners[0]?.phase!=='indexing'||await fingerprint(owners[0])!==plan.ownerSha256)
    throw Error('final_integrity_owner');
  const schema=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
  if(await restorationSchemaFingerprint(schema)!==plan.schemaSha256||canonical((await db.prepare('SELECT name FROM d1_migrations ORDER BY id').all<{name:string}>()).results.map(row=>row.name))!==canonical(releaseContract.requiredMigrations))
    throw Error('final_integrity_schema_or_ledger');
  if(await fingerprint(await restorationSourceJournalSnapshot(db))!==plan.journalsSha256||
    await fingerprint(await restorationIndexControlSnapshot(db))!==plan.indexControlsSha256||
    await fingerprint(await restorationFinalMaterializations(db))!==plan.materializationsSha256)
    throw Error('final_integrity_journal_or_materialization');
  const jobs=(await db.prepare('SELECT * FROM beedle_restore_jobs ORDER BY document_id').all<Record<string,unknown>>()).results,
    receipts=(await db.prepare('SELECT * FROM beedle_restore_source_results ORDER BY document_id').all()).results,
    documents=(await db.prepare('SELECT * FROM documents ORDER BY id').all()).results;
  if(jobs.length!==plan.documents||receipts.length!==plan.documents||documents.length!==plan.documents||
    jobs.some(row=>!['processed','held'].includes(String(row.phase))||row.source_verified!==1)||
    jobs.filter(row=>row.phase==='processed'&&row.extraction_policy!=='held-unreadable').length!==plan.publishedDocuments||
    await fingerprint(jobs)!==plan.jobsSha256||await fingerprint(receipts)!==plan.receiptsSha256||await fingerprint(documents)!==plan.documentsSha256)
    throw Error('final_integrity_source_or_document');
  const state=await db.prepare(`SELECT (SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1) revision,
    (SELECT active_version FROM legal_catalog_state WHERE singleton=1) catalog,
    (SELECT COUNT(*) FROM documents WHERE approved_at IS NOT NULL OR approved_revision IS NOT NULL OR qc_required_confirmed!=0) approvals,
    (SELECT COUNT(*) FROM documents WHERE searchable_at IS NOT NULL OR published_revision IS NOT NULL) publications,
    (SELECT COUNT(*) FROM documents d JOIN beedle_restore_jobs j ON j.document_id=d.id WHERE j.phase='processed' AND j.source_verified=1 AND j.extraction_policy!='held-unreadable' AND d.ingestion_state='ready' AND d.searchable_at IS NOT NULL AND d.published_revision=d.document_revision AND EXISTS(SELECT 1 FROM document_generations g WHERE g.id=d.active_text_generation AND g.document_id=d.id AND g.status='published' AND g.source_sha256=j.source_sha256 AND g.source_key=d.source_r2_key)) provisional,
    (SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) reviewerOwner`).first<{
      revision:number;catalog:string;approvals:number;publications:number;provisional:number;reviewerOwner:string|null}>();
  const reviewer=await inspectReviewerQueue({DB:db});
  if(!state||state.revision!==plan.corpusRevision||state.catalog!==plan.catalogVersion||state.approvals!==0||
    state.publications!==plan.publishedDocuments||state.provisional!==plan.publishedDocuments||state.reviewerOwner!==null||
    !(await inspectSearchReadiness({DB:db})).ready||!reviewer.ready||reviewer.sourceCount!==plan.documents||reviewer.indexedCount!==plan.documents)
    throw Error('final_integrity_frozen_publication_or_readiness');
}
async function token(operationId:string,kind:Kind){
  const value=await fingerprint([operationId,kind]);
  return value.slice(0,8)+'-'+value.slice(8,12)+'-4'+value.slice(13,16)+'-a'+value.slice(17,20)+'-'+value.slice(20,32);
}
function passed(row:Receipt,plan:Plan){
  return row.owner_id===plan.source.ownerId&&row.plan_sha256===plan.source.planSha256&&row.corpus_revision===plan.corpusRevision&&
    row.command_sha256===partitionIntegrityCommands[row.kind].sha256&&row.status==='passed'&&row.engine_acknowledged===1&&
    row.result_sha256!==null&&row.completed_at!==null;
}
/** Five full D1 checks, exactly once each, after an independently acknowledged
 * whole-partition source-byte receipt. This creates no source-byte proof, seal,
 * publication, content review, peak-capacity or release approval.
 */
export async function runRestorationFinalIntegrityWorkflow(env:RestorationFinalIntegrityCapabilities,
  event:Pick<WorkflowEvent<RestorationFinalIntegrityParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>){
  if(env.OPERATION_ENABLED!=='true')throw Error('final_integrity_disabled');
  const plan=parseRestorationFinalIntegrityPlan(env.OPERATION_JSON),planSha256=await fingerprint(plan);
  if(event.instanceId!==plan.operationId||!event.payload||Object.keys(event.payload).join('|')!=='operationPlanSha256'||event.payload.operationPlanSha256!==planSha256)
    throw Error('final_integrity_instance');
  const prefix='final-integrity/'+plan.operationId+'/',metadata={operationid:plan.operationId,plansha256:planSha256};
  async function read(name:string){
    const object=await env.PROOFS.get(prefix+name);
    if(!object||!object.version||!object.etag||object.size<1||object.size>65536||
      Object.entries(metadata).some(([key,value])=>object.customMetadata?.[key]!==value))throw Error('final_integrity_proof_identity');
    const bytes=new Uint8Array(await object.arrayBuffer());if(bytes.length!==object.size)throw Error('final_integrity_proof_size');
    return{object,text:new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(bytes)};
  }
  async function put(name:string,value:unknown){
    try{
      const text=canonical(value),object=await env.PROOFS.put(prefix+name,text,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
      if(!object)throw Error('final_integrity_proof_conflict');const stored=await read(name);
      if(stored.text!==text||stored.object.version!==object.version||stored.object.etag!==object.etag)throw Error('final_integrity_proof_readback');
    }catch{throw Error('final_integrity_proof_unacknowledged');}
  }
  async function inspectReceipts(db:D1Database,count:number,current?:Receipt){
    const rows=(await db.prepare('SELECT * FROM beedle_restore_integrity ORDER BY kind').all()).results.map(row=>receiptSchema.parse(row));
    const source=rows.find(row=>row.kind==='source_sha');
    if(!source||!passed(source,plan)||await fingerprint(source)!==plan.sourceShaReceiptSha256||rows.length!==count+1+(current?1:0))
      throw Error('final_integrity_source_receipt_or_count');
    for(const kind of kinds.slice(0,count)){
      const row=rows.find(row=>row.kind===kind);if(!row||!passed(row,plan)||row.claim_token!==await token(plan.operationId,kind))throw Error('final_integrity_prior_receipt');
      const prior=z.object({version:z.literal(1),operationId:uuid,planSha256:hash,step:z.string(),result:z.object({kind:z.enum(kinds),rowSha256:hash}).passthrough()}).passthrough().parse(JSON.parse((await read(kind+'-outcome.json')).text));
      if(prior.operationId!==plan.operationId||prior.planSha256!==planSha256||prior.step!==kind||prior.result.kind!==kind||prior.result.rowSha256!==await fingerprint(row))
        throw Error('final_integrity_prior_outcome');
    }
    if(current&&canonical(rows.find(row=>row.kind===current.kind))!==canonical(current))throw Error('final_integrity_current_receipt');
  }
  const options={retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const};
  async function once<T>(name:string,run:(db:D1Database)=>Promise<T>){
    if(await env.PROOFS.get(prefix+name+'-claim.json'))throw Error('final_integrity_already_claimed');
    await put(name+'-claim.json',{version:1,operationId:plan.operationId,planSha256,step:name});
    const capacity=restorationCapacityObserver(),meter=restorationBindingBudget(restorationMeasuredDatabase(env.DB,capacity,'retained'));
    try{
      const now=Date.now();if(now<Date.parse(plan.notBefore)||now>=Date.parse(name==='initialize'?plan.notAfter:plan.completionDeadline))throw Error('final_integrity_window');
      const result=await run(meter.db);
      await put(name+'-outcome.json',{version:1,operationId:plan.operationId,planSha256,step:name,result,
        capacity:capacity.snapshot(),bindingCalls:meter.calls,at:new Date().toISOString()});return result;
    }catch(error){
      // A command or receipt may have committed before its acknowledgment was
      // lost. Preserve the exact D1/R2 claim; do not repair, reset or reissue SQL.
      try{await put(name+'-failure.json',{version:1,operationId:plan.operationId,planSha256,step:name,
        errorChain:await restorationErrorEvidence(error),bindingFailure:await meter.failureEvidence()??null,
        capacity:capacity.snapshot(),noRetry:true,at:new Date().toISOString()});}catch{/* Preserve the original claim. */}
      throw Error('final_integrity_held');
    }
  }
  await step.do('final-integrity-initialize-once',options,()=>once('initialize',async db=>{
    await inspectFrozen(db,plan);await inspectReceipts(db,0);return{frozenRevision:plan.corpusRevision};
  }));
  for(const[index,kind]of kinds.entries()){
    await step.do('final-integrity-'+kind+'-once',options,()=>once(kind,async db=>{
      await inspectFrozen(db,plan);await inspectReceipts(db,index);
      const claimToken=await token(plan.operationId,kind),command=partitionIntegrityCommands[kind],now=new Date().toISOString();
      const claimed=await db.prepare(`INSERT INTO beedle_restore_integrity(kind,owner_id,plan_sha256,corpus_revision,claim_token,command_sha256,status,engine_acknowledged,claimed_at)
        SELECT ?,?,?,?,?,?,'claimed',0,? WHERE EXISTS(SELECT 1 FROM beedle_restore_owner WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND phase='indexing')
        AND (SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1)=? AND NOT EXISTS(SELECT 1 FROM beedle_restore_integrity WHERE kind=?) RETURNING *`)
        .bind(kind,plan.source.ownerId,plan.source.planSha256,plan.corpusRevision,claimToken,command.sha256,now,
          plan.source.ownerId,plan.source.planSha256,plan.corpusRevision,kind).all();
      if(claimed.results.length!==1||claimed.meta.changes!==1)throw Error('final_integrity_claim_unacknowledged');
      const claim=receiptSchema.parse(claimed.results[0]);
      if(claim.kind!==kind||claim.owner_id!==plan.source.ownerId||claim.plan_sha256!==plan.source.planSha256||
        claim.corpus_revision!==plan.corpusRevision||claim.claim_token!==claimToken||claim.command_sha256!==command.sha256||
        claim.status!=='claimed'||claim.engine_acknowledged!==0||claim.result_sha256!==null||claim.completed_at!==null)
        throw Error('final_integrity_claim_identity');
      const checked=await db.prepare(command.command).all<Record<string,unknown>>();
      if(!checked.success||(kind==='physical'?(checked.results.length!==1||checked.results[0]?.quick_check!=='ok'):checked.results.length!==0))
        throw Error('final_integrity_engine_report_not_passed');
      await inspectFrozen(db,plan);await inspectReceipts(db,index,claim);
      const resultSha256=await fingerprint({kind,commandSha256:command.sha256,results:checked.results}),completed=new Date().toISOString();
      const finalized=await db.prepare(`UPDATE beedle_restore_integrity SET status='passed',engine_acknowledged=1,result_sha256=?,completed_at=?
        WHERE kind=? AND owner_id=? AND plan_sha256=? AND corpus_revision=? AND claim_token=? AND command_sha256=? AND status='claimed'
        AND (SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1)=? RETURNING *`)
        .bind(resultSha256,completed,kind,plan.source.ownerId,plan.source.planSha256,plan.corpusRevision,claimToken,command.sha256,plan.corpusRevision).all();
      if(finalized.results.length!==1||finalized.meta.changes!==1)throw Error('final_integrity_outcome_unacknowledged');
      const row=receiptSchema.parse(finalized.results[0]);if(!passed(row,plan)||row.kind!==kind||row.claim_token!==claimToken)throw Error('final_integrity_outcome_identity');
      await inspectFrozen(db,plan);await inspectReceipts(db,index,row);
      return{kind,rowSha256:await fingerprint(row),commandSha256:command.sha256,resultSha256,engineAcknowledged:true};
    }));
  }
  await step.do('final-integrity-complete-once',options,()=>once('complete',async db=>{
    await inspectFrozen(db,plan);await inspectReceipts(db,kinds.length);return{fullD1Checks:5,wholePartitionSourceReceiptPreserved:true};
  }));
  return{state:'five_full_D1_integrity_checks_passed',operationId:plan.operationId,corpusRevision:plan.corpusRevision,
    sourceByteReceiptPreserved:true,sourceReviewAndPublicationUnchanged:true,noSealPeakCapacityOrReleaseApproval:true};
}
