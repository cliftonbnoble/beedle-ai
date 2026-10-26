import { ensureVersionedVector, VectorVisibilityPending } from "./vector-generations";
import { vectorProfile } from "./vector-profile";
import { z } from "zod";
import type { Env } from "../lib/types";
import { RequestConflictError, RequestQuotaError, RequestValidationError } from "../lib/http";
import { canonicalJson, activationWriteSchema } from "./activation-contract";
import { writeTrustedRetrievalActivation } from "./retrieval-activation";
import { buildDocumentReferenceValidationStatements, loadReferenceLookups } from "./legal-references";
import { withDocumentRevision } from "./document-revision";
import { catalogState } from "./legal-catalog";

const identifier = z.string().min(1).max(200).regex(/^[A-Za-z0-9_.:-]+$/);
const requestSchema = z.object({
  jobId: identifier.optional(), limit: z.number().int().min(1).max(5000).default(500),
  // OFFSET cannot describe a stable selection while documents are being added.
  offset: z.literal(0).default(0), after: z.string().max(256).default(""),
  batchSize: z.number().int().min(1).max(100).optional(),
  includeDocumentChunks: z.boolean().default(true), includeTrustedChunks: z.boolean().default(true),
  dryRun: z.boolean().default(false)
}).strict();
type Job = { id:string; kind:"vector_backfill"|"reference_backfill"|"activation"; state:string; payload_json:string; dry_run:number; owner_token:string|null; lease_expires_at:string|null; cancel_requested:number; failures:number; next_attempt_at:string|null; last_error:string|null; result_json:string|null; created_at:string; updated_at:string };
type Item = { job_id:string; item_key:string; document_id:string; document_revision:number; source_kind:string; payload_json:string; state:string; visibility_checks:number };
const now=()=>new Date().toISOString();
const leaseMs=120_000;
const ownerWhere="id=? AND owner_token=? AND state='running' AND cancel_requested=0 AND lease_expires_at>?";
const itemOwner=`EXISTS (SELECT 1 FROM maintenance_jobs WHERE id=? AND owner_token=? AND state='running' AND lease_expires_at>?)`;
const workSize=4;

async function readJob(env:Env,id:string) {
  const row=await env.DB.prepare("SELECT * FROM maintenance_jobs WHERE id=?").bind(identifier.parse(id)).first<Job>();
  if(!row) throw new RequestValidationError("Maintenance job not found");
  return row;
}
export async function inspectMaintenanceJob(env:Env,id:string) {
  const job=await readJob(env,id);
  const counts=(await env.DB.prepare("SELECT state,COUNT(*) AS count FROM maintenance_job_items WHERE job_id=? GROUP BY state").bind(id).all<{state:string;count:number}>()).results||[];
  const count=(state:string)=>counts.find(row=>row.state===state)?.count||0;
  const issues=(await env.DB.prepare("SELECT item_key AS itemKey,document_id AS documentId,document_revision AS expectedRevision,last_error AS reason FROM maintenance_job_items WHERE job_id=? AND state='skipped' ORDER BY item_key LIMIT 20").bind(id).all()).results||[];
  const last=await env.DB.prepare("SELECT item_key AS itemKey FROM maintenance_job_items WHERE job_id=? ORDER BY item_key DESC LIMIT 1").bind(id).first<{itemKey:string}>();
  const itemCounts={total:counts.reduce((sum,row)=>sum+row.count,0),pending:count('pending'),submitted:count('submitted'),completed:count('completed'),skipped:count('skipped')};
  if(job.kind==='activation') {
    const plan=JSON.parse(job.payload_json).plan;
    const progress=job.result_json?JSON.parse(job.result_json):null;
    itemCounts.total=plan.chunks.length;
    itemCounts.pending=job.state==='completed'?0:(progress?.pendingVectorCount ?? itemCounts.total);
    itemCounts.completed=itemCounts.total-itemCounts.pending;
  }
  return {jobId:id,kind:job.kind,state:job.state,dryRun:Boolean(job.dry_run),snapshotId:id,
    counts:itemCounts,issues,nextAfter:last?.itemKey??null,
    cancelRequested:Boolean(job.cancel_requested),failures:job.failures,nextAttemptAt:job.next_attempt_at,lastError:job.last_error,
    createdAt:job.created_at,updatedAt:job.updated_at,result:job.result_json?JSON.parse(job.result_json):null,
    statusUrl:`/admin/maintenance/jobs/${encodeURIComponent(id)}`,stepUrl:`/admin/maintenance/jobs/${encodeURIComponent(id)}/step`};
}

export async function createBackfillJob(env:Env,kind:"vector_backfill"|"reference_backfill",input:unknown) {
  const parsed=requestSchema.parse(input);
  if(kind==='vector_backfill'&&!parsed.includeDocumentChunks&&!parsed.includeTrustedChunks) throw new RequestValidationError("Select at least one vector source");
  if(parsed.after && !(kind==='vector_backfill'?/^(document|trusted):[A-Za-z0-9_.:-]{1,200}$/:/^[A-Za-z0-9_.:-]{1,200}$/).test(parsed.after)) throw new RequestValidationError("Invalid source keyset cursor");
  const id=parsed.jobId||`maintenance_${crypto.randomUUID()}`;
  // The catalog and model are immutable job inputs. A later configuration change
  // requires a fresh plan, rather than mixing reference/model versions mid-job.
  const payload={...parsed,jobId:id,kind,model:env.AI_EMBEDDING_MODEL ?? null,namespace:env.VECTOR_NAMESPACE ?? null,
    profileId:kind==='vector_backfill'?(await vectorProfile(env)).id:null,
    catalogVersion:kind==='reference_backfill'?(await catalogState(env)).version:null};
  const existing=await env.DB.prepare("SELECT payload_json FROM maintenance_jobs WHERE id=?").bind(id).first<{payload_json:string}>();
  if(existing) {
    if(existing.payload_json!==canonicalJson(payload)) throw new RequestConflictError("Job ID already identifies a different immutable plan");
    return inspectMaintenanceJob(env,id);
  }
  const timestamp=now();
  const insert=env.DB.prepare(`INSERT INTO maintenance_jobs(id,kind,payload_json,dry_run,created_at,updated_at) VALUES (?,?,?,?,?,?)`)
    .bind(id,kind,canonicalJson(payload),Number(parsed.dryRun),timestamp,timestamp);
  // Materialize the finite selection in one transaction. Subsequent steps keyset
  // over these immutable items, never OFFSET over a changing corpus. SQL keeps
  // the full manifest out of Worker memory; each delivery reads only four rows.
  let selection:D1PreparedStatement;
  if(kind==='reference_backfill') {
    selection=env.DB.prepare(`INSERT INTO maintenance_job_items(job_id,item_key,document_id,document_revision,source_kind,payload_json)
      SELECT ?,id,id,document_revision,'reference',json_object('indexCodes',json(index_codes_json),'rulesSections',json(rules_sections_json),'ordinanceSections',json(ordinance_sections_json))
      FROM documents WHERE id>? AND ingestion_state IN ('ready','legacy') ORDER BY id LIMIT ?`).bind(id,parsed.after,parsed.limit);
  } else {
    selection=env.DB.prepare(`INSERT INTO maintenance_job_items(job_id,item_key,document_id,document_revision,source_kind,payload_json)
      SELECT ?,itemKey,documentId,revision,sourceKind,payload FROM (
        SELECT * FROM (SELECT 'document:'||c.id AS itemKey,c.document_id AS documentId,d.document_revision AS revision,'document_chunk' AS sourceKind,
          json_object('chunkId',c.id,'sourceText',CASE WHEN length(c.chunk_text)<=20000 THEN c.chunk_text ELSE '' END,'sectionLabel',c.section_label,'paragraphAnchor',c.paragraph_anchor,'citationAnchor',c.citation_anchor,'generation',d.active_text_generation) AS payload
        FROM document_chunks c JOIN documents d ON d.id=c.document_id
        WHERE ?=1 AND c.id>? AND d.searchable_at IS NOT NULL AND d.rejected_at IS NULL AND d.ingestion_state='ready' ORDER BY c.id LIMIT ?)
        UNION ALL
        SELECT * FROM (SELECT 'trusted:'||c.chunk_id,c.document_id,d.document_revision,'trusted_chunk',
          json_object('chunkId',c.chunk_id,'sourceText',CASE WHEN length(c.chunk_text)<=20000 THEN c.chunk_text ELSE '' END,'sectionLabel',c.section_label,'paragraphAnchor',c.paragraph_anchor,'citationAnchor',c.citation_anchor,'batchId',c.batch_id)
        FROM retrieval_search_chunks c JOIN documents d ON d.id=c.document_id
        WHERE ?=1 AND c.chunk_id>? AND c.active=1 AND d.searchable_at IS NOT NULL AND d.rejected_at IS NULL AND d.ingestion_state='ready' ORDER BY c.chunk_id LIMIT ?)
      ) ORDER BY itemKey LIMIT ?`).bind(id,Number(parsed.includeDocumentChunks && !parsed.after.startsWith('trusted:')),parsed.after.startsWith('document:')?parsed.after.slice(9):'',parsed.limit,Number(parsed.includeTrustedChunks),parsed.after.startsWith('trusted:')?parsed.after.slice(8):'',parsed.limit,parsed.limit);
  }
  try { await env.DB.batch([insert,selection,
    env.DB.prepare(`UPDATE maintenance_jobs SET state=CASE WHEN dry_run=1 OR NOT EXISTS(SELECT 1 FROM maintenance_job_items WHERE job_id=?) THEN 'completed' ELSE 'queued' END WHERE id=?`).bind(id,id)]); }
  catch(error) {
    // Simultaneous identical submissions race on the primary key; reconcile the
    // committed plan, without adding items to somebody else's selection.
    const replay=await env.DB.prepare("SELECT payload_json FROM maintenance_jobs WHERE id=?").bind(id).first<{payload_json:string}>();
    if(!replay||replay.payload_json!==canonicalJson(payload)) throw error;
  }
  return inspectMaintenanceJob(env,id);
}

export async function createActivationJob(env:Env,input:unknown) {
  const parsed=activationWriteSchema.parse(input);
  const validation=await writeTrustedRetrievalActivation(env,{...parsed,dryRun:true});
  if(parsed.dryRun) return validation;
  const id=`activation:${parsed.plan.batchId}`;
  const payload=canonicalJson(parsed);
  await env.DB.prepare(`INSERT INTO maintenance_jobs(id,kind,payload_json,dry_run,created_at,updated_at)
    VALUES (?,'activation',?,0,?,?) ON CONFLICT(id) DO NOTHING`).bind(id,payload,now(),now()).run();
  const job=await readJob(env,id);
  if(job.payload_json!==payload) throw new RequestConflictError("Activation job already belongs to a different immutable plan");
  return inspectMaintenanceJob(env,id);
}

async function assertOwner(env:Env,id:string,token:string) {
  const result=await env.DB.prepare(`UPDATE maintenance_jobs SET lease_expires_at=?,updated_at=? WHERE ${ownerWhere}`)
    .bind(new Date(Date.now()+leaseMs).toISOString(),now(),id,token,now()).run();
  if(Number(result.meta.changes)!==1) throw new RequestConflictError("Maintenance job ownership changed or cancellation requested");
}
async function checkpoint(env:Env,item:Item,token:string,state:string) {
  const result=await env.DB.prepare(`UPDATE maintenance_job_items SET state=?,last_error=CASE WHEN ?='skipped' THEN 'Source revision changed, unavailable, or exceeds text limit' ELSE NULL END WHERE job_id=? AND item_key=? AND ${itemOwner}`)
    .bind(state,state,item.job_id,item.item_key,item.job_id,token,now()).run();
  if(Number(result.meta.changes)!==1) throw new RequestConflictError("Maintenance checkpoint ownership changed");
}
async function sourceCurrent(env:Env,item:Item,payload:Record<string,unknown>) {
  const doc=await env.DB.prepare("SELECT document_revision AS revision,ingestion_state AS state,searchable_at AS searchable,rejected_at AS rejected,active_text_generation AS generation FROM documents WHERE id=?")
    .bind(item.document_id).first<{revision:number;state:string;searchable:string|null;rejected:string|null;generation:string|null}>();
  if(!doc||doc.revision!==item.document_revision) return false;
  if(item.source_kind==='reference') return doc.state==='ready'||doc.state==='legacy';
  if(doc.state!=='ready') return false;
  if(!doc.searchable||doc.rejected) return false;
  const query=item.source_kind==='document_chunk'
    ? "SELECT chunk_text AS text FROM document_chunks WHERE id=? AND document_id=?"
    : "SELECT chunk_text AS text FROM retrieval_search_chunks WHERE chunk_id=? AND document_id=? AND active=1 AND batch_id=?";
  const row=await env.DB.prepare(query).bind(payload.chunkId,item.document_id,...(item.source_kind==='trusted_chunk'?[payload.batchId]:[])).first<{text:string}>();
  return row?.text===payload.sourceText&&(item.source_kind!=='document_chunk'||doc.generation===payload.generation);
}
async function vectorItem(env:Env,job:Job,item:Item,token:string,deadlineAt:number) {
  const payload=JSON.parse(item.payload_json);
  if(!payload.sourceText||!await sourceCurrent(env,item,payload)) {await checkpoint(env,item,token,'skipped');return;}
  if(item.visibility_checks>=60) throw new RequestConflictError("Submitted vectors remain invisible after 60 checks; inspect provider state before a fresh plan");
  await env.DB.prepare(`UPDATE maintenance_job_items SET visibility_checks=visibility_checks+1 WHERE job_id=? AND item_key=? AND ${itemOwner}`).bind(job.id,item.item_key,job.id,token,now()).run();
  try {
    await ensureVersionedVector(env,{documentId:item.document_id,chunkId:payload.chunkId,sourceKind:item.source_kind==='trusted_chunk'?'retrieval':'document',
      generation:payload.batchId||payload.generation,text:payload.sourceText,metadata:{sectionLabel:payload.sectionLabel,paragraphAnchor:payload.paragraphAnchor,citationAnchor:payload.citationAnchor}},async()=>{
        await assertOwner(env,job.id,token);
        if(!await sourceCurrent(env,item,payload)) throw new RequestConflictError("Maintenance source changed during embedding");
      },{deadlineAt});
    await checkpoint(env,item,token,await sourceCurrent(env,item,payload)?'completed':'skipped');
  }catch(error) {
    if(error instanceof VectorVisibilityPending) {await checkpoint(env,item,token,'submitted');return;}
    throw error;
  }
}

async function settleAbandonedCancellations(env:Env,id?:string) {
  await env.DB.prepare(`UPDATE maintenance_jobs SET state='cancelled',owner_token=NULL,lease_expires_at=NULL,updated_at=?
    WHERE cancel_requested=1 AND state!='completed' AND (owner_token IS NULL OR lease_expires_at IS NULL OR lease_expires_at<=?) ${id?'AND id=?':''}`)
    .bind(now(),now(),...(id?[id]:[])).run();
}

export async function stepMaintenanceJob(env:Env,id:string) {
  identifier.parse(id);
  await settleAbandonedCancellations(env,id);
  const token=crypto.randomUUID();
  await env.DB.prepare(`UPDATE maintenance_jobs SET state='failed',failures=5,owner_token=NULL,lease_expires_at=NULL,last_error='Abandoned step retry budget exhausted',updated_at=?
    WHERE id=? AND state='running' AND lease_expires_at<=? AND failures>=4`).bind(now(),id,now()).run();
  const claimed=await env.DB.prepare(`UPDATE maintenance_jobs SET failures=failures+CASE WHEN state='running' THEN 1 ELSE 0 END,state='running',owner_token=?,lease_expires_at=?,updated_at=?
    WHERE id=? AND cancel_requested=0 AND failures<5 AND (next_attempt_at IS NULL OR next_attempt_at<=?)
      AND (state IN ('queued','retryable') OR (state='running' AND lease_expires_at<=?))`)
    .bind(token,new Date(Date.now()+leaseMs).toISOString(),now(),identifier.parse(id),now(),now()).run();
  if(Number(claimed.meta.changes)!==1) return inspectMaintenanceJob(env,id);
  const job=await readJob(env,id);
  try {
    const payload=JSON.parse(job.payload_json);
    let result:unknown=null;
    let complete=false;
    if(job.kind==='activation') {
      const report=await writeTrustedRetrievalActivation(env,payload);
      complete=report.state==='published'||report.state==='rolled_back';result=report;
    } else {
      if(job.kind==='vector_backfill'&&(payload.profileId!==(await vectorProfile(env)).id)) throw new RequestConflictError("Embedding model/namespace changed; prepare a new job");
      const items=(await env.DB.prepare("SELECT * FROM maintenance_job_items WHERE job_id=? AND state IN ('pending','submitted') ORDER BY item_key LIMIT ?").bind(id,workSize).all<Item>()).results||[];
      if(job.kind==='vector_backfill') {
        // Observe all started work before releasing its lease, even if one fails.
        const outcomes=await Promise.allSettled(items.map(item=>vectorItem(env,job,item,token,Date.now()+55_000)));
        for(const outcome of outcomes) if(outcome.status==='rejected') throw outcome.reason;
      } else {
        const lookups=await loadReferenceLookups(env);
        if(lookups.catalogVersion!==payload.catalogVersion) throw new RequestConflictError("Reference catalog changed; prepare a new job");
        for(const item of items) {
          await assertOwner(env,id,token);
          const data=JSON.parse(item.payload_json);
          if(!await sourceCurrent(env,item,data)) {await checkpoint(env,item,token,'skipped');continue;}
          const statements=await buildDocumentReferenceValidationStatements(env,item.document_id,data,lookups);
          // Reference publication and checkpoint share the revision transaction.
          const guard=crypto.randomUUID();
          await withDocumentRevision(env,item.document_id,item.document_revision,[
            env.DB.prepare("INSERT INTO maintenance_revision_guards(token,job_id,owner_token) VALUES (?,?,?)").bind(guard,id,token),...statements,
            env.DB.prepare(`UPDATE maintenance_job_items SET state='completed' WHERE job_id=? AND item_key=? AND ${itemOwner}`).bind(id,item.item_key,id,token,now()),env.DB.prepare("DELETE FROM maintenance_revision_guards WHERE token=?").bind(guard)]);
        }
      }
      complete=!await env.DB.prepare("SELECT 1 FROM maintenance_job_items WHERE job_id=? AND state IN ('pending','submitted') LIMIT 1").bind(id).first();
    }
    await env.DB.prepare(`UPDATE maintenance_jobs SET state=?,result_json=?,owner_token=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=? WHERE ${ownerWhere}`)
      .bind(complete?'completed':'queued',result?JSON.stringify(result):null,now(),id,token,now()).run();
  } catch(error) {
    const quota=error instanceof RequestQuotaError;
    const terminal=error instanceof RequestConflictError||(!quota && job.failures+1>=5);
    await env.DB.prepare(`UPDATE maintenance_jobs SET state=?,failures=failures+?,next_attempt_at=?,last_error=?,owner_token=NULL,lease_expires_at=NULL,updated_at=?
      WHERE id=? AND owner_token=? AND state='running'`)
      .bind(terminal?'failed':'retryable',quota?0:1,new Date(Date.now()+(quota?error.retryAfterSeconds:Math.min(60*2**job.failures,3600))*1000).toISOString(),
        quota?'Application compute budget reached':error instanceof RequestConflictError?error.message.slice(0,220):terminal?'Retry budget exhausted; inspect before retry':'Provider or maintenance step failed; completed checkpoints retained',now(),id,token).run();
  } finally {
    await env.DB.prepare(`UPDATE maintenance_jobs SET state='cancelled',owner_token=NULL,lease_expires_at=NULL,updated_at=? WHERE id=? AND cancel_requested=1 AND (owner_token IS NULL OR owner_token=? OR lease_expires_at<=?)`).bind(now(),id,token,now()).run();
  }
  return inspectMaintenanceJob(env,id);
}

export async function controlMaintenanceJob(env:Env,id:string,action:'cancel'|'retry') {
  await readJob(env,id);
  if(action==='cancel') await env.DB.prepare(`UPDATE maintenance_jobs SET cancel_requested=1,state=CASE WHEN state='running' AND lease_expires_at>? THEN state ELSE 'cancelled' END,updated_at=? WHERE id=? AND state!='completed'`).bind(now(),now(),id).run();
  else await env.DB.prepare(`UPDATE maintenance_jobs SET state='queued',failures=0,cancel_requested=0,next_attempt_at=NULL,last_error=NULL,owner_token=NULL,lease_expires_at=NULL,updated_at=? WHERE id=? AND state IN ('failed','retryable','cancelled') AND (lease_expires_at IS NULL OR lease_expires_at<=?)`).bind(now(),id,now()).run();
  return inspectMaintenanceJob(env,id);
}
export async function runScheduledMaintenance(env:Env) {
  await settleAbandonedCancellations(env);
  const jobs=(await env.DB.prepare(`SELECT id FROM maintenance_jobs WHERE cancel_requested=0 AND failures<5 AND (next_attempt_at IS NULL OR next_attempt_at<=?)
    AND (state IN ('queued','retryable') OR (state='running' AND lease_expires_at<=?)) ORDER BY created_at,id LIMIT 2`).bind(now(),now()).all<{id:string}>()).results||[];
  await Promise.allSettled(jobs.map(job=>stepMaintenanceJob(env,job.id)));
}
