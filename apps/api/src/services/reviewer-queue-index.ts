import type { DatabaseEnv } from "../lib/types";
import type { buildReviewerRows } from "./reviewer-read-model";
import { APPROVAL_POLICY_VERSION } from "./approval-policy";
import { sha256Bytes } from "../lib/digests";
import { RequestConflictError, RequestValidationError } from "../lib/http";

export const REVIEWER_POLICY_VERSION = `reviewer-queue-3/taxonomy-evidence-2/mutation-bounds-1/${APPROVAL_POLICY_VERSION}`;
export type ReviewerDocument = Omit<Awaited<ReturnType<typeof buildReviewerRows>>[number],
  "metadataJson"|"extractionWarningsJson"|"indexCodesJson"|"rulesSectionsJson"|"ordinanceSectionsJson"|"unresolvedBytes"> & {triageGroupKey:string;peerCount:number};
type Control = {epoch:number;policy_version:string;catalog_version:string;activeCatalog:string;phase:string;after_id:string;upper_id:string;lease_owner:string|null;lease_until:number;last_token:string|null;pending:number;sourceCount:number;indexedCount:number};

export async function inspectReviewerQueue(env:DatabaseEnv) {
  const state = await env.DB.prepare(`SELECT c.*,(SELECT active_version FROM legal_catalog_state WHERE singleton=1) AS activeCatalog,
    (SELECT COUNT(*) FROM reviewer_queue_sources WHERE dirty=1) AS pending,
    (SELECT COUNT(*) FROM documents) AS sourceCount,(SELECT COUNT(*) FROM reviewer_queue_state) AS indexedCount FROM reviewer_queue_control c WHERE singleton=1`).first<Control>();
  if (!state) throw new Error("Reviewer queue schema is incomplete");
  return {...state,ready:state.phase==='ready' && state.policy_version===REVIEWER_POLICY_VERSION && state.catalog_version===state.activeCatalog && state.pending===0 && state.sourceCount===state.indexedCount};
}

export async function advanceReviewerQueue(env:DatabaseEnv,loadRows:typeof buildReviewerRows) {
  const owner=crypto.randomUUID();
  const claim=await env.DB.prepare(`UPDATE reviewer_queue_control SET lease_owner=?,lease_until=unixepoch()+60
    WHERE singleton=1 AND lease_until<=unixepoch() RETURNING singleton`).bind(owner).all();
  if (!claim.results.length) return {...await inspectReviewerQueue(env),busy:true};
  try {
    let state=await inspectReviewerQueue(env);
    if (state.policy_version!==REVIEWER_POLICY_VERSION || state.catalog_version!==state.activeCatalog || (state.phase==='ready' && state.pending===0 && state.sourceCount!==state.indexedCount)) {
      await env.DB.prepare(`UPDATE reviewer_queue_control SET policy_version=?,catalog_version=?,phase='backfill',after_id='',
        upper_id=COALESCE((SELECT MAX(id) FROM documents),'') WHERE singleton=1 AND lease_owner=?`)
        .bind(REVIEWER_POLICY_VERSION,state.activeCatalog,owner).run();
      state=await inspectReviewerQueue(env);
    }
    const sources=(await env.DB.prepare(state.phase==='backfill'
      ? `SELECT d.id,COALESCE(s.version,0) AS version FROM documents d LEFT JOIN reviewer_queue_sources s ON s.document_id=d.id
         WHERE d.id>? AND d.id<=? ORDER BY d.id LIMIT 25`
      : `SELECT document_id AS id,version FROM reviewer_queue_sources WHERE dirty=1 ORDER BY document_id LIMIT 25`)
      .bind(...(state.phase==='backfill'?[state.after_id,state.upper_id]:[])).all<{id:string;version:number}>()).results;
    const rows=await loadRows(env,sources.map(source=>source.id));
    if (rows.length!==sources.length) throw new RequestConflictError("Reviewer sources changed during materialization; retry the bounded step.");
    const versions=new Map(sources.map(source=>[source.id,source.version]));
    const statements:D1PreparedStatement[]=[env.DB.prepare('INSERT INTO reviewer_queue_guards(token,owner,catalog,sources_json) VALUES(?,?,?,?)')
      .bind(owner,owner,state.activeCatalog,JSON.stringify(sources))];
    for (const row of rows) {
      if (!Number.isFinite(row.extractionConfidence))
        throw new RequestValidationError("Stored extraction confidence is invalid; repair the source before refreshing the reviewer queue.");
      const key=await sha256Bytes(new TextEncoder().encode(JSON.stringify([(row.unresolvedBuckets||[]).slice().sort(),(row.recurringCitationFamilies||[]).slice().sort()])));
      // Raw parser metadata/serialized duplicates are unnecessary for queue policy or the UI.
      const payload:Record<string,unknown>={...row,triageGroupKey:key};
      for(const field of ['metadataJson','extractionWarningsJson','indexCodesJson','rulesSectionsJson','ordinanceSectionsJson','unresolvedBytes'])delete payload[field];
      const encoded=JSON.stringify(payload);
      if(new TextEncoder().encode(encoded).length>64*1024)throw new RequestValidationError("A reviewer row exceeds 64 KiB; inspect its reference/warning data before rebuilding the queue.");
      statements.push(env.DB.prepare(`INSERT INTO reviewer_queue_state(id,source_version,policy_version,catalog_version,created_at,title,
        confidence,warning_count,unresolved_count,critical_count,approval_score,reviewer_ready,risk_rank,effort_rank,group_key,batch_key,batch_safe,payload_json)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET source_version=excluded.source_version,
        policy_version=excluded.policy_version,catalog_version=excluded.catalog_version,created_at=excluded.created_at,title=excluded.title,
        confidence=excluded.confidence,warning_count=excluded.warning_count,unresolved_count=excluded.unresolved_count,critical_count=excluded.critical_count,
        approval_score=excluded.approval_score,reviewer_ready=excluded.reviewer_ready,risk_rank=excluded.risk_rank,effort_rank=excluded.effort_rank,
        group_key=excluded.group_key,batch_key=excluded.batch_key,batch_safe=excluded.batch_safe,payload_json=excluded.payload_json`)
        .bind(row.id,versions.get(row.id),REVIEWER_POLICY_VERSION,state.activeCatalog,row.createdAt,row.title,row.extractionConfidence??-1,
          row.warningCount,row.unresolvedReferenceCount,row.criticalExceptionCount,row.approvalReadiness.score,Number(row.reviewerReady),
          {low:3,medium:2,high:1}[row.reviewerRiskLevel],{low:1,medium:2,high:3}[row.estimatedReviewerEffort],key,row.blocked37xBatchKey||'',
          Number(row.blocked37xSafeToBatchReview),encoded));
    }
    statements.push(env.DB.prepare(`UPDATE reviewer_queue_sources SET dirty=0 WHERE EXISTS(SELECT 1 FROM json_each(?) j
      WHERE json_extract(j.value,'$.id')=reviewer_queue_sources.document_id AND json_extract(j.value,'$.version')=reviewer_queue_sources.version)`)
      .bind(JSON.stringify(sources)));
    const after=sources.at(-1)?.id||state.after_id;
    const phase=state.phase==='backfill'&&sources.length===25?'backfill':'ready';
    statements.push(env.DB.prepare(`UPDATE reviewer_queue_control SET after_id=?,phase=?,last_token=? WHERE singleton=1 AND lease_owner=?`)
      .bind(after,phase,owner,owner),env.DB.prepare('DELETE FROM reviewer_queue_guards WHERE token=?').bind(owner));
    try {await env.DB.batch(statements);}
    catch(error) {
      const afterState=await inspectReviewerQueue(env);
      if(afterState.last_token===owner)return {...afterState,visited:rows.length,reconciled:true};
      if(String(error).includes('reviewer_queue_changed'))throw new RequestConflictError("Reviewer sources or catalog changed; retry the bounded index step.");
      throw error;
    }
    return {...await inspectReviewerQueue(env),visited:rows.length};
  } finally {
    await env.DB.prepare('UPDATE reviewer_queue_control SET lease_owner=NULL,lease_until=0 WHERE singleton=1 AND lease_owner=?').bind(owner).run().catch(()=>{});
  }
}
