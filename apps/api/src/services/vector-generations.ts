import { projectVectorFilterMetadata, vectorEntryFilterProjection, stripVectorFilterMetadata, VECTOR_FILTER_VERSION, vectorMetadataIndexes, vectorScopeMetadataMatches } from "./vector-filter-metadata";
import { acquireVectorSync, releaseVectorSync, observeVectorMetadata, submitVectorMetadata } from "./vector-sync-lock";
import { publishedDocumentSql } from "./publication-policy";
import type { Env } from "../lib/types";
import { RequestConflictError } from "../lib/http";
import { withinProviderDeadline, type OperationContext } from "../lib/provider-operation";
import { sha256Bytes } from "../lib/digests";
import { reserveComputeBudget, computeBudgetBatchLimit } from "./compute-budget";
import { CHUNKER_VERSION, RETRIEVAL_CHUNKER_VERSION, vectorProfile, type VectorProfile } from "./vector-profile";
import { embedBatchWithOutcome } from "./embeddings";

export class VectorVisibilityPending extends Error {}
export type VectorSource={documentId:string;chunkId:string;sourceKind:'document'|'retrieval';generation:string;text:string;sourceSha256?:string;metadata?:Record<string,VectorizeVectorMetadata>};
export type VectorEntry={vector_id:string;profile_id:string;document_id:string;source_kind:'document'|'retrieval';source_generation:string;chunk_id:string;chunk_sha256:string;chunk_text:string;logical_key:string;source_sha256:string;parser_version:string;chunker_version:string;prepared_revision:number;section_label:string;filter_json:string;state:string;values_json:string|null;metadata_live:number;obsolete_at:string|null};
const now=()=>new Date().toISOString();
const liveSql=`EXISTS (SELECT 1 FROM documents d WHERE d.id=e.document_id AND ${publishedDocumentSql('d')} AND json_extract(d.metadata_json,'$.sourceSha256')=e.source_sha256
  AND (e.source_kind='document' AND d.active_text_generation=e.source_generation AND EXISTS(SELECT 1 FROM document_chunks c WHERE c.id=e.chunk_id AND c.document_id=d.id AND c.chunk_text=e.chunk_text)
    OR e.source_kind='retrieval' AND d.active_retrieval_batch=e.source_generation AND EXISTS(SELECT 1 FROM retrieval_search_chunks c WHERE c.chunk_id=e.chunk_id AND c.document_id=d.id AND c.active=1 AND c.chunk_text=e.chunk_text)))`;
async function registerProfile(env:Env,profile:VectorProfile) {
  await env.DB.prepare(`INSERT INTO vector_profiles(id,namespace,environment,model,dimensions,pipeline_version,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`)
    .bind(profile.id,profile.namespace,profile.environment,profile.model,profile.dimensions,profile.pipelineVersion,now()).run();
}
export async function prepareVectorEntry(env:Env,source:VectorSource) {
  const profile=await vectorProfile(env);
  const document=await env.DB.prepare("SELECT document_revision AS revision,metadata_json AS metadata,source_r2_key AS sourceKey FROM documents WHERE id=?").bind(source.documentId)
    .first<{revision:number;metadata:string|null;sourceKey:string}>();
  if(!document) throw new RequestConflictError("Vector source document no longer exists");
  let parsed:unknown;try{parsed=JSON.parse(document.metadata||'{}');}catch{}
  const metadata=parsed&&typeof parsed==='object'&&!Array.isArray(parsed)?parsed as Record<string,unknown>:{};
  const extraction=metadata.extraction&&typeof metadata.extraction==='object'&&!Array.isArray(metadata.extraction)?metadata.extraction as Record<string,unknown>:{};
  const chunkHash=await sha256Bytes(new TextEncoder().encode(source.text));
  const sourceHash=source.sourceSha256||metadata.sourceSha256;
  if(typeof sourceHash!=='string'||!/^[a-f0-9]{64}$/.test(sourceHash)) throw new RequestConflictError("Vector source requires captured source provenance");
  const sectionLabel=typeof source.metadata?.sectionLabel==='string'?source.metadata.sectionLabel:
    (await env.DB.prepare(source.sourceKind==='document'?"SELECT section_label FROM document_chunks WHERE id=? AND document_id=?":"SELECT section_label FROM retrieval_search_chunks WHERE chunk_id=? AND document_id=?")
      .bind(source.chunkId,source.documentId).first<{section_label:string}>())?.section_label||'';
  const parser=typeof extraction.parser==='string'?extraction.parser:'legacy-parser-unverified';
  const chunker=source.sourceKind==='retrieval'?RETRIEVAL_CHUNKER_VERSION:CHUNKER_VERSION;
  const digest=await sha256Bytes(new TextEncoder().encode(JSON.stringify([profile.id,source.documentId,source.sourceKind,source.generation,source.chunkId,sourceHash,chunkHash,parser,chunker])));
  // A physical ID is never reused after GC claims it. Even an acknowledged
  // deletion that becomes visible late cannot erase a restored incarnation.
  const inflight=await env.DB.prepare("SELECT vector_id,submitted_at FROM vector_entries WHERE logical_key=? AND state='submitting'").bind(digest).first<{vector_id:string;submitted_at:string|null}>();
  if(inflight) {
    if(Date.parse(inflight.submitted_at||'')>Date.now()-120_000) throw new VectorVisibilityPending("Another writer is submitting this generation");
    await env.DB.prepare("UPDATE vector_entries SET state='uncertain',obsolete_at=COALESCE(obsolete_at,?) WHERE vector_id=? AND state='submitting'").bind(now(),inflight.vector_id).run();
  }
  const cached=await env.DB.prepare("SELECT values_json FROM vector_entries WHERE logical_key=? AND state='uncertain' AND values_json IS NOT NULL ORDER BY created_at DESC LIMIT 1").bind(digest).first<{values_json:string}>();
  const active=await env.DB.prepare("SELECT vector_id FROM vector_entries WHERE logical_key=? AND state NOT IN ('uncertain','deleting','deleted')").bind(digest).first<{vector_id:string}>();
  const id=active?.vector_id||`v_${(await sha256Bytes(new TextEncoder().encode(digest+crypto.randomUUID()))).slice(0,62)}`;
  await registerProfile(env,profile);
  await env.DB.prepare(`INSERT INTO vector_entries(vector_id,profile_id,logical_key,document_id,source_kind,source_generation,chunk_id,chunk_sha256,chunk_text,source_sha256,parser_version,chunker_version,prepared_revision,state,created_at,values_json,section_label)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'prepared',?,?,?) ON CONFLICT DO NOTHING`)
    .bind(id,profile.id,digest,source.documentId,source.sourceKind,source.generation,source.chunkId,chunkHash,source.text,sourceHash,parser,chunker,document.revision,now(),cached?.values_json??null,sectionLabel).run();
  const entry=(await env.DB.prepare("SELECT * FROM vector_entries WHERE logical_key=? AND state NOT IN ('uncertain','deleting','deleted')").bind(digest).first<VectorEntry>())!;
  return {entry,profile,metadata:{...source.metadata,documentId:source.documentId,chunkId:source.chunkId,sourceKind:source.sourceKind,sourceGeneration:source.generation,profileId:profile.id,entryDigest:entry.vector_id,live:false}};
}
export async function ensureVersionedVectorBatch(env:Env,sources:VectorSource[],beforeSubmit:()=>Promise<void>=async()=>{},context:OperationContext={}) {
  if(!env.VECTOR_INDEX) throw new Error("Vector binding unavailable");
  if(!sources.length||sources.length>16) throw new Error("Vector delivery requires 1–16 sources");
  const prepared=await Promise.all(sources.map(source=>prepareVectorEntry(env,source)));
  const missing=prepared.map((value,index)=>({value,index})).filter(({value})=>value.entry.state==='prepared'&&!value.entry.values_json);
  const embedSize=computeBudgetBatchLimit(env,'embedding',4);
  for(let at=0;at<missing.length;at+=embedSize) {
    await beforeSubmit();
    const group=missing.slice(at,at+embedSize);
    const token=crypto.randomUUID();
    const ids=group.map(value=>value.value.entry.vector_id);
    await env.DB.prepare(`UPDATE vector_entries SET embedding_owner=?,embedding_lease_until=? WHERE vector_id IN(SELECT value FROM json_each(?))
      AND state='prepared' AND values_json IS NULL AND (embedding_owner IS NULL OR embedding_lease_until<=?)`)
      .bind(token,new Date(Date.now()+60_000).toISOString(),JSON.stringify(ids),now()).run();
    const owned=(await env.DB.prepare("SELECT vector_id FROM vector_entries WHERE embedding_owner=?").bind(token).all<{vector_id:string}>()).results||[];
    const claimed=group.filter(value=>owned.some(row=>row.vector_id===value.value.entry.vector_id));
    if(!claimed.length) continue;
    try {
      const outcomes=await embedBatchWithOutcome(env,claimed.map(({index})=>sources[index]!.text),context);
      const statements:D1PreparedStatement[]=[];
      for(let i=0;i<claimed.length;i++) {
        const outcome=outcomes[i]!;
        if(!outcome.vector) continue;
        const record=claimed[i]!.value;
        record.entry.values_json=JSON.stringify(outcome.vector);
        statements.push(env.DB.prepare("UPDATE vector_entries SET values_json=? WHERE vector_id=? AND state='prepared' AND embedding_owner=?").bind(record.entry.values_json,record.entry.vector_id,token));
      }
      if(statements.length) await env.DB.batch(statements);
      if(outcomes.some(outcome=>!outcome.vector)) throw new Error("Embedding unavailable or invalid for this profile");
    }finally {
      await env.DB.prepare("UPDATE vector_entries SET embedding_owner=NULL,embedding_lease_until=NULL WHERE embedding_owner=?").bind(token).run();
    }
  }
  const refreshed=(await env.DB.prepare("SELECT * FROM vector_entries WHERE vector_id IN(SELECT value FROM json_each(?))").bind(JSON.stringify(prepared.map(value=>value.entry.vector_id))).all<VectorEntry>()).results||[];
  for(const record of prepared) record.entry=refreshed.find(entry=>entry.vector_id===record.entry.vector_id)!;
  if(prepared.some(record=>record.entry.state==='submitting'||record.entry.state==='uncertain'||record.entry.state==='prepared'&&!record.entry.values_json)) throw new VectorVisibilityPending("A concurrent embedding/submission is still in progress");
  const pending=prepared.filter(value=>value.entry.state==='prepared');
  const upsertSize=computeBudgetBatchLimit(env,'vector_write',16);
  for(let at=0;at<pending.length;at+=upsertSize) {
    await beforeSubmit();
    const group=pending.slice(at,at+upsertSize);
    const vectors=group.map(({entry,profile,metadata})=>{
      const values:unknown=JSON.parse(entry.values_json||'null');
      if(!Array.isArray(values)||values.length!==profile.dimensions||!values.every(value=>typeof value==='number'&&Number.isFinite(value))) throw new Error("Cached embedding integrity failure");
      return {id:entry.vector_id,values,namespace:profile.namespace,metadata};
    });
    await reserveComputeBudget(env,'vector_write',vectors.length);
    const guard=crypto.randomUUID();
    try {
      await env.DB.batch([
        env.DB.prepare("INSERT INTO vector_submission_guards(token,expected_json) VALUES(?,?)").bind(guard,JSON.stringify(group.map(value=>value.entry.vector_id))),
        ...group.map(({entry})=>env.DB.prepare("UPDATE vector_entries SET state='submitting',submitted_at=? WHERE vector_id=? AND state='prepared'").bind(now(),entry.vector_id)),
        env.DB.prepare("DELETE FROM vector_submission_guards WHERE token=?").bind(guard)
      ]);
    }catch(error) {
      if(String(error).includes('vector_submission_conflict')) throw new VectorVisibilityPending("Another writer owns this submission");
      throw error;
    }
    try {
      await withinProviderDeadline(context,15_000,()=>env.VECTOR_INDEX!.upsert(vectors));
      await env.DB.batch(group.map(({entry})=>env.DB.prepare("UPDATE vector_entries SET state='submitted',values_json=NULL WHERE vector_id=? AND state='submitting'").bind(entry.vector_id)));
    }catch(error) {
      // Unknown acknowledgements never reuse a physical ID: a late initial
      // upsert with live=false must not overwrite a later live promotion.
      await env.DB.prepare("UPDATE vector_entries SET state='uncertain',obsolete_at=COALESCE(obsolete_at,?) WHERE vector_id IN(SELECT value FROM json_each(?)) AND state='submitting'")
        .bind(now(),JSON.stringify(group.map(value=>value.entry.vector_id))).run().catch(()=>{});
      throw error;
    }
    for(const record of group) record.entry.state='submitted';
  }
  const submitted=prepared.filter(value=>value.entry.state==='submitted');
  if(submitted.length) {
    const stored=await withinProviderDeadline(context,15_000,()=>env.VECTOR_INDEX!.getByIds(submitted.map(value=>value.entry.vector_id)));
    const visible=submitted.filter(({entry,profile,metadata})=>{
      const vector=stored.find(vector=>vector.id===entry.vector_id);
      return vector?.namespace===profile.namespace&&vector.metadata?.entryDigest===metadata.entryDigest&&vector.values.length===profile.dimensions&&Array.from(vector.values).every(Number.isFinite);
    });
    await beforeSubmit();
    if(visible.length) await env.DB.batch([
      ...visible.map(({entry})=>env.DB.prepare("UPDATE vector_entries SET state='verified',verified_at=? WHERE vector_id=? AND state='submitted'").bind(now(),entry.vector_id)),
      ...[...new Set(visible.map(value=>value.entry.document_id))].map(documentId=>env.DB.prepare(`INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES(?,?) ON CONFLICT(document_id) DO UPDATE SET dirty=1,revision=revision+1,updated_at=excluded.updated_at`).bind(documentId,now()))
    ]);
    for(const record of visible) record.entry.state='verified';
    if(visible.length!==submitted.length) throw new VectorVisibilityPending("Submitted vectors are not yet visible with their expected profile");
  }
  return prepared;
}
export async function ensureVersionedVector(env:Env,source:VectorSource,beforeSubmit:()=>Promise<void>=async()=>{},context:OperationContext={}) {
  return (await ensureVersionedVectorBatch(env,[source],beforeSubmit,context))[0]!;
}

// Receipt IDs are provider identities. Search/ranking continues to use D1 chunk
// identities, and never hydrates an obsolete or unknown provider match.
export async function resolveCurrentVectorMatches(env:Env,ids:string[],profileId:string) {
  if(!ids.length) return new Map<string,string>();
  const rows=(await env.DB.prepare(`SELECT e.vector_id,e.chunk_id FROM vector_entries e WHERE e.vector_id IN(SELECT value FROM json_each(?))
    AND e.profile_id=? AND e.state='verified' AND e.metadata_live=1 AND ${liveSql}`).bind(JSON.stringify(ids),profileId).all<{vector_id:string;chunk_id:string}>()).results||[];
  return new Map(rows.map(row=>[row.vector_id,row.chunk_id]));
}

export async function vectorLifecycleSnapshot(env:Env) {
  if(env.VECTOR_LIFECYCLE_READY!=='true') return {ready:false,epoch:-1};
  const profile=await vectorProfile(env);
  const row=await env.DB.prepare(`SELECT epoch.revision AS epoch,NOT EXISTS(SELECT 1 FROM vector_lifecycle_dirty s
    LEFT JOIN vector_profile_sync p ON p.document_id=s.document_id AND p.profile_id=?
    LEFT JOIN vector_filter_sync f ON f.document_id=s.document_id AND f.profile_id=p.profile_id WHERE p.revision IS NOT s.revision OR f.revision IS NOT s.revision OR f.projection_version IS NOT ${VECTOR_FILTER_VERSION}) AS ready
    FROM vector_lifecycle_epoch epoch WHERE singleton=1`).bind(profile.id).first<{epoch:number;ready:number}>();
  return {ready:Boolean(row?.ready),epoch:row?.epoch??-1};
}
export async function vectorLifecycleReady(env:Env) { return (await vectorLifecycleSnapshot(env)).ready; }

export async function synchronizeVectorLifecycle(env:Env,documentId?:string,recoverUncertain=false) {
  if(!env.VECTOR_INDEX) return {processed:0,pending:true};
  const metadataBatchSize=computeBudgetBatchLimit(env,'vector_write',16);
  const profile=await vectorProfile(env);
  await registerProfile(env,profile);
  const dirty=(await env.DB.prepare(`SELECT s.document_id,s.revision FROM vector_lifecycle_dirty s LEFT JOIN vector_profile_sync p ON p.document_id=s.document_id AND p.profile_id=? LEFT JOIN vector_filter_sync f ON f.document_id=s.document_id AND f.profile_id=p.profile_id WHERE (p.revision IS NOT s.revision OR f.revision IS NOT s.revision OR f.projection_version IS NOT ${VECTOR_FILTER_VERSION}) ${documentId?'AND s.document_id=?':''} ORDER BY s.updated_at,s.document_id LIMIT 4`)
    .bind(profile.id,...(documentId?[documentId]:[])).all<{document_id:string;revision:number}>()).results||[];
  let processed=0;
  for(const document of dirty) {
    const lock=await acquireVectorSync(env,profile,document.document_id,recoverUncertain);
    if(!lock) continue;
    try {
    if(!await observeVectorMetadata(env,lock)) continue;
    const current=await env.DB.prepare("SELECT revision FROM vector_lifecycle_dirty WHERE document_id=?").bind(document.document_id).first<{revision:number}>();
    if(!current) continue;
    document.revision=current.revision;
    const projection=await projectVectorFilterMetadata(env,document.document_id,document.revision);
    const docDigest=projection?await sha256Bytes(new TextEncoder().encode(JSON.stringify(projection))):'deleted';
    const repair=(await env.DB.prepare(`SELECT e.* FROM vector_entries e WHERE e.document_id=? AND e.profile_id=? AND ${liveSql}
      AND (e.state IN ('prepared','submitted') OR (e.state IN ('uncertain','deleted','deleting') AND NOT EXISTS(SELECT 1 FROM vector_entries newer WHERE newer.logical_key=e.logical_key AND newer.state NOT IN ('uncertain','deleting','deleted'))))
      ORDER BY e.vector_id LIMIT 4`).bind(document.document_id,profile.id).all<VectorEntry>()).results||[];
    if(repair.length) {
      const outcomes=await Promise.allSettled(repair.map(entry=>ensureVersionedVector(env,{documentId:entry.document_id,chunkId:entry.chunk_id,sourceKind:entry.source_kind,generation:entry.source_generation,text:entry.chunk_text,sourceSha256:entry.source_sha256},async()=>{
        const current=await env.DB.prepare(`SELECT 1 FROM vector_entries e WHERE e.vector_id=? AND ${liveSql}`).bind(entry.vector_id).first();
        if(!current) throw new RequestConflictError("Vector source changed during lifecycle repair");
      },{deadlineAt:Date.now()+20_000})));
      for(const outcome of outcomes) if(outcome.status==='rejected') throw outcome.reason;
      return {processed,pending:true,repaired:repair.length};
    }
    // Metadata updates replace a vector in full, so load and preserve its values
    // and provenance. Only reconcile this configured embedding space.
    const rows=(await env.DB.prepare(`SELECT e.*,CASE WHEN ${liveSql} THEN 1 ELSE 0 END AS desired_live FROM vector_entries e
      WHERE e.document_id=? AND e.profile_id=? AND e.state='verified' AND (e.metadata_live!=(CASE WHEN ${liveSql} THEN 1 ELSE 0 END) OR (NOT (${liveSql}) AND e.obsolete_at IS NULL) OR json_extract(e.filter_json,'$.docDigest') IS NOT ?) ORDER BY e.vector_id LIMIT ?`)
      .bind(document.document_id,profile.id,docDigest,metadataBatchSize).all<VectorEntry&{desired_live:number}>()).results||[];
    if(rows.length) {
      const stored=await withinProviderDeadline({},15_000,()=>env.VECTOR_INDEX!.getByIds(rows.map(row=>row.vector_id)));
      const updates:VectorizeVector[]=[];
      for(const entry of rows) {
        const vector=stored.find(vector=>vector.id===entry.vector_id);
        if(!vector||vector.namespace!==profile.namespace||vector.values.length!==profile.dimensions) throw new VectorVisibilityPending("Lifecycle reconciliation requires the stored generation");
        const scoped=await vectorEntryFilterProjection(projection||{},entry.section_label,docDigest);
        updates.push({...vector,metadata:{...stripVectorFilterMetadata(vector.metadata),...scoped.projection,scopeDigest:scoped.digest,scopeJson:scoped.json,live:Boolean(entry.desired_live)}});
      }
      if(!await submitVectorMetadata(env,lock,updates)) continue;
    }
    await env.DB.prepare(`INSERT INTO vector_profile_sync(profile_id,document_id,revision,updated_at) SELECT ?,document_id,revision,? FROM vector_lifecycle_dirty WHERE document_id=? AND revision=? AND NOT EXISTS(SELECT 1 FROM vector_entries e
      WHERE e.document_id=? AND e.profile_id=? AND e.state='verified' AND (e.metadata_live!=(CASE WHEN ${liveSql} THEN 1 ELSE 0 END) OR (NOT (${liveSql}) AND e.obsolete_at IS NULL) OR json_extract(e.filter_json,'$.docDigest') IS NOT ?)) ON CONFLICT(profile_id,document_id) DO UPDATE SET revision=excluded.revision,updated_at=excluded.updated_at`)
      .bind(profile.id,now(),document.document_id,document.revision,document.document_id,profile.id,docDigest).run();
    await env.DB.prepare(`INSERT INTO vector_filter_sync(profile_id,document_id,revision,projection_version) SELECT ?,s.document_id,s.revision,${VECTOR_FILTER_VERSION} FROM vector_lifecycle_dirty s
      JOIN vector_profile_sync p ON p.document_id=s.document_id AND p.profile_id=? AND p.revision=s.revision WHERE s.document_id=? AND s.revision=? AND NOT EXISTS(SELECT 1 FROM vector_entries e WHERE e.document_id=s.document_id AND e.profile_id=?
        AND e.state='verified' AND (e.metadata_live!=(CASE WHEN ${liveSql} THEN 1 ELSE 0 END) OR (NOT (${liveSql}) AND e.obsolete_at IS NULL) OR json_extract(e.filter_json,'$.docDigest') IS NOT ?))
      ON CONFLICT(profile_id,document_id) DO UPDATE SET revision=excluded.revision,projection_version=excluded.projection_version`).bind(profile.id,profile.id,document.document_id,document.revision,profile.id,docDigest).run();
    processed++;
    } finally { await releaseVectorSync(env,lock); }
  }
  return {processed,pending:!await vectorLifecycleReady({...env,VECTOR_LIFECYCLE_READY:"true"})};
}

export async function reconcileVectorGenerations(env:Env,options:{after?:string;probe?:boolean}={}) {
  const profile=await vectorProfile(env);
  const rows=(await env.DB.prepare(`SELECT e.state,e.metadata_live,COUNT(*) AS count,SUM(CASE WHEN ${liveSql} THEN 1 ELSE 0 END) AS current_sources
    FROM vector_entries e WHERE profile_id=? GROUP BY state,metadata_live`).bind(profile.id).all()).results||[];
  const dirty=await env.DB.prepare(`SELECT COUNT(*) AS count FROM vector_lifecycle_dirty s LEFT JOIN vector_profile_sync p ON p.document_id=s.document_id AND p.profile_id=? LEFT JOIN vector_filter_sync f ON f.document_id=s.document_id AND f.profile_id=p.profile_id WHERE p.revision IS NOT s.revision OR f.revision IS NOT s.revision OR f.projection_version IS NOT ${VECTOR_FILTER_VERSION}`).bind(profile.id).first<{count:number}>();
  const coverage=await env.DB.prepare(`WITH desired AS (
    SELECT d.id AS document_id,c.id AS chunk_id,'document' AS kind,d.active_text_generation AS generation,c.chunk_text AS text,json_extract(d.metadata_json,'$.sourceSha256') AS source_hash
      FROM documents d JOIN document_chunks c ON c.document_id=d.id WHERE ${publishedDocumentSql()}
    UNION ALL SELECT d.id,c.chunk_id,'retrieval',c.batch_id,c.chunk_text,json_extract(d.metadata_json,'$.sourceSha256')
      FROM documents d JOIN retrieval_search_chunks c ON c.document_id=d.id WHERE c.active=1 AND ${publishedDocumentSql()}
  ) SELECT COUNT(*) AS eligibleSources,COALESCE(SUM(EXISTS(SELECT 1 FROM vector_entries e WHERE e.profile_id=? AND e.document_id=desired.document_id
    AND e.chunk_id=desired.chunk_id AND e.source_kind=desired.kind AND e.source_generation=desired.generation AND e.chunk_text=desired.text AND e.source_sha256=desired.source_hash AND e.state='verified' AND e.metadata_live=1)),0) AS verifiedLiveSources FROM desired`)
    .bind(profile.id).first<{eligibleSources:number;verifiedLiveSources:number}>();
  const held=await env.DB.prepare("SELECT COUNT(*) AS count FROM vector_sync_locks WHERE profile_id=? AND uncertain=1").bind(profile.id).first<{count:number}>();
  let providerProbe:unknown=null;
  if(options.probe) {
    if(!env.VECTOR_INDEX) throw new Error("Vector binding unavailable");
    const entries=(await env.DB.prepare("SELECT vector_id,metadata_live,filter_json FROM vector_entries WHERE profile_id=? AND state='verified' AND vector_id>? ORDER BY vector_id LIMIT 33").bind(profile.id,options.after||'').all<{vector_id:string;metadata_live:number;filter_json:string}>()).results||[];
    const page=entries.slice(0,32);
    const stored=page.length?await withinProviderDeadline({},15_000,()=>env.VECTOR_INDEX!.getByIds(page.map(entry=>entry.vector_id))):[];
    providerProbe={checked:page.length,missingIds:page.filter(entry=>!stored.some(vector=>vector.id===entry.vector_id)).map(entry=>entry.vector_id),
      mismatchedIds:page.filter(entry=>stored.some(vector=>vector.id===entry.vector_id&&(vector.namespace!==profile.namespace||vector.metadata?.entryDigest!==entry.vector_id||vector.metadata?.live!==Boolean(entry.metadata_live)||vector.values.length!==profile.dimensions||!Array.from(vector.values).every(Number.isFinite)||!vectorScopeMetadataMatches(vector.metadata,entry.filter_json)))).map(entry=>entry.vector_id),
      nextAfter:entries.length>32?page.at(-1)?.vector_id:null};
  }
  return {profile,ready:await vectorLifecycleReady(env),filterSchemaVersion:VECTOR_FILTER_VERSION,filtersReady:env.VECTOR_FILTERS_READY==='true',requiredMetadataIndexes:vectorMetadataIndexes.indexes,dirtyDocuments:dirty?.count||0,heldSynchronizations:held?.count||0,coverage,counts:rows,providerProbe,legacyNamespaceExcluded:env.VECTOR_NAMESPACE};
}

export async function collectObsoleteVectors(env:Env,dryRun=true) {
  const profile=await vectorProfile(env);
  const deleteBatchSize=computeBudgetBatchLimit(env,'vector_write',32);
  // Published activation plans remain rollback evidence. Document generations
  // can be re-embedded from retained source when restored. Never delete a
  // currently eligible entry, a dirty document, or an unexpired retention row.
  const rows=(await env.DB.prepare(`SELECT e.vector_id FROM vector_entries e WHERE e.profile_id=? AND e.state IN ('verified','uncertain') AND e.metadata_live=0
    AND e.obsolete_at IS NOT NULL AND e.obsolete_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now','-30 days')
    AND (e.state='uncertain' OR NOT (${liveSql})) AND NOT EXISTS(SELECT 1 FROM vector_lifecycle_dirty s LEFT JOIN vector_profile_sync p ON p.document_id=s.document_id AND p.profile_id=e.profile_id WHERE s.document_id=e.document_id AND p.revision IS NOT s.revision)
    AND NOT(e.state='verified' AND e.source_kind='retrieval' AND EXISTS(SELECT 1 FROM retrieval_activation_plans p WHERE p.batch_id=e.source_generation AND p.state='published'))
    ORDER BY e.obsolete_at,e.vector_id LIMIT ?`).bind(profile.id,deleteBatchSize).all<{vector_id:string}>()).results||[];
  if(!dryRun) {
    if(!env.VECTOR_INDEX) throw new Error("Vector binding unavailable");
    const candidateIds=rows.map(row=>row.vector_id);
    await env.DB.prepare(`UPDATE vector_entries AS e SET state='deleting' WHERE vector_id IN(SELECT value FROM json_each(?))
      AND metadata_live=0 AND (e.state='uncertain' OR NOT (${liveSql})) AND NOT EXISTS(SELECT 1 FROM vector_lifecycle_dirty s LEFT JOIN vector_profile_sync p ON p.document_id=s.document_id AND p.profile_id=e.profile_id WHERE s.document_id=e.document_id AND p.revision IS NOT s.revision)`)
      .bind(JSON.stringify(candidateIds)).run();
    const claimed=(await env.DB.prepare("SELECT vector_id FROM vector_entries WHERE profile_id=? AND state='deleting' ORDER BY vector_id LIMIT ?").bind(profile.id,deleteBatchSize).all<{vector_id:string}>()).results||[];
    const ids=claimed.map(row=>row.vector_id);
    if(!ids.length) return {dryRun,profileId:profile.id,candidateIds:[],retentionDays:30};
    await reserveComputeBudget(env,"vector_write",ids.length);
    await withinProviderDeadline({},15_000,()=>env.VECTOR_INDEX!.deleteByIds(ids));
    const remaining=await withinProviderDeadline({},15_000,()=>env.VECTOR_INDEX!.getByIds(ids));
    const deleted=ids.filter(id=>!remaining.some(vector=>vector.id===id));
    if(deleted.length) await env.DB.prepare("UPDATE vector_entries SET state='deleted',values_json=NULL,deleted_at=? WHERE vector_id IN(SELECT value FROM json_each(?)) AND state='deleting'")
      .bind(now(),JSON.stringify(deleted)).run();
  }
  return {dryRun,profileId:profile.id,candidateIds:rows.map(row=>row.vector_id),retentionDays:30};
}


export async function runVectorLifecycleMaintenance(env:Env) {
  if(!env.VECTOR_INDEX) return;
  try {
    await synchronizeVectorLifecycle(env);
    await collectObsoleteVectors(env,false);
  }catch { env.TELEMETRY?.tag("vector_result","lifecycle_pending"); }
}
