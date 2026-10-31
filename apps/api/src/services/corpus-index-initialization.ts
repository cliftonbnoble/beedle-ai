import releaseContract from '../../release/contract.json';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {sha256Bytes} from '../lib/digests';
import {startIndexRecovery,indexRecoveryStatus,advanceIndexRecovery} from './search-index-recovery';
import {advanceFacetBuild,facetBuildStatus} from './search-facet-build';
import {advanceReviewerQueue,inspectReviewerQueue} from './reviewer-queue-index';
import {buildReviewerRows} from './reviewer-read-model';
import {inspectSearchReadiness} from './search-readiness';

export const corpusIndexInitializationSql=[
  `CREATE TABLE beedle_restore_index_init(singleton INTEGER PRIMARY KEY CHECK(singleton=1),owner_id TEXT NOT NULL,resource_plan_sha256 TEXT NOT NULL,
    resource_id TEXT NOT NULL,partition_index INTEGER NOT NULL CHECK(partition_index BETWEEN 0 AND 31),generation TEXT NOT NULL,
    plan_sha256 TEXT NOT NULL,catalog_sha256 TEXT NOT NULL,catalog_rows INTEGER NOT NULL CHECK(catalog_rows>=0),
    state TEXT NOT NULL CHECK(state IN ('planned','running','completed','held')),deliveries INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,updated_at TEXT NOT NULL,completed_at TEXT,error_sha256 TEXT)`,
  `CREATE TRIGGER beedle_restore_index_init_identity BEFORE UPDATE OF owner_id,resource_plan_sha256,resource_id,partition_index,generation,plan_sha256,catalog_sha256,catalog_rows ON beedle_restore_index_init
    BEGIN SELECT RAISE(ABORT,'index_initializer_owner_immutable'); END`,
  `CREATE TRIGGER beedle_restore_index_init_result BEFORE UPDATE ON beedle_restore_index_init WHEN OLD.state='completed'
    BEGIN SELECT RAISE(ABORT,'index_initializer_result_immutable'); END`,
  `CREATE TRIGGER beedle_restore_index_init_error BEFORE UPDATE OF error_sha256 ON beedle_restore_index_init WHEN OLD.error_sha256 IS NOT NULL
    BEGIN SELECT RAISE(ABORT,'index_initializer_error_immutable'); END`,
  `CREATE TRIGGER beedle_restore_index_init_delete BEFORE DELETE ON beedle_restore_index_init BEGIN SELECT RAISE(ABORT,'index_initializer_owner_immutable'); END`,
  `CREATE TABLE beedle_restore_index_init_tick(singleton INTEGER PRIMARY KEY CHECK(singleton=1),owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('idle','running','completed','held')),token TEXT,updated_at TEXT NOT NULL)`,
  `CREATE TRIGGER beedle_restore_index_init_tick_identity BEFORE UPDATE OF owner_id,plan_sha256 ON beedle_restore_index_init_tick BEGIN SELECT RAISE(ABORT,'index_initializer_tick_owner_immutable'); END`,
  `CREATE TRIGGER beedle_restore_index_init_tick_delete BEFORE DELETE ON beedle_restore_index_init_tick BEGIN SELECT RAISE(ABORT,'index_initializer_tick_owner_immutable'); END`,
  `CREATE TABLE beedle_restore_index_init_history(token TEXT PRIMARY KEY,owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,started_at TEXT NOT NULL,
    completed_at TEXT,state TEXT NOT NULL CHECK(state IN ('running','completed','held')),deliveries INTEGER NOT NULL DEFAULT 0,error_sha256 TEXT)`,
  `CREATE TRIGGER beedle_restore_index_init_history_identity BEFORE UPDATE OF token,owner_id,plan_sha256,started_at ON beedle_restore_index_init_history BEGIN SELECT RAISE(ABORT,'index_initializer_history_immutable'); END`,
  `CREATE TRIGGER beedle_restore_index_init_history_result BEFORE UPDATE ON beedle_restore_index_init_history WHEN OLD.state!='running' BEGIN SELECT RAISE(ABORT,'index_initializer_history_immutable'); END`,
  `CREATE TRIGGER beedle_restore_index_init_history_delete BEFORE DELETE ON beedle_restore_index_init_history BEGIN SELECT RAISE(ABORT,'index_initializer_history_immutable'); END`
  ,`CREATE TABLE beedle_restore_index_init_errors(token TEXT PRIMARY KEY,owner_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,recorded_at TEXT NOT NULL,code TEXT NOT NULL,error_sha256 TEXT NOT NULL CHECK(length(error_sha256)=64))`
  ,`CREATE TRIGGER beedle_restore_index_init_errors_update BEFORE UPDATE ON beedle_restore_index_init_errors BEGIN SELECT RAISE(ABORT,'index_initializer_error_immutable'); END`
  ,`CREATE TRIGGER beedle_restore_index_init_errors_delete BEFORE DELETE ON beedle_restore_index_init_errors BEGIN SELECT RAISE(ABORT,'index_initializer_error_immutable'); END`
] as const;
export interface IndexInitializationSpec {
  ownerId:string;resourcePlanSha256:string;resourceId:string;partitionIndex:number;generation:string;planSha256:string;
  catalogSha256:string;catalogRows:number;allowedSchemaSha256:readonly string[];finalSchemaSha256:string;
}
export interface IndexInitializationRow {
  owner_id:string;resource_plan_sha256:string;resource_id:string;partition_index:number;generation:string;plan_sha256:string;
  catalog_sha256:string;catalog_rows:number;state:'planned'|'running'|'completed'|'held';deliveries:number;
}
export class IndexInitializationError extends Error {constructor(readonly code:string){super(code);this.name='IndexInitializationError';}}
export async function readOwnedIndexInitializationIdentity(db:D1Database,spec:IndexInitializationSpec){
  const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/,digest=/^[a-f0-9]{64}$/;
  if(!uuid.test(spec.ownerId)||!uuid.test(spec.resourceId)||!uuid.test(spec.generation)||
    ![spec.resourcePlanSha256,spec.planSha256,spec.catalogSha256,spec.finalSchemaSha256].every(x=>digest.test(x))||
    !Number.isSafeInteger(spec.partitionIndex)||spec.partitionIndex<0||spec.partitionIndex>31||
    !Number.isSafeInteger(spec.catalogRows)||spec.catalogRows<0||spec.catalogRows>10_000||!spec.allowedSchemaSha256.length||spec.allowedSchemaSha256.length>32||
    !spec.allowedSchemaSha256.every(x=>digest.test(x))||!spec.allowedSchemaSha256.includes(spec.finalSchemaSha256))throw new IndexInitializationError('invalid_initialization_spec');
  const claim=await db.prepare('SELECT owner_id,resource_plan_sha256,resource_id,partition_index FROM beedle_restore_resource_claim WHERE singleton=1')
    .first<{owner_id:string;resource_plan_sha256:string;resource_id:string;partition_index:number}>();
  const row=await db.prepare('SELECT * FROM beedle_restore_index_init WHERE singleton=1').first<IndexInitializationRow>();
  if(!claim||claim.owner_id!==spec.ownerId||claim.resource_plan_sha256!==spec.resourcePlanSha256||claim.resource_id!==spec.resourceId||claim.partition_index!==spec.partitionIndex||
    !row||row.owner_id!==spec.ownerId||row.resource_plan_sha256!==spec.resourcePlanSha256||row.resource_id!==spec.resourceId||row.partition_index!==spec.partitionIndex||
    row.generation!==spec.generation||row.plan_sha256!==spec.planSha256||row.catalog_sha256!==spec.catalogSha256||row.catalog_rows!==spec.catalogRows)
    throw new IndexInitializationError('initialization_owner_changed');
  return row;
}
export async function inspectOwnedIndexInitialization(db:D1Database,spec:IndexInitializationSpec){
  const row=await readOwnedIndexInitializationIdentity(db,spec);
  const ledger=(await db.prepare('SELECT name FROM d1_migrations ORDER BY id').all<{name:string}>()).results.map(x=>x.name);
  const counts=await db.prepare(`SELECT (SELECT COUNT(*) FROM documents) AS documents,(SELECT COUNT(*) FROM document_chunks) AS chunks,
    (SELECT COUNT(*) FROM section_paragraphs) AS paragraphs,(SELECT COUNT(*) FROM retrieval_search_chunks) AS retrieval,
    (SELECT COUNT(*) FROM legal_catalog_rows WHERE catalog_id='legacy') AS catalog,
    (SELECT active_version FROM legal_catalog_state WHERE singleton=1) AS activeCatalog,
    (SELECT json_extract(descriptor_json,'$.policy') FROM legal_reference_editions WHERE catalog_id='legacy') AS editionPolicy`)
    .first<{documents:number;chunks:number;paragraphs:number;retrieval:number;catalog:number;activeCatalog:string;editionPolicy:string}>();
  if(JSON.stringify(ledger)!==JSON.stringify(releaseContract.requiredMigrations)||!counts||counts.documents!==0||counts.chunks!==0||counts.paragraphs!==0||counts.retrieval!==0||
    counts.catalog!==spec.catalogRows||counts.activeCatalog!=='legacy'||counts.editionPolicy!=='unknown')throw new IndexInitializationError('initialization_empty_schema_catalog_guard');
  const leases=await db.prepare(`SELECT (SELECT owner_token FROM search_index_rebuild WHERE singleton=1) AS indexOwner,
    (SELECT owner_token FROM search_facet_build WHERE singleton=1) AS facetOwner,
    (SELECT lease_owner FROM reviewer_queue_control WHERE singleton=1) AS reviewerOwner`).first<{indexOwner:string|null;facetOwner:string|null;reviewerOwner:string|null}>();
  if(!leases||leases.indexOwner!==null||leases.facetOwner!==null||leases.reviewerOwner!==null)throw new IndexInitializationError('initialization_existing_checkpoint_owner');
  const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string|null}>()).results;
  const schemaSha256=await restorationSchemaFingerprint(objects);
  if(!spec.allowedSchemaSha256.includes(schemaSha256)||row.state==='completed'&&schemaSha256!==spec.finalSchemaSha256)throw new IndexInitializationError('initialization_schema_changed');
  if(row.state==='completed'){
    const env={DB:db},index=await indexRecoveryStatus(env);
    if(index.generation!==spec.generation||index.status!=='completed'||index.last_error!==null||
      (await facetBuildStatus(env))?.phase!=='ready'||!(await inspectReviewerQueue(env)).ready||!(await inspectSearchReadiness(env)).ready)
      throw new IndexInitializationError('initialization_completion_changed');
  }
  return{row,schemaSha256};
}
// This advances supported, leased empty-index checkpoints on a NEW physical
// resource only. The coordinator owns its durable tick; no old recovery receipt
// is fabricated, no retired checkpoint is resumed, and no case is inserted here.
export async function advanceOwnedIndexInitialization(db:D1Database,spec:IndexInitializationSpec){
  const before=await inspectOwnedIndexInitialization(db,spec);if(before.row.state==='completed'||before.row.state==='held')return before.row;
  const env={DB:db},now=new Date().toISOString();
  try{
    const started=await db.prepare("UPDATE beedle_restore_index_init SET state='running',started_at=COALESCE(started_at,?),updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state IN ('planned','running')")
      .bind(now,now,spec.ownerId,spec.planSha256).run();
    if(Number(started.meta.changes)!==1)throw new IndexInitializationError('initialization_checkpoint_unacknowledged');
    const index=await indexRecoveryStatus(env);
    if(index.last_error!==null)throw new IndexInitializationError('initialization_prior_checkpoint_error');
    let completed=false;
    if(index.generation==='legacy'&&index.status==='idle')await startIndexRecovery(env,{dryRun:false,generation:spec.generation,expectedGeneration:'legacy',replaceArchive:true,restart:false});
    else if(index.generation!==spec.generation)throw new IndexInitializationError('initialization_generation_changed');
    else if(index.status==='running')await advanceIndexRecovery(env,spec.generation);
    else if(index.status!=='completed')throw new IndexInitializationError('initialization_checkpoint_state_changed');
    else if((await facetBuildStatus(env))?.phase!=='ready')await advanceFacetBuild(env);
    else if(!(await inspectReviewerQueue(env)).ready)await advanceReviewerQueue(env,buildReviewerRows);
    else{
      if(!(await inspectSearchReadiness(env)).ready)throw new IndexInitializationError('initialization_readiness_failed');
      const fresh=await inspectOwnedIndexInitialization(db,spec);if(fresh.schemaSha256!==spec.finalSchemaSha256)throw new IndexInitializationError('initialization_final_schema_changed');completed=true;
    }
    const result=await db.prepare(`UPDATE beedle_restore_index_init SET state=?,deliveries=deliveries+1,updated_at=?,completed_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state='running'`)
      .bind(completed?'completed':'running',now,completed?now:null,spec.ownerId,spec.planSha256).run();
    if(Number(result.meta.changes)!==1)throw new IndexInitializationError('initialization_result_unacknowledged');
    return(await inspectOwnedIndexInitialization(db,spec)).row;
  }catch(error){
    const hash=await sha256Bytes(new TextEncoder().encode(error instanceof Error?error.message:String(error)));
    try{await db.prepare("UPDATE beedle_restore_index_init SET state='held',error_sha256=?,updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state IN ('planned','running') AND error_sha256 IS NULL")
      .bind(hash,new Date().toISOString(),spec.ownerId,spec.planSha256).run();}catch{/* Preserve the unknown outcome; the controller must reconcile it. */}
    throw error;
  }
}
