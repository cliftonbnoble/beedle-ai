// Scheduled-only operator runner. No HTTP administration, provider, bucket or
// source database binding; the restored target receipt is checked before writes.
import { startIndexRecovery, indexRecoveryStatus, advanceIndexRecovery } from './services/search-index-recovery';
import { advanceFacetBuild, facetBuildStatus } from './services/search-facet-build';
import { inspectSearchReadiness } from './services/search-readiness';
import { advanceReviewerQueue, inspectReviewerQueue } from './services/reviewer-queue-index';
import { buildReviewerRows } from './services/reviewer-read-model';
import { RequestConflictError } from './lib/http';
import releaseContract from '../release/contract.json';

type Status = {run_id:string;state:string;stage:string;deliveries:number;failures:number;last_error:string|null};
export async function advanceMigrationBackfill(env:MigrationWorkerBindings) {
  if(env.MIGRATION_ENABLED!=='true')throw Error('Migration runner is disabled');
  if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(env.MIGRATION_RUN_ID)||![env.MIGRATION_TARGET_SHA256,env.MIGRATION_BUNDLE_SHA256].every(hash=>/^[a-f0-9]{64}$/.test(hash)))throw Error('Migration identity is incomplete');
  const receipt=await env.DB.prepare('SELECT run_id,bundle_sha256,target_sha256 FROM beedle_recovery_receipt WHERE singleton=1').first<{run_id:string;bundle_sha256:string;target_sha256:string}>();
  if(!receipt||receipt.run_id!==env.MIGRATION_RUN_ID||receipt.bundle_sha256!==env.MIGRATION_BUNDLE_SHA256||receipt.target_sha256!==env.MIGRATION_TARGET_SHA256)throw Error('Migration target receipt differs');
  const ledger=(await env.DB.prepare('SELECT name FROM d1_migrations ORDER BY id').all<{name:string}>()).results.map(row=>row.name);
  if(JSON.stringify(ledger)!==JSON.stringify(releaseContract.requiredMigrations))throw Error('Migration ledger does not match this runner');
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS beedle_recovery_backfill_status(run_id TEXT PRIMARY KEY,state TEXT NOT NULL,stage TEXT NOT NULL,deliveries INTEGER NOT NULL DEFAULT 0,failures INTEGER NOT NULL DEFAULT 0,last_error TEXT,started_at TEXT NOT NULL,updated_at TEXT NOT NULL,completed_at TEXT)`).run();
  await env.DB.prepare("INSERT INTO beedle_recovery_backfill_status(run_id,state,stage,started_at,updated_at) VALUES(?,'running','indexes',strftime('%Y-%m-%dT%H:%M:%fZ','now'),strftime('%Y-%m-%dT%H:%M:%fZ','now')) ON CONFLICT(run_id) DO NOTHING").bind(env.MIGRATION_RUN_ID).run();
  const inspect=()=>env.DB.prepare('SELECT * FROM beedle_recovery_backfill_status WHERE run_id=?').bind(env.MIGRATION_RUN_ID).first<Status>();
  const previous=await inspect();if(!previous)throw Error('Migration progress missing');
  if(previous.state==='held'||previous.state==='indexes_ready')return previous;
  let stage=previous.stage,deliveries=0;const started=Date.now();
  try{
    for(;deliveries<20&&Date.now()-started<20_000;deliveries++){
      const current=await inspect();
      if(!current||current.state!=='running')return current; // Respect an operator hold between checkpoints.
      const indexes=await indexRecoveryStatus(env);
      if(indexes.generation!==env.MIGRATION_RUN_ID){
        if(indexes.generation!=='legacy'||indexes.status!=='idle')throw Error('Unexpected index generation');
        await startIndexRecovery(env,{dryRun:false,generation:env.MIGRATION_RUN_ID,expectedGeneration:'legacy',replaceArchive:true,restart:false});
      }else if(indexes.status!=='completed'){stage='indexes';await advanceIndexRecovery(env,env.MIGRATION_RUN_ID);}
      else{
        const facets=await facetBuildStatus(env);if(!facets)throw Error('Facet checkpoint missing');
        if(facets.phase!=='ready'){stage='facets';await advanceFacetBuild(env);}
        else if(!(await inspectReviewerQueue(env)).ready){stage='reviewer';await advanceReviewerQueue(env,buildReviewerRows);}
        else{
          if(!(await inspectSearchReadiness(env)).ready)throw Error('Index readiness failed');
          await env.DB.prepare("UPDATE beedle_recovery_backfill_status SET state='indexes_ready',stage='complete',deliveries=deliveries+?,failures=0,last_error=NULL,completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE run_id=? AND state='running'").bind(deliveries,env.MIGRATION_RUN_ID).run();
          return await inspect(); // This is index readiness, never publication or cutover approval.
        }
      }
    }
    await env.DB.prepare("UPDATE beedle_recovery_backfill_status SET stage=?,deliveries=deliveries+?,failures=0,last_error=NULL,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE run_id=? AND state='running'").bind(stage,deliveries,env.MIGRATION_RUN_ID).run();
  }catch(error){
    if(error instanceof RequestConflictError)return await inspect(); // An existing builder owns its lease.
    await env.DB.prepare("UPDATE beedle_recovery_backfill_status SET stage=?,deliveries=deliveries+?,failures=failures+1,state=CASE WHEN failures+1>=5 THEN 'held' ELSE 'running' END,last_error='checkpoint_failed',updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE run_id=? AND state='running'").bind(stage,deliveries,env.MIGRATION_RUN_ID).run();
  }
  return await inspect();
}

export default {
  fetch(){return new Response('Migration maintenance worker',{status:503});},
  async scheduled(_controller:ScheduledController,env:MigrationWorkerBindings){
    try{const status=await advanceMigrationBackfill(env);console.log(JSON.stringify({event:'migration_backfill',state:status?.state,stage:status?.stage,deliveries:status?.deliveries}));}
    catch{console.error(JSON.stringify({event:'migration_backfill',state:'guard_or_schema_failed'}));}
  }
} satisfies ExportedHandler<MigrationWorkerBindings>;
