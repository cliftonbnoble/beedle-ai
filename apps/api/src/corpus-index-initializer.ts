import {advanceOwnedIndexInitialization,inspectOwnedIndexInitialization,readOwnedIndexInitializationIdentity,IndexInitializationError,type IndexInitializationSpec} from './services/corpus-index-initialization';
import {sha256Bytes} from './lib/digests';

export function parseIndexInitializationSpec(text:string):IndexInitializationSpec{
  if(text.length>16_384)throw new IndexInitializationError('initialization_spec_size');
  const value:unknown=JSON.parse(text);
  function isRecord(input:unknown):input is Record<string,unknown>{return typeof input==='object'&&input!==null&&!Array.isArray(input);}
  if(!isRecord(value))throw new IndexInitializationError('invalid_initialization_spec');
  const row=value;
  if(Object.keys(row).sort().join('|')!==['ownerId','resourcePlanSha256','resourceId','partitionIndex','generation','planSha256','catalogSha256','catalogRows','allowedSchemaSha256','finalSchemaSha256'].sort().join('|')||
    typeof row.ownerId!=='string'||typeof row.resourcePlanSha256!=='string'||typeof row.resourceId!=='string'||typeof row.partitionIndex!=='number'||
    typeof row.generation!=='string'||typeof row.planSha256!=='string'||typeof row.catalogSha256!=='string'||typeof row.catalogRows!=='number'||
    !Array.isArray(row.allowedSchemaSha256)||typeof row.finalSchemaSha256!=='string')throw new IndexInitializationError('invalid_initialization_spec');
  const schemas:string[]=[];for(const hash of row.allowedSchemaSha256){if(typeof hash!=='string')throw new IndexInitializationError('invalid_initialization_schema_spec');schemas.push(hash);}
  return{ownerId:row.ownerId,resourcePlanSha256:row.resourcePlanSha256,resourceId:row.resourceId,partitionIndex:row.partitionIndex,generation:row.generation,
    planSha256:row.planSha256,catalogSha256:row.catalogSha256,catalogRows:row.catalogRows,allowedSchemaSha256:schemas,finalSchemaSha256:row.finalSchemaSha256};
}
export async function advanceIndexInitializationTick(env:IndexInitializerBindings){
  if(env.RESTORE_ENABLED!=='true')return{state:'disabled',deliveries:0};
  const spec=parseIndexInitializationSpec(env.RESTORE_SPEC_JSON);
  const registered=await readOwnedIndexInitializationIdentity(env.DB,spec);
  const control=await env.DB.prepare('SELECT owner_id,plan_sha256,state,token FROM beedle_restore_index_init_tick WHERE singleton=1')
    .first<{owner_id:string;plan_sha256:string;state:string;token:string|null}>();
  if(!control||control.owner_id!==spec.ownerId||control.plan_sha256!==spec.planSha256)throw new IndexInitializationError('initializer_tick_owner_changed');
  if(control.state==='running')return{state:'busy',deliveries:0};
  if(control.state==='held'||registered.state==='held')return{state:'held',deliveries:0};
  const before=await inspectOwnedIndexInitialization(env.DB,spec);
  if(control.state==='completed'){
    if(before.row.state!=='completed')throw new IndexInitializationError('initializer_completion_changed');
    return{state:'completed',deliveries:0};
  }
  const token=crypto.randomUUID(),now=new Date().toISOString();let deliveries=0;
  try{
    const claimed=await env.DB.batch<{token:string}>([
      env.DB.prepare("UPDATE beedle_restore_index_init_tick SET state='running',token=?,updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state='idle' RETURNING token")
        .bind(token,now,spec.ownerId,spec.planSha256),
      env.DB.prepare("INSERT INTO beedle_restore_index_init_history(token,owner_id,plan_sha256,started_at,state) SELECT ?,?,?,?,'running' WHERE EXISTS(SELECT 1 FROM beedle_restore_index_init_tick WHERE singleton=1 AND token=? AND state='running')")
        .bind(token,spec.ownerId,spec.planSha256,now,token)
    ]);
    const claim=claimed[0],history=claimed[1];
    if(claimed.length!==2||!claim||!history||claimed.some(x=>x.success!==true))throw new IndexInitializationError('initializer_tick_unacknowledged');
    if(claim.results.length===0&&Number(history.meta.changes)===0)return{state:'busy',deliveries:0};
    if(claim.results[0]?.token!==token||Number(history.meta.changes)!==1)throw new IndexInitializationError('initializer_tick_unacknowledged');
    let complete=before.row.state==='completed';const started=Date.now();
    while(!complete&&deliveries<20&&Date.now()-started<20_000){
      const row=await advanceOwnedIndexInitialization(env.DB,spec);deliveries++;
      if(row.state==='held')throw new IndexInitializationError('initializer_checkpoint_held');
      complete=row.state==='completed';
    }
    const finished=new Date().toISOString();
    const results=await env.DB.batch([
      env.DB.prepare("UPDATE beedle_restore_index_init_history SET state='completed',completed_at=?,deliveries=? WHERE token=? AND owner_id=? AND plan_sha256=? AND state='running'")
        .bind(finished,deliveries,token,spec.ownerId,spec.planSha256),
      env.DB.prepare("UPDATE beedle_restore_index_init_tick SET state=?,token=NULL,updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state='running' AND token=?")
        .bind(complete?'completed':'idle',finished,spec.ownerId,spec.planSha256,token)
    ]);
    if(results.length!==2||results.some(x=>x.success!==true||Number(x.meta.changes)!==1))throw new IndexInitializationError('initializer_tick_completion_unacknowledged');
    return{state:complete?'completed':'running',deliveries};
  }catch(error){
    const hash=await sha256Bytes(new TextEncoder().encode(error instanceof Error?error.message:String(error))),finished=new Date().toISOString();
    // An older failure cannot modify a different or already-completed tick.
    try{await env.DB.batch([
      env.DB.prepare(`INSERT INTO beedle_restore_index_init_errors(token,owner_id,plan_sha256,recorded_at,code,error_sha256)
        SELECT ?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM beedle_restore_index_init_history WHERE token=? AND owner_id=? AND plan_sha256=?) ON CONFLICT(token) DO NOTHING`)
        .bind(token,spec.ownerId,spec.planSha256,finished,error instanceof IndexInitializationError?error.code:'provider_or_transport_error',hash,token,spec.ownerId,spec.planSha256),
      env.DB.prepare("UPDATE beedle_restore_index_init_history SET state='held',completed_at=?,deliveries=?,error_sha256=? WHERE token=? AND owner_id=? AND plan_sha256=? AND state='running'")
        .bind(finished,deliveries,hash,token,spec.ownerId,spec.planSha256),
      env.DB.prepare("UPDATE beedle_restore_index_init_tick SET state='held',updated_at=? WHERE singleton=1 AND owner_id=? AND plan_sha256=? AND state='running' AND token=?")
        .bind(finished,spec.ownerId,spec.planSha256,token)
    ]);}catch{/* Keep the unknown claim; no automatic replay or lease stealing. */}
    throw error;
  }
}
export default{
  fetch(){return new Response('Corpus index initialization maintenance worker',{status:503});},
  async scheduled(_controller:ScheduledController,env:IndexInitializerBindings){
    try{const result=await advanceIndexInitializationTick(env);console.log(JSON.stringify({event:'empty_corpus_index_initialization',state:result.state,deliveries:result.deliveries}));
      if(result.state==='held')throw new Error('Empty corpus initializer held');}
    catch{throw new Error('Corpus index initializer guard or checkpoint failed; inspect private ownership evidence');}
  }
} satisfies ExportedHandler<IndexInitializerBindings>;
