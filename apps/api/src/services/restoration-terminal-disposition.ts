import {z} from 'zod';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
import {parseRestorationSourceSuccessorSpec} from '../restoration-source-successor-worker';
import {inspectRestorationSourceSuccessor} from './restoration-source-successor-control';
import {generationTables} from './generation-tables';

const hash=z.string().regex(/^[a-f0-9]{64}$/);
const schema=z.object({version:z.literal(1),operationId:z.string().uuid(),successorSpecJson:z.string().min(1).max(20000),
  parentTerminalProofSha256:hash,parentEndedAt:z.string().datetime(),notBefore:z.string().datetime(),notAfter:z.string().datetime(),
  controlSha256:hash,tickSha256:hash,historySha256:hash,documentId:z.string().min(1).max(200),jobSha256:hash,
  documentSha256:hash,generationsSha256:hash,stagedArtifactsSha256:hash,sourceResults:z.number().int().min(1),
  abandonStagingGenerationId:z.string().uuid().nullable()}).strict();
export type RestorationTerminalDispositionPlan=z.infer<typeof schema>;
export function parseRestorationTerminalDispositionPlan(input:unknown){return schema.parse(input);}

// An operator must independently prove the exact parent instance errored with
// no retries/rollback and is quiescent. This action records that terminal state;
// it neither clears a claim nor supplies a source/publication success receipt.
// Call only behind a distinct immutable attempt claim. Lost acknowledgments
// require observation; they never authorize another call to this mutation.
export async function terminalizeRestorationAttempt(db:D1Database,input:unknown,now=Date.now()){
  const plan=parseRestorationTerminalDispositionPlan(input),spec=parseRestorationSourceSuccessorSpec(plan.successorSpecJson),x=spec.identity;
  const begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter);
  if(plan.operationId===x.successorId||end<=begin||end-begin>30*60000||now<begin||now>=end||
    begin<Date.parse(plan.parentEndedAt)+10*60000+30000+60000)throw Error('terminal_disposition_admission_or_quiescence');
  const objects=(await db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all<{name:string;type:string;sql:string}>()).results;
  if(await restorationSchemaFingerprint(objects)!==spec.source.schemaSha256)throw Error('terminal_disposition_schema_changed');
  const state=await inspectRestorationSourceSuccessor(db,x);
  const one=async(sql:string,args:unknown[]=[])=>{
    const rows=(await db.prepare(sql).bind(...args).all<Record<string,unknown>>()).results;
    if(rows.length!==1||!rows[0])throw Error('terminal_disposition_expected_one');return rows[0];
  };
  const history=await one('SELECT * FROM beedle_restore_source_successor_history WHERE token=?',[state.tick.token]);
  if(state.row.state!=='building'||state.tick.state!=='running'||history.state!=='running'||
    await fingerprint(state.row)!==plan.controlSha256||await fingerprint(state.tick)!==plan.tickSha256||await fingerprint(history)!==plan.historySha256)
    throw Error('terminal_disposition_parent_changed');
  const job=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.documentId]);
  const document=await one('SELECT * FROM documents WHERE id=?',[plan.documentId]);
  const generations=(await db.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').bind(plan.documentId).all<Record<string,unknown>>()).results;
  const counts=await one("SELECT (SELECT COUNT(*) FROM beedle_restore_source_results) receipts,(SELECT COUNT(*) FROM beedle_restore_jobs WHERE phase IN ('claimed','source_copied','seeded')) unresolved,(SELECT COUNT(*) FROM documents WHERE searchable_at IS NOT NULL OR approved_at IS NOT NULL OR approved_revision IS NOT NULL OR qc_required_confirmed!=0) publicationsOrApprovals");
  const anchor=await one('SELECT document_id FROM document_generations WHERE id=?',[x.preservedGenerationId]);
  if(anchor.document_id===plan.documentId||job.phase!=='seeded'||job.source_verified!==0||!job.claim_token||
    await fingerprint(job)!==plan.jobSha256||await fingerprint(document)!==plan.documentSha256||await fingerprint(generations)!==plan.generationsSha256||
    counts.receipts!==plan.sourceResults||counts.unresolved!==1||counts.publicationsOrApprovals!==0)
    throw Error('terminal_disposition_unresolved_checkpoint_changed');
  const stages=async()=>{
    const result:Record<string,Record<string,unknown>[]>={};
    for(const table of Object.keys(generationTables))result[table]=(await db.prepare('SELECT * FROM staged_'+table+
      ' WHERE generation_id IN (SELECT id FROM document_generations WHERE document_id=?) ORDER BY generation_id,id').bind(plan.documentId).all<Record<string,unknown>>()).results;
    return result;
  };
  if(await fingerprint(await stages())!==plan.stagedArtifactsSha256)throw Error('terminal_disposition_staged_artifacts_changed');
  const abandoned=generations.find(g=>g.id===plan.abandonStagingGenerationId);
  if(plan.abandonStagingGenerationId!==null&&(!abandoned||abandoned.status!=='staging'||abandoned.published_at!==null||
    document.active_text_generation===abandoned.id||generations.some(g=>g.status==='published')))
    throw Error('terminal_disposition_generation_not_abandoned_staging');
  if(plan.abandonStagingGenerationId===null&&(!generations.some(g=>g.id===document.active_text_generation&&g.status==='published')||
    generations.some(g=>g.status==='staging'||g.status==='ready')))
    throw Error('terminal_disposition_published_generation_changed');
  const at=new Date(now).toISOString(),errorSha256=await fingerprint({code:'owned_workflow_ended_before_checkpoint',
    operationId:plan.operationId,parentTerminalProofSha256:plan.parentTerminalProofSha256});
  const statements=[
    db.prepare("UPDATE beedle_restore_source_successor SET state='held',updated_at=?,error_sha256=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND state='building' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND token=? AND state='running')")
      .bind(at,errorSha256,x.successorId,x.successorPlanSha256,state.tick.token),
    db.prepare("UPDATE beedle_restore_source_successor_history SET state='held',completed_at=?,error_sha256=? WHERE token=? AND successor_id=? AND successor_plan_sha256=? AND state='running' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor WHERE singleton=1 AND successor_id=? AND state='held' AND error_sha256=?)")
      .bind(at,errorSha256,state.tick.token,x.successorId,x.successorPlanSha256,x.successorId,errorSha256),
    db.prepare("UPDATE beedle_restore_source_successor_tick SET state='held',updated_at=? WHERE singleton=1 AND successor_id=? AND successor_plan_sha256=? AND token=? AND state='running' AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_history WHERE token=? AND state='held' AND error_sha256=?)")
      .bind(at,x.successorId,x.successorPlanSha256,state.tick.token,state.tick.token,errorSha256)
  ];
  const technicalDisposition='Owned workflow ended before generation publication; staged rows retained';
  if(abandoned)statements.push(db.prepare("UPDATE document_generations SET status='failed',error=? WHERE id=? AND document_id=? AND status='staging' AND published_at IS NULL AND EXISTS(SELECT 1 FROM documents WHERE id=? AND active_text_generation!=?) AND EXISTS(SELECT 1 FROM beedle_restore_source_successor_tick WHERE singleton=1 AND successor_id=? AND token=? AND state='held')")
    .bind(technicalDisposition,abandoned.id,plan.documentId,plan.documentId,abandoned.id,x.successorId,state.tick.token));
  const result=await db.batch(statements);
  if(result.length!==statements.length||result.some(row=>row.success!==true||Number(row.meta.changes)!==1))
    throw Error('terminal_disposition_batch_unacknowledged');
  const after=await inspectRestorationSourceSuccessor(db,x);
  if(after.row.state!=='held'||after.tick.state!=='held'||after.tick.token!==state.tick.token||
    await fingerprint(await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[plan.documentId]))!==plan.jobSha256||
    await fingerprint(await one('SELECT * FROM documents WHERE id=?',[plan.documentId]))!==plan.documentSha256||
    await fingerprint(await stages())!==plan.stagedArtifactsSha256)throw Error('terminal_disposition_bookend_changed');
  const finalGenerations=(await db.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').bind(plan.documentId).all<Record<string,unknown>>()).results;
  const expected=generations.map(g=>g.id===abandoned?.id?{...g,status:'failed',error:technicalDisposition}:g);
  if(await fingerprint(finalGenerations)!==await fingerprint(expected))throw Error('terminal_disposition_generation_bookend_changed');
  return {state:'terminal_disposition_acknowledged' as const,operationId:plan.operationId,successorId:x.successorId,
    sourceResults:plan.sourceResults,claimAndStagedRowsPreserved:true,stagingGenerationTechnicallyFailed:!!abandoned,
    publishedGenerationChanged:false,sourceReprocessed:false,publicationOrIntegrityApproval:false};
}
