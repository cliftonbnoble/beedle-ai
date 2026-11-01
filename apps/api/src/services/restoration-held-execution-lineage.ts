import {z} from 'zod';
import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {parseRestorationSourceExecutionIdentity} from './restoration-source-execution-control';
import {generationTables} from './generation-tables';
import type {RestorationIdentity} from './restoration-job-control';

const hash=z.string().regex(/^[a-f0-9]{64}$/);
export const heldRestorationExecutionSchema=z.object({executionId:z.string().uuid(),executionPlanSha256:hash,
  controlSha256:hash,tickSha256:hash,historySha256:hash.nullable(),currentSha256:hash,journalSha256:hash}).strict();
const tables=[['beedle_restore_execution_runs','execution_id'],['beedle_restore_execution_ticks','execution_id'],
  ['beedle_restore_execution_history','token'],['beedle_restore_execution_current','singleton']] as const;
export async function restorationExecutionJournalSnapshot(db:D1Database){
  const names=(await db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'beedle_restore_execution_%' ORDER BY name").all<{name:string}>()).results.map(row=>row.name);
  if(!names.length)return null;
  if(names.join('|')!==tables.map(([name])=>name).sort().join('|'))throw Error('restoration_execution_lineage_schema');
  const result:Record<string,Record<string,unknown>[]>={};
  for(const[table,order]of tables){
    const rows=(await db.prepare('SELECT * FROM '+table+' ORDER BY '+order).all<Record<string,unknown>>()).results;
    if(rows.length>(table==='beedle_restore_execution_history'?16000:table==='beedle_restore_execution_current'?1:17))
      throw Error('restoration_execution_lineage_rows');result[table]=rows;
  }
  if(JSON.stringify(result).length>8*1024*1024)throw Error('restoration_execution_lineage_bytes');
  return result;
}
// A later recovery must declare the newest writer as well as the original root
// and V2 source successor. An older plan cannot silently ignore this journal.
export async function verifyHeldRestorationExecution(db:D1Database,source:RestorationIdentity,
  expected:z.infer<typeof heldRestorationExecutionSchema>|null){
  const journal=await restorationExecutionJournalSnapshot(db);
  if(expected===null){if(journal!==null)throw Error('restoration_unplanned_execution_lineage');return;}
  if(!journal||await fingerprint(journal)!==expected.journalSha256)throw Error('restoration_execution_journal_changed');
  const runs=journal.beedle_restore_execution_runs!,ticks=journal.beedle_restore_execution_ticks!,histories=journal.beedle_restore_execution_history!,
    current=journal.beedle_restore_execution_current?.[0],run=runs.find(row=>row.execution_id===expected.executionId),
    tick=ticks.find(row=>row.execution_id===expected.executionId);
  if(!run||!tick||!current||run.state!=='held'||tick.state!=='held'||!run.error_sha256||
    run.execution_plan_sha256!==expected.executionPlanSha256||tick.execution_plan_sha256!==expected.executionPlanSha256||
    run.owner_id!==source.ownerId||run.source_plan_sha256!==source.planSha256||
    current.owner_id!==source.ownerId||current.source_plan_sha256!==source.planSha256||
    current.execution_id!==expected.executionId||current.execution_plan_sha256!==expected.executionPlanSha256||
    await fingerprint(run)!==expected.controlSha256||await fingerprint(tick)!==expected.tickSha256||await fingerprint(current)!==expected.currentSha256||
    runs.some(row=>!['held','completed'].includes(String(row.state)))||ticks.some(row=>row.state==='running')||histories.some(row=>row.state==='running'))
    throw Error('restoration_held_execution_changed');
  if(tick.token===null){if(expected.historySha256!==null)throw Error('restoration_execution_history_declaration');}
  else{
    const history=histories.find(row=>row.token===tick.token);
    if(!history||history.state!=='held'||history.execution_id!==expected.executionId||history.execution_plan_sha256!==expected.executionPlanSha256||
      history.error_sha256!==run.error_sha256||!history.completed_at||await fingerprint(history)!==expected.historySha256)
      throw Error('restoration_held_execution_history_changed');
  }
  if(typeof run.identity_json!=='string')throw Error('restoration_execution_identity_missing');
  const identity=parseRestorationSourceExecutionIdentity(JSON.parse(run.identity_json));
  if(canonical(identity)!==run.identity_json||await fingerprint(identity)!==run.identity_sha256||
    await fingerprint(identity.source)!==await fingerprint(source))throw Error('restoration_execution_identity_changed');
  const one=async(sql:string,args:unknown[])=>{
    const rows=(await db.prepare(sql).bind(...args).all<Record<string,unknown>>()).results;
    if(rows.length!==1||!rows[0])throw Error('restoration_execution_preserved_case_missing');return rows[0];
  };
  for(const item of identity.preservedCases){
    const document=await one('SELECT * FROM documents WHERE id=?',[item.documentId]),job=await one('SELECT * FROM beedle_restore_jobs WHERE document_id=?',[item.documentId]),
      generations=(await db.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').bind(item.documentId).all()).results,
      stages:Record<string,unknown[]>={};
    for(const table of Object.keys(generationTables))stages[table]=(await db.prepare('SELECT * FROM staged_'+table+
      ' WHERE generation_id IN(SELECT id FROM document_generations WHERE document_id=?) ORDER BY generation_id,id').bind(item.documentId).all()).results;
    if(Object.values(stages).some(rows=>rows.length>10000)||JSON.stringify(stages).length>8*1024*1024||
      await fingerprint(document)!==item.documentSha256||await fingerprint(job)!==item.jobSha256||
      await fingerprint(generations)!==item.generationsSha256||await fingerprint(stages)!==item.stagesSha256)
      throw Error('restoration_execution_preserved_case_changed');
  }
}
