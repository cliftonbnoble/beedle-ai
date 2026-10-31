import {z} from 'zod';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {generationTables} from './generation-tables';

const hash=z.string().regex(/^[a-f0-9]{64}$/);
export const heldSourceSuccessorSchema=z.object({successorId:z.string().uuid(),successorPlanSha256:hash,
  controlSha256:hash,tickSha256:hash,historySha256:hash}).strict();
type Identity={ownerId:string;planSha256:string};

// The original held root is checked by each caller. A new recovery must also
// pin every known successor rather than silently ignoring its later hold.
export async function verifyHeldSourceLineage(db:D1Database,identity:Identity,
  expected:z.infer<typeof heldSourceSuccessorSchema>|null){
  const names=(await db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'beedle_restore_source_%' AND name!='beedle_restore_source_results' ORDER BY name")
    .all<{name:string}>()).results.map(row=>row.name);
  if(expected===null){if(names.length)throw Error('restoration_unplanned_source_lineage');return;}
  if(names.join('|')!=='beedle_restore_source_successor|beedle_restore_source_successor_history|beedle_restore_source_successor_tick')
    throw Error('restoration_source_lineage_schema');
  const control=await db.prepare('SELECT * FROM beedle_restore_source_successor WHERE singleton=1').first<Record<string,unknown>>();
  const tick=await db.prepare('SELECT * FROM beedle_restore_source_successor_tick WHERE singleton=1').first<Record<string,unknown>>();
  const history=tick?await db.prepare('SELECT * FROM beedle_restore_source_successor_history WHERE token=?').bind(tick.token)
    .first<Record<string,unknown>>():null;
  if(!control||!tick||!history||control.state!=='held'||tick.state!=='held'||history.state!=='held'||
    !tick.token||!history.completed_at||!history.error_sha256||control.owner_id!==identity.ownerId||
    control.source_plan_sha256!==identity.planSha256||control.successor_id!==expected.successorId||
    control.successor_plan_sha256!==expected.successorPlanSha256||tick.successor_id!==expected.successorId||
    history.successor_id!==expected.successorId||tick.successor_plan_sha256!==expected.successorPlanSha256||
    history.successor_plan_sha256!==expected.successorPlanSha256||await fingerprint(control)!==expected.controlSha256||
    await fingerprint(tick)!==expected.tickSha256||await fingerprint(history)!==expected.historySha256)
    throw Error('restoration_held_source_lineage_changed');
}

export async function verifyPreservedFailedStage(db:D1Database,generationId:string,expected:string|null){
  const stages:Record<string,Record<string,unknown>[]>={};
  for(const table of Object.keys(generationTables)){
    const rows=(await db.prepare('SELECT * FROM staged_'+table+' WHERE generation_id=? ORDER BY id')
      .bind(generationId).all<Record<string,unknown>>()).results;
    if(rows.length>10000||JSON.stringify(rows).length>8*1024*1024||expected===null&&rows.length!==0)
      throw Error('restoration_failed_stage_changed');
    stages[table]=rows;
  }
  if(expected!==null&&await fingerprint(stages)!==expected)throw Error('restoration_failed_stage_changed');
}
