import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {sourceExecutionWorkflowFixture} from './helpers/restoration-source-execution-workflow-fixture.mjs';
import {fingerprint} from '../scripts/lib/release-contract.mjs';
const lineage=loadTs('src/services/restoration-held-execution-lineage.ts');
const control=loadTs('src/services/restoration-source-execution-control.ts');
async function fixture(){
  const x=await sourceExecutionWorkflowFixture('held-successor');
  await control.initializeRestorationSourceExecution(x.env.DB,x.execution,x.beforeSchemaSha256,x.afterSchemaSha256);
  await control.claimRestorationSourceExecutionTick(x.env.DB,x.execution);
  await control.holdRestorationSourceExecution(x.env.DB,x.execution,await fingerprint('synthetic terminal CPU guard'));
  const journal=await lineage.restorationExecutionJournalSnapshot(x.env.DB),run=journal.beedle_restore_execution_runs[0],
    tick=journal.beedle_restore_execution_ticks[0],history=journal.beedle_restore_execution_history[0],current=journal.beedle_restore_execution_current[0];
  const expected={executionId:x.execution.executionId,executionPlanSha256:x.execution.executionPlanSha256,
    controlSha256:await fingerprint(run),tickSha256:await fingerprint(tick),historySha256:await fingerprint(history),
    currentSha256:await fingerprint(current),journalSha256:await fingerprint(journal)};
  return{...x,journal,expected};
}
test('an older recovery cannot ignore a newer execution journal',async()=>{
  const x=await fixture();try{
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await assert.rejects(lineage.verifyHeldRestorationExecution(x.env.DB,x.identity,null),/unplanned_execution_lineage/);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
  }finally{x.sqlite.close();}
});
test('exact terminal lineage preserves all run, history, current and source-case rows',async()=>{
  const x=await fixture();try{
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
    await lineage.verifyHeldRestorationExecution(x.env.DB,x.identity,x.expected);
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);
    assert.deepEqual(await lineage.restorationExecutionJournalSnapshot(x.env.DB),x.journal);
  }finally{x.sqlite.close();}
});
test('a differing source owner or any declared control digest fails closed',async()=>{
  const x=await fixture();try{
    await assert.rejects(lineage.verifyHeldRestorationExecution(x.env.DB,{...x.identity,ownerId:crypto.randomUUID()},x.expected));
    for(const key of ['controlSha256','tickSha256','historySha256','currentSha256','journalSha256'])
      await assert.rejects(lineage.verifyHeldRestorationExecution(x.env.DB,x.identity,{...x.expected,[key]:'0'.repeat(64)}));
  }finally{x.sqlite.close();}
});
test('a preserved peer case cannot drift behind unchanged operational rows',async()=>{
  const x=await fixture();try{
    const id=x.execution.preservedCases[0].documentId;
    x.sqlite.prepare("UPDATE documents SET title='Synthetic changed peer' WHERE id=?").run(id);
    assert.equal(await fingerprint(await lineage.restorationExecutionJournalSnapshot(x.env.DB)),x.expected.journalSha256);
    await assert.rejects(lineage.verifyHeldRestorationExecution(x.env.DB,x.identity,x.expected),/preserved_case_changed/);
  }finally{x.sqlite.close();}
});
test('a newly active execution cannot be accepted by merely refreshing the journal hash',async()=>{
  const x=await fixture();try{
    const id=crypto.randomUUID(),next={...x.execution,executionId:id,executionPlanSha256:await fingerprint(id),
      priorJournalSha256:await fingerprint(await control.restorationPriorExecutionJournal(x.env.DB,id)),
      previousCurrentSha256:await fingerprint(x.journal.beedle_restore_execution_current[0])};
    await control.initializeRestorationSourceExecution(x.env.DB,next,x.afterSchemaSha256,x.afterSchemaSha256);
    const fresh=await fingerprint(await lineage.restorationExecutionJournalSnapshot(x.env.DB));
    await assert.rejects(lineage.verifyHeldRestorationExecution(x.env.DB,x.identity,{...x.expected,journalSha256:fresh}),/held_execution_changed/);
  }finally{x.sqlite.close();}
});
test('an explicitly absent journal remains valid only before its first initializer',async()=>{
  const x=await sourceExecutionWorkflowFixture();try{
    assert.equal(await lineage.restorationExecutionJournalSnapshot(x.env.DB),null);
    await lineage.verifyHeldRestorationExecution(x.env.DB,x.identity,null);
  }finally{x.sqlite.close();}
});
