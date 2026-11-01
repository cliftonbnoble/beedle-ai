import {loadTs} from './load-ts.mjs';
import {restorationSourceJobFixture} from './restoration-source-job-fixture.mjs';
import {successorJobFixture} from './restoration-source-successor-job-fixture.mjs';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';
const control=loadTs('src/services/restoration-source-execution-control.ts');
const old=loadTs('src/services/restoration-source-successor-control.ts');
export async function sourceExecutionFixture(mode='seeded',{policy,extraDocumentId,partitionPlan=false}={}){
  if(!['seeded','held','held-successor'].includes(mode))throw Error('Explicit fixture mode');
  const x=mode==='seeded'?await restorationSourceJobFixture(policy,{idle:true,extraDocumentId,partitionPlan}):
    await successorJobFixture({initialize:mode==='held-successor',preservedGenerationStatus:'published'});
  if(extraDocumentId){
    if(mode!=='seeded')throw Error('Extra synthetic job requires seeded fixture');
    const metadata=await loadTs('src/services/restoration-source-job.ts').restorationSeedMetadataSha256(x.db,extraDocumentId);
    x.extraJob={...x.job,documentId:extraDocumentId,originalKey:'next.md',initialMetadataSha256:metadata.sha256,initialRevision:metadata.row.document_revision};
    const job=x.extraJob;x.sqlite.prepare("INSERT INTO beedle_restore_jobs(document_id,source_sha256,source_bytes,original_key,plan_object_key,extraction_policy,phase) VALUES(?,?,?,?,?,?,'planned')")
      .run(job.documentId,job.sourceSha256,job.sourceBytes,job.originalKey,job.planObjectKey,job.extractionPolicy);
  }
  if(mode==='seeded')x.sqlite.exec("UPDATE beedle_restore_owner SET phase='seeded'");
  if(mode==='held-successor'){
    const tick=await old.claimRestorationSourceSuccessorTick(x.db,x.successor);
    await old.holdRestorationSourceSuccessorTick(x.db,x.successor,tick.token,'e'.repeat(64));
  }
  const one=sql=>x.sqlite.prepare(sql).get(),schemaRows=()=>x.sqlite.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all();
  const owner=one('SELECT * FROM beedle_restore_owner'),tick=one('SELECT * FROM beedle_restore_tick'),
    histories=x.sqlite.prepare('SELECT * FROM beedle_restore_tick_history ORDER BY token').all(),preservedCases=[];
  if(mode!=='seeded'){
    const stages={};for(const table of ['document_sections','section_paragraphs','document_chunks','document_reference_links','document_reference_issues'])
      stages[table]=x.sqlite.prepare('SELECT * FROM staged_'+table+' WHERE generation_id IN(SELECT id FROM document_generations WHERE document_id=?) ORDER BY generation_id,id').all(x.first);
    preservedCases.push({documentId:x.first,documentSha256:await fingerprint(x.sqlite.prepare('SELECT * FROM documents WHERE id=?').get(x.first)),
      jobSha256:await fingerprint(x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(x.first)),
      generationsSha256:await fingerprint(x.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').all(x.first)),stagesSha256:await fingerprint(stages)});
  }
  let heldSuccessor=null,heldRows=null;
  if(mode==='held-successor'){
    const c=one('SELECT * FROM beedle_restore_source_successor'),t=one('SELECT * FROM beedle_restore_source_successor_tick'),h=x.sqlite.prepare('SELECT * FROM beedle_restore_source_successor_history WHERE token=?').get(t.token);
    heldRows={control:c,tick:t,history:h};heldSuccessor={successorId:c.successor_id,successorPlanSha256:c.successor_plan_sha256,controlSha256:await fingerprint(c),tickSha256:await fingerprint(t),historySha256:await fingerprint(h)};
  }
  const executionId=crypto.randomUUID(),identity={source:x.identity,executionId,executionPlanSha256:await fingerprint(executionId),
    initialProcessed:one('SELECT COUNT(*) n FROM beedle_restore_source_results').n,initialRemaining:one("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='planned'").n,
    parent:{phase:owner.phase,ownerSha256:await fingerprint(owner),tickSha256:await fingerprint(tick),historySha256:histories.length?await fingerprint(histories[0]):null,heldSuccessor},
    priorJournalSha256:await fingerprint(await control.restorationPriorExecutionJournal(x.db,executionId)),previousCurrentSha256:null,preservedCases};
  const beforeSchemaSha256=await restorationSchemaFingerprint(schemaRows());let afterSchemaSha256;
  x.sqlite.exec('BEGIN');try{for(const sql of control.restorationSourceExecutionSql)x.sqlite.exec(sql);afterSchemaSha256=await restorationSchemaFingerprint(schemaRows());}finally{x.sqlite.exec('ROLLBACK');}
  const oldRows={owner,tick,histories,heldRows};
  return {...x,execution:identity,beforeSchemaSha256,afterSchemaSha256,oldRows,mode};
}
