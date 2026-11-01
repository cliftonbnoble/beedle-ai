import assert from 'node:assert/strict';
import {sourceExecutionWorkflowFixture} from './restoration-source-execution-workflow-fixture.mjs';
import {loadTs} from './load-ts.mjs';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';

export async function claimedSourceRecoveryFixture(){
  const x=await sourceExecutionWorkflowFixture();
  const proofKeys=new Set([x.job.planObjectKey]),targetKeys=new Set();
  const proofPut=x.proofs.put,targetPut=x.target.put;
  x.proofs.put=async(...args)=>{const value=await proofPut(...args);proofKeys.add(args[0]);return value;};
  x.target.put=async(...args)=>{const value=await targetPut(...args);targetKeys.add(args[0]);return value;};
  const copy=await loadTs('src/services/restoration-source.ts').copyVerifiedRestorationSource(x.originals,x.target,{
    ownerId:x.identity.ownerId,planSha256:x.identity.planSha256,partitionIndex:x.identity.partitionIndex,...x.job});
  const failedTarget={async get(){throw Error('Synthetic target read failure');},async put(){throw Error('Source copy write forbidden');}};
  x.env.TARGET_ORIGINALS=failedTarget;
  await assert.rejects(loadTs('src/services/restoration-source-execution-workflow.ts').runSourceExecutionWorkflow(x.env,{
    instanceId:x.execution.executionId,payload:{operationPlanSha256:await fingerprint(x.plan)}
  },{async do(_name,options,callback){assert.equal(options.retries.limit,0);return callback();}}),/step_held/);
  x.env.TARGET_ORIGINALS=x.target;
  const journal=await loadTs('src/services/restoration-held-execution-lineage.ts').restorationExecutionJournalSnapshot(x.db);
  const run=journal.beedle_restore_execution_runs[0],tick=journal.beedle_restore_execution_ticks[0],history=journal.beedle_restore_execution_history.find(row=>row.token===tick.token);
  const one=sql=>x.sqlite.prepare(sql).get(),all=sql=>x.sqlite.prepare(sql).all();
  const job=one('SELECT * FROM beedle_restore_jobs');
  assert.equal(job.phase,'claimed');assert.equal(job.source_copy_key,null);assert.equal(job.generation_id,null);
  assert.equal(history.deliveries,0);assert.equal(run.error_sha256,await fingerprint('target_access_failed'));
  // memoryD1 all() omits mutation metadata. Derive it from actual SQLite changes,
  // while retaining the fixture's acknowledged LOCAL page-allocation samples.
  function prepare(sql,values=[]){const p=x.env.DB.prepare(sql).bind(...values);return new Proxy(p,{get(target,key){
    if(key==='bind')return(...args)=>prepare(sql,args);
    if(key==='all')return async()=>{const before=one('SELECT total_changes() n').n,result=await target.all();return{...result,meta:{...result.meta,changes:one('SELECT total_changes() n').n-before}};};
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});}
  const db=new Proxy(x.env.DB,{get(target,key){if(key==='prepare')return prepare;const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
  const now=Date.now(),plan={version:1,operationId:crypto.randomUUID(),notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+29*60000).toISOString(),
    identity:x.identity,schemaSha256:await restorationSchemaFingerprint(all("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name")),
    ownerSha256:await fingerprint(one('SELECT * FROM beedle_restore_owner')),rootTickSha256:await fingerprint(one('SELECT * FROM beedle_restore_tick')),
    rootHistorySha256:null,heldSuccessor:null,heldExecution:{executionId:run.execution_id,executionPlanSha256:run.execution_plan_sha256,
      controlSha256:await fingerprint(run),tickSha256:await fingerprint(tick),historySha256:await fingerprint(history),
      currentSha256:await fingerprint(journal.beedle_restore_execution_current[0]),journalSha256:await fingerprint(journal)},
    expectedSourceResults:0,jobSha256:await fingerprint(job),otherJobsSha256:await fingerprint([]),otherDocumentsSha256:await fingerprint([]),
    sourceResultsSha256:await fingerprint([]),job:{...x.job,originalVersion:copy.originalVersion},copy:{version:copy.targetVersion,etag:copy.targetEtag},profile:x.profile};
  return{...x,db,recoveryPlan:plan,failedJournal:journal,claimedJob:job,proofKeys,targetKeys,bindings:{db,originals:x.originals,target:x.target,proofs:x.proofs}};
}
