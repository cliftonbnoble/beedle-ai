import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {claimedSourceRecoveryFixture} from './helpers/restoration-claimed-source-recovery-fixture.mjs';
import {canonical,fingerprint} from '../scripts/lib/release-contract.mjs';

const {recoverClaimedRestorationSource:recover}=loadTs('src/services/restoration-claimed-source-recovery.ts');
const {restorationExecutionJournalSnapshot:journal}=loadTs('src/services/restoration-held-execution-lineage.ts');
const changes=x=>x.sqlite.prepare('SELECT total_changes() n').get().n;
const read=async(x,name)=>JSON.parse(new TextDecoder().decode(await(await x.proofs.get('claimed-source-recovery/'+x.recoveryPlan.operationId+'/'+name+'.json')).arrayBuffer()));

test('a separately planned forward recovery preserves the original claim and every failed executor row',async()=>{
  const x=await claimedSourceRecoveryFixture();try{
    const result=await recover(x.recoveryPlan,x.bindings);
    assert.equal(result.state,'claimed_source_recovered_forward_unpublished_unreviewed');
    assert.equal(result.sourceResults,1);assert.equal(result.humanApproval,false);assert.equal(result.fullIntegrityCredited,false);
    assert.equal(result.oldTargetReadFailureCauseStillUnknown,true);assert(result.sqlBindingCalls<=700);
    assert(result.capacity.samples>0);assert.equal(result.capacity.unobservedTransientPeakCredited,false);
    assert.equal(canonical(await journal(x.db)),canonical(x.failedJournal));
    const job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs').get();assert.equal(job.phase,'processed');assert.equal(job.claim_token,x.claimedJob.claim_token);assert.equal(job.claimed_at,x.claimedJob.claimed_at);
    const document=x.sqlite.prepare('SELECT * FROM documents').get();assert.equal(document.searchable_at,null);assert.equal(document.published_revision,null);assert.equal(document.approved_at,null);assert.equal(document.qc_required_confirmed,0);
    const receipt=x.sqlite.prepare('SELECT * FROM beedle_restore_source_results').get(),proof=JSON.parse(receipt.proof_json);
    assert.equal(proof.version,4);assert.equal(proof.claimedSourceRecovery.operationId,x.recoveryPlan.operationId);assert.equal(proof.claimToken,x.claimedJob.claim_token);assert.equal(proof.oldExecutionStillHeld,true);
    assert.equal((await read(x,'outcome')).state,result.state);
  }finally{x.sqlite.close();}
});

for(const key of ['schemaSha256','ownerSha256','rootTickSha256','jobSha256','otherJobsSha256','otherDocumentsSha256','sourceResultsSha256']){
  test('changed '+key+' cannot advance the original claimed job',async()=>{
    const x=await claimedSourceRecoveryFixture();try{const before=changes(x);x.recoveryPlan[key]='f'.repeat(64);await assert.rejects(recover(x.recoveryPlan,x.bindings));assert.equal(changes(x),before);assert.equal(canonical(await journal(x.db)),canonical(x.failedJournal));}finally{x.sqlite.close();}
  });
}
for(const key of ['controlSha256','tickSha256','historySha256','currentSha256','journalSha256','executionPlanSha256']){
  test('changed held executor '+key+' cannot advance a source',async()=>{
    const x=await claimedSourceRecoveryFixture();try{const before=changes(x);x.recoveryPlan.heldExecution[key]='f'.repeat(64);await assert.rejects(recover(x.recoveryPlan,x.bindings));assert.equal(changes(x),before);}finally{x.sqlite.close();}
  });
}
for(const mode of ['expired','future','long-window','copy-version','copy-etag','original-version','initial-metadata','initial-revision','coverage','held-policy','unknown-schema','missing-copy','copy-permission','binding-alias','missing-capacity','capacity-limit']){
  test(mode+' remains held without generation writes or a copy fallback',async()=>{
    const x=await claimedSourceRecoveryFixture();try{
      const p=x.recoveryPlan;
      if(mode==='expired'){p.notBefore=new Date(Date.now()-30000).toISOString();p.notAfter=new Date(Date.now()-1000).toISOString();}
      if(mode==='future')p.notBefore=new Date(Date.now()+10000).toISOString();
      if(mode==='long-window')p.notAfter=new Date(Date.parse(p.notBefore)+31*60000).toISOString();
      if(mode==='copy-version')p.copy.version='different-version';if(mode==='copy-etag')p.copy.etag='different-etag';
      if(mode==='original-version')p.job.originalVersion='different-version';if(mode==='initial-metadata')p.job.initialMetadataSha256='f'.repeat(64);
      if(mode==='initial-revision')p.job.initialRevision++;if(mode==='coverage')p.expectedSourceResults++;
      if(mode==='held-policy')p.job.extractionPolicy='held-unreadable';
      if(mode==='unknown-schema')x.sqlite.exec('CREATE TABLE unplanned_writer(value TEXT)');
      if(mode==='missing-copy')x.bindings.target={...x.target,async get(){return null;},async put(){throw Error('Copy fallback forbidden');}};
      if(mode==='copy-permission')x.bindings.target={...x.target,async get(){throw Error('Synthetic R2 permission denied');},async put(){throw Error('Copy fallback forbidden');}};
      if(mode==='binding-alias')x.bindings.originals=x.bindings.target;
      if(mode==='missing-capacity')x.capacityFixtureState.omitSize=true;if(mode==='capacity-limit')x.capacityFixtureState.sizeOverride=9000000000;
      const before=changes(x),puts=x.puts();await assert.rejects(recover(p,x.bindings));assert.equal(changes(x),before);assert.equal(x.puts(),puts);
      assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM document_generations').get().n,0);assert.equal(canonical(await journal(x.db)),canonical(x.failedJournal));
    }finally{x.sqlite.close();}
  });
}

for(const name of ['claim','source-read','source-result','outcome']){
  test('lost '+name+' acknowledgment cannot replay the forward operation',async()=>{
    const x=await claimedSourceRecoveryFixture();try{
      const original=x.proofs.put;x.proofs.put=async(...args)=>{const result=await original(...args);if(args[0].endsWith('/'+name+'.json'))throw Error('Synthetic lost '+name+' acknowledgment');return result;};
      await assert.rejects(recover(x.recoveryPlan,x.bindings));x.proofs.put=original;const before=changes(x),puts=x.puts();
      assert.deepEqual(await recover(x.recoveryPlan,x.bindings),{state:'already_claimed',sqlReissued:false});assert.equal(changes(x),before);assert.equal(x.puts(),puts);
      assert.equal(canonical(await journal(x.db)),canonical(x.failedJournal));
      assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,name==='outcome'?1:0);
    }finally{x.sqlite.close();}
  });
}

for(const mode of ['copy-checkpoint','pointer-checkpoint','finalization','capacity-after-copy']){
  test('acknowledged or uncertain '+mode+' retains its actual checkpoint without replay',async()=>{
    const x=await claimedSourceRecoveryFixture();try{
      const original=x.db;
      function prepare(sql,values=[]){const statement=original.prepare(sql).bind(...values);return new Proxy(statement,{get(target,key){
        if(key==='bind')return(...args)=>prepare(sql,args);
        if(key==='all')return async()=>{
          const result=await target.all();
          if((mode==='copy-checkpoint'&&sql.startsWith("UPDATE beedle_restore_jobs SET phase='source_copied'"))||
            (mode==='pointer-checkpoint'&&sql.startsWith('UPDATE documents SET source_r2_key=')))throw Error('Synthetic lost SQL acknowledgment');
          if(mode==='capacity-after-copy'&&sql.startsWith("UPDATE beedle_restore_jobs SET phase='source_copied'"))return{...result,meta:{...result.meta,size_after:9000000000}};
          return result;
        };
        const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
      }});}
      x.bindings.db=new Proxy(original,{get(target,key){if(key==='prepare')return prepare;
        if(key==='batch')return async(...args)=>{const result=await target.batch(...args);if(mode==='finalization'&&await x.proofs.get('claimed-source-recovery/'+x.recoveryPlan.operationId+'/source-result.json'))throw Error('Synthetic lost final SQL acknowledgment');return result;};
        const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
      }});
      await assert.rejects(recover(x.recoveryPlan,x.bindings));const before=changes(x),puts=x.puts();
      assert.deepEqual(await recover(x.recoveryPlan,x.bindings),{state:'already_claimed',sqlReissued:false});assert.equal(changes(x),before);assert.equal(x.puts(),puts);
      const job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs').get();assert.equal(job.phase,mode==='finalization'?'processed':'source_copied');assert.equal(job.claim_token,x.claimedJob.claim_token);
      assert.equal(x.sqlite.prepare('SELECT COUNT(*) n FROM beedle_restore_source_results').get().n,mode==='finalization'?1:0);
      assert.equal(canonical(await journal(x.db)),canonical(x.failedJournal));
      if(mode==='capacity-after-copy')assert.equal((await read(x,'failure')).capacity.limitReached,true);
    }finally{x.sqlite.close();}
  });
}

test('the Workflow requires its exact enabled instance and zero-retry single-case step',async()=>{
  const {runClaimedSourceRecoveryWorkflow:run}=loadTs('src/services/restoration-claimed-source-recovery-workflow.ts');
  const x=await claimedSourceRecoveryFixture();try{
    const env={DB:x.db,ORIGINALS:x.originals,TARGET_ORIGINALS:x.target,PROOFS:x.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(x.recoveryPlan)};
    const event={instanceId:x.recoveryPlan.operationId,payload:{operationPlanSha256:await fingerprint(x.recoveryPlan)}};
    const step={async do(name,options,callback){assert.equal(name,'recover-original-claimed-source-forward-once');assert.equal(options.retries.limit,0);assert.equal(options.timeout,'10 minutes');return callback();}};
    const before=changes(x);await assert.rejects(run({...env,OPERATION_ENABLED:'false'},event,step));await assert.rejects(run(env,{...event,instanceId:crypto.randomUUID()},step));await assert.rejects(run(env,{...event,payload:{...event.payload,extra:true}},step));assert.equal(changes(x),before);
    assert.equal((await run(env,event,step)).state,'claimed_source_recovered_forward_unpublished_unreviewed');
  }finally{x.sqlite.close();}
});
