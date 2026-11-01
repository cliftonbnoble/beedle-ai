import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {provisionalRestorationReviewerFixture} from './helpers/restoration-reviewer-workflow-fixture.mjs';
import {canonical,fingerprint} from '../scripts/lib/release-contract.mjs';
import {partitionIntegrityCommands} from '../scripts/lib/corpus-partition-contract.mjs';
const service=loadTs('src/services/restoration-final-integrity-workflow.ts'),reviewer=loadTs('src/services/restoration-reviewer-workflow.ts');
const kinds=['metadata','body','phrase','physical','foreign_keys'];
const steps=()=>({async do(_name,options,callback){assert.equal(options.retries.limit,0);return callback();}});
const event=async x=>({instanceId:x.plan.operationId,payload:{operationPlanSha256:await fingerprint(x.plan)}});
async function fixture({withSource=true}={}){
  const x=await provisionalRestorationReviewerFixture();
  await reviewer.runRestorationReviewerWorkflow(x.env,await event(x),steps());
  const one=sql=>x.sqlite.prepare(sql).get(),all=sql=>x.sqlite.prepare(sql).all(),now=new Date().toISOString(),revision=one('SELECT revision FROM vector_lifecycle_epoch').revision;
  // Synthetic local protocol prerequisite only. This supplies no remote source
  // proof and is never inserted by the five-command component under test.
  if(withSource)x.sqlite.prepare("INSERT INTO beedle_restore_integrity VALUES('source_sha',?,?,?,?,?,'passed',1,?,?,?)")
    .run(x.identity.ownerId,x.identity.planSha256,revision,crypto.randomUUID(),partitionIntegrityCommands.source_sha.sha256,'a'.repeat(64),now,now);
  const plan={version:1,operationId:crypto.randomUUID(),source:x.identity,publicationPolicy:'unreviewed-provisional-v1',
    schemaSha256:x.plan.schemaSha256,ownerSha256:x.plan.ownerSha256,journalsSha256:x.plan.journalsSha256,
    jobsSha256:x.plan.jobsSha256,receiptsSha256:x.plan.receiptsSha256,indexControlsSha256:x.plan.indexControlsSha256,
    documentsSha256:await fingerprint(all('SELECT * FROM documents ORDER BY id')),
    materializationsSha256:await fingerprint(await service.restorationFinalMaterializations(x.env.DB)),
    sourceShaReceiptSha256:withSource?await fingerprint(one("SELECT * FROM beedle_restore_integrity WHERE kind='source_sha'")):'0'.repeat(64),
    corpusRevision:revision,catalogVersion:x.plan.catalogVersion,documents:x.plan.documents,publishedDocuments:x.plan.publishedDocuments,
    notBefore:new Date(Date.now()-1000).toISOString(),notAfter:new Date(Date.now()+20*60000).toISOString(),completionDeadline:new Date(Date.now()+60*60000).toISOString()};
  return{...x,plan,env:{DB:x.env.DB,PROOFS:x.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(plan)}};
}
const domain=x=>Object.fromEntries(['documents','document_generations','beedle_restore_owner','beedle_restore_jobs','beedle_restore_source_results',
  'beedle_restore_tick','beedle_restore_tick_history','beedle_restore_execution_runs','beedle_restore_execution_ticks','beedle_restore_execution_history',
  'beedle_restore_execution_current','document_index_codes','document_rules_sections','document_ordinance_sections','reviewer_queue_control','reviewer_queue_state']
  .map(table=>[table,x.sqlite.prepare('SELECT * FROM '+table+' ORDER BY 1').all()]));
function instrument(x,handler){
  const counts={commands:0},db=x.env.DB;
  function prepare(sql,values=[]){const statement=db.prepare(sql).bind(...values);return new Proxy(statement,{get(target,key){
    if(key==='bind')return(...args)=>prepare(sql,args);
    if(key==='all'||key==='run')return async()=>{
      const kind=kinds.find(kind=>partitionIntegrityCommands[kind].command===sql);if(kind)counts.commands++;
      // memoryD1.all() omits changes; emulate D1's acknowledged metadata using
      // actual SQLite changes, without issuing a second application statement.
      const before=x.sqlite.prepare('SELECT total_changes() n').get().n,raw=await target[key](),
        result={...raw,meta:{...raw.meta,changes:raw.meta.changes??x.sqlite.prepare('SELECT total_changes() n').get().n-before}};
      return handler?handler({sql,kind,result}):result;
    };
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});}
  x.env.DB=new Proxy(db,{get(target,key){if(key==='prepare')return prepare;const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
  return counts;
}
test('five fixed full commands require the source receipt, retain domain rows and produce exact revision-bound acknowledgments',async()=>{
  const x=await fixture();try{
    const before=domain(x),source=x.sqlite.prepare("SELECT * FROM beedle_restore_integrity WHERE kind='source_sha'").get(),counts=instrument(x),names=[];
    const result=await service.runRestorationFinalIntegrityWorkflow(x.env,await event(x),{async do(name,options,callback){assert.equal(options.retries.limit,0);names.push(name);return callback();}});
    assert.equal(result.state,'five_full_D1_integrity_checks_passed');assert.equal(result.noSealPeakCapacityOrReleaseApproval,true);assert.equal(counts.commands,5);assert.equal(names.length,7);
    assert.deepEqual(domain(x),before);assert.deepEqual(x.sqlite.prepare("SELECT * FROM beedle_restore_integrity WHERE kind='source_sha'").get(),source);
    const rows=x.sqlite.prepare('SELECT * FROM beedle_restore_integrity ORDER BY kind').all();assert.equal(rows.length,6);
    for(const row of rows){assert.equal(row.status,'passed');assert.equal(row.engine_acknowledged,1);assert.equal(row.command_sha256,partitionIntegrityCommands[row.kind].sha256);assert.equal(row.corpus_revision,x.plan.corpusRevision);}
    await assert.rejects(async()=>service.runRestorationFinalIntegrityWorkflow(x.env,await event(x),steps()),/already_claimed/);assert.equal(counts.commands,5);
  }finally{x.sqlite.close();}
});
for(const mode of ['owner','schema','ledger','journal','jobs','receipts','documents','materialization','corpus','catalog','approval','publication','source_receipt','missing_source','extra_receipt','lease','disabled','instance','expired','capacity_missing','capacity_limit']){
  test('final integrity holds '+mode+' before any integrity command',async()=>{
    const x=await fixture({withSource:mode!=='missing_source'});try{
      if(mode==='owner')x.sqlite.exec("UPDATE beedle_restore_owner SET last_outcome='changed'");
      if(mode==='schema')x.plan.schemaSha256='0'.repeat(64);if(mode==='ledger')x.sqlite.exec('DELETE FROM d1_migrations WHERE id=(SELECT MAX(id) FROM d1_migrations)');
      for(const[key,field]of [['journal','journalsSha256'],['jobs','jobsSha256'],['receipts','receiptsSha256'],['documents','documentsSha256'],['materialization','materializationsSha256']])if(mode===key)x.plan[field]='0'.repeat(64);
      if(mode==='corpus')x.plan.corpusRevision++;if(mode==='catalog')x.plan.catalogVersion='unknown';
      if(mode==='approval')x.sqlite.exec("UPDATE documents SET qc_required_confirmed=1 WHERE id=(SELECT MIN(id) FROM documents)");
      if(mode==='publication')x.plan.publishedDocuments--;
      if(mode==='source_receipt')x.plan.sourceShaReceiptSha256='0'.repeat(64);
      if(mode==='extra_receipt')x.sqlite.prepare("INSERT INTO beedle_restore_integrity SELECT 'metadata',owner_id,plan_sha256,corpus_revision,?,command_sha256,status,engine_acknowledged,result_sha256,claimed_at,completed_at FROM beedle_restore_integrity WHERE kind='source_sha'").run(crypto.randomUUID());
      if(mode==='lease')x.sqlite.exec("UPDATE reviewer_queue_control SET lease_owner='other',lease_until=unixepoch()+60");
      if(mode==='disabled')x.env.OPERATION_ENABLED='false';if(mode==='expired'){x.plan.notBefore=new Date(Date.now()-25*60000).toISOString();x.plan.notAfter=new Date(Date.now()-1000).toISOString();}
      if(mode==='capacity_missing')x.capacityFixtureState.omitSize=true;if(mode==='capacity_limit')x.capacityFixtureState.sizeOverride=9000000000;
      x.env.OPERATION_JSON=JSON.stringify(x.plan);const e=await event(x);if(mode==='instance')e.instanceId=crypto.randomUUID();
      const before=domain(x),counts=instrument(x);await assert.rejects(()=>service.runRestorationFinalIntegrityWorkflow(x.env,e,steps()));assert.equal(counts.commands,0);assert.deepEqual(domain(x),before);
    }finally{x.sqlite.close();}
  });
}
for(const kind of ['metadata','body','phrase']){
  test('full rank=1 check catches missing '+kind+' postings without repair or later commands',async()=>{
    const x=await fixture();try{
      const table={metadata:'search_metadata_substrings',body:'search_body_substrings',phrase:'search_chunks_fts'}[kind];
      x.sqlite.exec(`INSERT INTO ${table}(${table}) VALUES('delete-all')`);
      const before=domain(x),counts=instrument(x);await assert.rejects(async()=>service.runRestorationFinalIntegrityWorkflow(x.env,await event(x),steps()),/held/);
      assert.equal(counts.commands,kinds.indexOf(kind)+1);assert.deepEqual(domain(x),before);
      const row=x.sqlite.prepare('SELECT * FROM beedle_restore_integrity WHERE kind=?').get(kind);assert.equal(row.status,'claimed');assert.equal(row.engine_acknowledged,0);
    }finally{x.sqlite.close();}
  });
}
for(const mode of ['physical_not_ok','physical_multiple','foreign_key_violation','query_lost_ack','claim_lost_ack','receipt_lost_ack','query_missing_sample']){
  test('engine '+mode+' retains its claim and cannot reissue a command',async()=>{
    const x=await fixture();try{
      let injected=false;const counts=instrument(x,({sql,kind,result})=>{
        if(mode==='physical_not_ok'&&kind==='physical')return{...result,results:[{quick_check:'not ok'}]};
        if(mode==='physical_multiple'&&kind==='physical')return{...result,results:[{quick_check:'ok'},{quick_check:'ok'}]};
        if(mode==='foreign_key_violation'&&kind==='foreign_keys')return{...result,results:[{table:'synthetic',rowid:1,parent:'synthetic',fkid:0}]};
        if(!injected&&((mode==='query_lost_ack'&&kind==='metadata')||(mode==='claim_lost_ack'&&sql.startsWith('INSERT INTO beedle_restore_integrity'))||(mode==='receipt_lost_ack'&&sql.startsWith('UPDATE beedle_restore_integrity')))){injected=true;throw Error('Synthetic lost SQL acknowledgment');}
        if(mode==='query_missing_sample'&&kind==='metadata')return{...result,meta:{...result.meta,size_after:undefined}};
        return result;
      });
      await assert.rejects(async()=>service.runRestorationFinalIntegrityWorkflow(x.env,await event(x),steps()),/held/);const before=counts.commands;
      await assert.rejects(async()=>service.runRestorationFinalIntegrityWorkflow(x.env,await event(x),steps()),/already_claimed/);assert.equal(counts.commands,before);
      assert.equal(x.sqlite.prepare("SELECT COUNT(*) n FROM beedle_restore_integrity WHERE kind!='source_sha'").get().n,mode.startsWith('physical')?4:mode==='foreign_key_violation'?5:1);
      assert.equal(x.sqlite.prepare('SELECT phase FROM beedle_restore_owner').get().phase,'indexing');
    }finally{x.sqlite.close();}
  });
}
for(const name of ['initialize-claim.json','metadata-claim.json','metadata-outcome.json']){
  test('lost R2 '+name+' acknowledgment preserves the object and prevents replay',async()=>{
    const x=await fixture();try{
      const put=x.proofs.put;let lost=false;x.proofs.put=async(...args)=>{const result=await put(...args);if(args[0].endsWith('/'+name)&&!lost){lost=true;throw Error('Synthetic unknown R2 acknowledgment');}return result;};
      const counts=instrument(x);await assert.rejects(async()=>service.runRestorationFinalIntegrityWorkflow(x.env,await event(x),steps()));const before=counts.commands;
      assert.ok(await x.proofs.get('final-integrity/'+x.plan.operationId+'/'+name));await assert.rejects(async()=>service.runRestorationFinalIntegrityWorkflow(x.env,await event(x),steps()),/already_claimed/);assert.equal(counts.commands,before);
    }finally{x.sqlite.close();}
  });
}
test('final integrity plan accepts no arbitrary command, unbounded window, unknown policy or oversized configuration',async()=>{
  const x=await fixture();try{
    for(const bad of [{...x.plan,sql:'SELECT 1'},{...x.plan,notAfter:new Date(Date.parse(x.plan.notBefore)+31*60000).toISOString()},
      {...x.plan,completionDeadline:new Date(Date.parse(x.plan.notBefore)+5*3600000).toISOString()},{...x.plan,publicationPolicy:'trusted'}])
      assert.throws(()=>service.parseRestorationFinalIntegrityPlan(canonical(bad)));
    assert.throws(()=>service.parseRestorationFinalIntegrityPlan(' '.repeat(5001)),/size/);
  }finally{x.sqlite.close();}
});
