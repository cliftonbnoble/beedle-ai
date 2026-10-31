import assert from 'node:assert/strict';
import {loadTs} from './load-ts.mjs';
import {successorJobFixture} from './restoration-source-successor-job-fixture.mjs';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
const {claimRestorationSourceSuccessorTick}=loadTs('src/services/restoration-source-successor-control.ts');
const {runOwnedRestorationSourceJob}=loadTs('src/services/restoration-source-job.ts');
const {generationTables}=loadTs('src/services/generation-tables.ts');

export async function terminalDispositionFixture(kind='staging',{partitionIndex=0}={}){
  const x=await successorJobFixture({partitionIndex}),tick=await claimRestorationSourceSuccessorTick(x.db,x.successor);
  let expired=false;
  const wrap=statement=>new Proxy(statement,{get(target,key){
    if(key==='bind')return(...args)=>wrap(target.bind(...args));
    if(['first','all','run','raw'].includes(key))return(...args)=>{if(expired)throw Error('Too many subrequests');return target[key](...args);};
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  const faultDb=new Proxy(x.db,{get(target,key){
    if(key==='prepare')return sql=>wrap(target.prepare(sql));
    if(key==='batch')return statements=>{if(expired||statements.some(row=>row.sql.includes('INSERT INTO staged_document_chunks'))){expired=true;throw Error('Too many subrequests');}return target.batch(statements);};
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  const faultProofs={get:(...args)=>x.proofs.get(...args),put:(...args)=>{
    if(args[0].startsWith('restoration/'))throw Error('Too many subrequests');return x.proofs.put(...args);
  }};
  await assert.rejects(runOwnedRestorationSourceJob({db:kind==='staging'?faultDb:x.db,originals:x.originals,target:x.target,
    proofs:kind==='published'?faultProofs:x.proofs,identity:x.identity,tickToken:tick.token,successor:x.successor,job:x.job,profile:x.profile}),/Too many subrequests/);
  const one=sql=>x.sqlite.prepare(sql).get(),job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(x.next),
    document=x.sqlite.prepare('SELECT * FROM documents WHERE id=?').get(x.next),generations=x.sqlite.prepare('SELECT * FROM document_generations WHERE document_id=? ORDER BY id').all(x.next),stages={};
  for(const table of Object.keys(generationTables))stages[table]=x.sqlite.prepare('SELECT * FROM staged_'+table+' WHERE generation_id IN (SELECT id FROM document_generations WHERE document_id=?) ORDER BY generation_id,id').all(x.next);
  const current=one('SELECT * FROM beedle_restore_source_successor'),currentTick=one('SELECT * FROM beedle_restore_source_successor_tick'),history=x.sqlite.prepare('SELECT * FROM beedle_restore_source_successor_history WHERE token=?').get(tick.token),now=Date.now();
  const plan={version:1,operationId:'77777777-7777-4777-8777-777777777777',successorSpecJson:x.env.RESTORE_SPEC_JSON,
    parentTerminalProofSha256:'e'.repeat(64),parentEndedAt:new Date(now-12*60000).toISOString(),notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+20*60000).toISOString(),
    controlSha256:await fingerprint(current),tickSha256:await fingerprint(currentTick),historySha256:await fingerprint(history),
    documentId:x.next,jobSha256:await fingerprint(job),documentSha256:await fingerprint(document),generationsSha256:await fingerprint(generations),
    stagedArtifactsSha256:await fingerprint(stages),sourceResults:1,abandonStagingGenerationId:kind==='staging'?generations.find(g=>g.status==='staging').id:null};
  return {...x,plan,terminalBefore:{current,currentTick,history,job,document,generations,stages}};
}
