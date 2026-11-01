import {sourceExecutionFixture} from './restoration-source-execution-fixture.mjs';
// This adapter reports the actual in-memory SQLite page allocation for native
// service tests. These samples are explicitly local, never remote evidence.
export async function sourceExecutionWorkflowFixture(mode='seeded',options={}){
  const x=await sourceExecutionFixture(mode,{...options,partitionPlan:true}),state={sizeOverride:null,omitSize:false,calls:0};
  const size=()=>state.sizeOverride??x.sqlite.prepare('PRAGMA page_count').get().page_count*x.sqlite.prepare('PRAGMA page_size').get().page_size;
  const meta=result=>({...result,meta:{...result.meta,...(!state.omitSize?{size_after:size()}:{})}});
  function prepare(sql,values=[]){const statement=x.db.prepare(sql).bind(...values);return new Proxy(statement,{get(target,key){
    if(key==='bind')return(...args)=>prepare(sql,args);
    if(key==='all'||key==='run')return async()=>{state.calls++;return meta(await target[key]());};
    if(key==='first')return async(...args)=>{state.calls++;return target.first(...args);};
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});}
  const DB=new Proxy(x.db,{get(target,key){
    if(key==='prepare')return prepare;
    if(key==='batch')return async statements=>{state.calls++;return (await target.batch(statements)).map(meta);};
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  const source=x.source?{...x.source,schemaSha256:x.afterSchemaSha256}:{version:1,identity:x.identity,partitionPlanSha256:x.partitionPlanSha256,schemaSha256:x.afterSchemaSha256,
    originalBucket:'beedle-sources-migration-20261027',targetBucket:'beedle-restored-11111111',proofBucket:'beedle-restoration-proof-11111111',profile:x.profile};
  const now=Date.now(),plan={version:1,sourceSpecJson:JSON.stringify(source),execution:x.execution,beforeSchemaSha256:x.beforeSchemaSha256,
    notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+29*60000).toISOString(),completionDeadline:new Date(now+3*60*60000).toISOString(),maximumSteps:x.execution.initialRemaining+2};
  const env={DB,ORIGINALS:x.originals,TARGET_ORIGINALS:x.target,PROOFS:x.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(plan)};
  return {...x,env,plan,capacityFixtureState:state,nativeAllocationOnly:true};
}
