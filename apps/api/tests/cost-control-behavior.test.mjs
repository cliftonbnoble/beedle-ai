import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
const {default:worker,routeInventory,costClassFor}=loadTs('src/index.ts');
const {reserveComputeBudget,defaultComputeBudgets}=loadTs('src/services/compute-budget.ts');
const {RequestQuotaError,ServiceUnavailableError,toErrorResponse}=loadTs('src/lib/http.ts');
const {runDraftTemplate}=loadTs('src/services/draft-template.ts');
const {embed}=loadTs('src/services/embeddings.ts');
function policy(overrides={}){return JSON.stringify({...defaultComputeBudgets,...overrides});}
const bindings=['SEARCH_RATE_LIMIT','INGEST_RATE_LIMIT','LLM_RATE_LIMIT','ADMIN_WRITE_RATE_LIMIT','REVIEW_READ_RATE_LIMIT','EXPORT_RATE_LIMIT'];
function limitedEnvironment(limit=async()=>({success:false})){
  return {APP_ENV:'local',DEPLOYMENT_READY:'true',VECTOR_NAMESPACE:'beedle-docs-local',COMPUTE_BUDGETS_JSON:policy(),DB:{prepare(){throw new Error('handler must not run');}},...Object.fromEntries(bindings.map(name=>[name,{limit}]))};
}
function request(route){const path=route.path.replace(/:[^/]+/g,'example');return new Request('https://local.example'+path+'?limit=1',{method:route.method,...(route.method==='POST'?{headers:{'content-type':'application/json'},body:'{}'}:{})});}
test('every registered expensive route and alias returns 429 before executing its handler',async()=>{
  assert.ok(routeInventory.length>40);assert.equal(new Set(routeInventory.map(r=>r.method+':'+r.path)).size,routeInventory.length);
  for(const route of routeInventory.filter(route=>route.cost!=='free')){
    const response=await worker.fetch(request(route),limitedEnvironment(),{});
    assert.equal(response.status,429,route.method+' '+route.path);assert.equal(response.headers.get('retry-after'),'60');
  }
  const critical={ '/admin/draft/debug':'llm','/api/draft/template':'llm','/decisions/:documentId/approve':'admin-write','/admin/ingestion/reviewer-export':'export','/admin/ingestion/reviewer-adjudication-template':'export','/decisions/:documentId/chunks':'review-read','/admin/retrieval/documents/:documentId/chunks':'review-read'};
  for(const [path,cost] of Object.entries(critical))assert.equal(routeInventory.find(row=>row.path===path)?.cost,cost,path);
});
test('cost classification handles query strings, trailing slashes, HEAD, preflight and missing limiters',async()=>{
  assert.equal(costClassFor(new Request('https://local.example/admin/draft/debug/?query=x',{method:'POST'})),'llm');
  assert.equal(costClassFor(new Request('https://local.example/decisions/a/chunks',{method:'HEAD'})),'review-read');
  assert.equal(costClassFor(new Request('https://local.example/admin/draft/debug',{method:'OPTIONS'})),null);
  assert.equal(costClassFor(new Request('https://local.example/unknown')),null);
  const env=limitedEnvironment();env.LLM_RATE_LIMIT=undefined;
  const response=await worker.fetch(new Request('https://local.example/admin/draft/debug',{method:'POST'}),env,{});assert.equal(response.status,503);
  assert.equal((await worker.fetch(new Request('https://local.example/health'),limitedEnvironment(),{})).status,200);
  assert.equal((await worker.fetch(new Request('https://local.example/admin/draft/debug',{method:'OPTIONS'}),limitedEnvironment(),{})).status,204);
});
test('a blank scaffold performs no retrieval, storage or provider work even with contextual inputs',async()=>{
  const fail=()=>{throw new Error('unexpected paid work');};
  const result=await runDraftTemplate({DB:{prepare:fail},AI:{run:fail},VECTOR_INDEX:{query:fail}}, {case_type:'general_administrative',template_mode:'blank_scaffold',findings_text:'The tenant gave notice of repairs.',law_text:'Rule 1.2 applies to the hearing.'});
  assert.equal(result.template_mode,'blank_scaffold');assert.deepEqual(result.supporting_authorities,[]);assert.deepEqual(result.citations,[]);
});
test('shared compute counters reserve atomically across concurrent callers and reset expired windows',async()=>{
  const {DB,sqlite}=memoryD1();const env={DB,APP_ENV:'preview',COMPUTE_BUDGETS_JSON:policy({embedding:{perMinute:3,perDay:5}})};
  try{
    const results=await Promise.allSettled(Array.from({length:12},()=>reserveComputeBudget(env,'embedding')));
    assert.equal(results.filter(r=>r.status==='fulfilled').length,3);
    assert.ok(results.filter(r=>r.status==='rejected').every(r=>r.reason instanceof RequestQuotaError));
    let row=sqlite.prepare('SELECT * FROM compute_budget_counters').get();assert.equal(row.minute_used,3);assert.equal(row.day_used,3);
    sqlite.prepare('UPDATE compute_budget_counters SET minute_bucket=minute_bucket-1').run();
    await reserveComputeBudget(env,'embedding',2);row=sqlite.prepare('SELECT * FROM compute_budget_counters').get();assert.equal(row.minute_used,2);assert.equal(row.day_used,5);
    await assert.rejects(reserveComputeBudget(env,'embedding'),error=>error instanceof RequestQuotaError&&error.retryAfterSeconds>60);
    sqlite.prepare('UPDATE compute_budget_counters SET day_bucket=day_bucket-1,minute_bucket=minute_bucket-1').run();
    await reserveComputeBudget(env,'embedding');row=sqlite.prepare('SELECT * FROM compute_budget_counters').get();assert.equal(row.minute_used,1);assert.equal(row.day_used,1);
    await reserveComputeBudget({...env,APP_ENV:'local'},'embedding');assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM compute_budget_counters').get().n,2);
  }finally{sqlite.close();}
});
test('provider budget exhaustion and missing schema/config fail before outbound calls',async()=>{
  const {DB,sqlite}=memoryD1();let calls=0;const env={DB,APP_ENV:'preview',COMPUTE_BUDGETS_JSON:policy({embedding:{perMinute:1,perDay:1}}),AI_EMBEDDING_MODEL:'local', AI_EMBEDDING_DIMENSIONS:"2",AI:{async run(){calls++;return {data:[[0.1,0.2]]};}}};
  try{
    assert.deepEqual(await embed(env,'first'),[0.1,0.2]);await assert.rejects(embed(env,'second'),RequestQuotaError);assert.equal(calls,1);
    await assert.rejects(embed({...env,COMPUTE_BUDGETS_JSON:'malformed'},'bad policy'),ServiceUnavailableError);assert.equal(calls,1);
    sqlite.exec('DROP TABLE compute_budget_counters');await assert.rejects(embed(env,'missing schema'),ServiceUnavailableError);assert.equal(calls,1);
    assert.equal(toErrorResponse(new RequestQuotaError('Budget reached',60)).status,429);
  }finally{sqlite.close();}
});

test('router path normalization cannot bypass the declared handler cost',async()=>{
  for(const path of ['/admin/draft/debug/','/admin/draft/debug///','/admin/draft/%64ebug']){
    const response=await worker.fetch(new Request('https://local.example'+path,{method:'POST',headers:{'content-type':'application/json'},body:'{}'}),limitedEnvironment(),{});
    assert.ok([404,429].includes(response.status),path+': '+response.status);
  }
});
