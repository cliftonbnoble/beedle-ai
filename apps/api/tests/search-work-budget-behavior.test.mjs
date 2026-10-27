import test from 'node:test';
import assert from 'node:assert/strict';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {SearchWorkBudget}=loadTs('src/services/search-work-budget.ts');
const {toErrorResponse}=loadTs('src/lib/http.ts');

test('one request budget bounds query count, returned rows and scoring across caught errors',async()=>{
  const {DB,sqlite}=memoryD1();try {
    const budget=new SearchWorkBudget(undefined,{statements:2,rowsPerQuery:5});const limited=budget.wrap(DB);
    assert.equal(await limited.prepare('SELECT ? AS n').bind(7).first('n'),7);
    assert.deepEqual((await limited.prepare('SELECT 1 AS n').all()).results.map(row=>row.n),[1]);
    await assert.rejects(limited.prepare('SELECT 2').all(),error=>error.reason==='statements');
    assert.throws(()=>budget.assertAvailable(),error=>error.reason==='statements');
    const rows=new SearchWorkBudget(undefined,{rowsPerQuery:2});
    await assert.rejects(rows.wrap(DB).prepare('SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3').all(),error=>error.reason==='rowsPerQuery');
    const score=new SearchWorkBudget(undefined,{scoringOperations:2});score.consume('scoringOperations',2);assert.throws(()=>score.consume('scoringOperations'),error=>error.reason==='scoringOperations');
  }finally{sqlite.close();}
});
test('stalled database operations and request cancellation return safe budget failures',async()=>{
  const raw={prepare(){return {bind(){return this;},all(){return new Promise(()=>{});}};}};
  const budget=new SearchWorkBudget(undefined,{milliseconds:20});
  await assert.rejects(budget.wrap(raw).prepare('SELECT 1').all(),asyncError=>asyncError.reason==='deadline');
  const controller=new AbortController();const cancelled=new SearchWorkBudget(controller.signal);controller.abort();assert.throws(()=>cancelled.assertAvailable(),error=>error.reason==='cancelled');
  const response=toErrorResponse(new (loadTs('src/lib/http.ts').SearchWorkBudgetError)('deadline',{}));assert.equal(response.status,503);assert.match((await response.json()).error,/Narrow the query/);
});
test('wrapping an already budgeted database does not double-charge operations',async()=>{
  const {DB,sqlite}=memoryD1();try{
    const budget=new SearchWorkBudget();const first=budget.wrap(DB);assert.equal(budget.wrap(first),first);await first.prepare('SELECT 1 AS n').first();assert.equal(budget.snapshot().statements,1);
  }finally{sqlite.close();}
});
test('a budget is shared across searches and failures do not restart the pipeline',async()=>{
  const {DB,sqlite}=memoryD1();try{
    const {search}=loadTs('src/services/search.ts');
    sqlite.exec("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,ingestion_state,ingestion_completed_at,expected_chunk_count,searchable_at,published_revision) VALUES('published','decision_docx','Test','Published','P','source','https://example.invalid','now','now','ready','now',1,'now',1)");
    // Readiness, corpus revision and the empty-publication guard precede retrieval.
    const budget=new SearchWorkBudget(undefined,{statements:4});
    const env={DB,SEARCH_WORK_BUDGET:budget,VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test'};
    await assert.rejects(search(env,{query:'zeroresult',corpusMode:'trusted_plus_provisional',filters:{approvedOnly:false}}));
    assert.equal(budget.snapshot().pipelinePasses,1);
    await assert.rejects(search(env,{query:'another',corpusMode:'trusted_plus_provisional',filters:{approvedOnly:false}}));
    assert.equal(budget.snapshot().pipelinePasses,1,'sticky failure stops the next research pass');
  }finally{sqlite.close();}
});
