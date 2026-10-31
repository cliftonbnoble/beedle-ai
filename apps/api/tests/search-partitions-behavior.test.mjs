import test from 'node:test';
import assert from 'node:assert/strict';
import {createPartitionFixture as fixture} from './helpers/corpus-partitions.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {documentPartitionEnvironment}=loadTs('src/services/corpus-partitions.ts');
const {search}=loadTs('src/services/search.ts');
const {handleDecisionRetrievalChunks}=loadTs('src/routes/retrieval-debug.ts');
const {SearchWorkBudget,partitionedSearchWorkPolicy}=loadTs('src/services/search-work-budget.ts');
const {corpusReviewCacheSql,inspectCorpusReviewerQueue}=loadTs('src/services/corpus-review-cache.ts');
const {listIngestionDocuments}=loadTs('src/services/reviewer-queue.ts');
const {readyReviewerQueue}=await import('./helpers/reviewer-queue.mjs');
const {handleDashboardSummary}=loadTs('src/routes/admin-dashboard.ts');
const {createHash}=await import('node:crypto');


const input={query:'possession',corpusMode:'trusted_plus_provisional',filters:{approvedOnly:false},limit:7};

test('partitioned search merges all 32 databases and pages without duplicate or missing document identities',async()=>{
  const f=await fixture();try{
    const budget=new SearchWorkBudget(undefined,partitionedSearchWorkPolicy);const env={...f.env,SEARCH_WORK_BUDGET:budget};
    const first=await search(env,input);assert.equal(first.rankingVersion,'partitioned-search-m08-v1');assert.equal(first.total,32);assert.equal(first.results.length,7);
    assert.equal(first.corpusExhaustive,false);assert.equal(first.countKind,'bounded_candidates');assert.ok(first.results.every(row=>row.corpusTier==='provisional'));
    const ids=first.results.map(row=>row.documentId);let next=first.nextCursor;
    while(next){const page=await search(f.env,{...input,cursor:next});ids.push(...page.results.map(row=>row.documentId));next=page.nextCursor;}
    assert.deepEqual(ids.slice().sort(),f.ids.slice().sort());assert.equal(new Set(ids).size,32);assert.ok(budget.snapshot().statements<768);
    const repeat=await search(f.env,input);assert.deepEqual(repeat.results.map(row=>row.documentId),first.results.map(row=>row.documentId));
    const filtered=await search(f.env,{...input,filters:{approvedOnly:false,documentIds:[f.ids[17]]}});assert.deepEqual(filtered.results.map(row=>row.documentId),[f.ids[17]]);
    assert.equal((await search(f.env,{...input,corpusMode:'trusted_only'})).total,0);
    assert.equal((await search(f.env,{...input,query:'zzzzabsentprobe'})).total,0);
  }finally{f.close();}
});

test('cross-partition drift invalidates stored cursors and readers even if coordinator revision stays fixed',async()=>{
  const f=await fixture();try{
    const first=await search(f.env,input);const id=f.ids[8],routed=await documentPartitionEnvironment(f.env,id);
    const request=new Request('https://api.example/decisions/'+id+'/chunks');
    const before=await handleDecisionRetrievalChunks(request,routed,id);assert.equal(before.status,200);
    const body=await before.json();assert.equal(body.document.documentId,id);assert.equal(body.document.corpusTier,'provisional');assert.equal(body.corpusRevision,first.corpusRevision);
    f.stores[17].sqlite.exec('UPDATE vector_lifecycle_epoch SET revision=revision+1');
    await assert.rejects(search(f.env,{...input,cursor:first.nextCursor}),/corpus changed/);
    assert.equal((await handleDecisionRetrievalChunks(request,routed,id)).status,409);
  }finally{f.close();}
});

test('a single incomplete partition fails the complete search instead of returning partial results',async()=>{
  const f=await fixture();try{
    f.stores[31].sqlite.exec("UPDATE search_phrase_build SET phase='retrieval'");
    await assert.rejects(search(f.env,input),/maintenance is incomplete/);
    const semantic={...f.env,AI:{}};await assert.rejects(search(semantic,input),/lexical-only/);
  }finally{f.close();}
});

test('the global read-only review cache retains exact counts, ordering and peers across canonical partitions',async()=>{
  const f=await fixture();try{
    await assert.rejects(listIngestionDocuments(f.env),/review cache is unavailable/);
    for(const s of f.stores)await readyReviewerQueue({DB:s.DB});
    const coordinator=f.stores[0].sqlite;
    for(const sql of corpusReviewCacheSql)coordinator.exec(sql);
    for(const s of f.stores){
      for(const row of s.sqlite.prepare('SELECT * FROM reviewer_queue_state').all()){
        const columns=Object.keys(row);coordinator.prepare(`INSERT INTO beedle_corpus_review_rows(${columns.join(',')}) VALUES(${columns.map(()=>'?').join(',')})`).run(...columns.map(c=>row[c]));
      }
    }
    coordinator.exec('INSERT INTO beedle_corpus_review_groups SELECT group_key,COUNT(*) FROM beedle_corpus_review_rows GROUP BY group_key');
    const revision=JSON.parse(f.env.CORPUS_PARTITIONS_JSON).partitions[0].revision;
    const policy=coordinator.prepare('SELECT policy_version FROM reviewer_queue_control').get().policy_version;
    const manifestSha=createHash('sha256').update(f.env.CORPUS_PARTITIONS_JSON).digest('hex');
    coordinator.prepare('INSERT INTO beedle_corpus_review_control VALUES(1,?,?,?, ?,?, ?,?)').run(revision,policy,'legacy',manifestSha,'a'.repeat(64),'ready',32);
    assert.equal((await inspectCorpusReviewerQueue(f.env)).ready,true);
    const before=coordinator.prepare('SELECT total_changes() n').get().n;
    const first=await listIngestionDocuments(f.env,{limit:7,sort:'titleAsc'});const rows=[...first.documents];let cursor=first.nextCursor;
    assert.equal(first.summary.total,32);assert.equal(first.summary.approved,0);assert.equal(first.summary.searchable,32);
    while(cursor){const page=await listIngestionDocuments(f.env,{limit:7,sort:'titleAsc',cursor});rows.push(...page.documents);cursor=page.nextCursor;}
    assert.deepEqual(rows.map(row=>row.id).sort(),f.ids.slice().sort());assert.ok(rows.every(row=>row.peerCount===31));
    assert.equal((await (await handleDashboardSummary(new Request('https://api.example'),f.env)).json()).searchableDecisionCount,32);
    assert.equal(coordinator.prepare('SELECT total_changes() n').get().n,before,'cache reads never repair or mutate canonical/cache rows');
    coordinator.exec('DELETE FROM beedle_corpus_review_rows WHERE id=(SELECT id FROM beedle_corpus_review_rows LIMIT 1)');
    await assert.rejects(listIngestionDocuments(f.env),/refresh is pending/);
  }finally{f.close();}
});
