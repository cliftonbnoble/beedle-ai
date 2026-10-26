import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
const {ingestDocumentBytes}=loadTs('src/services/ingest.ts');
const {ensureVersionedVector,synchronizeVectorLifecycle,vectorLifecycleReady}=loadTs('src/services/vector-generations.ts');
const {vectorSearch}=loadTs('src/services/search-fts.ts');
const {buildSearchScope}=loadTs('src/services/search-query-analysis.ts');
const {planVectorFilters,vectorFilterHash}=loadTs('src/services/vector-filter-metadata.ts');
const {searchRequestSchema}=loadTs('../../packages/shared/src/index.ts');
function matches(metadata,filter) {
  return Object.entries(filter).every(([key,value])=>{
    if(value&&typeof value==='object')return Object.entries(value).every(([op,target])=>op==='$in'?target.includes(metadata[key]):op==='$gte'?metadata[key]>=target:op==='$lte'?metadata[key]<=target:false);
    return metadata[key]===value;
  });
}
async function setup() {
  const {DB,sqlite}=memoryD1();const objects=new Map();const stored=new Map();const calls=[];const stats={embeddedTexts:0};let score=1000;
  const env={DB,VECTOR_NAMESPACE:'local',AI_EMBEDDING_MODEL:'synthetic',AI_EMBEDDING_DIMENSIONS:'2',VECTOR_LIFECYCLE_READY:'true',VECTOR_FILTERS_READY:'true',R2_PUBLIC_BASE_URL:'https://example.invalid',SOURCE_PROXY_BASE_URL:'https://api.example',
    SOURCE_BUCKET:{async put(key,bytes){objects.set(key,bytes);},async get(key){const bytes=objects.get(key);return bytes?{size:bytes.length,arrayBuffer:async()=>bytes.slice().buffer}:null;},async delete(key){objects.delete(key);}},
    AI:{async run(_model,{text}){stats.embeddedTexts+=text.length;return {data:text.map(()=>[0.2,0.8])};}},VECTOR_INDEX:{async upsert(rows){for(const row of rows)stored.set(row.id,{...structuredClone(row),score:stored.get(row.id)?.score??score--/1000});},async getByIds(ids){return ids.map(id=>stored.get(id)).filter(Boolean);},async deleteByIds(ids){for(const id of ids)stored.delete(id);},async query(_vector,options){calls.push(options);return {matches:[...stored.values()].filter(row=>row.namespace===options.namespace&&matches(row.metadata,options.filter)).sort((a,b)=>b.score-a.score).slice(0,options.topK).map(row=>({id:row.id,score:row.score}))};}}};
  async function doc({judge='Erin E. Katayama',jurisdiction='Test',code='100',title='Tenant repairs',citation='T-1',date='2024-01-15',trusted=false}={}) {
    const source='# Index Codes\n\n'+code+'\n\n# Findings of Fact\n\nThe tenant reported a defect and gave the landlord notice of repairs. The landlord disputed the evidence.\n\n# Conclusions of Law\n\nThe Board reviewed the evidence of notice and the condition.';
    const result=await ingestDocumentBytes(env,{title,citation,jurisdiction,fileType:'decision_docx',performVectorUpsert:false,sourceFile:{filename:'filter.md',mimeType:'text/markdown'}},new TextEncoder().encode(source));const id=result.documentId;
    sqlite.prepare('UPDATE documents SET author_name=?,decision_date=? WHERE id=?').run(judge,date,id);
    sqlite.prepare('UPDATE documents SET searchable_at=?,published_revision=document_revision WHERE id=?').run(new Date().toISOString(),id);
    if(trusted) {
      sqlite.prepare('UPDATE documents SET qc_required_confirmed=1 WHERE id=?').run(id);
      sqlite.prepare("UPDATE documents SET approved_at='now',approved_revision=document_revision,approval_policy_version='reviewer-policy-1',published_revision=document_revision,reference_catalog_version=(SELECT active_version FROM legal_catalog_state WHERE singleton=1) WHERE id=?").run(id);
    }
    const chunk=sqlite.prepare('SELECT id,chunk_text,section_label FROM document_chunks WHERE document_id=? ORDER BY chunk_order LIMIT 1').get(id);
    const record=await ensureVersionedVector(env,{documentId:id,chunkId:chunk.id,sourceKind:'document',generation:'initial',text:chunk.chunk_text,metadata:{sectionLabel:chunk.section_label}});
    return {id,chunkId:chunk.id,record};
  }
  async function sync(){for(let i=0;i<30&&!await vectorLifecycleReady(env);i++)await synchronizeVectorLifecycle(env);assert.equal(await vectorLifecycleReady(env),true);}
  function scope(filters,corpusMode='trusted_plus_provisional'){const parsed=searchRequestSchema.parse({query:'repair notice',filters:{approvedOnly:false,...filters},corpusMode});return {filters:parsed.filters,corpusMode,...buildSearchScope(parsed,corpusMode)};}
  return {env,sqlite,stored,calls,stats,doc,sync,scope};
}

test('a judge-filtered neighbor below the global top 100 is recalled before cutoff',async()=>{
  const {env,sqlite,stored,calls,doc,sync,scope}=await setup();try {
    const target=await doc();await sync();const entry=stored.get(target.record.entry.vector_id);entry.score=0.7;
    for(let i=0;i<120;i++)stored.set(`irrelevant-${i}`,{...structuredClone(entry),id:`irrelevant-${i}`,score:0.99-i/10000,metadata:{...entry.metadata,judgeKey:await vectorFilterHash('andrew yick')}});
    assert.equal((await vectorSearch(env,['repair'],50)).scores.size,0,'global top-K loses the filtered target');
    const result=await vectorSearch(env,['repair'],50,scope({judgeName:'Katayama'}));
    assert.equal(result.scores.get(target.chunkId),0.7);assert.ok(calls.at(-1).filter.judgeKey);
    assert.equal(calls.at(-1).namespace,target.record.profile.namespace);
  }finally{sqlite.close();}
});

test('combined scalar and cohort filters preserve Boolean index-code membership',async()=>{
  const {env,sqlite,doc,sync,scope,calls}=await setup();try {
    const a=await doc({code:'100',citation:'T-1',trusted:true});await doc({code:'200',citation:'T-2',trusted:true});await doc({code:'100',judge:'Andrew Yick',citation:'T-3',trusted:true});await sync();
    const selected=scope({judgeNames:['Katayama'],indexCodes:['100'],indexCodeOperator:'and',jurisdiction:'Test',fromDate:'2024-01-01',toDate:'2024-12-31',decisionSeries:'T',chunkType:'Index Codes'},'trusted_only');
    const result=await vectorSearch(env,['repair'],50,selected);assert.equal(result.scores.size,1);assert.ok(result.scores.has(a.chunkId));
    const filter=calls.at(-1).filter;assert.equal(filter.trusted,true);assert.ok(filter.cohortKey.$in.length);assert.ok(filter.sectionKey);assert.deepEqual(filter.decisionDate,{$gte:'2024-01-01',$lte:'2024-12-31'});
    const none=await vectorSearch(env,['repair'],50,scope({indexCodes:['100','200'],indexCodeOperator:'and'}));assert.equal(none.scores.size,0);
    const either=await vectorSearch(env,['repair'],50,scope({indexCodes:['100','200'],indexCodeOperator:'or'}));assert.equal(either.scores.size,3);
  }finally{sqlite.close();}
});

test('metadata changes and catalog switches invalidate filter projection before reuse',async()=>{
  const {env,sqlite,doc,sync,scope}=await setup();try {
    const target=await doc({trusted:true});await sync();
    assert.throws(()=>sqlite.prepare("UPDATE vector_entries SET filter_json='malformed'").run(),/CHECK constraint/);
    sqlite.prepare("UPDATE documents SET author_name='Andrew Yick' WHERE id=?").run(target.id);
    // Simulate an older Worker acknowledging only its v1 synchronization.
    sqlite.prepare('UPDATE vector_profile_sync SET revision=(SELECT revision FROM vector_lifecycle_dirty s WHERE s.document_id=vector_profile_sync.document_id)').run();
    assert.equal((await vectorSearch(env,['repair'],50,scope({judgeName:'Katayama'}))).errored,true);
    sqlite.prepare('UPDATE documents SET searchable_at=?,published_revision=document_revision WHERE id=?').run(new Date().toISOString(),target.id);await sync();
    assert.equal((await vectorSearch(env,['repair'],50,scope({judgeName:'Katayama'}))).scores.size,0);
    assert.equal((await vectorSearch(env,['repair'],50,scope({judgeName:'Yick'}))).scores.size,1);
    sqlite.prepare('UPDATE legal_catalog_state SET epoch=epoch+1 WHERE singleton=1').run();assert.equal(await vectorLifecycleReady(env),false);
    await sync();assert.equal(await vectorLifecycleReady(env),true);
  }finally{sqlite.close();}
});

test('contains scopes use exact document partitions, and oversized scopes never run global recall',async()=>{
  const {env,sqlite,doc,sync,scope,calls,stats}=await setup();try {
    const target=await doc({title:'Tenant Willow repair proceeding'});await doc({title:'Different tenant proceeding',citation:'T-2'});await sync();
    const result=await vectorSearch(env,['repair'],50,scope({partyName:'Willow'}));assert.equal(result.scores.size,1);assert.ok(result.scores.has(target.chunkId));assert.ok(calls.at(-1).filter.docKey.$in);
    const original=env.DB.prepare;
    env.DB.prepare=sql=>sql.includes('SELECT d.id AS value FROM documents d')?{bind(){return this;},async all(){return {results:Array.from({length:113},(_,i)=>({value:`doc-${i}`}))};}}:original(sql);
    const before=calls.length;const embedded=stats.embeddedTexts;const refused=await vectorSearch(env,['repair'],50,scope({partyName:'Tenant'}));assert.equal(refused.attempted,false);assert.equal(calls.length,before);assert.equal(stats.embeddedTexts,embedded);assert.match(refused.firstErrorMessage,/partition_budget/);
  }finally{sqlite.close();}
});

test('filter readiness fails closed and scalar hashes avoid provider prefix collisions',async()=>{
  const {env,sqlite,scope}=await setup();try {
    const a='x'.repeat(90)+'a',b='x'.repeat(90)+'b';
    const first=await planVectorFilters(env,scope({jurisdiction:a,documentId:a}));const second=await planVectorFilters(env,scope({jurisdiction:b,documentId:b}));
    assert.notEqual(first.filters[0].docKey,second.filters[0].docKey);assert.equal(first.filters[0].docKey.length,64);assert.notEqual(first.filters[0].jurisdictionKey,second.filters[0].jurisdictionKey);
    assert.ok(new TextEncoder().encode(JSON.stringify(first.filters[0])).length<2048);
    assert.equal((await planVectorFilters({...env,VECTOR_FILTERS_READY:'false'},scope({judgeName:'Katayama'}))).reason,'vector_filter_indexes_unready');
  }finally{sqlite.close();}
});

test('direct facet repairs advance cohort state and never reuse stale membership',async()=>{
  const {env,sqlite,doc,sync,scope}=await setup();try {
    const target=await doc({code:'100'});await sync();
    assert.equal((await vectorSearch(env,['repair'],50,scope({indexCodes:['100'],indexCodeOperator:'and'}))).scores.size,1);
    sqlite.prepare("UPDATE document_index_codes SET code='200',normalized_code='200' WHERE document_id=?").run(target.id);
    assert.equal(await vectorLifecycleReady(env),false);await sync();
    assert.equal((await vectorSearch(env,['repair'],50,scope({indexCodes:['100'],indexCodeOperator:'and'}))).scores.size,0);
    assert.equal((await vectorSearch(env,['repair'],50,scope({indexCodes:['200'],indexCodeOperator:'and'}))).scores.size,1);
  }finally{sqlite.close();}
});

test('a catalog replacement removes stale trusted metadata from recall',async()=>{
  const {env,sqlite,stored,doc,sync,scope}=await setup();try {
    const target=await doc({trusted:true});await sync();assert.equal((await vectorSearch(env,['repair'],50,scope({},'trusted_only'))).scores.size,1);
    const {rebuildLegalReferences}=loadTs('src/services/legal-references.ts');const {catalogPayload}=await import('./helpers/catalog-fixture.mjs');
    await rebuildLegalReferences(env,catalogPayload());assert.equal(await vectorLifecycleReady(env),false);await sync();
    assert.equal(stored.get(target.record.entry.vector_id).metadata.trusted,false);
    assert.equal((await vectorSearch(env,['repair'],50,scope({},'trusted_only'))).scores.size,0);
    assert.equal((await vectorSearch(env,['repair'],50,scope({approvedOnly:false}))).scores.size,1);
  }finally{sqlite.close();}
});

test('large valid cohort filters partition below byte and fan-out limits',async()=>{
  const {env,sqlite,scope}=await setup();try {
    const judges=['René Juárez','Andrew Yick','Connie Brandon','Deborah K. Lim','Dorothy Chou Proudfoot','Erin E. Katayama','Harrison Nam','Jeffrey Eckber','Jill Figg Dayal','Joseph Koomas','Michael J. Berg','Peter Kearns'];
    const original=env.DB.prepare;let size=20;
    env.DB.prepare=sql=>sql.includes('SELECT DISTINCT m.cohort_key AS value')?{bind(){return this;},async all(){return {results:Array.from({length:size},(_,i)=>({value:i.toString(16).padStart(64,'0')}))};}}:original(sql);
    const request=scope({judgeNames:judges,indexCodes:['100'],indexCodeOperator:'and',jurisdiction:'Test',fromDate:'2024-01-01',toDate:'2024-12-31'});
    const plan=await planVectorFilters(env,request);assert.equal(plan.reason,null);assert.ok(plan.filters.length>1&&plan.filters.length<=4);assert.ok(plan.filters.every(filter=>new TextEncoder().encode(JSON.stringify(filter)).length<1900));
    size=112;assert.equal((await planVectorFilters(env,request)).reason,'vector_scope_exceeds_partition_budget');
    const manifest=JSON.parse(await(await import('node:fs/promises')).readFile(new URL('../vector-metadata-indexes.json',import.meta.url),'utf8'));
    assert.equal(manifest.indexes.length,10);assert.equal(new Set(manifest.indexes.map(index=>index.propertyName)).size,10);
  }finally{sqlite.close();}
});

test('namespace errors cannot trigger an unscoped compatibility retry',async()=>{
  const {env,sqlite,doc,sync,scope}=await setup();try {
    await doc();await sync();let calls=0;
    env.VECTOR_INDEX.query=async(_vector,options)=>{calls++;assert.ok(options.namespace);throw Error('namespace unavailable');};
    const result=await vectorSearch(env,['repair'],50,scope({judgeName:'Katayama'}));assert.equal(result.errored,true);assert.equal(result.scores.size,0);assert.equal(calls,1);
  }finally{sqlite.close();}
});
