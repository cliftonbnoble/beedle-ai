import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
const {assistantChatRequestSchema,searchRequestSchema}=loadTs('../../packages/shared/src/index.ts');
const {prepareAssistantFilters,assistantScopeKey,resolveAssistantRetrieval}=loadTs('src/services/assistant-retrieval-plan.ts');
const {runAssistantChat}=loadTs('src/services/assistant-chat.ts');
const {handleAssistantChat}=loadTs('src/routes/assistant-chat.ts');
const {buildDocumentTextArtifactStatements}=loadTs('src/services/ingest.ts');
const {buildSearchScope}=loadTs('src/services/search-query-analysis.ts');
const {addAssistantPassages,eligibleAssistantDocuments}=loadTs('src/services/assistant-passages.ts');
const {searchBudgetEnvironment,SearchWorkBudget}=loadTs('src/services/search-work-budget.ts');
const {search}=loadTs('src/services/search.ts');
const question='What happened concerning repair notice?';
const request=(content,extra={})=>assistantChatRequestSchema.parse({messages:[{role:'user',content}],...extra});
const filters=()=>prepareAssistantFilters(request(question));
const context=async(active=['a','b'])=>({version:'reference-context-1',scopeKey:await assistantScopeKey(filters()),subjectQuery:'repair notice',referenceDocumentIds:['a','b','c'],activeDocumentIds:active});

async function doc(env,id,{judge='Erin E. Katayama',trusted=true,title='Willow tenant repairs',citation='T-1',date='2024-01-15'}={}){
 await env.DB.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,ingestion_state,expected_chunk_count,index_codes_json,rules_sections_json,ordinance_sections_json) VALUES (?,'decision_docx','Test',?,?,'source','https://example.invalid','now','now','preparing',2,'["100"]','["1.2"]','["37.8"]')`).bind(id,title,citation).run();
 await env.DB.batch(buildDocumentTextArtifactStatements(env,{documentId:id,citation,sections:[
  {canonicalKey:'conclusions_of_law',heading:'Conclusions of Law',order:0,paragraphs:[{anchor:'p1',order:0,text:'The board reviewed the tenant repair notice before the proceeding.'}]},
  {canonicalKey:'findings_of_fact',heading:'Findings of Fact',order:1,paragraphs:[{anchor:'p2',order:0,text:'The delivery receipt was returned undelivered; the hearing considered the envelope.'}]}
 ]}).statements);
 await env.DB.prepare("UPDATE documents SET author_name=?,decision_date=?,ingestion_state='ready',ingestion_completed_at='now' WHERE id=?").bind(judge,date,id).run();
 await env.DB.prepare("UPDATE documents SET reference_catalog_version=(SELECT active_version FROM legal_catalog_state WHERE singleton=1) WHERE id=?").bind(id).run();
 await env.DB.prepare("UPDATE documents SET qc_required_confirmed=? WHERE id=?").bind(trusted?1:0,id).run();
 await env.DB.prepare("UPDATE documents SET searchable_at='now',published_revision=document_revision WHERE id=?").bind(id).run();
 if(trusted)await env.DB.prepare("UPDATE documents SET approved_at='now',approved_revision=document_revision,approval_policy_version='reviewer-policy-1' WHERE id=?").bind(id).run();
}

test('ordinal and pronoun resolution retains the original reference order across narrowed follow-ups',async()=>{
 const previous=await context();const key=previous.scopeKey;
 const second=resolveAssistantRetrieval('What about the second case?',filters(),key,previous);
 assert.equal(second.resolution,'ordinal');assert.deepEqual(second.filters.documentIds,['b']);assert.match(second.query,/repair notice/);assert.equal(second.preserveReferences,true);
 assert.deepEqual(resolveAssistantRetrieval('What about the second?',filters(),key,previous).filters.documentIds,['b']);
 assert.equal(resolveAssistantRetrieval('Compare the first case with case 2',filters(),key,previous).resolution,'clarification');
 assert.equal(resolveAssistantRetrieval('Compare both cases',filters(),key,{...previous,activeDocumentIds:['a','b','c']}).resolution,'clarification');
 const next={...previous,activeDocumentIds:['b']};
 assert.deepEqual(resolveAssistantRetrieval('Why did it matter?',filters(),key,next).filters.documentIds,['b']);
 assert.deepEqual(resolveAssistantRetrieval('What happened in the last case?',filters(),key,next).filters.documentIds,['c']);
 assert.equal(resolveAssistantRetrieval('Compare the first and second cases',filters(),key,next).resolution,'clarification');
 assert.equal(resolveAssistantRetrieval('Compare case 1 and case 2',filters(),key,next).resolution,'clarification');
 assert.deepEqual(resolveAssistantRetrieval('Explain the evidence in this decision concerning the delivery receipt and the complete sequence of events before the hearing',filters(),key,next).filters.documentIds,['b']);
 assert.equal(resolveAssistantRetrieval('What about the eighth case?',filters(),key,next).resolution,'clarification');
 const reset=resolveAssistantRetrieval('Explain the roof replacement dispute',filters(),key,next);
 assert.equal(reset.resolution,'standalone');assert.equal(reset.preserveReferences,false);assert.equal(reset.query,'Explain the roof replacement dispute');assert.equal(reset.filters.documentIds,undefined);
});

test('ambiguous, absent, conflicting and changed-scope references clarify rather than widen',async()=>{
 const previous=await context();const key=previous.scopeKey;
 for(const [content,scope,prior] of [
  ['What did it hold?',filters(),previous],['What about the second case?',filters(),undefined],
  ['What about the second case?',{...filters(),documentIds:['a']},previous],
  ['What about the second case?',{...filters(),judgeNames:['Andrew Yick']},previous],
  ['Show other cases',{...filters(),documentIds:['a']},previous]
 ]){
  assert.equal(resolveAssistantRetrieval(content,scope,await assistantScopeKey(scope),prior).resolution,'clarification');
 }
 const selected={...filters(),documentIds:['b']};
 assert.deepEqual(resolveAssistantRetrieval('Why did it matter?',selected,await assistantScopeKey(selected),previous).filters.documentIds,['b']);
 assert.deepEqual(resolveAssistantRetrieval('Compare both cases',filters(),key,previous).filters.documentIds,['a','b']);
 assert.equal(resolveAssistantRetrieval('Find more cases',filters(),key,previous).resolution,'more_cases');
 assert.equal(resolveAssistantRetrieval('Is it legal to increase rent?',filters(),key).resolution,'standalone');
 assert.equal(resolveAssistantRetrieval('What did it hold?',filters(),key,undefined,'Earlier issue').resolution,'clarification');
});

test('strict scope, legacy conflicts and malformed reference pointers reject before database work',async()=>{
 let calls=0;const env={DB:{prepare(){calls++;throw new Error('No database work expected');}}};
 const invalid=[{filters:{fileType:'rules_pdf'}},{filters:{approvedOnly:false}},{filters:{chunkType:'Findings'}},{filters:{unknown:true}},
  {unknown:true},{judgeNames:['Katayama'],filters:{judgeNames:['Yick']}},{indexCodes:['100'],filters:{indexCodes:['200']}},
  {filters:{documentId:'a',documentIds:['b']}},{filters:{fromDate:'2025-01-01',toDate:'2024-01-01'}},
  {previousResearch:{...await context(),activeDocumentIds:['outside']}}];
 for(const extra of invalid){const response=await handleAssistantChat(new Request('https://example.invalid/api/assistant/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({messages:[{role:'user',content:question}],...extra})}),env);assert.equal(response.status,400);}
 assert.equal(calls,0);
 assert.equal(prepareAssistantFilters(request(question,{judgeNames:['Katayama'],filters:{judgeNames:['Erin E. Katayama']}})).judgeNames[0],'Erin E. Katayama');
});

test('all accepted search constraints intersect selected cases and judges in actual SQL',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env={DB};await doc(env,'a');await doc(env,'b',{judge:'Andrew Yick'});await doc(env,'c',{trusted:false});
  const chosen=prepareAssistantFilters(request(question,{judgeNames:['Katayama'],filters:{documentIds:['a','b','c'],jurisdiction:'Test',indexCodes:['100'],rulesSection:'1.2',ordinanceSection:'37.8',partyName:'Willow',decisionSeries:'T',fromDate:'2024-01-01',toDate:'2024-12-31'}}));
  const parsed=searchRequestSchema.parse({query:'repair notice',filters:chosen});
  assert.deepEqual(await eligibleAssistantDocuments(env,parsed,['a','b','c']),['a']);
  const response=await search(env,parsed,{session:false});assert.ok(response.results.length);assert.deepEqual([...new Set(response.results.map(row=>row.documentId))],['a']);
  for(const override of [{jurisdiction:'Other'},{judgeNames:['Yick'],documentIds:['a']},{indexCodes:['200']},{rulesSection:'9.9'},{ordinanceSection:'37.99'},{partyName:'Unknown'},{decisionSeries:'L'},{fromDate:'2025-01-01',toDate:'2025-12-31'},{fromDate:'2023-01-01',toDate:'2023-12-31'}]){
   const scoped=searchRequestSchema.parse({...parsed,filters:{...chosen,...override}});assert.deepEqual(await eligibleAssistantDocuments(env,scoped,['a']),[]);
  }
  const scope=buildSearchScope(searchRequestSchema.parse({query:'notice',filters:{documentId:'a',documentIds:['b']}}),'trusted_only');
  assert.deepEqual((await DB.prepare(`SELECT d.id FROM documents d ${scope.where}`).bind(...scope.params).all()).results,[]);
 }finally{sqlite.close();}
});

test('complementary passages find follow-up evidence beyond the first passage within a shared budget',async()=>{
 const {DB,sqlite}=memoryD1();try{
  const env=searchBudgetEnvironment({DB});await doc({DB},'a');await doc({DB},'outside');
  const initial=sqlite.prepare("SELECT id FROM document_chunks WHERE document_id='a' AND paragraph_anchor='p1'").get();
  const rows=[{documentId:'a',primaryAuthorityPassage:{chunkId:initial.id},score:1}];
  const request=searchRequestSchema.parse({query:'repair notice undelivered',filters:{approvedOnly:true,documentIds:['a']}});
  const enriched=await addAssistantPassages(env,request,'Was the receipt undelivered?',rows);
  assert.equal(enriched[0].additionalPassages.length,1);assert.match(enriched[0].additionalPassages[0].snippet,/undelivered/);
  assert.equal(enriched[0].additionalPassages[0].paragraphAnchor,'p2');assert.ok(env.SEARCH_WORK_BUDGET.snapshot().statements<=2);
  const exhausted=new SearchWorkBudget(undefined,{statements:0});
  await assert.rejects(addAssistantPassages({...env,DB:exhausted.wrap(DB),SEARCH_WORK_BUDGET:exhausted},request,'undelivered',rows),error=>error.reason==='statements');
 }finally{sqlite.close();}
});

test('chat revalidates focus, preserves case 2 and keeps unavailable cases out of generation',async()=>{
 const {DB,sqlite}=memoryD1();const original=globalThis.fetch;try{
  const env={DB,LLM_API_KEY:'synthetic',LLM_MODEL:'synthetic',LLM_BASE_URL:'https://example.invalid'};
  await doc(env,'a');await doc(env,'b');let providerCalls=0;let data;
  globalThis.fetch=async(_url,init)=>{providerCalls++;data=JSON.parse(JSON.parse(init.body).messages[1].content);const source=data.evidence[0];return Response.json({choices:[{message:{content:JSON.stringify({claims:[{text:source.text,evidence:[{sourceId:source.id,quote:source.text}]}]})},finish_reason:'stop'}]});};
  const previous=await context(['b']);const result=await runAssistantChat(env,{messages:[{role:'user',content:'Was the second case about an undelivered receipt?'}],previousResearch:previous});
  assert.deepEqual(result.retrievedSources.map(row=>row.documentId),['b']);assert.equal(result.retrievedSources[0].referenceNumber,2);assert.deepEqual(result.referenceContext.referenceDocumentIds,['a','b','c']);
  assert.ok(data.evidence.every(row=>row.documentId==='b' && row.referenceNumber===2));assert.equal(data.resolvedRequest.resolution,'ordinal');
  const limited=await runAssistantChat(env,{messages:[{role:'user',content:'repair notice'}],filters:{documentIds:['a','b']},limit:1});
  assert.equal(limited.retrievalContext.unrepresentedDocumentIds.length,1);assert.match(limited.answer,/not represented/);
  sqlite.prepare("UPDATE documents SET rejected_at='now' WHERE id='b'").run();const before=providerCalls;
  const missing=await runAssistantChat(env,{messages:[{role:'user',content:'Why did it matter?'}],previousResearch:result.referenceContext});
  assert.equal(missing.retrievalContext.resolution,'clarification');assert.equal(missing.retrievedCount,0);assert.equal(providerCalls,before);
  const ambiguous=await runAssistantChat(env,{messages:[{role:'user',content:'What did it hold?'}],previousResearch:await context()});
  assert.equal(ambiguous.retrievalContext.resolution,'clarification');assert.equal(providerCalls,before);
 }finally{globalThis.fetch=original;sqlite.close();}
});
