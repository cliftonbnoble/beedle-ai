import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from './helpers/load-ts.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
const {assistantContextWindow,assistantContextLimits:limits,assistantQuestionError,boundAssistantRetrievalQuery,assistantChatRequestSchema,searchRequestSchema}=loadTs('../../packages/shared/src/index.ts');
const {runAssistantChat}=loadTs('src/services/assistant-chat.ts');
const {handleAssistantChat}=loadTs('src/routes/assistant-chat.ts');

test('35 exchanges keep full history while every request has a validated recent window',()=>{
 const history=[];
 for(let turn=0;turn<35;turn++){
  history.push({role:'user',content:`Question ${turn} about required repair notice`});
  const window=assistantContextWindow(history);assert.ok(window.messages.length<=limits.modelMessages);assert.ok(window.retainedChars<=limits.historyChars);
  const request=assistantChatRequestSchema.parse({messages:window.messages});assert.equal(request.messages.at(-1).content,history.at(-1).content);
  history.push({role:'assistant',content:`Retrieved excerpt for question ${turn}`});
 }
 assert.equal(history.length,70);assert.equal(history[0].content,'Question 0 about required repair notice');
 assert.equal(assistantContextWindow(history).omittedMessages,62);
 // The API's bounded transport contract remains in place for clients which skip window selection.
 assert.equal(assistantChatRequestSchema.safeParse({messages:history}).success,false);
});

test('character budget, pending/blank artifacts and oversized legacy history do not mutate stored messages',()=>{
 const history=[{role:'user',content:'Earlier issue'},...Array.from({length:8},(_,i)=>({role:i%2?'user':'assistant',content:'x'.repeat(12000)})),{role:'user',content:'The current issue is notice.'},{role:'assistant',content:'Thinking',pending:true}];
 const before=JSON.stringify(history);const window=assistantContextWindow(history);
 assert.equal(JSON.stringify(history),before);assert.equal(window.messages.at(-1).content,'The current issue is notice.');assert.ok(window.retainedChars<=24000);assert.ok(window.messages.length<8);
 assert.deepEqual(assistantContextWindow([{role:'assistant',content:'  '},{role:'user',content:'Current notice issue'}]).messages,[{role:'user',content:'Current notice issue'}]);
 const legacy=[{role:'assistant',content:'𐐀'.repeat(8000)},{role:'user',content:'Current legal issue'}];
 const shortened=assistantContextWindow(legacy);assert.equal(legacy[0].content.length,16000);assert.equal(shortened.legacyMessageClipped,true);assert.equal(shortened.messages[0].content.length,12000);
 assert.equal(shortened.messages[0].content.isWellFormed(),true);
});

test('long retrieval uses both ends, stays within 2000 UTF-16 units and retains valid Unicode boundaries',()=>{
 const text='Notice '+('𐐀'.repeat(3500))+' Rule 6.13 repair evidence';
 const bounded=boundAssistantRetrievalQuery(text);assert.equal(bounded.truncated,true);assert.ok(bounded.query.length<=2000);assert.ok(bounded.query.startsWith('Notice '));assert.ok(bounded.query.endsWith('Rule 6.13 repair evidence'));assert.equal(bounded.query.isWellFormed(),true);
 assert.deepEqual(boundAssistantRetrievalQuery('  Rule 6.13   notice  '),{query:'Rule 6.13 notice',truncated:false});
 const noise=boundAssistantRetrievalQuery('?'.repeat(3000)+'notice'+'!'.repeat(3000));assert.equal(noise.query,'notice');
});

test('accepted 12000-character questions keep their full model input and use a valid bounded search request',async()=>{
 const {DB,sqlite}=memoryD1();const original=globalThis.fetch;let capturedQuery;let modelQuestion;let historyChars;
 try{
  const head='Notice facts\n',tail='\nRule 6.13';const question=head+('The tenant received a repair notice. '.repeat(400)).slice(0,12000-head.length-tail.length)+tail;assert.equal(question.length,12000);
  const text='The tenant received a repair notice concerning the dwelling.';
  const row={documentId:'a',title:'Decision',citation:'RB-a',authorName:null,sourceLink:'https://example.invalid',score:1,
   primaryAuthorityPassage:{chunkId:'chunk',snippet:text,sectionLabel:'Findings',sectionHeading:'Findings',citationAnchor:'a#p1',paragraphAnchor:'p1',chunkType:'analysis',score:1}};
  globalThis.fetch=async(_url,init)=>{const data=JSON.parse(JSON.parse(init.body).messages[1].content);modelQuestion=data.question;historyChars=data.question.length+data.conversation.reduce((sum,message)=>sum+message.content.length,0);
   return Response.json({choices:[{message:{content:JSON.stringify({claims:[{text,evidence:[{sourceId:'S1',quote:text}]}]})},finish_reason:'stop'}]});};
  const env={DB,LLM_API_KEY:'synthetic',LLM_BASE_URL:'https://example.invalid',LLM_MODEL:'synthetic'};
  const result=await runAssistantChat(env,{messages:[{role:'assistant',content:'Earlier context '.repeat(700)},{role:'user',content:question}]},{},async(_env,input)=>{capturedQuery=searchRequestSchema.parse(input).query;return {results:[row]};});
  assert.equal(modelQuestion,question);assert.ok(capturedQuery.length<=2000);assert.ok(capturedQuery.endsWith('Rule 6.13'));assert.ok(historyChars<=24000);
  assert.equal(result.retrievalContext.query,capturedQuery);assert.equal(result.retrievalContext.queryTruncated,true);
 }finally{globalThis.fetch=original;sqlite.close();}
});

test('empty, whitespace, punctuation-only, too-short, oversized and assistant-ended requests reject before database/provider work',async()=>{
 let calls=0;const env={DB:{prepare(){calls++;throw new Error('No DB work expected');}}};
 for(const messages of [[{role:'user',content:''}],[{role:'user',content:'  '}],[{role:'user',content:'??!'}],[{role:'user',content:'Hi'}],[{role:'user',content:'a'.repeat(12001)}],[{role:'assistant',content:'Prior answer only'}]]){
  const response=await handleAssistantChat(new Request('https://api.example/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({messages})}),env);
  assert.equal(response.status,400);assert.equal(calls,0);
 }
 assert.equal(assistantQuestionError('§37.8'),null);assert.equal(assistantQuestionError('租赁案'),null);
 assert.match(assistantQuestionError('x'.repeat(12001)),/12,000/);
});
