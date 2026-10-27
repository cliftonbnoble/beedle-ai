import test from "node:test";
import assert from "node:assert/strict";
import { loadTs } from "./helpers/load-ts.mjs";
import { memoryD1 } from "./helpers/d1-memory.mjs";
const { boundedCompletion, withinProviderDeadline } = loadTs("src/lib/provider-operation.ts");
const { embedWithOutcome } = loadTs("src/services/embeddings.ts");
const ok = (content = "A complete grounded response.", finish_reason = "stop") => new Response(JSON.stringify({choices:[{message:{content},finish_reason}]}));
async function withFetch(fake, run) { const original = globalThis.fetch; globalThis.fetch = fake; try { await run(); } finally { globalThis.fetch = original; } }
const reason = expected => error => error.reason === expected;

test("provider deadline covers stalled headers, stalled bodies and cancellation", async () => {
  await withFetch(() => new Promise(() => {}), async () => {
    const start = Date.now();
    await assert.rejects(boundedCompletion("https://example.invalid", {}, {}, 25), reason("timeout"));
    assert.ok(Date.now() - start < 1000);
  });
  let cancelled = false;
  await withFetch(async () => new Response(new ReadableStream({start(controller) {controller.enqueue(new TextEncoder().encode('{"choices":'));},cancel() {cancelled=true;}})), async () => {
    await assert.rejects(boundedCompletion("https://example.invalid", {}, {}, 25), reason("timeout"));
    assert.equal(cancelled,true);
  });
  const controller = new AbortController();
  await withFetch(() => new Promise(() => {}), async () => {
    const pending = boundedCompletion("https://example.invalid", {}, {signal:controller.signal}, 500);
    controller.abort();
    await assert.rejects(pending, reason("cancelled"));
  });
  let called = false;
  await assert.rejects(withinProviderDeadline({deadlineAt:Date.now()-1},100, async () => {called=true;}), reason("timeout"));
  assert.equal(called,false);
});

test("provider success/error bodies are bounded even without truthful content-length", async () => {
  for (const [status,size] of [[200,128*1024+1],[500,8*1024+1]]) {
    let cancelled=false;
    await withFetch(async () => new Response(new ReadableStream({start(c) {c.enqueue(new Uint8Array(size));},cancel(){cancelled=true;}}),{status}), async () => {
      await assert.rejects(boundedCompletion("https://example.invalid",{}), reason("response_too_large"));
      assert.equal(cancelled,true);
    });
  }
  await withFetch(async () => new Response("x",{headers:{"content-length":"1000000"}}), async () => {
    await assert.rejects(boundedCompletion("https://example.invalid",{}),reason("response_too_large"));
  });
});

test("provider shape, token truncation, JSON and network failures have safe explicit reasons", async () => {
  for (const [response,expected] of [[ok("partial", "length"),"truncated"],[ok("", "stop"),"invalid_response"],[ok("text","content_filter"),"invalid_response"],[new Response('{"choices":[{"message":{"content":42}}]}'),"invalid_response"],[new Response("not json"),"invalid_response"],[new Response("secret provider error",{status:402}),"provider_rejected"]]) {
    await withFetch(async () => response,async () => { await assert.rejects(boundedCompletion("https://example.invalid",{}),reason(expected)); });
  }
  await withFetch(async () => {throw new Error("secret network payload");}, async () => {
    await assert.rejects(boundedCompletion("https://example.invalid",{}),error=>error.reason==="network_failure" && !error.message.includes("secret"));
  });
  await withFetch(async () => ok(), async () => assert.equal(await boundedCompletion("https://example.invalid",{}),"A complete grounded response."));
});

test("embedding outcomes distinguish timeout, unavailable, invalid data and cancellation", async () => {
  const { DB } = memoryD1();
  const env={DB,AI_EMBEDDING_MODEL:"test", AI_EMBEDDING_DIMENSIONS:"2",AI:{run:async()=>new Promise(()=>{})}};
  assert.deepEqual(await embedWithOutcome(env,"query",{timeoutMs:20}),{vector:null,reason:"timeout"});
  assert.deepEqual(await embedWithOutcome({...env,AI:undefined},"query"),{vector:null,reason:"unavailable"});
  for(const data of [[],[[NaN]],[[Infinity]],[["1"]]]) assert.equal((await embedWithOutcome({...env,AI:{run:async()=>({data})}},"query")).reason,"invalid_response");
  const controller=new AbortController();controller.abort();
  assert.equal((await embedWithOutcome(env,"query",{signal:controller.signal})).reason,"cancelled");
  assert.deepEqual(await embedWithOutcome({...env,AI:{run:async()=>({data:[[0.1,0.2]]})}},"query"),{vector:[0.1,0.2],reason:null});
});

test("draft assembly preserves its review scaffold for rejected/truncated providers", async () => {
  const { assembleDraft, retrieveDraftResearch } = loadTs("src/services/draft-conclusions.ts");
  const { draftConclusionsRequestSchema } = loadTs("../../packages/shared/src/index.ts");
  const { DB } = memoryD1();
  const env={DB,LLM_API_KEY:"synthetic",LLM_MODEL:"synthetic",LLM_BASE_URL:"https://example.invalid"};
  const parsed=draftConclusionsRequestSchema.parse({findings_text:"The tenant continued to occupy the apartment."});
  const research=await retrieveDraftResearch(env,parsed,async()=>({results:[]}));
  await withFetch(async(_url,init)=>{
    assert.equal(JSON.parse(init.body).max_tokens,900);
    return ok("Incomplete draft", "length");
  },async()=>{
    const {draft}=await assembleDraft(parsed,research,env);
    assert.equal(draft.generation_mode,"heuristic_fallback");
    assert.equal(draft.review_required,true);
    assert.match(draft.fallback_reason,/truncated/);
    assert.doesNotMatch(draft.draft_text,/Incomplete draft/);
  });
});

test('embedding envelopes validate dimensions, row counts and every vector while retaining valid partial outcomes',async()=>{
 const {DB,sqlite}=memoryD1();const {embedBatchWithOutcome}=loadTs('src/services/embeddings.ts');
 try{const env={DB,AI_EMBEDDING_MODEL:'fixture',AI_EMBEDDING_DIMENSIONS:'2'};
 for(const response of [null,[],{data:[new Float32Array([0.1,0.2])]},{data:[[0.1,0.2]],shape:[1,3]},{data:[[0.1,0.2]],shape:[2,2]},{data:[[0.1,0.2]],shape:'bad'}])assert.equal((await embedWithOutcome({...env,AI:{run:async()=>response}},'text')).reason,'invalid_response');
 const oversized=new Proxy([],{get(target,key){if(key==='length')return 1000000;if(key==='map'||key===Symbol.iterator)throw Error('Oversized array was traversed');return Reflect.get(target,key);}});
 for(const response of [{data:oversized},{data:[oversized]},{data:[[0.1,0.2]],shape:oversized}])assert.equal((await embedWithOutcome({...env,AI:{run:async()=>response}},'text')).reason,'invalid_response');
 const result=await embedBatchWithOutcome({...env,AI:{run:async()=>({data:[[0.1,0.2],[Infinity,0.3]],shape:[2,2]})}},['first','second']);assert.deepEqual(result,[{vector:[0.1,0.2],reason:null},{vector:null,reason:'invalid_response'}]);
 }finally{sqlite.close();}
});

test('invalid UTF-8 and non-byte provider chunks are rejected with observed cancellation',async()=>{
 for(const chunk of [new Uint8Array([0xff]),'not a byte chunk']){let cancelled=false;
 await withFetch(async()=>new Response(new ReadableStream({start(controller){controller.enqueue(chunk);},cancel(){cancelled=true;}})),async()=>{await assert.rejects(boundedCompletion('https://example.invalid',{}, {},100),reason('invalid_response'));assert.equal(cancelled,true);});
 }
});
