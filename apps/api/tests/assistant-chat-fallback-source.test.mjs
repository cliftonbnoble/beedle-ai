import test from "node:test";
import assert from "node:assert/strict";
import { loadTs } from "./helpers/load-ts.mjs";
import { memoryD1 } from "./helpers/d1-memory.mjs";
const {callAssistantModel}=loadTs("src/services/assistant-chat.ts");
test("assistant preserves grounded fallback and safe reasons for provider failures",async()=>{
  const {DB}=memoryD1();
  const input={env:{DB,LLM_API_KEY:"synthetic",LLM_MODEL:"synthetic",LLM_BASE_URL:"https://example.invalid"},scopeLabel:"All decisions",messages:[{role:"user",content:"Question?"}],decisions:[{documentId:"test",title:"Test",citation:"T1",sourceLink:"https://example.invalid",score:1,primaryAuthorityPassage:{chunkId:"test-chunk",snippet:"The tenant retained possession of the dwelling.",sectionLabel:"Conclusions",citationAnchor:"T1#p1",paragraphAnchor:"p1"}}]};
  const original=globalThis.fetch;
  try {
    for(const status of [401,402,429,503]) {
      globalThis.fetch=async()=>new Response("private provider error",{status});
      const result=await callAssistantModel(input);
      assert.equal(result.model,"grounded-extractive-fallback");
      assert.equal(result.fallbackReason,status===429?"provider_rate_limited":"provider_rejected");
      assert.doesNotMatch(result.answer,/private provider/);
    }
    globalThis.fetch=async(_url,init)=>{
      assert.equal(JSON.parse(init.body).max_tokens,1200);
      return new Response('{"choices":[{"message":{"content":"A complete answer"},"finish_reason":"length"}]}');
    };
    assert.equal((await callAssistantModel(input)).fallbackReason,"truncated");
    const controller=new AbortController();controller.abort();
    assert.equal((await callAssistantModel({...input,context:{signal:controller.signal}})).fallbackReason,"cancelled");
    assert.equal((await callAssistantModel({...input,env:{}})).fallbackReason,"unconfigured");
    assert.equal((await callAssistantModel({...input,env:{},decisions:[]})).fallbackReason,"no_evidence");
  }finally {globalThis.fetch=original;}
});
