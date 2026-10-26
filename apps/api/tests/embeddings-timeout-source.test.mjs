import test from "node:test";
import assert from "node:assert/strict";
import { loadTs } from "./helpers/load-ts.mjs";
import { memoryD1 } from "./helpers/d1-memory.mjs";
const {embed,embedWithOutcome}=loadTs("src/services/embeddings.ts");
test("embedding compatibility returns null on a bounded timeout",async()=>{
  const {DB}=memoryD1();
  assert.equal(await embed({DB,AI_EMBEDDING_DIMENSIONS:"2",AI:{run:async()=>new Promise(()=>{})}},"query",{timeoutMs:15}),null);
});
test("local binding unavailability degrades distinctly; provider faults still surface",async()=>{
  const {DB}=memoryD1();
  const env={DB,AI_EMBEDDING_DIMENSIONS:"2",AI:{run:async()=>{throw new Error("Binding AI needs to be run remotely");}}};
  assert.deepEqual(await embedWithOutcome(env,"text"),{vector:null,reason:"unavailable"});
  env.AI.run=async()=>{throw new Error("provider outage");};
  await assert.rejects(embed(env,"text"),/provider outage/);
});
