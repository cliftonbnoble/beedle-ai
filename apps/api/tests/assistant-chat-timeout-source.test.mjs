import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
test("both HTTP LLM clients use the shared bounded completion reader", async () => {
  for (const name of ["assistant-chat", "draft-conclusions"]) {
    const source=await fs.readFile(`src/services/${name}.ts`,"utf8");
    assert.match(source,/await boundedCompletion\(/);
    assert.match(source,/max_tokens: (900|1200)/);
    assert.doesNotMatch(source,/response\.(json|text)\(/);
  }
});
