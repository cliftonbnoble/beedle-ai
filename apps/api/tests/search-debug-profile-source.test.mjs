import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const sharedPath = path.resolve(process.cwd(), "../../packages/shared/src/index.ts");
const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

test("retrieval debug response explicitly distinguishes debug query type from production search path", async () => {
  const shared = await fs.readFile(sharedPath, "utf8");
  const search = await fs.readFile(searchServicePath, "utf8");

  assert.match(shared, /debugProfile: z\.object/);
  assert.match(shared, /endpoint: z\.literal\("admin_retrieval_debug"\)/);
  assert.match(shared, /productionSearchQueryType: z\.literal\("keyword"\)/);
  assert.match(shared, /matchesProductionSearchPath: z\.boolean\(\)/);
  assert.match(search, /debugProfile:\s*\{/);
  assert.match(search, /requestedQueryType: queryType/);
  assert.match(search, /productionSearchQueryType: "keyword"/);
  assert.match(search, /matchesProductionSearchPath: queryType === "keyword"/);
  assert.match(search, /return await runSearchInternal\(env, parsed, "keyword", false\)/);
  assert.match(search, /return await runSearchInternal\(env, parsed, parsed\.queryType, true\)/);
});
