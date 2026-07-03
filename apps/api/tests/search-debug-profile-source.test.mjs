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
  // requestedQueryType must be captured BEFORE the NS-03 quoted-phrase upgrade mutates queryType, so
  // the debug response reports what the caller asked for, not what the engine upgraded it to.
  assert.match(search, /const requestedQueryType = queryType/);
  assert.match(search, /requestedQueryType,/);
  assert.match(search, /productionSearchQueryType: "keyword"/);
  // Production hardcodes keyword and applies the same quoted-phrase upgrade, so a keyword request
  // matches the production path even when upgraded to exact_phrase.
  assert.match(search, /matchesProductionSearchPath: requestedQueryType === "keyword"/);
  assert.match(search, /wholeQueryQuotedPhrase\(parsed\.query\)/);
  assert.match(search, /return await runSearchInternal\(env, parsed, "keyword", false\)/);
  assert.match(search, /return await runSearchInternal\(env, parsed, parsed\.queryType, true\)/);
});
