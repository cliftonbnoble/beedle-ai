import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

// SEARCH-05: a search sub-query that overflows SQLite's bind-variable limit ("too many SQL variables")
// must DEGRADE the affected recall/scope stage to empty rather than throw out to an HTTP 400. The fetch
// helpers already degrade on transient errors via isRetryableSearchError; this guards that the
// resource-limit case is included, so a broad query (e.g. a large curated keyword family + a structured
// filter) can no longer 400 the whole search. The golden net (search-golden-ranking) exercises the
// runtime behavior; this pins the source contract.
test("isRetryableSearchError degrades search sub-queries on the SQLite bind-variable limit", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");

  const start = src.indexOf("function isRetryableSearchError");
  assert.ok(start > -1, "isRetryableSearchError must exist");
  const body = src.slice(start, start + 400);

  // Still covers the transient cases.
  assert.match(body, /error code: 1031/);
  assert.match(body, /fetch failed/);
  // And now the bind-variable resource limit.
  assert.match(body, /too many SQL variables/i);
});
