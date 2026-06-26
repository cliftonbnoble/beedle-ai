import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const retrievalPagePath = path.resolve(process.cwd(), "src/app/admin/retrieval/page.tsx");

test("retrieval diagnostics page displays when debug query type differs from production search", async () => {
  const src = await fs.readFile(retrievalPagePath, "utf8");

  assert.match(src, /result\.debugProfile\.requestedQueryType/);
  assert.match(src, /result\.debugProfile\.productionSearchQueryType/);
  assert.match(src, /result\.debugProfile\.matchesProductionSearchPath/);
  assert.match(src, /differs from production search/);
});
