import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiPath = path.resolve(process.cwd(), "src/lib/api.ts");

test("detail and debug API helpers parse backend responses with schemas", async () => {
  const src = await fs.readFile(apiPath, "utf8");

  assert.match(src, /retrievalPreviewResponseSchema/);
  assert.match(src, /dashboardSummarySchema/);
  assert.match(src, /searchDebugResponseSchema/);
  assert.match(src, /return retrievalPreviewResponseSchema\.parse\(json\)/);
  assert.match(src, /return dashboardSummarySchema\.parse\(json\)/);
  assert.match(src, /return searchDebugResponseSchema\.parse\(json\)/);
  assert.doesNotMatch(src, /as Promise<RetrievalPreviewResponse>/);
  assert.doesNotMatch(src, /as Promise<DashboardSummary>/);
  assert.doesNotMatch(src, /json as SearchDebugResponse/);
});
