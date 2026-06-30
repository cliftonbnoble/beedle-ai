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

test("admin ingestion list/detail helpers validate response shape before returning", async () => {
  const src = await fs.readFile(apiPath, "utf8");

  // The shape guard exists and rejects non-object / error-shaped responses.
  assert.match(src, /function expectObjectResponse\(json: unknown, label: string, requireArrayKey\?: string\)/);
  assert.match(src, /typeof json === "object" && !Array\.isArray\(json\)/);
  assert.match(src, /throw new Error\(`Unexpected \$\{label\} response shape`\)/);

  // Both GET helpers route their response through the guard instead of returning a raw fetch.
  assert.match(src, /return expectObjectResponse\(json, "ingestion documents list", "documents"\)/);
  assert.match(src, /return expectObjectResponse\(json, "ingestion document"\)/);
  // The list/detail GETs must no longer return an unvalidated fetchJson directly (POST mutation
  // helpers legitimately still do).
  assert.doesNotMatch(src, /const query = search\.toString\(\);\s*return fetchJson/);
  assert.doesNotMatch(src, /getIngestionDocument\(documentId: string\) \{\s*return fetchJson/);
});
