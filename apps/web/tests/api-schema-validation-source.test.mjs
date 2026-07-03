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
  // Responses are schema-validated through parseResponse, which converts a zod dump into a
  // user-safe message while logging the technical detail (WEB-05).
  assert.match(src, /return parseResponse\(retrievalPreviewResponseSchema, json, "retrieval preview"\)/);
  assert.match(src, /return parseResponse\(dashboardSummarySchema, json, "dashboard summary"\)/);
  assert.match(src, /return parseResponse\(searchDebugResponseSchema, json, "search debug"\)/);
  assert.doesNotMatch(src, /as Promise<RetrievalPreviewResponse>/);
  assert.doesNotMatch(src, /as Promise<DashboardSummary>/);
  assert.doesNotMatch(src, /json as SearchDebugResponse/);
});

// WEB-05: pages render err.message verbatim, so fetchJson must throw user-safe messages — the raw
// status line/response body/zod dump goes to console.error, never the UI.
test("API failures surface user-safe messages, with technical detail logged not thrown", async () => {
  const src = await fs.readFile(apiPath, "utf8");

  assert.match(src, /export class ApiError extends Error/);
  assert.match(src, /function userSafeApiMessage\(status: number, body: string\): string/);
  assert.match(src, /if \(status >= 500\) return "The service hit a temporary problem\. Please try again\."/);
  assert.match(src, /console\.error\(`API failed \(\$\{response\.status\}\) for \$\{path\}: \$\{body\}`\)/);
  assert.match(src, /throw new ApiError\(response\.status, userSafeApiMessage\(response\.status, body\)\)/);
  assert.doesNotMatch(src, /throw new Error\(`API failed/);
  // Schema-drift failures are wrapped the same way.
  assert.match(src, /function parseResponse<T>\(/);
  assert.match(src, /Received an unexpected \$\{label\} response from the service/);
  assert.doesNotMatch(src, /return \w+ResponseSchema\.parse\(json\)/);
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

// WEB-09: a dev server must never silently talk to production — the unconditional prod fallback is gone.
test("dev builds default to the local worker, not production", async () => {
  const src = await fs.readFile(apiPath, "utf8");
  assert.match(src, /NODE_ENV === "development" \? "http:\/\/127\.0\.0\.1:8787" : "https:\/\/beedle-api/);
  assert.doesNotMatch(src, /NEXT_PUBLIC_API_BASE_URL \|\| "https:\/\/beedle-api/);
});
