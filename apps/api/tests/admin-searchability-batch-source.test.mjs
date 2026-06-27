import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const servicePath = path.resolve(process.cwd(), "src/services/admin-ingestion.ts");

test("bulk searchability updates execute chunked updates through a D1 batch", async () => {
  const src = await fs.readFile(servicePath, "utf8");
  const start = src.indexOf("export async function bulkEnableSearchability");
  assert.notEqual(start, -1);
  const bulkFn = src.slice(start);

  assert.match(bulkFn, /const maxSearchabilityUpdateBatchSize = 25/);
  assert.match(bulkFn, /const updateStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(bulkFn, /updateStatements\.push\(/);
  assert.match(bulkFn, /UPDATE documents\s+SET searchable_at = COALESCE\(searchable_at, \?\),\s+updated_at = \?\s+WHERE id IN/);
  assert.match(bulkFn, /await env\.DB\.batch\(updateStatements\)/);
  assert.doesNotMatch(bulkFn, /WHERE id IN \(\$\{placeholders\}\)`\s*\)\s*\.bind\([\s\S]*?\.run\(\)/);
});
