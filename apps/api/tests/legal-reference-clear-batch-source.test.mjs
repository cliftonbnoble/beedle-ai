import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const legalReferencesPath = path.resolve(process.cwd(), "src/services/legal-references.ts");

test("legal reference table clearing uses one D1 batch", async () => {
  const src = await fs.readFile(legalReferencesPath, "utf8");
  const start = src.indexOf("async function clearReferenceTables");
  assert.notEqual(start, -1);
  const end = src.indexOf("async function restoreReferenceSnapshot", start);
  assert.notEqual(end, -1);
  const clearFn = src.slice(start, end);

  assert.match(clearFn, /await env\.DB\.batch\(\[/);
  assert.match(clearFn, /DELETE FROM legal_reference_crosswalk/);
  assert.match(clearFn, /DELETE FROM legal_reference_sources/);
  assert.match(clearFn, /DELETE FROM legal_index_codes/);
  assert.match(clearFn, /DELETE FROM legal_ordinance_sections/);
  assert.match(clearFn, /DELETE FROM legal_rules_sections/);
  assert.doesNotMatch(clearFn, /\.run\(\)/);
});
