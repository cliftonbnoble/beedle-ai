import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const servicePath = path.resolve(process.cwd(), "src/services/admin-ingestion.ts");

test("admin metadata update computes QC flags before a single document update", async () => {
  const src = await fs.readFile(servicePath, "utf8");
  const start = src.indexOf("export async function updateIngestionMetadata");
  assert.notEqual(start, -1);
  const end = src.indexOf("export async function reprocessIngestionDocument", start);
  assert.notEqual(end, -1);
  const updateFn = src.slice(start, end);

  assert.match(updateFn, /index_codes_json as indexCodesJson/);
  assert.match(updateFn, /const persistedIndexCodes = updateIndexCodes \?\? parseJsonArray\(row\.indexCodesJson\)/);
  assert.match(updateFn, /const hasIndexCodes = persistedIndexCodes\.length > 0/);
  assert.match(updateFn, /qc_has_index_codes = \?/);
  assert.match(updateFn, /qc_has_rules_section = \?/);
  assert.match(updateFn, /qc_has_ordinance_section = \?/);
  assert.match(updateFn, /qc_passed = \?/);
  assert.match(updateFn, /await refreshDocumentReferenceValidation/);
  assert.doesNotMatch(updateFn, /SELECT index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson, ordinance_sections_json as ordinanceSectionsJson\s+FROM documents\s+WHERE id = \?/);
});
