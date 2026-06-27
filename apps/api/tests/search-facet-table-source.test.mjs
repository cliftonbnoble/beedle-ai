import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

test("search issue fallback prefers indexed document facet tables over JSON scans", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /function isMissingDocumentFacetTableError\(error: unknown\): boolean/);
  assert.match(src, /no such table:\\s\*document_\(\?:index_codes\|rules_sections\|ordinance_sections\)/);
  assert.match(src, /async function fetchOwnerMoveInOrdinanceFallbackDocumentIds/);
  assert.match(src, /FROM document_ordinance_sections dos[\s\S]*JOIN documents d ON d\.id = dos\.document_id/);
  assert.match(src, /dos\.normalized_section = \?/);
  assert.match(src, /normalizeFilterValue\("ordinance_section", "37\.9"\)/);
  assert.match(src, /isMissingDocumentFacetTableError\(error\)[\s\S]*ordinance_sections_json/);
});
