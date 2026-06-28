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

test("explicit index-code scope checks indexed facet table before reference-link compatibility fallback", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");
  const helper = src.match(
    /function buildDirectIndexCodeCompatibilityClause\(values: string\[\]\): string \{[\s\S]*?\n\}/
  )?.[0] || "";
  const bindHelper = src.match(
    /function bindIndexCodeMatchValues\(params: Array<string \| number>, values: string\[\]\) \{[\s\S]*?\n\}/
  )?.[0] || "";

  assert.match(src, /function buildDirectIndexCodeCompatibilityClause\(values: string\[\]\): string/);
  assert.match(bindHelper, /for \(const value of values\) \{\s*params\.push\(normalizeFilterValue\("index_code", value\), value\);\s*\}\s*for \(const value of values\)/);
  assert.match(helper, /const facetClauses = values\.map\(\(\) => "\(dic\.normalized_code = \? OR lower\(dic\.code\) = lower\(\?\)\)"/);
  assert.match(helper, /FROM document_index_codes dic[\s\S]*dic\.document_id = d\.id[\s\S]*\$\{facetClauses\}/);
  assert.match(helper, /OR EXISTS \([\s\S]*FROM document_reference_links l[\s\S]*l\.reference_type = 'index_code'[\s\S]*\$\{referenceClauses\}/);
  assert.match(src, /function bindIndexCodeMatchValues\(params: Array<string \| number>, values: string\[\]\)/);
  assert.match(src, /buildExactIndexCodeIntersectionClauses[\s\S]*bindIndexCodeMatchValues\(params, directValues\)/);
  assert.match(src, /compatibilityClauses\.push\(buildDirectIndexCodeCompatibilityClause\(directIndexCodeValues\)\)/);
  assert.doesNotMatch(
    src,
    /if \(directIndexCodeValues\.length > 0\) \{[\s\S]*FROM document_reference_links l[\s\S]*l\.reference_type = 'index_code'[\s\S]*\}\s*if \(indexCodeFilterContext\.relatedRulesSections/,
    "single index-code scope should not inline a reference-links-only clause"
  );
});
