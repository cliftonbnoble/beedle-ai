import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

test("search issue fallback prefers indexed document facet tables over JSON scans", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");

  assert.match(searchQueryAnalysisSrc, /function isMissingDocumentFacetTableError\(error: unknown\): boolean/);
  assert.match(searchQueryAnalysisSrc, /no such table:\\s\*document_\(\?:index_codes\|rules_sections\|ordinance_sections\)/);
  assert.match(src, /async function fetchOwnerMoveInOrdinanceFallbackDocumentIds/);
  assert.match(src, /FROM document_ordinance_sections dos[\s\S]*JOIN documents d ON d\.id = dos\.document_id/);
  assert.match(src, /dos\.normalized_section = \?/);
  assert.match(src, /dos\.normalized_section LIKE \?/);
  assert.match(src, /normalizeFilterValue\("ordinance_section", "37\.9"\)/);
  assert.match(src, /isMissingDocumentFacetTableError\(error\)[\s\S]*ordinance_sections_json/);
});

test("explicit index-code scope checks indexed facet table before reference-link compatibility fallback", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");
  const helper = src.match(
    /function buildDirectIndexCodeCompatibilityClause\(values: string\[\]\): string \{[\s\S]*?\n\}/
  )?.[0] || "";
  const bindHelper = src.match(
    /function bindIndexCodeMatchValues\(params: Array<string \| number>, values: string\[\]\) \{[\s\S]*?\n\}/
  )?.[0] || "";

  assert.match(searchQueryAnalysisSrc, /function buildDirectIndexCodeCompatibilityClause\(values: string\[\]\): string/);
  assert.match(bindHelper, /for \(const value of values\) \{\s*params\.push\(normalizeFilterValue\("index_code", value\), value\);\s*\}\s*for \(const value of values\)/);
  assert.match(helper, /const facetClauses = values\.map\(\(\) => "\(dic\.normalized_code = \? OR lower\(dic\.code\) = lower\(\?\)\)"/);
  assert.match(helper, /FROM document_index_codes dic[\s\S]*dic\.document_id = d\.id[\s\S]*\$\{facetClauses\}/);
  assert.match(helper, /OR EXISTS \([\s\S]*FROM document_reference_links l[\s\S]*l\.reference_type = 'index_code'[\s\S]*\$\{referenceClauses\}/);
  assert.match(searchQueryAnalysisSrc, /function bindIndexCodeMatchValues\(params: Array<string \| number>, values: string\[\]\)/);
  assert.match(searchQueryAnalysisSrc, /buildExactIndexCodeIntersectionClauses[\s\S]*bindIndexCodeMatchValues\(params, directValues\)/);
  assert.match(searchQueryAnalysisSrc, /compatibilityClauses\.push\(buildDirectIndexCodeCompatibilityClause\(directIndexCodeValues\)\)/);
  assert.doesNotMatch(
    src,
    /if \(directIndexCodeValues\.length > 0\) \{[\s\S]*FROM document_reference_links l[\s\S]*l\.reference_type = 'index_code'[\s\S]*\}\s*if \(indexCodeFilterContext\.relatedRulesSections/,
    "single index-code scope should not inline a reference-links-only clause"
  );
});

test("explicit rules and ordinance scopes check indexed facet tables before reference-link fallback", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");
  const searchTypesSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-types.ts"), "utf8");
  const helper = src.match(
    /function buildReferenceSectionCompatibilityClause\([\s\S]*?\): string \{[\s\S]*?\n\}/
  )?.[0] || "";
  const bindHelper = src.match(
    /function bindReferenceSectionMatchValues\([\s\S]*?\n\}/
  )?.[0] || "";

  assert.match(searchTypesSrc, /type DocumentReferenceSectionFacet = "rules_section" \| "ordinance_section"/);
  assert.match(helper, /const table = isRules \? "document_rules_sections" : "document_ordinance_sections"/);
  assert.match(helper, /const alias = isRules \? "drs" : "dos"/);
  assert.match(helper, /\$\{alias\}\.normalized_section = \? OR lower\(\$\{alias\}\.section\) = lower\(\?\)/);
  assert.match(helper, /\$\{alias\}\.normalized_section LIKE \?/);
  assert.match(helper, /l\.normalized_value LIKE \?/);
  assert.match(helper, /FROM \$\{table\} \$\{alias\}[\s\S]*\$\{alias\}\.document_id = d\.id/);
  assert.match(helper, /FROM document_reference_links l[\s\S]*l\.reference_type = '\$\{referenceType\}'/);
  assert.match(bindHelper, /const normalizedValue = normalizeFilterValue\(referenceType, value\);[\s\S]*params\.push\(normalizedValue, value\);[\s\S]*if \(options\.includePrefixMatch\) params\.push\(`\$\{normalizedValue\}%`, `\$\{value\}%`\);/);
  assert.match(searchQueryAnalysisSrc, /buildReferenceSectionCompatibilityClause\("rules_section", \[parsed\.filters\.rulesSection\]\)/);
  assert.match(searchQueryAnalysisSrc, /buildReferenceSectionCompatibilityClause\("ordinance_section", \[parsed\.filters\.ordinanceSection\]\)/);
  assert.match(searchQueryAnalysisSrc, /bindReferenceSectionMatchValues\(params, "rules_section", \[parsed\.filters\.rulesSection\]\)/);
  assert.match(searchQueryAnalysisSrc, /bindReferenceSectionMatchValues\(params, "ordinance_section", \[parsed\.filters\.ordinanceSection\]\)/);
});

test("issue-hint candidate lookup uses document facet compatibility clauses", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const fn = src.match(
    /async function fetchIssueCandidateDocumentIds\([\s\S]*?\nasync function fetchOwnerMoveInOrdinanceFallbackDocumentIds/
  )?.[0] || "";

  assert.match(fn, /clauses\.push\(buildDirectIndexCodeCompatibilityClause\(directCodes\)\)/);
  assert.match(fn, /bindIndexCodeMatchValues\(bindings, directCodes\)/);
  assert.match(fn, /buildReferenceSectionCompatibilityClause\("rules_section", hintedRulesSections, \{ includePrefixMatch: true \}\)/);
  assert.match(fn, /bindReferenceSectionMatchValues\(bindings, "rules_section", hintedRulesSections, \{ includePrefixMatch: true \}\)/);
  assert.match(fn, /buildReferenceSectionCompatibilityClause\("ordinance_section", hintedOrdinanceSections, \{ includePrefixMatch: true \}\)/);
  assert.match(fn, /SELECT DISTINCT d\.id as documentId\s+FROM documents d/);
  assert.doesNotMatch(fn, /FROM document_reference_links l\s+JOIN documents d ON d\.id = l\.document_id/);
});
