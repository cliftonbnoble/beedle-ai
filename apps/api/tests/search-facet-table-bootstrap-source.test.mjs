import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");
const migrationPath = path.resolve(process.cwd(), "migrations/0009_document_facets.sql");

// FACET-01: production migrations are decoupled from code deploys (REL-02), so the facet-querying code
// can ship before migration 0009 is applied. `no such table` is NOT a retryable search error, so a
// missing facet table would throw on any index/rules/ordinance filter. The search runtime must lazily
// provision the facet tables (mirroring ensureSearchFts) so filtered searches never throw.
test("search runtime lazily provisions the document facet tables (0009 safety net)", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");

  // The bootstrap exists and is invoked from the once-per-worker runtime bootstrap.
  assert.match(src, /async function ensureDocumentFacetTables\(env: Env\): Promise<void>/);
  assert.match(src, /await ensureDocumentFacetTables\(env\);/);
  const ensureRuntimeIdx = src.indexOf("async function ensureSearchRuntimeIndexes");
  const callIdx = src.indexOf("await ensureDocumentFacetTables(env);");
  assert.ok(callIdx > ensureRuntimeIdx, "ensureDocumentFacetTables must be called from ensureSearchRuntimeIndexes");

  // Creates all three facet tables + their normalized indexes.
  for (const table of ["document_index_codes", "document_rules_sections", "document_ordinance_sections"]) {
    assert.match(src, new RegExp(`CREATE TABLE IF NOT EXISTS ${table}`));
  }
  for (const idx of [
    "idx_document_index_codes_normalized",
    "idx_document_rules_sections_normalized",
    "idx_document_ordinance_sections_normalized"
  ]) {
    assert.match(src, new RegExp(`CREATE INDEX IF NOT EXISTS ${idx}`));
  }

  // Creates the same sync triggers as the migration so new/updated docs stay current during the bridge.
  for (const trig of [
    "documents_ai_document_facets",
    "documents_au_document_facets",
    "documents_ad_document_facets"
  ]) {
    assert.match(src, new RegExp(`CREATE TRIGGER IF NOT EXISTS ${trig}`));
  }

  // Backfill runs only when the tables are empty (a migrated/populated corpus skips the json_each scan).
  assert.match(src, /EXISTS\(SELECT 1 FROM document_index_codes\)/);
  assert.match(src, /if \(!populated\?\.populated\)/);
  assert.match(src, /documentFacetIndexCodeBackfillSql/);
  assert.match(src, /documentFacetRulesBackfillSql/);
  assert.match(src, /documentFacetOrdinanceBackfillSql/);

  // Errors that are not retryable still surface (we never silently swallow a real failure).
  const fnStart = src.indexOf("async function ensureDocumentFacetTables");
  const fnEnd = src.indexOf("async function ensureSearchRuntimeIndexes");
  const body = src.slice(fnStart, fnEnd);
  assert.match(body, /if \(!isRetryableSearchError\(error\)\) throw error;/);
});

// Guard against drift: the bootstrap DDL is copied from the (immutable) migration, so the table and
// trigger names it provisions must match exactly what 0009 declares.
test("bootstrap facet objects match migration 0009 declarations", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const migration = await fs.readFile(migrationPath, "utf8");

  const names = [
    "document_index_codes",
    "document_rules_sections",
    "document_ordinance_sections",
    "idx_document_index_codes_normalized",
    "idx_document_rules_sections_normalized",
    "idx_document_ordinance_sections_normalized",
    "documents_ai_document_facets",
    "documents_au_document_facets",
    "documents_ad_document_facets"
  ];
  for (const name of names) {
    assert.ok(migration.includes(name), `migration 0009 should declare ${name}`);
    assert.ok(src.includes(name), `search bootstrap should provision ${name}`);
  }

  // Normalization rules must match the migration so facet rows resolve identically (e.g. ic- prefix
  // stripping). Spot-check the distinctive prefixes from each backfill.
  for (const token of ["'ic-%'", "'rule %'", "'ordinance %'", "'ordinance.%'"]) {
    assert.ok(migration.includes(token), `migration should normalize using ${token}`);
    assert.ok(src.includes(token), `bootstrap should normalize using ${token}`);
  }
});
