import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

function fnBody(src, signature) {
  const start = src.indexOf(signature);
  assert.ok(start > -1, `expected to find ${signature}`);
  // Body runs until the next top-level `function ` declaration.
  const after = src.indexOf("\nfunction ", start + signature.length);
  return src.slice(start, after === -1 ? undefined : after);
}

// PERF-01: query-derived work and row-text normalization must be computed once and reused, not
// recomputed inside the per-row scoring/hot loops. This guard locks in the caching invariants so
// the optimization can't silently regress (e.g. someone reintroducing normalize(combinedSearchableText(row))
// inside scoreRow, or a non-memoized getQueryDerivedContext).
test("query-derived context is memoized once per search", async () => {
  const src = ((await fs.readFile(searchServicePath, "utf8")) + (await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8")));
  const body = fnBody(src, "function getQueryDerivedContext(context: SearchContext): QueryDerivedContext {");
  assert.match(body, /if \(!context\.derived\)/);
  assert.match(body, /context\.derived = buildQueryDerivedContext\(context\)/);
  assert.match(body, /return context\.derived/);
});

test("per-row text caching helpers exist and back the hot path", async () => {
  const src = ((await fs.readFile(searchServicePath, "utf8")) + (await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8")));
  for (const helper of [
    "function cachedCombinedSearchableText(row: ChunkRow, context: SearchContext)",
    "function cachedNormalizedSearchableText(row: ChunkRow, context: SearchContext)",
    "function cachedNormalizedChunkText(row: ChunkRow, context: SearchContext)",
    "function cachedRowMetadata(row: ChunkRow, context: SearchContext)"
  ]) {
    assert.ok(src.includes(helper), `missing caching helper: ${helper}`);
  }
  // Cache is used pervasively, not bypassed.
  const cachedUses = (src.match(/cachedNormalizedSearchableText\(/g) || []).length;
  assert.ok(cachedUses >= 20, `expected cachedNormalizedSearchableText to be widely used, found ${cachedUses}`);
});

test("scoreRow does not re-normalize row text per row", async () => {
  const src = ((await fs.readFile(searchServicePath, "utf8")) + (await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8")));
  const body = fnBody(src, "function scoreRow(row: ChunkRow, vectorScore: number, context: SearchContext): RankingDiagnostics {");
  assert.match(body, /getQueryDerivedContext\(context\)/);
  // No per-row re-normalization of row/combined/chunk text inside the hottest function.
  assert.doesNotMatch(body, /normalize\(combinedSearchableText\(/);
  assert.doesNotMatch(body, /normalize\(cachedCombinedSearchableText\(/);
  assert.doesNotMatch(body, /normalize\(row\.chunkText/);
});

test("no cache-bypassing row-text normalization anywhere outside the cache definitions", async () => {
  const src = ((await fs.readFile(searchServicePath, "utf8")) + (await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8")));
  // The only legitimate normalize(combinedSearchableText...) / normalize(row.chunkText...) lives inside
  // the cache helpers themselves; nothing else may recompute it.
  const combinedBypass = (src.match(/normalize\(combinedSearchableText\(/g) || []).length;
  assert.equal(combinedBypass, 0, "normalize(combinedSearchableText(...)) must go through cachedNormalizedSearchableText");
  const chunkTextNormalizations = (src.match(/normalize\(row\.chunkText/g) || []).length;
  assert.equal(chunkTextNormalizations, 1, "normalize(row.chunkText...) must exist only in cachedNormalizedChunkText");
});

test("per-row issue/procedural matchers read the memoized context + cached text", async () => {
  const src = ((await fs.readFile(searchServicePath, "utf8")) + (await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8")));
  for (const sig of [
    "function chunkMatchesIssueTerms(row: ChunkRow, context: SearchContext): boolean {",
    "function chunkMatchesProceduralTerms(row: ChunkRow, context: SearchContext): boolean {"
  ]) {
    const body = fnBody(src, sig);
    assert.match(body, /getQueryDerivedContext\(context\)/);
    assert.match(body, /cachedNormalizedSearchableText\(row, context\)/);
  }
});
