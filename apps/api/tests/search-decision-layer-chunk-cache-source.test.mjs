import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

// The authority and supporting-fact decision-layer fallbacks request overlapping document
// sets with identical where/params and the same section prefilter. A shared request-scoped
// per-document cache lets the second pass reuse the first pass's fetched rows instead of
// re-running the expensive all-chunks fetch. This must stay wired up (and stay a per-document
// cache that only fetches the missing ids) so the redundant round-trip does not return.
test("decision-layer fallbacks share a per-document chunk cache", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  const cacheFn = src.match(/async function fetchDecisionLayerChunksCached\([\s\S]*?\n\}/)?.[0] || "";
  assert.ok(cacheFn, "fetchDecisionLayerChunksCached must exist");
  // Only the not-yet-cached document ids are fetched.
  assert.match(cacheFn, /const missing = documentIds\.filter\(\(documentId\) => !cache\.has\(documentId\)\)/);
  assert.match(cacheFn, /await fetchChunksByDocumentIds\(env, missing, where, params, true\)/);
  // Fetched rows are grouped and cached per document (including empty results, to avoid refetch).
  assert.match(cacheFn, /cache\.set\(documentId, grouped\.get\(documentId\) \?\? \[\]\)/);

  // Both fallbacks accept an optional cache and use the cached helper when given one.
  const authFn = src.match(/async function fetchAuthorityChunksByDocumentIds\([\s\S]*?\n\}/)?.[0] || "";
  const supFn =
    src.match(/async function fetchSupportingFactChunksByDocumentIds\([\s\S]*?const supportRows =/)?.[0] || "";
  for (const [name, fn] of [
    ["fetchAuthorityChunksByDocumentIds", authFn],
    ["fetchSupportingFactChunksByDocumentIds", supFn]
  ]) {
    assert.match(fn, /cache\?: Map<string, ChunkRow\[\]>/, `${name} must accept an optional cache`);
    assert.match(
      fn,
      /cache\s*\?\s*await fetchDecisionLayerChunksCached\(env, documentIds, where, params, cache\)\s*:\s*await fetchChunksByDocumentIds\(env, documentIds, where, params, true\)/,
      `${name} must use the cache when provided and fall back to a direct fetch otherwise`
    );
  }

  // Finalize builds one shared cache and threads it through both fallback calls.
  assert.match(src, /const decisionLayerChunkCache = new Map<string, ChunkRow\[\]>\(\);/);
  assert.match(
    src,
    /fetchAuthorityChunksByDocumentIds\(\s*env,\s*authorityFallbackDocumentIds,\s*where,\s*params,\s*decisionLayerChunkCache\s*\)/
  );
  assert.match(
    src,
    /fetchSupportingFactChunksByDocumentIds\(\s*env,\s*missingSupportingFactDocumentIds,\s*where,\s*params,\s*context,\s*decisionLayerChunkCache\s*\)/
  );
});
