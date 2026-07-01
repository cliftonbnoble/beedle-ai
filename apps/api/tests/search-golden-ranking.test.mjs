import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

// SEARCH-04: behavioral golden-query ranking net. Unlike the source-pattern relevance tests (which pin
// the implementation) and the keyword-regression harness (which checks recall thresholds), this asserts
// the EXACT ordered top-N result identity for a representative query set. It is the regression net that
// makes a ranking refactor (SEARCH-02 de-bloat) provably behavior-preserving: capture the golden, do the
// refactor, re-run — any reshuffle fails loudly.
//
// It runs against the local wrangler-dev corpus (env.AI is inert locally, so the vector stage is skipped
// and this pins the lexical + decision-layer ranking — exactly the surface SEARCH-02 changes). It is NOT
// part of test:source / the push CI gate (those have no corpus); run it locally before/after a refactor:
//   pnpm --filter @beedle/api test:search-golden
// Regenerate after an intended ranking/corpus change (review the diff before committing):
//   UPDATE_SEARCH_GOLDEN=1 pnpm --filter @beedle/api test:search-golden

const GOLDEN_PATH = path.resolve(process.cwd(), "tests/fixtures/search-golden-ranking.json");
const BASE_URL = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/+$/, "");
const UPDATE = process.env.UPDATE_SEARCH_GOLDEN === "1";
const PER_QUERY_TIMEOUT_MS = 120000;

const golden = JSON.parse(await fs.readFile(GOLDEN_PATH, "utf8"));

async function serverReachable() {
  try {
    const res = await fetch(`${BASE_URL}/health`, { signal: AbortSignal.timeout(4000) });
    return res.ok;
  } catch {
    return false;
  }
}

async function fetchTopN(q, attempt = 0) {
  const res = await fetch(`${BASE_URL}/admin/retrieval/debug`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      query: q.query,
      queryType: q.queryType,
      corpusMode: q.corpusMode,
      limit: q.limit,
      filters: q.filters
    }),
    signal: AbortSignal.timeout(PER_QUERY_TIMEOUT_MS)
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    // Retry a couple of times on transient local-worker restarts (503) so a mid-request reload of the
    // dev server does not corrupt a capture/compare with a spurious failure.
    if (res.status === 503 && attempt < 3) {
      await new Promise((resolve) => setTimeout(resolve, 1500));
      return fetchTopN(q, attempt + 1);
    }
    throw new Error(`${q.id}: HTTP ${res.status} ${body.slice(0, 200)}`);
  }
  const body = await res.json();
  return (body.results || []).slice(0, q.limit).map((row) => row.citation || row.documentId || "?");
}

const reachable = await serverReachable();

if (!reachable) {
  test(`search golden ranking — SKIPPED (no local server at ${BASE_URL})`, { skip: true }, () => {});
} else if (UPDATE) {
  test("update search golden ranking snapshot", async () => {
    let ok = 0;
    let failed = 0;
    for (const q of golden.queries) {
      try {
        q.expectedTopN = await fetchTopN(q);
        delete q.captureError;
        ok += 1;
      } catch (error) {
        // Resilient capture: one broken query must not block pinning the rest. Record the error so the
        // golden documents which query is currently failing instead of silently dropping it.
        q.expectedTopN = null;
        q.captureError = String(error instanceof Error ? error.message : error);
        failed += 1;
      }
    }
    await fs.writeFile(GOLDEN_PATH, `${JSON.stringify(golden, null, 2)}\n`);
    console.log(`Updated golden snapshot: ${ok} pinned, ${failed} failed to capture.`);
  });
} else {
  for (const q of golden.queries) {
    if (!Array.isArray(q.expectedTopN)) {
      // Captured while erroring — a known-broken query tracked as a TODO (see captureError) so the net
      // stays green while the underlying bug is fixed. Re-pin via UPDATE_SEARCH_GOLDEN=1 once fixed.
      test(`golden ranking: ${q.id} ("${q.query}") [known-broken]`, { todo: q.captureError || "no golden pinned" }, () => {});
      continue;
    }
    test(`golden ranking: ${q.id} ("${q.query}")`, async () => {
      const actual = await fetchTopN(q);
      assert.deepEqual(
        actual,
        q.expectedTopN,
        `Top-${q.limit} ranking changed for "${q.query}" (${q.id}).\n` +
          `  expected: ${JSON.stringify(q.expectedTopN)}\n` +
          `  actual:   ${JSON.stringify(actual)}`
      );
    });
  }
}
