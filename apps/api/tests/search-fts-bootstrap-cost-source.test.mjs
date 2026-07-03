import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

// The FTS bootstrap runs once per cold isolate inside the first search request. It only
// needs to know whether search_chunks_fts is empty (to decide whether to backfill). An
// FTS5 COUNT(*) scans the whole index (~3s on a ~1M-row table) and was the dominant
// first-request latency. The emptiness check must stay a cheap existence probe.
test("FTS bootstrap checks emptiness with a LIMIT 1 existence probe, not COUNT(*)", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");

  assert.match(src, /async function ensureSearchFts\(env: Env\): Promise<boolean>/);
  assert.match(src, /SELECT 1 as present FROM search_chunks_fts LIMIT 1/);
  assert.match(src, /if \(!existingRow\)/);

  assert.doesNotMatch(
    src,
    /COUNT\(\*\)[^\n]*FROM search_chunks_fts/,
    "FTS emptiness check must not use COUNT(*) on search_chunks_fts (FTS5 COUNT scans the whole index)"
  );
});
