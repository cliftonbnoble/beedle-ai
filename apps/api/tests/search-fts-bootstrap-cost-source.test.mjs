import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

// FTS schema and backfill are deployed through migrations. Request handling must only probe
// availability: a cold-isolate backfill races with other isolates and duplicates FTS rows.
test("FTS request path probes the migrated index without runtime bootstrap work", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");

  assert.match(src, /async function ensureSearchFts\(env: Env\): Promise<boolean>/);
  assert.match(src, /FTS schema and the one-time corpus backfill belong to D1 migrations/);
  assert.match(src, /SELECT rowid FROM search_chunks_fts WHERE search_chunks_fts MATCH \? LIMIT 1/);

  assert.doesNotMatch(
    src,
    /INSERT INTO search_chunks_fts \(/,
    "search requests must not backfill FTS rows"
  );
});
