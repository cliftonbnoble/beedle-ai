import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

function fnBody(src, signature) {
  const start = src.indexOf(signature);
  assert.ok(start > -1, `expected to find ${signature}`);
  const after = src.indexOf("\nasync function ", start + signature.length);
  const after2 = src.indexOf("\nfunction ", start + signature.length);
  const end = Math.min(after === -1 ? Infinity : after, after2 === -1 ? Infinity : after2);
  return src.slice(start, end === Infinity ? undefined : end);
}

// SEARCH-01: vector search runs one Workers-AI embedding + Vectorize query per query variant. In
// production those are network round-trips, so they must run concurrently, not sequentially. The
// merge keeps the max score per chunk id (order-independent), so parallelizing is result-identical.
test("vectorSearch runs query variants concurrently and merges by max score", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const body = fnBody(src, "async function vectorSearch(");
  assert.ok(body, "vectorSearch must exist");

  // Concurrent fan-out over the variant list. NS-22: query embeds carry the bge instruction prefix.
  assert.match(body, /await Promise\.all\(\s*\n\s*queryList\.map\(async \(query\)/);
  assert.match(body, /await embed\(env, query, \{ isQuery: true \}\)/);
  assert.match(body, /env\.VECTOR_INDEX\.query\(vector/);

  // Order-independent max-score merge is preserved.
  assert.match(body, /const prior = out\.get\(match\.id\) \?\? 0;/);
  assert.match(body, /if \(score > prior\) out\.set\(match\.id, score\);/);

  // No sequential per-variant embed loop remains (the thing we removed).
  assert.doesNotMatch(body, /for \(const query of queryList\) \{[\s\S]*await embed\(env, query\)/);

  // The early-return when AI is unavailable still precedes any work (keeps local/lexical path inert).
  const aiGuardIdx = body.indexOf("if (!env.AI)");
  const promiseAllIdx = body.indexOf("await Promise.all(");
  assert.ok(aiGuardIdx > -1 && aiGuardIdx < promiseAllIdx, "the !env.AI early return must precede the parallel work");
});
