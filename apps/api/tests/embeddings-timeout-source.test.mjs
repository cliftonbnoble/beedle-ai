import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const embeddingsPath = path.resolve(process.cwd(), "src/services/embeddings.ts");

// LLM-02: every outbound AI/LLM call must be time-bounded. The Workers AI embedding call has
// no AbortSignal, so it must be raced against a timeout and degrade to null (callers already
// handle a null embedding). This guards against the embedding call silently becoming unbounded
// again and hanging search/ingest/backfill on a stalled model.
test("embed() bounds the Workers AI call with a timeout and degrades to null", async () => {
  const src = await fs.readFile(embeddingsPath, "utf8");

  assert.match(src, /const EMBEDDING_TIMEOUT_MS = \d+/);
  assert.match(src, /Promise\.race\(\[/);
  assert.match(src, /env\.AI\.run\(env\.AI_EMBEDDING_MODEL/);
  assert.match(src, /setTimeout\(\(\) => resolve\(null\), EMBEDDING_TIMEOUT_MS\)/);
  assert.match(src, /clearTimeout\(timeout\)/);
  // The AI.run call must be inside the race, not awaited directly.
  assert.doesNotMatch(
    src,
    /const response = await env\.AI\.run\(/,
    "env.AI.run must be raced against a timeout, not awaited unbounded"
  );
});
