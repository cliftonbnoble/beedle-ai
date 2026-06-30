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

// Local dev: env.AI is truthy but env.AI.run() throws "Binding AI needs to be run remotely". The
// `if (!env.AI)` guard does not catch this (the binding object exists), so embed() must catch the
// error and degrade to null — otherwise any search that reaches the vector stage 400s locally. This
// is inert in production (env.AI.run succeeds there) and still re-throws genuine AI errors.
test("embed() degrades to null when the AI binding can only run remotely (local dev)", async () => {
  const src = await fs.readFile(embeddingsPath, "utf8");

  assert.match(src, /function isAiBindingUnavailableError\(error: unknown\): boolean/);
  assert.match(src, /needs to be run remotely/i);
  // There is a catch around the AI call that degrades on this specific error and re-throws others.
  assert.match(src, /catch \(error\) \{\s*\n\s*if \(isAiBindingUnavailableError\(error\)\) \{\s*\n\s*return null;\s*\n\s*\}\s*\n\s*throw error;\s*\n\s*\}/);
});
