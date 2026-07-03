import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const storePath = path.resolve(process.cwd(), "src/components/assistant-thread-store.ts");

// WEB-08: threads persist to localStorage the moment a request starts, so a `pending` placeholder or a
// stale `streaming` flag saved mid-request must never survive normalization — otherwise a reload renders
// a permanent "Thinking…" bubble for a request that no longer exists.
test("persisted assistant threads drop pending placeholders and clear streaming flags", async () => {
  const src = await fs.readFile(storePath, "utf8");

  assert.match(src, /\.filter\(\(message\) => !message\.pending\)/);
  assert.match(src, /\.map\(\(message\) => \(message\.streaming \? \{ \.\.\.message, streaming: false \} : message\)\)/);
  // Both load and save routes go through normalizeThreads, so the strip covers reload healing and
  // storage hygiene.
  assert.match(src, /return normalizeThreads\(JSON\.parse\(raw\)\)/);
  assert.match(src, /const normalized = normalizeThreads\(threads\)/);
});
