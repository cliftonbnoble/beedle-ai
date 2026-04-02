import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const root = path.resolve(process.cwd());

test("retrieval chunks route exposes raw debug endpoint", async () => {
  const src = await fs.readFile(path.join(root, "src", "index.ts"), "utf8");
  assert.match(src, /\/admin\/retrieval\/documents\/:documentId\/chunks-debug/);
});

test("retrieval foundation text query coerces paragraph text to TEXT for stability", async () => {
  const src = await fs.readFile(path.join(root, "src", "services", "retrieval-foundation.ts"), "utf8");
  assert.match(src, /COALESCE\(CAST\(p\.text AS TEXT\), ''\) as text/);
});
