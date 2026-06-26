import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const sourceRoutePath = path.resolve(process.cwd(), "src/routes/source.ts");

test("source route preserves R2-first behavior and falls back to reconstructed DB text", async () => {
  const src = await fs.readFile(sourceRoutePath, "utf8");

  assert.match(src, /const object = await env\.SOURCE_BUCKET\.get\(row\.sourceKey\)/);
  assert.match(src, /if \(!object\) \{[\s\S]*reconstructedSourceMarkdown\(env, documentId, row\)/);
  assert.match(src, /FROM document_sections s[\s\S]*JOIN section_paragraphs p ON p\.section_id = s\.id/);
  assert.match(src, /FROM document_chunks[\s\S]*WHERE document_id = \?/);
  assert.match(src, /headers\.set\("x-beedle-source-fallback", "r2-missing-db-text"\)/);
  assert.match(src, /text\/markdown; charset=utf-8/);
  assert.match(src, /safeFilename/);
});
