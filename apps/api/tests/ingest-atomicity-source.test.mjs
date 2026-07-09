import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestPath = path.resolve(process.cwd(), "src/services/ingest.ts");

test("ingest commits source and searchability only after derived DB artifacts persist", async () => {
  const src = await fs.readFile(ingestPath, "utf8");
  const ingestFn = src.slice(src.indexOf("export async function ingestDocument"));

  const parseAt = ingestFn.indexOf("const extracted =");
  const persistAt = ingestFn.indexOf("await executeTextArtifactStatementBatches");
  const sourceAt = ingestFn.indexOf("await storeSourceFile");
  assert.ok(parseAt >= 0 && persistAt > parseAt && sourceAt > persistAt, "parse, DB persistence, then R2 storage is required");
  assert.match(ingestFn, /UPDATE documents SET searchable_at = \?, updated_at = \? WHERE id = \?/);
  assert.match(ingestFn, /env\.DB\.prepare\("DELETE FROM documents WHERE id = \?"\)\.bind\(documentId\)\.run\(\)/);
  assert.match(ingestFn, /env\.SOURCE_BUCKET\.delete\(sourceKey\)/);
});
