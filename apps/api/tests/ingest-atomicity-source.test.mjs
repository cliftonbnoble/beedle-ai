import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestPath = path.resolve(process.cwd(), "src/services/ingest.ts");

test("ingest commits source and searchability only after derived DB artifacts persist", async () => {
  const src = await fs.readFile(ingestPath, "utf8");
  const ingestFn = src.slice(src.indexOf("export async function ingestDocument"));

  const parseAt = ingestFn.indexOf("let extracted:");
  const persistAt = ingestFn.indexOf("await executeTextArtifactStatementBatches");
  const sourceAt = ingestFn.indexOf("await storeSourceFile", persistAt);
  assert.ok(parseAt >= 0 && persistAt > parseAt && sourceAt > persistAt, "parse, DB persistence, then R2 storage is required");
  assert.match(ingestFn, /UPDATE documents SET ingestion_state = 'ready'/);
  assert.match(ingestFn, /DELETE FROM documents WHERE id=\? AND ingestion_state='preparing' AND document_revision=1/);
  assert.match(ingestFn, /SELECT ingestion_state,source_r2_key FROM documents/);
  assert.match(ingestFn, /env\.SOURCE_BUCKET\.delete\(sourceKey\)/);
});
