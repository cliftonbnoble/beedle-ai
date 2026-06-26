import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestPath = path.resolve(process.cwd(), "src/services/ingest.ts");

function sliceBetween(src, startPattern, endPattern) {
  const start = src.search(startPattern);
  assert.notEqual(start, -1);
  const afterStart = src.slice(start);
  const end = afterStart.search(endPattern);
  assert.notEqual(end, -1);
  return afterStart.slice(0, end);
}

test("document text artifact rebuild mutations use ordered D1 batches", async () => {
  const src = await fs.readFile(ingestPath, "utf8");

  assert.match(src, /const textArtifactBatchSize = 50/);
  assert.match(src, /async function executeTextArtifactStatementBatches\(env: Env, statements: D1PreparedStatement\[\]\)/);
  assert.match(src, /await env\.DB\.batch\(batch\)/);

  const deleteFn = sliceBetween(src, /async function deleteDocumentTextArtifacts/, /export async function rebuildDocumentTextArtifacts/);
  assert.match(deleteFn, /await env\.DB\.batch\(\[/);
  assert.match(deleteFn, /DELETE FROM document_chunks/);
  assert.match(deleteFn, /DELETE FROM section_paragraphs/);
  assert.match(deleteFn, /DELETE FROM document_sections/);
  assert.doesNotMatch(deleteFn, /\.run\(\)/);

  const sectionInsertFn = sliceBetween(src, /async function insertSectionsAndParagraphs/, /async function deleteDocumentTextArtifacts/);
  assert.match(sectionInsertFn, /const statements: D1PreparedStatement\[\] = \[\]/);
  assert.match(sectionInsertFn, /statements\.push\(/);
  assert.match(sectionInsertFn, /INSERT INTO document_sections/);
  assert.match(sectionInsertFn, /INSERT INTO section_paragraphs/);
  assert.match(sectionInsertFn, /await executeTextArtifactStatementBatches\(env, statements\)/);
  assert.doesNotMatch(sectionInsertFn, /\.run\(\)/);

  const rebuildFn = sliceBetween(src, /export async function rebuildDocumentTextArtifacts/, /function qcPassed/);
  assert.match(rebuildFn, /const chunkStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(rebuildFn, /chunkStatements\.push\(/);
  assert.match(rebuildFn, /INSERT INTO document_chunks/);
  assert.match(rebuildFn, /await executeTextArtifactStatementBatches\(env, chunkStatements\)/);
  assert.doesNotMatch(rebuildFn, /INSERT INTO document_chunks[\s\S]*?\.run\(\)/);
});
