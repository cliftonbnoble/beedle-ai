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
  assert.match(src, /export async function executeTextArtifactStatementBatches\(env: Env, statements: D1PreparedStatement\[\]\)/);
  assert.match(src, /await env\.DB\.batch\(batch\)/);

  const deleteFn = sliceBetween(src, /function buildDeleteDocumentTextArtifactStatements/, /export function buildDocumentTextArtifactStatements/);
  assert.match(deleteFn, /return \[/);
  assert.match(deleteFn, /DELETE FROM document_chunks/);
  assert.match(deleteFn, /DELETE FROM section_paragraphs/);
  assert.match(deleteFn, /DELETE FROM document_sections/);
  assert.doesNotMatch(deleteFn, /\.run\(\)/);
  assert.doesNotMatch(deleteFn, /await env\.DB\.batch/);

  const sectionInsertFn = sliceBetween(src, /function buildSectionAndParagraphStatements/, /function buildDeleteDocumentTextArtifactStatements/);
  assert.match(sectionInsertFn, /const statements: D1PreparedStatement\[\] = \[\]/);
  assert.match(sectionInsertFn, /statements\.push\(/);
  assert.match(sectionInsertFn, /INSERT INTO document_sections/);
  assert.match(sectionInsertFn, /INSERT INTO section_paragraphs/);
  assert.match(sectionInsertFn, /return \{ paragraphRows, statements \}/);
  assert.doesNotMatch(sectionInsertFn, /await executeTextArtifactStatementBatches/);
  assert.doesNotMatch(sectionInsertFn, /\.run\(\)/);

  const artifactPlanFn = sliceBetween(src, /export function buildDocumentTextArtifactStatements/, /export async function rebuildDocumentTextArtifacts/);
  assert.match(artifactPlanFn, /const deleteStatements = buildDeleteDocumentTextArtifactStatements/);
  assert.match(artifactPlanFn, /const \{ paragraphRows, statements: sectionStatements \} = buildSectionAndParagraphStatements/);
  assert.match(artifactPlanFn, /const chunkStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(artifactPlanFn, /chunkStatements\.push\(/);
  assert.match(artifactPlanFn, /INSERT INTO document_chunks/);
  assert.match(artifactPlanFn, /statements: \[\.\.\.deleteStatements, \.\.\.sectionStatements, \.\.\.chunkStatements\]/);
  assert.doesNotMatch(artifactPlanFn, /await executeTextArtifactStatementBatches/);

  const rebuildFn = sliceBetween(src, /export async function rebuildDocumentTextArtifacts/, /function qcPassed/);
  assert.match(rebuildFn, /const artifacts = buildDocumentTextArtifactStatements\(env, params\)/);
  assert.match(rebuildFn, /await executeTextArtifactStatementBatches\(env, artifacts\.statements\)/);
  assert.match(rebuildFn, /await insertChunkVectors\(env, params\.documentId, artifacts\.chunks\)/);
  assert.doesNotMatch(rebuildFn, /INSERT INTO document_chunks[\s\S]*?\.run\(\)/);
});
