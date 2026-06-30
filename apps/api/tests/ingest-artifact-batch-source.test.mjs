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
  // DATA-01 write-then-swap: capture prior ids, write replacements first, then delete prior rows.
  assert.match(rebuildFn, /SELECT id FROM document_sections WHERE document_id = \?/);
  assert.match(rebuildFn, /SELECT id FROM document_chunks WHERE document_id = \?/);
  assert.match(rebuildFn, /executeTextArtifactStatementBatches\(env, artifacts\.insertStatements\)/);
  assert.match(rebuildFn, /buildDeletePriorTextArtifactStatements\(env, params\.documentId, priorSectionIds, priorChunkIds\)/);
  assert.match(rebuildFn, /await insertChunkVectors\(env, params\.documentId, artifacts\.chunks\)/);
  assert.doesNotMatch(rebuildFn, /INSERT INTO document_chunks[\s\S]*?\.run\(\)/);
  const insertIdx = rebuildFn.indexOf("artifacts.insertStatements");
  const deleteIdx = rebuildFn.indexOf("buildDeletePriorTextArtifactStatements");
  assert.ok(insertIdx > -1 && deleteIdx > -1 && insertIdx < deleteIdx, "inserts must run before prior-row deletes");
});

test("destructive corpus rewrites are protected (DATA-01 model)", async () => {
  const ingestSrc = await fs.readFile(ingestPath, "utf8");
  const adminSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/admin-ingestion.ts"), "utf8");
  const refSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/legal-references.ts"), "utf8");

  // 1) Reprocess only (re)builds text artifacts when the document is empty — it never destructively
  //    re-chunks a document that still has content.
  assert.match(
    adminSrc,
    /const shouldRebuildTextArtifacts =\s*\n\s*Number\(row\.sectionCount \|\| 0\) === 0 \|\| Number\(row\.paragraphCount \|\| 0\) === 0 \|\| Number\(row\.chunkCount \|\| 0\) === 0/
  );

  // 2) The legal-reference rebuild snapshots first and restores on any failure (clear-then-rebuild
  //    spans many batches, which D1 cannot make atomic).
  assert.match(refSrc, /const snapshot = await takeReferenceSnapshot\(env\)/);
  assert.match(refSrc, /await clearReferenceTables\(env\)/);
  assert.match(refSrc, /await restoreReferenceSnapshot\(env, snapshot\)/);

  // 3) The canonical artifact rebuild uses write-then-swap with a bind-limit-respecting chunked delete.
  assert.match(ingestSrc, /const priorArtifactIdBatchSize = 100/);
  assert.match(ingestSrc, /function buildDeletePriorTextArtifactStatements/);
});
