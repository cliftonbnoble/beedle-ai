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

  const artifactPlanFn = sliceBetween(src, /export function buildDocumentTextArtifactStatements/, /function qcPassed/);
  assert.match(artifactPlanFn, /const deleteStatements = buildDeleteDocumentTextArtifactStatements/);
  assert.match(artifactPlanFn, /const \{ paragraphRows, statements: sectionStatements \} = buildSectionAndParagraphStatements/);
  assert.match(artifactPlanFn, /const chunkStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(artifactPlanFn, /chunkStatements\.push\(/);
  assert.match(artifactPlanFn, /INSERT INTO document_chunks/);
  assert.match(artifactPlanFn, /statements: \[\.\.\.deleteStatements, \.\.\.sectionStatements, \.\.\.chunkStatements\]/);
  assert.doesNotMatch(artifactPlanFn, /await executeTextArtifactStatementBatches/);
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

  // 3) (Removed 2026-07-02: the standalone write-then-swap rebuild was never wired into any production
  //    path — admin reprocess uses buildDocumentTextArtifactStatements behind the empty-document gate
  //    asserted in (1). The dead machinery was deleted in CODE-01; protections (1) and (2) are what
  //    actually guard production writes.)
});
