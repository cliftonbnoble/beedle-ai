import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestPath = path.resolve(process.cwd(), "src/services/ingest.ts");

test("initial ingest batches document insert with reference validation and text artifacts", async () => {
  const src = await fs.readFile(ingestPath, "utf8");
  const start = src.indexOf("export async function ingestDocument");
  assert.notEqual(start, -1);
  const end = src.indexOf("export async function approveDecision", start);
  assert.notEqual(end, -1);
  const ingestFn = src.slice(start, end);

  assert.match(src, /buildDocumentReferenceValidationStatements/);
  assert.match(src, /buildDocumentTextArtifactStatements/);
  assert.match(src, /executeTextArtifactStatementBatches/);
  assert.match(ingestFn, /const documentInsertStatement = env\.DB\.prepare/);
  assert.match(ingestFn, /const referenceValidationStatements = await buildDocumentReferenceValidationStatements\(env, documentId,/);
  assert.match(ingestFn, /const artifacts = buildDocumentTextArtifactStatements\(env,/);
  assert.match(ingestFn, /await executeTextArtifactStatementBatches\(env, \[[\s\S]*?documentInsertStatement,[\s\S]*?\.\.\.referenceValidationStatements,[\s\S]*?\.\.\.artifacts\.statements[\s\S]*?\]\)/);
  assert.match(ingestFn, /const vectorJobStatement = parsedInput\.performVectorUpsert/);
  assert.match(ingestFn, /await enqueueVectorJob\(env, documentId\)/);
  assert.doesNotMatch(ingestFn, /await insertChunkVectors\(env, documentId, artifacts\.chunks\)/);
  assert.doesNotMatch(ingestFn, /await env\.DB\.prepare\([\s\S]*?INSERT INTO documents[\s\S]*?\.run\(\)[\s\S]*?await refreshDocumentReferenceValidation/);
});
