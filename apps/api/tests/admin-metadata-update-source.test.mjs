import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const servicePath = path.resolve(process.cwd(), "src/services/admin-ingestion.ts");

test("admin metadata update batches QC flags with reference validation refresh", async () => {
  const src = await fs.readFile(servicePath, "utf8");
  const start = src.indexOf("export async function updateIngestionMetadata");
  assert.notEqual(start, -1);
  const end = src.indexOf("export async function reprocessIngestionDocument", start);
  assert.notEqual(end, -1);
  const updateFn = src.slice(start, end);

  assert.match(updateFn, /index_codes_json as indexCodesJson/);
  assert.match(updateFn, /const persistedIndexCodes = updateIndexCodes \?\? parseJsonArray\(row\.indexCodesJson\)/);
  assert.match(updateFn, /const hasIndexCodes = persistedIndexCodes\.length > 0/);
  assert.match(updateFn, /qc_has_index_codes = \?/);
  assert.match(updateFn, /qc_has_rules_section = \?/);
  assert.match(updateFn, /qc_has_ordinance_section = \?/);
  assert.match(updateFn, /qc_passed = \?/);
  assert.match(updateFn, /const documentUpdateStatement = env\.DB\.prepare/);
  assert.match(updateFn, /const referenceValidationStatements = await buildDocumentReferenceValidationStatements\(env, documentId,/);
  assert.match(updateFn, /await executeReferenceStatementBatches\(env, \[documentUpdateStatement, \.\.\.referenceValidationStatements\]\)/);
  assert.doesNotMatch(updateFn, /await env\.DB\.prepare\([\s\S]*?\.run\(\)[\s\S]*?await refreshDocumentReferenceValidation/);
  assert.doesNotMatch(updateFn, /SELECT index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson, ordinance_sections_json as ordinanceSectionsJson\s+FROM documents\s+WHERE id = \?/);
});

test("admin reprocess batches document metadata with reference validation and rebuilt artifacts", async () => {
  const src = await fs.readFile(servicePath, "utf8");
  const start = src.indexOf("export async function reprocessIngestionDocument");
  assert.notEqual(start, -1);
  const end = src.indexOf("export async function rejectIngestionDocument", start);
  assert.notEqual(end, -1);
  const reprocessFn = src.slice(start, end);

  assert.match(src, /buildDocumentReferenceValidationStatements/);
  assert.match(src, /buildDocumentTextArtifactStatements/);
  assert.match(src, /executeTextArtifactStatementBatches/);
  assert.match(src, /executeReferenceStatementBatches/);
  assert.match(reprocessFn, /const documentUpdateStatement = env\.DB\.prepare/);
  assert.match(reprocessFn, /const referenceValidationStatements = await buildDocumentReferenceValidationStatements\(env, documentId,/);
  assert.match(reprocessFn, /const textArtifacts = shouldRebuildTextArtifacts[\s\S]*buildDocumentTextArtifactStatements/);
  assert.match(reprocessFn, /const documentMutationStatements = \[[\s\S]*documentUpdateStatement,[\s\S]*\.\.\.referenceValidationStatements,[\s\S]*\.\.\.\(textArtifacts\?\.statements \?\? \[\]\)/);
  assert.match(reprocessFn, /await executeTextArtifactStatementBatches\(env, documentMutationStatements\)/);
  assert.match(reprocessFn, /await executeReferenceStatementBatches\(env, documentMutationStatements\)/);
  assert.doesNotMatch(reprocessFn, /await env\.DB\.prepare\([\s\S]*?\.run\(\)[\s\S]*?await refreshDocumentReferenceValidation/);
});
