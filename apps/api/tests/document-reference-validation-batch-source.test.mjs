import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const legalReferencesPath = path.resolve(process.cwd(), "src/services/legal-references.ts");

test("document reference validation refresh writes use ordered D1 batches", async () => {
  const src = await fs.readFile(legalReferencesPath, "utf8");
  const start = src.indexOf("export async function buildDocumentReferenceValidationStatements");
  assert.notEqual(start, -1);
  const end = src.indexOf("export async function inspectLegalReferences", start);
  assert.notEqual(end, -1);
  const refreshSource = src.slice(start, end);

  assert.match(refreshSource, /export async function buildDocumentReferenceValidationStatements/);
  assert.match(refreshSource, /const resetStatements: D1PreparedStatement\[\] = \[/);
  assert.match(refreshSource, /DELETE FROM document_reference_links/);
  assert.match(refreshSource, /DELETE FROM document_reference_issues/);
  assert.match(refreshSource, /const validationStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(refreshSource, /validationStatements\.push\(/);
  assert.match(refreshSource, /INSERT INTO document_reference_links/);
  assert.match(refreshSource, /INSERT INTO document_reference_issues/);
  assert.match(refreshSource, /return \[\.\.\.resetStatements, \.\.\.validationStatements\]/);
  assert.match(refreshSource, /export async function refreshDocumentReferenceValidation/);
  assert.match(refreshSource, /const statements = await buildDocumentReferenceValidationStatements\(env, documentId, input\)/);
  assert.match(refreshSource, /await executeReferenceStatementBatches\(env, statements\)/);
  assert.doesNotMatch(refreshSource, /\.run\(\)/);
});
