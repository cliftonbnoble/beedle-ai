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
  // The refreshDocumentReferenceValidation wrapper was retired (no callers — every consumer batches
  // the prepared statements itself); the batching invariant lives in the builder pins above.
  assert.doesNotMatch(refreshSource, /refreshDocumentReferenceValidation/);
  assert.doesNotMatch(refreshSource, /\.run\(\)/);
});

test("document reference validation backfill prepares a page before batched writes", async () => {
  const src = await fs.readFile(legalReferencesPath, "utf8");
  const start = src.indexOf("export async function backfillReferenceValidation");
  assert.notEqual(start, -1);
  const end = src.indexOf("export async function verifyCitations", start);
  assert.notEqual(end, -1);
  const backfillSource = src.slice(start, end);

  assert.match(backfillSource, /const resultRows = rows\.results \?\? \[\]/);
  assert.match(backfillSource, /const statements: D1PreparedStatement\[\] = \[\]/);
  // PERF-02: the reference lookup tables are loaded ONCE for the whole backfill page and threaded into
  // every per-document call — never per document, and never per reference value (the old N+1).
  assert.match(backfillSource, /const lookups = await loadReferenceLookups\(env\);\s*\n\s*for \(const row of resultRows\)/);
  assert.match(backfillSource, /buildDocumentReferenceValidationStatements\(\s*env,\s*row\.id,/);
  assert.match(backfillSource, /lookups\s*\n\s*\)/);
  assert.match(backfillSource, /await executeReferenceStatementBatches\(env, statements\)/);
  assert.match(backfillSource, /processed: resultRows\.length/);
  assert.doesNotMatch(backfillSource, /await refreshDocumentReferenceValidation\(env, row\.id/);
});
