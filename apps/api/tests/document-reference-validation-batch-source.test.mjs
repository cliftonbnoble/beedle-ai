import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const legalReferencesPath = path.resolve(process.cwd(), "src/services/legal-references.ts");

test("document reference validation refresh writes use ordered D1 batches", async () => {
  const src = await fs.readFile(legalReferencesPath, "utf8");
  const start = src.indexOf("export async function refreshDocumentReferenceValidation");
  assert.notEqual(start, -1);
  const end = src.indexOf("export async function inspectLegalReferences", start);
  assert.notEqual(end, -1);
  const refreshFn = src.slice(start, end);

  assert.match(refreshFn, /await env\.DB\.batch\(\[/);
  assert.match(refreshFn, /DELETE FROM document_reference_links/);
  assert.match(refreshFn, /DELETE FROM document_reference_issues/);
  assert.match(refreshFn, /const validationStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(refreshFn, /validationStatements\.push\(/);
  assert.match(refreshFn, /INSERT INTO document_reference_links/);
  assert.match(refreshFn, /INSERT INTO document_reference_issues/);
  assert.match(refreshFn, /await executeReferenceStatementBatches\(env, validationStatements\)/);
  assert.doesNotMatch(refreshFn, /\.run\(\)/);
});
