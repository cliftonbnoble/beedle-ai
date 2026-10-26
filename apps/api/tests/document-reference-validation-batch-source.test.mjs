import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src=fs.readFileSync(new URL('../src/services/legal-references.ts',import.meta.url),'utf8');
test('reference refresh retains prior validation and records its pinned catalog',()=>{
  assert.match(src,/INSERT INTO document_reference_validation_history/);
  assert.match(src,/INSERT INTO reference_validation_guards/);
  assert.match(src,/catalog_version/);
  assert.match(src,/snapshot_json=json_set/);
});
test('reference backfill pins one catalog and commits each document with a revision guard',()=>{
  const body=src.slice(src.indexOf('export async function backfillReferenceValidation'),src.indexOf('async function verifyCitationsSnapshot'));
  assert.match(body,/const lookups = await loadReferenceLookups\(env\)/);
  assert.match(body,/withDocumentRevision\(env, row.id, row.documentRevision, statements\)/);
  assert.doesNotMatch(body,/executeReferenceStatementBatches/);
});
