import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src=fs.readFileSync(new URL('../src/services/legal-catalog.ts',import.meta.url),'utf8');
test('catalog projection replacement has one guarded publication transaction',()=>{
  const body=src.slice(src.indexOf('export async function publishLegalCatalog'),src.indexOf('export async function listLegalCatalogVersions'));
  assert.match(body,/INSERT INTO legal_catalog_guards/);assert.match(body,/DELETE FROM \$\{table\}/);
  assert.match(body,/INSERT INTO \$\{table\}/);assert.match(body,/await env.DB.batch\(statements\)/);
  assert.doesNotMatch(body,/executeReferenceStatementBatches/);
});
