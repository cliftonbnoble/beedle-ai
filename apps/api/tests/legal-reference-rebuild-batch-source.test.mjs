import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src=fs.readFileSync(new URL('../src/services/legal-catalog.ts',import.meta.url),'utf8');
test('catalog rebuild stages bounded inserts and retains durable rollback evidence',()=>{
  const stage=src.slice(src.indexOf('export async function stageLegalCatalog'),src.indexOf('export async function publishLegalCatalog'));
  assert.match(stage,/INSERT INTO legal_catalog_rows/);assert.match(stage,/statements.slice\(at, at \+ 50\)/);
  assert.doesNotMatch(stage,/DELETE FROM legal_/);assert.match(src,/manifest mismatch/);
});
