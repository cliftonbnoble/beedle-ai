import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync(new URL('../src/services/retrieval-activation.ts', import.meta.url), 'utf8');
test('rollback retains evidence and uses JSON sets without expanding D1 bind counts', () => {
  const rollback = src.slice(src.indexOf('export async function rollbackTrustedRetrievalActivation'));
  assert.match(rollback, /json_each\(\?\)/);
  assert.match(rollback, /await transactional\(env,/);
  assert.doesNotMatch(rollback, /DELETE FROM retrieval_(search|embedding|activation_(chunks|documents|batches))/);
});
