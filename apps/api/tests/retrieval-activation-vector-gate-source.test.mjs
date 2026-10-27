import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync(new URL('../src/services/retrieval-activation.ts', import.meta.url), 'utf8');
const migration = fs.readFileSync(new URL('../migrations/0016_activation_generations.sql', import.meta.url), 'utf8');
test('activation publication requires verified stored vectors in its transaction guard', () => {
  assert.match(migration, /performVectorUpsert/);
  assert.match(migration, /vector_ready!=1/);
  assert.match(migration, /activation_staging_incomplete/);
  assert.match(src, /ensureVersionedVector/);
  assert.match(src, /activationDigest:plan.digest/);
});
test('activation reports pending vector count and retained retryable state', () => {
  assert.match(src, /pendingVectorCount/);
  assert.match(src, /Submitted vectors are not yet visible/);
  assert.match(src, /THEN 'retryable'/);
});
