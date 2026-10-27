import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync(new URL('../src/services/retrieval-activation.ts', import.meta.url), 'utf8');
test('activation never replaces a cascading parent or accepts caller trust labels', () => {
  assert.doesNotMatch(src, /INSERT OR REPLACE|baselineTrusted|promotedDocument/);
  assert.match(src, /verifyCurrentActivationPlan/);
  assert.match(src, /ensureVersionedVector/);
});
test('publication uses an atomic guarded batch after staged vector verification', () => {
  assert.match(src, /await transactional\(env,/);
  assert.match(src, /INSERT INTO retrieval_activation_guards/);
  assert.match(src, /active_retrieval_batch/);
});
