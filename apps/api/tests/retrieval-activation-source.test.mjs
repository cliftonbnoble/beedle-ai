import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const src = fs.readFileSync(new URL("../src/services/retrieval-activation.ts", import.meta.url), "utf8");

function sliceBetween(source, startPattern, endPattern) {
  const start = source.search(startPattern);
  assert.notEqual(start, -1, `Missing start pattern ${startPattern}`);
  const end = source.slice(start + 1).search(endPattern);
  assert.notEqual(end, -1, `Missing end pattern ${endPattern}`);
  return source.slice(start, start + 1 + end);
}

test("activation batches document activation state writes", () => {
  assert.match(src, /const ACTIVATION_STATEMENT_BATCH_SIZE = 50/);
  assert.match(src, /async function executeActivationStatementBatches\(env: Env, statements: D1PreparedStatement\[\]\)/);

  const activationWriteFn = sliceBetween(src, /export async function writeTrustedRetrievalActivation/, /export async function rollbackTrustedRetrievalActivation/);
  assert.match(activationWriteFn, /const documentActivationStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(activationWriteFn, /documentActivationStatements\.push\([\s\S]*INSERT OR REPLACE INTO retrieval_activation_documents/);
  assert.match(activationWriteFn, /documentActivationStatements\.push\([\s\S]*UPDATE documents[\s\S]*SET searchable_at = COALESCE\(searchable_at, \?\), updated_at = \?/);
  assert.match(activationWriteFn, /await executeActivationStatementBatches\(env, documentActivationStatements\)/);
  assert.doesNotMatch(activationWriteFn, /for \(const row of documentsActivated\) \{[\s\S]*?\.run\(\)[\s\S]*?UPDATE documents[\s\S]*?\.run\(\)/);
});

test("activation batches per-chunk D1 writes after vector status is known", () => {
  const activationWriteFn = sliceBetween(src, /export async function writeTrustedRetrievalActivation/, /export async function rollbackTrustedRetrievalActivation/);

  assert.match(activationWriteFn, /row\.vectorWriteStatus = vectorWriteStatus/);
  assert.match(activationWriteFn, /const chunkActivationStatements = \[/);
  assert.match(activationWriteFn, /chunkActivationStatements = \[[\s\S]*INSERT OR REPLACE INTO retrieval_embedding_rows/);
  assert.match(activationWriteFn, /chunkActivationStatements = \[[\s\S]*INSERT OR REPLACE INTO retrieval_search_rows/);
  assert.match(activationWriteFn, /chunkActivationStatements = \[[\s\S]*INSERT OR REPLACE INTO retrieval_search_chunks/);
  assert.match(activationWriteFn, /chunkActivationStatements = \[[\s\S]*INSERT OR REPLACE INTO retrieval_activation_chunks/);
  assert.match(activationWriteFn, /await executeActivationStatementBatches\(env, chunkActivationStatements\)/);
  assert.doesNotMatch(activationWriteFn, /INSERT OR REPLACE INTO retrieval_embedding_rows[\s\S]*?\.run\(\)/);
  assert.doesNotMatch(activationWriteFn, /INSERT OR REPLACE INTO retrieval_search_chunks[\s\S]*?\.run\(\)/);
});
