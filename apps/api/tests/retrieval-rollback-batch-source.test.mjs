import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const activationPath = path.resolve(process.cwd(), "src/services/retrieval-activation.ts");

// DATA-03: rollback manifests are corpus-scale, and D1 rejects a statement with more than ~100 bound
// parameters — a single IN(<whole manifest>) batch threw `too many SQL variables` at >~100 ids, breaking
// the recovery path exactly when it was needed. The invariant is now: every destructive mutation runs in
// ≤SQLITE_BIND_LIMIT id chunks, batched through the shared executor, preserving cross-table order; a
// mid-way failure is recoverable because re-running the same manifest converges (idempotent statements).
test("retrieval activation rollback chunks destructive mutations under the D1 bind limit", async () => {
  const src = await fs.readFile(activationPath, "utf8");

  // The limit constant reflects D1's real ~100-param cap (with headroom for appended batch binds).
  assert.match(src, /const SQLITE_BIND_LIMIT = 90;/);

  const rollbackStart = src.indexOf("export async function rollbackTrustedRetrievalActivation");
  assert.notEqual(rollbackStart, -1);
  const rollbackSrc = src.slice(rollbackStart);

  // Reads over manifest ids go through the chunked select helper, never a raw IN(<manifest>) expansion.
  assert.match(rollbackSrc, /selectRowsForIdBatches</);
  assert.doesNotMatch(rollbackSrc, /manifestChunkIds\.map\(\(\) => "\?"\)/);
  assert.doesNotMatch(rollbackSrc, /manifestDocIds\.map\(\(\) => "\?"\)/);
  assert.doesNotMatch(rollbackSrc, /\.bind\(\.\.\.manifestChunkIds/);
  assert.doesNotMatch(rollbackSrc, /\.bind\(\.\.\.manifestDocIds/);

  // Destructive statements are built per ≤bind-limit id chunk and run through the shared batch executor.
  assert.match(rollbackSrc, /const statementChunkSize = Math\.max\(1, SQLITE_BIND_LIMIT - rollbackBatchIds\.length\)/);
  assert.match(rollbackSrc, /const chunkIdChunks = chunkValues\(manifestChunkIds, statementChunkSize\)/);
  assert.match(rollbackSrc, /const docIdChunks = chunkValues\(manifestDocIds, statementChunkSize\)/);
  assert.match(rollbackSrc, /const rollbackStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(rollbackSrc, /await executeActivationStatementBatches\(env, rollbackStatements\)/);

  // Cross-table order preserved: deactivate chunks, then delete rows/embeddings/activation rows/docs.
  const order = [
    rollbackSrc.search(/UPDATE retrieval_search_chunks[\s\S]{0,80}SET active = 0/),
    rollbackSrc.indexOf("DELETE FROM retrieval_search_rows"),
    rollbackSrc.indexOf("DELETE FROM retrieval_embedding_rows"),
    rollbackSrc.indexOf("DELETE FROM retrieval_activation_chunks"),
    rollbackSrc.indexOf("DELETE FROM retrieval_activation_documents")
  ];
  for (const position of order) assert.notEqual(position, -1);
  for (let i = 1; i < order.length; i++) {
    assert.ok(order[i] > order[i - 1], `destructive statement order changed (index ${i})`);
  }
});
