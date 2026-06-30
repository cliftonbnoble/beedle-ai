import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const activationPath = path.resolve(process.cwd(), "src/services/retrieval-activation.ts");

test("retrieval activation does not mark vector-backed chunks active when vector writes fail", async () => {
  const src = await fs.readFile(activationPath, "utf8");

  assert.match(src, /const searchChunkActive = !input\.performVectorUpsert \|\| vectorWriteStatus === "vector_upserted"/);
  assert.match(src, /row\.searchWriteStatus = searchChunkActive \? "written" : "blocked_by_vector_write_failure"/);
  assert.match(src, /searchChunkActive \? 1 : 0/);
  assert.match(src, /const vectorWriteFailuresCount =/);
  assert.match(src, /vectorWriteFailuresCount === 0/);
  assert.match(src, /vectorWritesReady: vectorWriteFailuresCount === 0/);
  assert.doesNotMatch(src, /has_canonical_reference_alignment, active, created_at\)[\s\S]{0,120}VALUES \([^)]*1, \?\)/);
});

test("activation report surfaces which chunks/documents were blocked by vector write failures", async () => {
  const src = await fs.readFile(activationPath, "utf8");

  // Blocked chunks + their documents are identified, and failures are broken down by reason.
  assert.match(src, /const blockedSearchChunks = chunksActivated\.filter\(\(row\) => row\.searchWriteStatus === "blocked_by_vector_write_failure"\)/);
  assert.match(src, /const blockedDocumentIds = uniqueSorted\(blockedSearchChunks\.map\(\(row\) => row\.documentId\)\)/);
  assert.match(src, /const vectorWriteStatusBreakdown = countBy\(/);

  // The report exposes a dedicated surfacing block with the per-status breakdown and blocked lists.
  assert.match(src, /vectorWriteSurfacing: \{/);
  assert.match(src, /blockedSearchChunkCount: blockedSearchChunks\.length/);
  assert.match(src, /vectorWriteStatusBreakdown,/);
  assert.match(src, /blockedDocumentIds,/);
  assert.match(src, /blockedSearchChunks: blockedSearchChunks\.slice\(0, 50\)\.map/);
  // And the headline writeCounts/summary carry the breakdown too.
  assert.match(src, /blockedSearchChunkCount: blockedSearchChunks\.length,\s*\n\s*vectorWriteStatusBreakdown\s*\n\s*\};/);
});
