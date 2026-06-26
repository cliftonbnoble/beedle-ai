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
