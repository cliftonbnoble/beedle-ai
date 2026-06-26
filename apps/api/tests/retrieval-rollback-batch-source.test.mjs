import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const activationPath = path.resolve(process.cwd(), "src/services/retrieval-activation.ts");

test("retrieval activation rollback groups destructive mutations in one D1 batch", async () => {
  const src = await fs.readFile(activationPath, "utf8");
  const rollbackStart = src.indexOf("export async function rollbackTrustedRetrievalActivation");
  assert.notEqual(rollbackStart, -1);
  const rollbackSrc = src.slice(rollbackStart);

  assert.match(rollbackSrc, /const rollbackStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(rollbackSrc, /rollbackStatements\.push\(/);
  assert.match(rollbackSrc, /await env\.DB\.batch\(rollbackStatements\)/);
  assert.match(rollbackSrc, /UPDATE retrieval_search_chunks[\s\S]*SET active = 0/);
  assert.match(rollbackSrc, /DELETE FROM retrieval_search_rows/);
  assert.match(rollbackSrc, /DELETE FROM retrieval_embedding_rows/);
  assert.match(rollbackSrc, /DELETE FROM retrieval_activation_chunks/);
  assert.match(rollbackSrc, /DELETE FROM retrieval_activation_documents/);
  assert.doesNotMatch(rollbackSrc, /DELETE FROM retrieval_activation_documents[\s\S]{0,220}\.run\(\)/);
});
