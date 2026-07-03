import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const legalReferencesPath = path.resolve(process.cwd(), "src/services/legal-references.ts");

function sliceBetween(src, startPattern, endPattern) {
  const start = src.search(startPattern);
  assert.notEqual(start, -1);
  const afterStart = src.slice(start);
  const end = afterStart.search(endPattern);
  assert.notEqual(end, -1);
  return afterStart.slice(0, end);
}

test("legal reference rebuild and restore inserts execute through bounded D1 batches", async () => {
  const src = await fs.readFile(legalReferencesPath, "utf8");

  assert.match(src, /const REFERENCE_BATCH_SIZE = 50/);
  assert.match(src, /async function executeReferenceStatementBatches\(env: Env, statements: D1PreparedStatement\[\]\)/);
  assert.match(src, /await env\.DB\.batch\(statements\.slice\(i, i \+ REFERENCE_BATCH_SIZE\)\)/);

  const restoreFn = sliceBetween(src, /async function restoreReferenceSnapshot/, /function compactWhitespace/);
  assert.match(restoreFn, /const restoreStatements: D1PreparedStatement\[\] = \[\]/);
  assert.match(restoreFn, /restoreStatements\.push\(/);
  assert.match(restoreFn, /await executeReferenceStatementBatches\(env, restoreStatements\)/);
  assert.doesNotMatch(restoreFn, /\.run\(\)/);

  const rebuildTryBlock = sliceBetween(src, /try \{\n\s+await clearReferenceTables\(env\);/, /\n\s+\} catch \(error\)/);
  assert.match(rebuildTryBlock, /const rebuildStatements: D1PreparedStatement\[\] = \[/);
  assert.match(rebuildTryBlock, /rebuildStatements\.push\(/);
  assert.match(rebuildTryBlock, /await executeReferenceStatementBatches\(env, rebuildStatements\)/);
  assert.doesNotMatch(rebuildTryBlock, /\.run\(\)/);
});
