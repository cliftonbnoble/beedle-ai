import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("FTS runtime-bootstrap retirement migration never scans or rewrites the full corpus", async () => {
  const sql = await fs.readFile(path.resolve(process.cwd(), "migrations/0010_search_fts_backfill.sql"), "utf8");

  assert.match(sql, /SELECT 1;/);
  assert.doesNotMatch(sql, /DELETE\s+FROM\s+search_chunks_fts/i);
  assert.doesNotMatch(sql, /INSERT\s+INTO\s+search_chunks_fts/i);
});
