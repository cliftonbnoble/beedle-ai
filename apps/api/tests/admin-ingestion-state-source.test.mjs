import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const src = fs.readFileSync(new URL("../src/services/admin-ingestion.ts", import.meta.url), "utf8");

function sliceBetween(source, startPattern, endPattern) {
  const start = source.search(startPattern);
  assert.notEqual(start, -1, `Missing start pattern ${startPattern}`);
  const end = source.slice(start + 1).search(endPattern);
  assert.notEqual(end, -1, `Missing end pattern ${endPattern}`);
  return source.slice(start, start + 1 + end);
}

test("rejecting an ingestion document clears approved and searchable state together", () => {
  const rejectFn = sliceBetween(src, /export async function rejectIngestionDocument/, /export async function approveIngestionDocument/);

  assert.match(rejectFn, /SET rejected_at = \?, rejected_reason = \?, approved_at = NULL, searchable_at = NULL, updated_at = \?/);
  assert.match(rejectFn, /\.bind\(now, parsed\.reason, now, documentId\)/);
});
