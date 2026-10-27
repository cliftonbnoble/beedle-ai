import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestionPagePath = path.resolve(process.cwd(), "src/app/admin/ingestion/page.tsx");

test("review forms use session identity fences for loading and mutations", async () => {
  const src = await fs.readFile(ingestionPagePath, "utf8");
  assert.match(src, /sessionRef.current.acceptLoad/);
  assert.match(src, /sessionRef.current.beginMutation\(form\)/);
  assert.match(src, /sessionRef.current.finishMutation\(ticket\)/);
  assert.match(src, /committed && stillCurrent/);
  assert.match(src, /updateIngestionMetadata\(ticket.documentId/);
  assert.match(src, /rejectIngestionDocument\(ticket.documentId/);
  assert.match(src, /<fieldset[^>]*disabled=\{!canMutate\}/);
  assert.doesNotMatch(src, /await loadDetail\(selectedId\)/);
});

// WEB-04: the list is refetched by a 26-dependency effect; typing must not fire a full 600-row request
// per keystroke, and an out-of-order response must not display rows for a stale filter combination.
test("ingestion admin list debounces typing and discards out-of-order refreshes", async () => {
  const src = await fs.readFile(ingestionPagePath, "utf8");

  // Typing is debounced: the request and the effect key off the mirror, not the live input value.
  assert.match(src, /const \[debouncedQueryText, setDebouncedQueryText\] = useState\(""\)/);
  assert.match(src, /setTimeout\(\(\) => setDebouncedQueryText\(queryText\), 300\)/);
  assert.match(src, /query: debouncedQueryText\.trim\(\) \|\| undefined/);
  assert.doesNotMatch(src, /query: queryText\.trim\(\)/);

  // List epoch: only the latest refresh applies its rows/summary.
  assert.match(src, /const listRequestRef = useRef\(0\)/);
  assert.match(src, /const requestId = \+\+listRequestRef\.current;[\s\S]*?const response = await listIngestionDocuments\(/);
  assert.match(src, /if \(listRequestRef\.current !== requestId\) return;\s*\n\s*setDocuments\(/);
});

// WEB-11: opening a document — the primary QC flow — must be keyboard-reachable, not row-onClick-only.
test("ingestion QC rows expose a keyboard-focusable open control", async () => {
  const src = await fs.readFile(ingestionPagePath, "utf8");
  assert.match(src, /<button\s*\n\s*type="button"\s*\n\s*aria-pressed=\{selectedId === doc\.id\}\s*\n\s*onClick=\{\(event\) => \{ event.stopPropagation\(\); selectDocument\(doc\.id\); \}\}/);
});
