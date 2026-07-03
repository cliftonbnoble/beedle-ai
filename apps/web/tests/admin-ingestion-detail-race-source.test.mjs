import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestionPagePath = path.resolve(process.cwd(), "src/app/admin/ingestion/page.tsx");

// WEB-03 guard: the QC detail pane must never apply an out-of-order response. Without the epoch guard,
// clicking doc A then doc B lets A's slower response fill the metadata form while `selectedId` is B —
// and approve/save-metadata act on `selectedId`, so an operator could approve B while reviewing A.
test("ingestion admin detail load discards superseded responses (no approve-wrong-doc race)", async () => {
  const src = await fs.readFile(ingestionPagePath, "utf8");

  assert.match(src, /const detailRequestRef = useRef\(0\)/);
  assert.match(src, /const requestId = \+\+detailRequestRef\.current/);
  assert.match(src, /const isCurrent = \(\) => detailRequestRef\.current === requestId/);
  // every state application after the await is gated on still being the latest request
  assert.match(src, /await getIngestionDocument\(documentId\);\s*\n\s*if \(!isCurrent\(\)\) return;/);
  assert.match(src, /catch \(err\) \{\s*\n\s*if \(!isCurrent\(\)\) return;/);
  assert.match(src, /finally \{\s*\n\s*if \(isCurrent\(\)\) setLoading\(false\);/);
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
  assert.match(src, /const requestId = \+\+listRequestRef\.current;\s*\n\s*const response = await listIngestionDocuments\(/);
  assert.match(src, /if \(listRequestRef\.current !== requestId\) return;\s*\n\s*setDocuments\(/);
});

// WEB-11: opening a document — the primary QC flow — must be keyboard-reachable, not row-onClick-only.
test("ingestion QC rows expose a keyboard-focusable open control", async () => {
  const src = await fs.readFile(ingestionPagePath, "utf8");
  assert.match(src, /<button\s*\n\s*type="button"\s*\n\s*onClick=\{\(\) => setSelectedId\(doc\.id\)\}/);
});
