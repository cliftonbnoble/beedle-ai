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
