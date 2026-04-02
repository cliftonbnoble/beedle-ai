import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { loadTrustedActivatedDocumentIds } from "../scripts/retrieval-live-search-qa-utils.mjs";

test("trusted-id loader prefers rollback state over stale batch-activation trustedAfter set", async () => {
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "retrieval-baseline-restoration-"));
  try {
    await fs.writeFile(
      path.join(tmp, "retrieval-batch-activation-report.json"),
      JSON.stringify({ manifests: { trustedAfterDocIds: ["doc_a", "doc_b", "doc_c"] } }, null, 2)
    );
    await fs.writeFile(
      path.join(tmp, "retrieval-batch-rollback-report.json"),
      JSON.stringify({ summary: { rollbackVerificationPassed: true, removedDocumentCount: 10 } }, null, 2)
    );
    await fs.writeFile(
      path.join(tmp, "retrieval-activation-write-report.json"),
      JSON.stringify({ documentsActivated: [{ documentId: "doc_a" }, { documentId: "doc_b" }] }, null, 2)
    );

    const loaded = await loadTrustedActivatedDocumentIds({ reportsDir: tmp });
    assert.deepEqual(loaded.trustedDocumentIds, ["doc_a", "doc_b"]);
    assert.ok(!loaded.sources.includes("batch_activation_report"));
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
});

test("citation lookup diversification cap remains strict", async () => {
  const src = await fs.readFile(path.resolve(process.cwd(), "src/services/search.ts"), "utf8");
  assert.match(src, /queryType === "rules_ordinance" \|\| queryType === "index_code" \? 1/);
  assert.match(src, /queryType === "citation_lookup" \? 2/);
  assert.match(src, /rerank = Math\.pow\(Math\.max\(0, rerank\), 0\.6\);/);
});
