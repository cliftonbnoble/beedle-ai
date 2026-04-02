import test from "node:test";
import assert from "node:assert/strict";
import { resolveCorrectedTrustedDocIds } from "../scripts/retrieval-r42-reference-lineage-fix-report.mjs";

test("resolveCorrectedTrustedDocIds prefers R36 baseline lineage when present", () => {
  const out = resolveCorrectedTrustedDocIds({
    r36Manifest: { baselineTrustedDocIds: ["doc_b", "doc_a"] },
    r27Manifest: { baselineTrustedDocIds: ["doc_x"] },
    r34Activation: { summary: { keepOrRollbackDecision: "keep_batch_active" }, docsActivatedExact: ["doc_y"] }
  });

  assert.deepEqual(out.correctedTrustedDocIds, ["doc_a", "doc_b"]);
  assert.deepEqual(out.lineageSourcesUsed, ["retrieval-r36-next-safe-single-manifest.json"]);
});

test("resolveCorrectedTrustedDocIds falls back to R27 + kept R34 activation", () => {
  const out = resolveCorrectedTrustedDocIds({
    r36Manifest: null,
    r27Manifest: { baselineTrustedDocIds: ["doc_a", "doc_b"] },
    r34Activation: { summary: { keepOrRollbackDecision: "keep_batch_active" }, docsActivatedExact: ["doc_c"] }
  });

  assert.deepEqual(out.correctedTrustedDocIds, ["doc_a", "doc_b", "doc_c"]);
  assert.ok(out.lineageSourcesUsed.includes("retrieval-r27-next-manifest.json"));
  assert.ok(out.lineageSourcesUsed.includes("retrieval-r34-gate-revision-activation-report.json"));
});

