import test from "node:test";
import assert from "node:assert/strict";
import { evaluateR46HardGate } from "../scripts/retrieval-r46-single-activation-write.mjs";

function mkQa({ quality = 69, citationShare = 0.2, lowSignal = 0.05, outOfCorpus = 0, zeroTrusted = 0, prov = 1, anchor = 1 }) {
  return {
    summary: {
      averageQualityScore: quality,
      outOfCorpusHitQueryCount: outOfCorpus,
      zeroTrustedResultQueryCount: zeroTrusted,
      provenanceCompletenessAverage: prov,
      citationAnchorCoverageAverage: anchor
    },
    queryResults: [
      { queryId: "citation_rule_direct", metrics: { topDocumentShare: citationShare }, topResults: [{ documentId: "doc_a" }, { documentId: "doc_b" }] },
      { queryId: "citation_ordinance_direct", metrics: { topDocumentShare: citationShare }, topResults: [{ documentId: "doc_c" }, { documentId: "doc_d" }] },
      { queryId: "analysis", topResults: [{ chunkType: lowSignal > 0.05 ? "caption_title" : "analysis_reasoning" }, { chunkType: "analysis_reasoning" }] },
      { queryId: "procedural", topResults: [{ chunkType: lowSignal > 0.05 ? "issue_statement" : "procedural_history" }, { chunkType: "procedural_history" }] }
    ]
  };
}

test("R46 hard gate passes when all checks are satisfied", () => {
  const before = mkQa({ quality: 69, citationShare: 0.2, lowSignal: 0.05 });
  const after = mkQa({ quality: 69.3, citationShare: 0.2, lowSignal: 0.05 });
  const gate = evaluateR46HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, true);
  assert.deepEqual(gate.failures, []);
});

test("R46 hard gate fails on quality regression", () => {
  const before = mkQa({ quality: 69, citationShare: 0.2, lowSignal: 0.05 });
  const after = mkQa({ quality: 67.5, citationShare: 0.2, lowSignal: 0.05 });
  const gate = evaluateR46HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("qualityNotMateriallyRegressed"));
});

test("R46 hard gate fails on low-signal/provenance regressions", () => {
  const before = mkQa({ quality: 69, citationShare: 0.2, lowSignal: 0.05, prov: 1, anchor: 1 });
  const after = mkQa({ quality: 69, citationShare: 0.2, lowSignal: 0.2, prov: 0.9, anchor: 0.8 });
  const gate = evaluateR46HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("lowSignalStructuralShareNotWorsened"));
  assert.ok(gate.failures.includes("provenanceCompletenessOne"));
  assert.ok(gate.failures.includes("citationAnchorCoverageOne"));
});
