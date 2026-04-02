import test from "node:test";
import assert from "node:assert/strict";
import { evaluateR46HardGate } from "../scripts/retrieval-r46-single-activation-write.mjs";

function mkQa({ quality = 69.02, citationShare = 0.2, lowSignal = 0, outOfCorpus = 0, zeroTrusted = 0, prov = 1, anchor = 1 }) {
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
      { queryId: "analysis", topResults: [{ chunkType: lowSignal > 0 ? "caption_title" : "analysis_reasoning" }, { chunkType: "analysis_reasoning" }] },
      { queryId: "procedural", topResults: [{ chunkType: lowSignal > 0 ? "issue_statement" : "procedural_history" }, { chunkType: "procedural_history" }] }
    ]
  };
}

test("R47 hard gate baseline pass scenario", () => {
  const before = mkQa({ quality: 69.02, citationShare: 0.2, lowSignal: 0 });
  const after = mkQa({ quality: 69.1, citationShare: 0.2, lowSignal: 0 });
  const gate = evaluateR46HardGate({ beforeQa: before, afterQa: after });
  assert.equal(gate.passed, true);
  assert.deepEqual(gate.failures, []);
});

test("R47 hard gate catches citation and quality failures", () => {
  const before = mkQa({ quality: 69.02, citationShare: 0.2, lowSignal: 0 });
  const after = mkQa({ quality: 68.1, citationShare: 0.7, lowSignal: 0 });
  const gate = evaluateR46HardGate({ beforeQa: before, afterQa: after });
  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("qualityNotMateriallyRegressed"));
  assert.ok(gate.failures.includes("citationTopDocumentShareAtOrBelowEffectiveCeiling"));
});
