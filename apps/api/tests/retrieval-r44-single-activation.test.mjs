import test from "node:test";
import assert from "node:assert/strict";
import { evaluateR44HardGate } from "../scripts/retrieval-r44-single-activation-write.mjs";

function mkQa({
  quality = 69,
  citationShare = 0.1667,
  lowSignal = 0.05,
  outOfCorpus = 0,
  zeroTrusted = 0,
  prov = 1,
  anchor = 1
}) {
  return {
    summary: {
      averageQualityScore: quality,
      outOfCorpusHitQueryCount: outOfCorpus,
      zeroTrustedResultQueryCount: zeroTrusted,
      provenanceCompletenessAverage: prov,
      citationAnchorCoverageAverage: anchor
    },
    queryResults: [
      { queryId: "citation_rule_direct", metrics: { topDocumentShare: citationShare }, topResults: [{ documentId: "doc_a" }] },
      { queryId: "citation_ordinance_direct", metrics: { topDocumentShare: citationShare }, topResults: [{ documentId: "doc_b" }] },
      { queryId: "legal_standard", topResults: [{ chunkType: lowSignal > 0.05 ? "caption_title" : "analysis_reasoning" }] },
      { queryId: "procedural_history", topResults: [{ chunkType: lowSignal > 0.05 ? "issue_statement" : "procedural_history" }] }
    ]
  };
}

test("R44 hard gate passes when dynamic-floor citation and all safety checks pass", () => {
  const before = mkQa({ quality: 69, citationShare: 0.1667, lowSignal: 0.05 });
  const after = mkQa({ quality: 69.1, citationShare: 0.1667, lowSignal: 0.05 });
  const gate = evaluateR44HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, true);
  assert.deepEqual(gate.failures, []);
  assert.equal(gate.checks.citationTopDocumentShareAtOrBelowEffectiveCeiling, true);
});

test("R44 hard gate fails if quality regresses below floor", () => {
  const before = mkQa({ quality: 69, citationShare: 0.1667, lowSignal: 0.05 });
  const after = mkQa({ quality: 68.2, citationShare: 0.1667, lowSignal: 0.05 });
  const gate = evaluateR44HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("qualityNotMateriallyRegressed"));
});

test("R44 hard gate fails if low-signal worsens or provenance drops", () => {
  const before = mkQa({ quality: 69, citationShare: 0.1667, lowSignal: 0.05, prov: 1, anchor: 1 });
  const after = mkQa({ quality: 69.1, citationShare: 0.1667, lowSignal: 0.2, prov: 0.9, anchor: 0.8 });
  const gate = evaluateR44HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("lowSignalStructuralShareNotWorsened"));
  assert.ok(gate.failures.includes("provenanceCompletenessOne"));
  assert.ok(gate.failures.includes("citationAnchorCoverageOne"));
});

