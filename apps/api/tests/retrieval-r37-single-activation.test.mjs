import test from "node:test";
import assert from "node:assert/strict";
import { evaluateR37HardGate } from "../scripts/retrieval-r37-single-activation-write.mjs";

function mkQa({ quality = 69, citationShare = 0.1, lowSignal = 0.05, outOfCorpus = 0, zeroTrusted = 0, prov = 1, anchor = 1 }) {
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
      { queryId: "legal_standard", topResults: [{ chunkType: lowSignal > 0.05 ? "caption_title" : "analysis_reasoning" }, { chunkType: "analysis_reasoning" }] },
      { queryId: "procedural_history", topResults: [{ chunkType: lowSignal > 0.05 ? "issue_statement" : "procedural_history" }, { chunkType: "procedural_history" }] }
    ]
  };
}

test("R37 hard gate passes when all constraints are satisfied", () => {
  const before = mkQa({ quality: 69, citationShare: 0.125, lowSignal: 0.05 });
  const after = mkQa({ quality: 69.3, citationShare: 0.1, lowSignal: 0.05 });
  const gate = evaluateR37HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, true);
  assert.deepEqual(gate.failures, []);
  assert.equal(gate.checks.outOfCorpusHitQueryCountZero, true);
  assert.equal(gate.checks.provenanceCompletenessOne, true);
});

test("R37 hard gate fails when citation concentration breaches effective ceiling", () => {
  const before = mkQa({ quality: 69, citationShare: 0.1, lowSignal: 0.05 });
  const after = mkQa({ quality: 69.4, citationShare: 0.6, lowSignal: 0.05 });
  const gate = evaluateR37HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("citationTopDocumentShareAtOrBelowEffectiveCeiling"));
});

test("R37 hard gate fails on low-signal or provenance regressions", () => {
  const before = mkQa({ quality: 69, citationShare: 0.1, lowSignal: 0.05, prov: 1, anchor: 1 });
  const after = mkQa({ quality: 68.8, citationShare: 0.1, lowSignal: 0.2, prov: 0.9, anchor: 0.8 });
  const gate = evaluateR37HardGate({ beforeQa: before, afterQa: after });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("lowSignalStructuralShareNotWorsened"));
  assert.ok(gate.failures.includes("provenanceCompletenessOne"));
  assert.ok(gate.failures.includes("citationAnchorCoverageOne"));
});
