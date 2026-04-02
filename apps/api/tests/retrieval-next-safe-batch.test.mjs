import test from "node:test";
import assert from "node:assert/strict";
import {
  evaluateNextSafeBatchGate,
  selectNextBatchCandidates
} from "../scripts/retrieval-batch-expansion-utils.mjs";

function mkRow(overrides = {}) {
  return {
    include: true,
    documentId: "doc_x",
    title: "Doc X",
    isLikelyFixture: false,
    corpusAdmissionStatus: "hold_for_repair_review",
    keyStats: {
      chunkTypeSpread: 3,
      chunksFlaggedMixedTopic: 0,
      usedFallbackChunking: false
    },
    preEnrichmentCanonicalAlignmentCount: 1,
    postEnrichmentCanonicalAlignmentCount: 3,
    ...overrides
  };
}

test("strict safe selection excludes previously-regressed family and low-signal dominated docs", () => {
  const rows = [
    mkRow({ documentId: "doc_good", title: "R12 Candidate Good" }),
    mkRow({ documentId: "doc_messy", title: "Retrieval Messy Headings mmix" }),
    mkRow({ documentId: "doc_low_signal", title: "Low Signal Candidate" })
  ];

  const profiles = new Map([
    ["doc_good", { lowSignalChunkShare: 0.1 }],
    ["doc_messy", { lowSignalChunkShare: 0.2 }],
    ["doc_low_signal", { lowSignalChunkShare: 0.9 }]
  ]);

  const selection = selectNextBatchCandidates({
    corpusAdmissionRows: rows,
    referenceEnrichmentRows: rows,
    rehearsalRows: [],
    trustedDocumentIds: [],
    batchSize: 5,
    documentChunkProfiles: profiles,
    excludeLowSignalDominated: true,
    lowSignalShareThreshold: 0.45
  });

  assert.deepEqual(selection.selected.map((row) => row.documentId), ["doc_good"]);
  const excluded = new Map(selection.excluded.map((row) => [row.documentId, row.exclusionReasons]));
  assert.ok((excluded.get("doc_messy") || []).includes("previous_regression_family_excluded"));
  assert.ok((excluded.get("doc_low_signal") || []).includes("low_signal_structural_dominated"));
});

test("strict safe gate fails on concentration or baseline regression", () => {
  const gate = evaluateNextSafeBatchGate({
    expandedSimSummary: {
      averageQualityScore: 64.7,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1,
      zeroTrustedResultQueryCount: 0,
      outOfCorpusHitQueryCount: 0
    },
    baselineTargetScore: 65.37,
    maxBaselineRegression: 0.5,
    baselineCitationTopDocumentShareAvg: 0.2,
    expandedCitationTopDocumentShareAvg: 0.3,
    baselineLowSignalShare: 0.1,
    expandedLowSignalShare: 0.12
  });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("qualityWithinBaselineTolerance"));
  assert.ok(gate.failures.includes("noCitationConcentrationIncrease"));
  assert.ok(gate.failures.includes("noLowSignalStructuralIncrease"));
});

