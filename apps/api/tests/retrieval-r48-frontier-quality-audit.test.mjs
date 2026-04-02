import test from "node:test";
import assert from "node:assert/strict";
import { buildR48Audit } from "../scripts/retrieval-r48-frontier-quality-audit-report.mjs";

test("R48 computes misprediction and recommends do-not-activate when no trustworthy class remains", () => {
  const r46 = {
    candidatesScanned: 3,
    averageQualityScore: 69.02,
    effectiveCitationCeiling: 0.2,
    safeSingleCandidates: [
      {
        documentId: "doc_a",
        title: "A",
        projectedAverageQualityScore: 69.4,
        projectedQualityDelta: 0.38,
        projectedCitationTopDocumentShare: 0.2,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        blockerFamilies: [],
        improvementSignals: ["quality_improved"],
        regressionSignals: []
      }
    ]
  };

  const r47 = {
    docActivatedExact: "doc_a",
    keepOrRollbackDecision: "rollback_batch",
    beforeLiveMetrics: { averageQualityScore: 69.02 },
    afterLiveMetrics: { averageQualityScore: 67.84 },
    hardGate: { failures: ["qualityNotMateriallyRegressed"] },
    anomalyFlags: ["hard_gate_failed"]
  };

  const profiles = new Map([
    [
      "doc_a",
      {
        familyLabel: "low_signal_absent::short::analysis_reasoning+authority_discussion",
        lowSignalChunkShare: 0,
        chunkTypeCounts: [{ key: "analysis_reasoning", count: 5 }],
        sectionLabelCounts: [{ key: "analysis_reasoning", count: 5 }]
      }
    ]
  ]);

  const out = buildR48Audit({ r46Report: r46, r47Report: r47, profilesByDocId: profiles });

  assert.equal(out.qualityMispredictionCount, 1);
  assert.equal(out.candidatesWithKnownRealOutcome, 1);
  assert.equal(out.activationRecommendation, "no");
  assert.equal(out.recommendedNextStep, "do_not_activate_any_more_singles");
});

test("R48 returns next best candidate when at least one lower-risk row qualifies", () => {
  const r46 = {
    candidatesScanned: 4,
    averageQualityScore: 69.02,
    effectiveCitationCeiling: 0.2,
    safeSingleCandidates: [
      {
        documentId: "doc_a",
        projectedAverageQualityScore: 69.6,
        projectedQualityDelta: 0.58,
        projectedCitationTopDocumentShare: 0.18,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        blockerFamilies: [],
        improvementSignals: ["quality_improved"],
        regressionSignals: []
      },
      {
        documentId: "doc_b",
        projectedAverageQualityScore: 70,
        projectedQualityDelta: 0.98,
        projectedCitationTopDocumentShare: 0.12,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        blockerFamilies: [],
        improvementSignals: ["quality_improved"],
        regressionSignals: []
      }
    ]
  };

  const profiles = new Map([
    ["doc_a", { familyLabel: "low_signal_absent::short::analysis_reasoning+authority_discussion", lowSignalChunkShare: 0, chunkTypeCounts: [], sectionLabelCounts: [] }],
    ["doc_b", { familyLabel: "low_signal_absent::short::analysis_reasoning+authority_discussion", lowSignalChunkShare: 0, chunkTypeCounts: [], sectionLabelCounts: [] }]
  ]);

  const out = buildR48Audit({ r46Report: r46, r47Report: null, profilesByDocId: profiles });

  assert.equal(out.activationRecommendation, "yes");
  assert.equal(out.recommendedNextStep, "safe_single_doc_activation_candidate:doc_b");
  assert.equal(out.nextBestCandidateIfAny.documentId, "doc_b");
});
