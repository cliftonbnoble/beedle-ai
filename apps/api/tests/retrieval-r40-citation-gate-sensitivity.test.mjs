import test from "node:test";
import assert from "node:assert/strict";
import {
  analyzeR40Sensitivity,
  evaluateReferenceAgainstPolicy
} from "../scripts/retrieval-r40-citation-gate-sensitivity-report.mjs";

test("evaluateReferenceAgainstPolicy enforces non-citation gates deterministically", () => {
  const out = evaluateReferenceAgainstPolicy({
    reference: {
      averageQualityScore: 69,
      citationTopDocumentShare: 0.2,
      lowSignalStructuralShare: 0,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    thresholdReference: {
      qualityFloor: 64.72,
      lowSignalStructuralShareCeiling: 0
    },
    citationCeilingResolved: 0.2
  });

  assert.equal(out.passes, true);
  assert.deepEqual(out.failures, []);
});

test("analyzeR40Sensitivity computes unlock counts and policy risks deterministically", () => {
  const r38 = {
    summary: { effectiveCitationCeiling: 0.1 },
    dataMode: "live",
    baselineLiveMetrics: {
      averageQualityScore: 69,
      citationTopDocumentShare: 0.2,
      lowSignalStructuralShare: 0,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    }
  };
  const r39 = {
    candidatesScanned: 3,
    candidateRows: [
      {
        documentId: "doc_a",
        title: "A",
        keepOrDoNotActivate: "do_not_activate",
        failingGates: ["citationTopDocumentShareAtOrBelowEffectiveCeiling"],
        projectedAverageQualityScore: 69.2,
        projectedCitationTopDocumentShare: 0.2,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        documentFamilyLabel: "family_a"
      },
      {
        documentId: "doc_b",
        title: "B",
        keepOrDoNotActivate: "do_not_activate",
        failingGates: [
          "citationTopDocumentShareAtOrBelowEffectiveCeiling",
          "lowSignalStructuralShareNotWorsened"
        ],
        projectedAverageQualityScore: 69.1,
        projectedCitationTopDocumentShare: 0.2,
        projectedLowSignalStructuralShare: 0.05,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        documentFamilyLabel: "family_b"
      },
      {
        documentId: "doc_c",
        title: "C",
        keepOrDoNotActivate: "do_not_activate",
        failingGates: ["citationTopDocumentShareAtOrBelowEffectiveCeiling"],
        projectedAverageQualityScore: 69.0,
        projectedCitationTopDocumentShare: 0.22,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        documentFamilyLabel: "family_c"
      }
    ],
    nearMissCandidates: [{ documentId: "doc_a" }, { documentId: "doc_c" }]
  };
  const r34Live = {
    summary: {
      after: {
        averageQualityScore: 69,
        outOfCorpusHitQueryCount: 0,
        zeroTrustedResultQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1
      },
      hardGate: {
        measured: {
          citationTopDocumentShare: 0.1667,
          afterLowSignalStructuralShare: 0
        },
        thresholds: {
          qualityFloor: 64.72,
          lowSignalStructuralShareCeiling: 0,
          citationTopDocumentShareCeilingConfigured: 0.1,
          citationTopDocumentShareCeilingAttainableFloor: 0.2
        }
      }
    }
  };

  const out = analyzeR40Sensitivity({ r38, r39, r34Live, r34Report: null });

  assert.equal(out.policiesEvaluated.length, 7);
  const fixed20 = out.policyRows.find((row) => row.policyId === "fixed_0_20");
  assert.equal(fixed20.currentlyBlockedCandidatesUnlockedCount, 1);
  assert.equal(fixed20.nearMissCandidatesUnlockedCount, 1);
  assert.equal(fixed20.candidatesStillBlockedByLowSignalCount, 1);
  assert.equal(out.recommendedPolicy, "dynamic_floor_plus_0_02");
});
