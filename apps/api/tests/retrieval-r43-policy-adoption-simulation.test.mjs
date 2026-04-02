import test from "node:test";
import assert from "node:assert/strict";
import { analyzeR43PolicyAdoption } from "../scripts/retrieval-r43-policy-adoption-simulation-report.mjs";

test("analyzeR43PolicyAdoption computes newly eligible set and safeToProceed deterministically", () => {
  const out = analyzeR43PolicyAdoption({
    r39: {
      candidateRows: [
        {
          documentId: "doc_a",
          title: "A",
          documentFamilyLabel: "family_a",
          blockerFamilies: ["citation_concentration_above_effective_ceiling"],
          projectedAverageQualityScore: 69.2,
          projectedQualityDelta: 0.5,
          projectedCitationTopDocumentShare: 0.2,
          projectedLowSignalStructuralShare: 0,
          projectedOutOfCorpusHitQueryCount: 0,
          projectedZeroTrustedResultQueryCount: 0,
          projectedProvenanceCompletenessAverage: 1,
          projectedCitationAnchorCoverageAverage: 1
        },
        {
          documentId: "doc_b",
          title: "B",
          documentFamilyLabel: "family_b",
          blockerFamilies: ["citation_concentration_above_effective_ceiling", "low_signal_structural_share_increase"],
          projectedAverageQualityScore: 69.1,
          projectedQualityDelta: 0.2,
          projectedCitationTopDocumentShare: 0.2,
          projectedLowSignalStructuralShare: 0.1,
          projectedOutOfCorpusHitQueryCount: 0,
          projectedZeroTrustedResultQueryCount: 0,
          projectedProvenanceCompletenessAverage: 1,
          projectedCitationAnchorCoverageAverage: 1
        }
      ]
    },
    r42Fix: {
      correctedReference: { trustedDocumentCount: 33 },
      lineageSourcesUsed: ["retrieval-r36-next-safe-single-manifest.json"]
    },
    r42Rerun: {
      policyRows: [
        {
          policyId: "current_effective_policy",
          citationCeilingResolved: 0.1,
          currentlyBlockedCandidatesUnlockedCount: 0,
          baselinePasses: false,
          currentLivePasses: false
        },
        {
          policyId: "dynamic_floor",
          citationCeilingResolved: 0.2,
          currentlyBlockedCandidatesUnlockedCount: 1,
          baselinePasses: true,
          currentLivePasses: true
        }
      ]
    }
  });

  assert.equal(out.newlyEligibleCandidateCount, 1);
  assert.equal(out.stillBlockedCandidateCount, 1);
  assert.equal(out.newlyEligibleCandidates[0].documentId, "doc_a");
  assert.equal(out.recommendedNextStep, "safe_single_doc_activation_candidate:doc_a");
  assert.equal(out.safeToProceed, true);
});

