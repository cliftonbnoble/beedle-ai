import test from "node:test";
import assert from "node:assert/strict";
import { buildR56RemediationValidation } from "../scripts/retrieval-r56-remediation-validation-report.mjs";

test("R56 validates experiment 1 and recommends rehearsal when remediated safe candidate appears", () => {
  const r55 = {
    experimentImplemented: { experimentId: "r54_exp_01_predictor_error_budget" },
    baselinePredictionError: 12.7,
    remediatedPredictionError: 0,
    familyRiskPatternChanges: [{ familyLabel: "frozen_family_a", appliedErrorBudget: 2 }]
  };

  const r39 = {
    baselineLiveMetrics: {
      averageQualityScore: 69,
      effectiveCitationCeiling: 0.2,
      lowSignalStructuralShare: 0
    },
    safeCandidateCount: 0,
    blockedCandidateCount: 2,
    candidateRows: [
      {
        documentId: "doc_safe",
        title: "Safe Doc",
        documentFamilyLabel: "other_family",
        projectedQualityDelta: 1.2,
        projectedCitationTopDocumentShare: 0.1,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        keepOrDoNotActivate: "do_not_activate"
      },
      {
        documentId: "doc_frozen",
        title: "Frozen Doc",
        documentFamilyLabel: "frozen_family_a",
        projectedQualityDelta: 1.2,
        projectedCitationTopDocumentShare: 0.1,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        keepOrDoNotActivate: "do_not_activate"
      }
    ]
  };

  const r53 = {
    frozenFamilies: ["frozen_family_a"]
  };

  const out = buildR56RemediationValidation({ r55Report: r55, r39Report: r39, r53Report: r53 });
  assert.equal(out.phase, "R56");
  assert.equal(out.summary.newlySafeSimulatedCandidateCount, 1);
  assert.equal(out.newlySafeSimulatedCandidates[0].documentId, "doc_safe");
  assert.equal(out.querySafetyChecks.citationConcentrationNoRegression, true);
  assert.equal(out.recommendedNextStep, "proceed_to_controlled_dry_run_activation_rehearsal_for_top_remediated_candidate");
});

test("R56 recommends next experiment when no candidate is newly safe", () => {
  const r55 = {
    experimentImplemented: { experimentId: "r54_exp_01_predictor_error_budget" },
    baselinePredictionError: 12.7,
    remediatedPredictionError: 9.3,
    familyRiskPatternChanges: [{ familyLabel: "family_a", appliedErrorBudget: 5 }]
  };
  const r39 = {
    baselineLiveMetrics: {
      averageQualityScore: 69,
      effectiveCitationCeiling: 0.2,
      lowSignalStructuralShare: 0
    },
    safeCandidateCount: 0,
    blockedCandidateCount: 1,
    candidateRows: [
      {
        documentId: "doc_x",
        title: "Doc X",
        documentFamilyLabel: "family_a",
        projectedQualityDelta: 0.1,
        projectedCitationTopDocumentShare: 0.1,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        keepOrDoNotActivate: "do_not_activate"
      }
    ]
  };
  const r53 = { frozenFamilies: ["family_a"] };

  const out = buildR56RemediationValidation({ r55Report: r55, r39Report: r39, r53Report: r53 });
  assert.equal(out.summary.newlySafeSimulatedCandidateCount, 0);
  assert.equal(out.recommendedNextStep, "no_safe_reopen_under_experiment_1_try_next_r54_experiment");
});
