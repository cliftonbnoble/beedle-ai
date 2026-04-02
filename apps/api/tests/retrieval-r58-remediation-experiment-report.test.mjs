import test from "node:test";
import assert from "node:assert/strict";
import { buildR58RemediationExperiment } from "../scripts/retrieval-r58-remediation-experiment-report.mjs";

test("R58 applies experiment 3 insertion-effect penalty and can recommend redesign when no candidates reopen", () => {
  const r54 = {
    recommendedExperiments: [
      { experimentId: "r54_exp_01_predictor_error_budget" },
      { experimentId: "r54_exp_02_citation_intent_risk_feature" },
      { experimentId: "r54_exp_03_insertion_effect_probe" }
    ],
    frozenFamilies: ["frozen_family"]
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
        documentId: "doc_a",
        title: "Doc A",
        documentFamilyLabel: "frozen_family",
        projectedQualityDelta: 0.8,
        projectedCitationTopDocumentShare: 0.2,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        keepOrDoNotActivate: "do_not_activate"
      },
      {
        documentId: "doc_b",
        title: "Doc B",
        documentFamilyLabel: "other_family",
        projectedQualityDelta: 0.6,
        projectedCitationTopDocumentShare: 0.15,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        keepOrDoNotActivate: "do_not_activate"
      }
    ]
  };
  const r48 = {
    simulationVsRealityRows: [
      {
        documentId: "doc_a",
        documentFamilyLabel: "frozen_family",
        simulatedQualityDelta: 11.96,
        actualKnownQualityDelta: -1.18
      }
    ]
  };
  const r52 = {
    frozenFamilyLabel: "other_family",
    candidateRows: [
      {
        documentId: "doc_b",
        simulatedQualityDelta: 11.55,
        actualQualityDeltaIfKnown: -0.8
      }
    ]
  };
  const r53 = { frozenFamilies: ["frozen_family"] };

  const out = buildR58RemediationExperiment({
    r54Report: r54,
    r39Report: r39,
    r48Report: r48,
    r52Report: r52,
    r53Report: r53
  });

  assert.equal(out.phase, "R58");
  assert.equal(out.experimentImplemented.experimentId, "r54_exp_03_insertion_effect_probe");
  assert.ok(out.baselinePredictionError > out.remediatedPredictionError);
  assert.equal(out.summary.newlySafeSimulatedCandidateCount, 0);
  assert.equal(out.recommendedNextStep, "stop_activation_work_and_move_to_model_ranking_redesign");
  assert.equal(out.querySafetyChecks.citationConcentrationNoRegression, true);
  assert.equal(out.querySafetyChecks.lowSignalShareNoRegression, true);
  assert.equal(out.querySafetyChecks.provenanceCompletenessMaintained, true);
  assert.equal(out.querySafetyChecks.citationAnchorCoverageMaintained, true);
  assert.equal(out.querySafetyChecks.zeroTrustedResultQueriesMaintained, true);
  assert.equal(out.querySafetyChecks.outOfCorpusLeakageMaintained, true);
});

test("R58 recommends dry-run rehearsal when a candidate reopens under insertion model", () => {
  const r54 = {
    recommendedExperiments: [
      { experimentId: "r54_exp_01_predictor_error_budget" },
      { experimentId: "r54_exp_02_citation_intent_risk_feature" },
      { experimentId: "r54_exp_03_insertion_effect_probe" }
    ],
    frozenFamilies: []
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
        documentId: "doc_open",
        title: "Open",
        documentFamilyLabel: "family_open",
        projectedQualityDelta: 0.75,
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
  const r48 = {
    simulationVsRealityRows: [
      {
        documentId: "doc_known",
        documentFamilyLabel: "family_open",
        simulatedQualityDelta: 1.0,
        actualKnownQualityDelta: 0.9
      }
    ]
  };
  const out = buildR58RemediationExperiment({
    r54Report: r54,
    r39Report: r39,
    r48Report: r48,
    r52Report: { frozenFamilyLabel: "", candidateRows: [] },
    r53Report: { frozenFamilies: [] }
  });

  assert.equal(out.summary.newlySafeSimulatedCandidateCount, 1);
  assert.equal(out.recommendedNextStep, "proceed_to_controlled_dry_run_activation_rehearsal_for_top_remediated_candidate");
});
