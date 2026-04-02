import test from "node:test";
import assert from "node:assert/strict";
import { buildR57RemediationExperiment } from "../scripts/retrieval-r57-remediation-experiment-report.mjs";

test("R57 applies experiment 2 citation-intent penalty and keeps dry-run safety checks intact", () => {
  const r54 = {
    recommendedExperiments: [
      { experimentId: "r54_exp_01_predictor_error_budget" },
      { experimentId: "r54_exp_02_citation_intent_risk_feature" }
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
        projectedQualityDelta: 1.2,
        projectedCitationTopDocumentShare: 0.25,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        blockerFamilies: ["citation_concentration_above_effective_ceiling"],
        regressionSignals: ["query_regressed:citation_rule_direct"],
        keepOrDoNotActivate: "do_not_activate"
      },
      {
        documentId: "doc_b",
        title: "Doc B",
        documentFamilyLabel: "other_family",
        projectedQualityDelta: 0.9,
        projectedCitationTopDocumentShare: 0.1,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        blockerFamilies: ["quality_regression"],
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
    frozenFamilyLabel: "frozen_family",
    candidateRows: []
  };
  const r53 = { frozenFamilies: ["frozen_family"] };
  const r46 = { safeSingleCandidates: [{ documentId: "doc_a" }] };

  const out = buildR57RemediationExperiment({
    r54Report: r54,
    r39Report: r39,
    r48Report: r48,
    r52Report: r52,
    r53Report: r53,
    r46Report: r46
  });

  assert.equal(out.phase, "R57");
  assert.equal(out.experimentImplemented.experimentId, "r54_exp_02_citation_intent_risk_feature");
  assert.equal(out.summary.candidatesScanned, 2);
  assert.ok(out.baselinePredictionError > out.remediatedPredictionError);
  assert.equal(out.querySafetyChecks.provenanceCompletenessMaintained, true);
  assert.equal(out.querySafetyChecks.citationAnchorCoverageMaintained, true);
  assert.equal(out.querySafetyChecks.zeroTrustedResultQueriesMaintained, true);
  assert.equal(out.querySafetyChecks.outOfCorpusLeakageMaintained, true);
});

test("R57 recommends experiment 3 when no newly safe candidates emerge", () => {
  const r54 = {
    recommendedExperiments: [
      { experimentId: "r54_exp_01_predictor_error_budget" },
      { experimentId: "r54_exp_02_citation_intent_risk_feature" }
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
        documentId: "doc_x",
        title: "Doc X",
        documentFamilyLabel: "family_x",
        projectedQualityDelta: 0.1,
        projectedCitationTopDocumentShare: 0.3,
        projectedLowSignalStructuralShare: 0,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        blockerFamilies: ["citation_concentration_above_effective_ceiling"],
        regressionSignals: [],
        keepOrDoNotActivate: "do_not_activate"
      }
    ]
  };

  const out = buildR57RemediationExperiment({
    r54Report: r54,
    r39Report: r39,
    r48Report: { simulationVsRealityRows: [] },
    r52Report: { frozenFamilyLabel: "", candidateRows: [] },
    r53Report: { frozenFamilies: [] },
    r46Report: null
  });

  assert.equal(out.summary.newlySafeSimulatedCandidateCount, 0);
  assert.equal(out.recommendedNextStep, "no_safe_reopen_under_experiment_2_move_to_r54_experiment_3");
});
