import test from "node:test";
import assert from "node:assert/strict";
import { buildR55RemediationExperiment } from "../scripts/retrieval-r55-remediation-experiment-report.mjs";

test("R55 applies first R54 experiment and reports lower prediction error", () => {
  const r54 = {
    frozenFamilies: [
      "low_signal_absent::medium::analysis_reasoning+none",
      "low_signal_heavy::short::analysis_reasoning+holding_disposition"
    ],
    queryRegressionPatterns: [
      { queryId: "citation_rule_direct", qualityDelta: -4.2 },
      { queryId: "citation_ordinance_direct", qualityDelta: -3.8 }
    ],
    recommendedExperiments: [
      {
        experimentId: "r54_exp_01_predictor_error_budget",
        hypothesis: "family error budget"
      }
    ]
  };

  const r52 = {
    frozenFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
    candidateRows: [
      {
        documentId: "doc_b",
        simulatedQualityDelta: 11.55,
        actualQualityDeltaIfKnown: -0.8
      },
      {
        documentId: "doc_c",
        simulatedQualityDelta: 11.55,
        actualQualityDeltaIfKnown: null
      }
    ]
  };

  const r48 = {
    simulationVsRealityRows: [
      {
        documentId: "doc_a",
        documentFamilyLabel: "low_signal_heavy::short::analysis_reasoning+holding_disposition",
        simulatedQualityDelta: 11.96,
        actualKnownQualityDelta: -1.18
      }
    ]
  };

  const out = buildR55RemediationExperiment({ r54Report: r54, r52Report: r52, r48Report: r48 });

  assert.equal(out.phase, "R55");
  assert.equal(out.readOnly, true);
  assert.equal(out.experimentImplemented.experimentId, "r54_exp_01_predictor_error_budget");
  assert.ok(out.baselinePredictionError > out.remediatedPredictionError);
  assert.equal(out.frozenFamiliesEvaluated.length, 2);
  assert.equal(out.candidateOutcomeChanges.length, 3);
  assert.equal(out.recommendedNextStep, "proceed_to_second_dry_run_validation_phase_for_experiment_1");
});
