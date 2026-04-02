import test from "node:test";
import assert from "node:assert/strict";
import { buildR54RemediationPlan } from "../scripts/retrieval-r54-remediation-plan-report.mjs";

test("R54 builds deterministic remediation plan with frozen families and experiments", () => {
  const r48 = {
    simulationVsRealityRows: [
      {
        documentId: "doc_a",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
        qualityPredictionError: 13.14,
        actualKnownQualityDelta: -1.18
      },
      {
        documentId: "doc_b",
        documentFamilyLabel: "low_signal_heavy::short::analysis_reasoning+holding_disposition",
        qualityPredictionError: 8.2,
        actualKnownQualityDelta: -0.9
      }
    ]
  };

  const r52 = {
    frozenFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
    familyRiskFactors: [
      "query_level_regressions_hidden_by_average_projection",
      "analysis_reasoning_monoculture"
    ],
    queryLevelRegressionBreakdown: [
      {
        queryId: "citation_rule_direct",
        query: "Rule citation",
        qualityDelta: -4.33,
        activatedDocTop10Hits: 2,
        topDocumentShareBefore: 0.1,
        topDocumentShareAfter: 0.2
      },
      {
        queryId: "citation_ordinance_direct",
        query: "Ordinance citation",
        qualityDelta: -3.2,
        activatedDocTop10Hits: 2,
        topDocumentShareBefore: 0.1,
        topDocumentShareAfter: 0.2
      }
    ]
  };

  const r53 = {
    frozenFamilies: ["low_signal_heavy::short::analysis_reasoning+holding_disposition"],
    guardrailHitCounts: { hasCitationIntentSensitivityRisk: 9 }
  };

  const activationReports = [
    {
      docActivatedExact: "doc_a",
      keepOrRollbackDecision: "rollback_batch",
      beforeLiveMetrics: { averageQualityScore: 69.02 },
      afterLiveMetrics: { averageQualityScore: 67.84 },
      anomalyFlags: ["hard_gate_failed", "qualityNotMateriallyRegressed"]
    },
    {
      docActivatedExact: "doc_x",
      keepOrRollbackDecision: "keep_batch_active",
      beforeLiveMetrics: { averageQualityScore: 69.02 },
      afterLiveMetrics: { averageQualityScore: 69.5 },
      anomalyFlags: []
    }
  ];

  const out = buildR54RemediationPlan({ r48Report: r48, r52Report: r52, r53Report: r53, activationReports });

  assert.equal(out.phase, "R54");
  assert.equal(out.readOnly, true);
  assert.deepEqual(out.frozenFamilies, [
    "low_signal_absent::medium::analysis_reasoning+none",
    "low_signal_heavy::short::analysis_reasoning+holding_disposition"
  ]);
  assert.equal(out.knownFailedActivations.length, 1);
  assert.equal(out.knownFailedActivations[0].docActivatedExact, "doc_a");
  assert.equal(out.queryRegressionPatterns.length, 2);
  assert.equal(out.recommendedExperiments.length, 3);
  assert.equal(
    out.recommendedNextStep,
    "stop_all_activations_until_at_least_one_remediation_experiment_is_implemented_and_revalidated"
  );
});
