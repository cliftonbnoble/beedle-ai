import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10_6ContractRepairSummary } from "../scripts/retrieval-r60_10_6-contract-repair-report.mjs";

test("R60.10.6 summarizes rewired benchmark contract and comparison", () => {
  const summary = buildR60_10_6ContractRepairSummary({
    beforeR60_8: { tasksRecoveredByNormalizationCount: 1, emptyTasksAfter: 20, top1DecisionHitRate: 0.1, sectionTypeHitRate: 0 },
    beforeR60_10: { tasksRecoveredByWeightingCount: 0, emptyTasksAfter: 24, top1DecisionHitRate: 0 },
    beforeR60_10_1: {
      tasksWhereWeightingReducedRawResults: [],
      tasksWhereWeightingReducedParsedResults: [],
      tasksWhereWeightingChangedEndpointBehavior: ["q1"],
      rootCauseClassification: "runtime_endpoint_interpretation_mismatch"
    },
    afterR60_8: { tasksRecoveredByNormalizationCount: 2, emptyTasksAfter: 18, top1DecisionHitRate: 0.2, sectionTypeHitRate: 0.1 },
    afterR60_10: { tasksRecoveredByWeightingCount: 2, emptyTasksAfter: 18, top1DecisionHitRate: 0.2, top3DecisionHitRate: 0.2, top5DecisionHitRate: 0.2, sectionTypeHitRate: 0.1 },
    afterR60_10_1: {
      tasksWhereWeightingReducedRawResults: [],
      tasksWhereWeightingReducedParsedResults: [],
      tasksWhereWeightingChangedEndpointBehavior: [],
      rootCauseClassification: "overconstrained_query_text"
    }
  });

  assert.equal(summary.scriptsRewiredCount, 4);
  assert.equal(summary.contractMismatchResolved, true);
  assert.equal(summary.requestShapeAfter.all.includes("buildBenchmarkDebugPayload"), true);
  assert.equal(summary.comparison.r60_10.after.tasksRecoveredByWeightingCount, 2);
});
