import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_6EndpointTraceReport } from "../scripts/retrieval-r60_6-endpoint-trace-report.mjs";

test("R60.6 detects aggregation mismatch when reconciled rates differ from prior report", async () => {
  const rewrittenTasks = [
    {
      queryId: "q1",
      adoptedQuery: "rule 37.8",
      intent: "citation_direct",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["authority_discussion"]
    },
    {
      queryId: "q2",
      adoptedQuery: "findings",
      intent: "findings",
      expectedDecisionIds: ["doc_b"],
      expectedSectionTypes: ["findings"]
    }
  ];

  const report = await buildR60_6EndpointTraceReport({
    rewrittenTasks,
    trustedDecisionIds: ["doc_a", "doc_b"],
    priorRerunReport: {
      tasksStillEmptyCount: 2,
      top1DecisionHitRate: 0.5,
      top3DecisionHitRate: 0.5,
      top5DecisionHitRate: 0.5,
      sectionTypeHitRate: 0.5
    }
  });

  assert.equal(report.tasksEvaluated, 2);
  assert.equal(report.tasksClassifiedEmptyCount, 2);
  assert.equal(report.priorVsReconciledMismatch, true);
  assert.ok(["endpoint_response_shape_bug", "mixed_benchmark_pipeline_bug", "hit_rate_aggregation_bug"].includes(report.rootCauseClassification));
});

test("R60.6 reports no inconsistency when prior metrics match reconciled metrics", async () => {
  const rewrittenTasks = [
    {
      queryId: "q1",
      adoptedQuery: "x",
      intent: "analysis_reasoning",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["analysis_reasoning"]
    }
  ];

  const report = await buildR60_6EndpointTraceReport({
    rewrittenTasks,
    trustedDecisionIds: ["doc_a"],
    priorRerunReport: {
      tasksStillEmptyCount: 1,
      top1DecisionHitRate: 0,
      top3DecisionHitRate: 0,
      top5DecisionHitRate: 0,
      sectionTypeHitRate: 0
    }
  });

  assert.equal(report.reconciledTop1DecisionHitRate, 0);
  assert.equal(report.priorVsReconciledMismatch, false);
});
