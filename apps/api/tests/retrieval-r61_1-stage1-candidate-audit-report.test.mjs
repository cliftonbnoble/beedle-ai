import test from "node:test";
import assert from "node:assert/strict";
import { buildR61_1Stage1AuditReport } from "../scripts/retrieval-r61_1-stage1-candidate-audit-report.mjs";

test("R61.1 detects stage1 mapping failure when R60.8 had expected decisions but R61 stage1 has none", () => {
  const tasks = [
    { queryId: "q1", expectedDecisionIds: ["doc_a"], adoptedQuery: "analysis standard" },
    { queryId: "q2", expectedDecisionIds: ["doc_b"], adoptedQuery: "procedural history" }
  ];
  const normalizedRows = [
    { queryId: "q1", normalizedQuery: "analysis standard" },
    { queryId: "q2", normalizedQuery: "procedural history" }
  ];
  const r608Report = {
    emptyTasksAfter: 0,
    taskEvaluations: [
      {
        queryId: "q1",
        bestVariantType: "normalized",
        variantRows: [{ variantType: "normalized", topReturnedDecisionIds: ["doc_a"] }]
      },
      {
        queryId: "q2",
        bestVariantType: "normalized",
        variantRows: [{ variantType: "normalized", topReturnedDecisionIds: ["doc_b"] }]
      }
    ]
  };
  const r61Report = {
    taskRows: [
      { queryId: "q1", stage1DecisionCandidates: [] },
      { queryId: "q2", stage1DecisionCandidates: [] }
    ]
  };

  const report = buildR61_1Stage1AuditReport({ tasks, normalizedRows, r608Report, r61Report });
  assert.equal(report.phase, "R61.1");
  assert.equal(report.tasksEvaluated, 2);
  assert.equal(report.tasksWhereR60_8HadDecisionResults, 2);
  assert.equal(report.tasksWhereR61Stage1HadCandidates, 0);
  assert.equal(report.tasksWhereR60_8WorkedButR61Stage1Failed, 2);
  assert.equal(report.rootCauseClassification, "stage1_candidate_mapping_bug");
});
