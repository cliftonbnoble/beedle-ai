import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_7FailureClusteringReport } from "../scripts/retrieval-r60_7-failure-clustering-report.mjs";

test("R60.7 clusters tasks into required failure groups with bottleneck classification", () => {
  const traceReport = {
    perTaskRows: [
      {
        queryId: "gold_analysis_001",
        adoptedQuery: "analysis standard",
        parsedResultCount: 1,
        taskClassifiedEmpty: false,
        expectedDecisionIds: ["doc_a"],
        topReturnedDecisionIds: ["doc_a"],
        expectedSectionTypes: ["analysis_reasoning"],
        topReturnedSectionTypes: ["analysis_reasoning"]
      },
      {
        queryId: "gold_findings_001",
        adoptedQuery: "findings",
        parsedResultCount: 1,
        taskClassifiedEmpty: false,
        expectedDecisionIds: ["doc_b"],
        topReturnedDecisionIds: ["doc_x"],
        expectedSectionTypes: ["findings"],
        topReturnedSectionTypes: ["analysis_reasoning"]
      },
      {
        queryId: "gold_issue_disposition_001",
        adoptedQuery: "holding",
        parsedResultCount: 1,
        taskClassifiedEmpty: false,
        expectedDecisionIds: ["doc_c"],
        topReturnedDecisionIds: ["doc_c"],
        expectedSectionTypes: ["holding_disposition"],
        topReturnedSectionTypes: ["analysis_reasoning"]
      },
      {
        queryId: "gold_citation_001",
        adoptedQuery: "rule 37.8",
        parsedResultCount: 0,
        taskClassifiedEmpty: true,
        expectedDecisionIds: ["doc_d"],
        topReturnedDecisionIds: [],
        expectedSectionTypes: ["authority_discussion"],
        topReturnedSectionTypes: []
      }
    ]
  };

  const out = buildR60_7FailureClusteringReport(traceReport);
  assert.equal(out.phase, "R60.7");
  assert.equal(out.clusterCounts.recovered_and_correct, 1);
  assert.equal(out.clusterCounts.recovered_but_wrong_decision, 1);
  assert.equal(out.clusterCounts.recovered_but_wrong_section_type, 1);
  assert.equal(out.clusterCounts.empty_even_after_rewrite, 1);
  assert.ok(
    [
      "query_formulation_gap",
      "decision_retrieval_gap",
      "section_routing_gap",
      "mixed_retrieval_and_section_gap"
    ].includes(out.primarySystemBottleneck)
  );
});
