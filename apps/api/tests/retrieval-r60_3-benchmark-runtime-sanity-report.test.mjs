import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_3RuntimeSanityReport } from "../scripts/retrieval-r60_3-benchmark-runtime-sanity-report.mjs";

test("R60.3 classifies endpoint behavior mismatch when all tasks fail fetch", async () => {
  const repairedTasks = [
    {
      queryId: "q1",
      query: "Rule 37.8",
      intent: "citation_direct",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["authority_discussion"]
    },
    {
      queryId: "q2",
      query: "findings",
      intent: "findings",
      expectedDecisionIds: ["doc_b"],
      expectedSectionTypes: ["findings"]
    }
  ];

  const out = await buildR60_3RuntimeSanityReport({
    repairedTasks,
    trustedDocumentIds: ["doc_a", "doc_b"],
    apiBaseUrl: "http://example.test",
    fetchFn: async () => ({ ok: false, status: 500, results: [] })
  });

  assert.equal(out.summary.tasksEvaluated, 2);
  assert.equal(out.summary.tasksWithRuntimeResultsCount, 0);
  assert.equal(out.summary.tasksStillEmptyCount, 2);
  assert.equal(out.overallCauseClassification, "endpoint_behavior_mismatch");
});

test("R60.3 reports no issue when runtime returns aligned decisions and section types", async () => {
  const repairedTasks = [
    {
      queryId: "q1",
      query: "Rule 37.8",
      intent: "citation_direct",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["authority_discussion"]
    }
  ];

  const out = await buildR60_3RuntimeSanityReport({
    repairedTasks,
    trustedDocumentIds: ["doc_a"],
    apiBaseUrl: "http://example.test",
    fetchFn: async () => ({
      ok: true,
      status: 200,
      results: [{ documentId: "doc_a", sectionLabel: "authority_discussion" }]
    })
  });

  assert.equal(out.summary.tasksWithRuntimeResultsCount, 1);
  assert.equal(out.tasksWithRuntimeResults.length, 1);
  assert.equal(out.overallCauseClassification, "no_issue_detected");
});
