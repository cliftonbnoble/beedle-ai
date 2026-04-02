import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_4QueryRelaxationReport } from "../scripts/retrieval-r60_4-query-relaxation-report.mjs";

test("R60.4 recovers tasks via relaxation variants and emits rewrites", async () => {
  const repairedTasks = [
    {
      queryId: "q1",
      query: "ordinance 37.2 authority discussion for permit denial",
      intent: "authority_lookup",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["analysis_reasoning"]
    },
    {
      queryId: "q2",
      query: "findings of fact regarding witness credibility",
      intent: "findings",
      expectedDecisionIds: ["doc_b"],
      expectedSectionTypes: ["findings"]
    }
  ];

  const responses = {
    q1: {
      original_query: [],
      simplified_legal_phrase_query: [{ documentId: "doc_a", sectionLabel: "Body" }],
      citation_focused_query: [],
      keyword_compressed_query: [],
      section_intent_query: []
    },
    q2: {
      original_query: [],
      simplified_legal_phrase_query: [],
      citation_focused_query: [],
      keyword_compressed_query: [{ documentId: "doc_b", sectionLabel: "FINDINGS" }],
      section_intent_query: []
    }
  };

  const report = await buildR60_4QueryRelaxationReport({
    repairedTasks,
    trustedDecisionIds: ["doc_a", "doc_b"],
    apiBaseUrl: "http://example.test",
    fetchFn: async ({ task, variant }) => ({
      ok: true,
      status: 200,
      results: responses[task.queryId]?.[variant.variantType] || []
    })
  });

  assert.equal(report.summary.tasksEvaluated, 2);
  assert.equal(report.tasksRecoveredByRelaxationCount, 2);
  assert.equal(report.tasksStillEmptyAfterAllVariantsCount, 0);
  assert.equal(report.overallRecommendation, "adopt_relaxed_variant_rewrites_and_rerun_r60_goldset_eval");
  assert.equal(report.candidateBenchmarkRewrites.length, 2);
});

test("R60.4 flags all-empty outcomes with runtime/scope recommendation", async () => {
  const repairedTasks = [
    {
      queryId: "q1",
      query: "Rule 37.8",
      intent: "citation_direct",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["authority_discussion"]
    }
  ];

  const report = await buildR60_4QueryRelaxationReport({
    repairedTasks,
    trustedDecisionIds: ["doc_a"],
    apiBaseUrl: "http://example.test",
    fetchFn: async () => ({ ok: false, status: 500, results: [] })
  });

  assert.equal(report.tasksStillEmptyAfterAllVariantsCount, 1);
  assert.equal(report.overallRecommendation, "runtime_scope_or_endpoint_investigation_required_before_benchmark_use");
});
