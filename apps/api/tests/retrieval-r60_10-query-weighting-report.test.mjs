import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10QueryWeightingReport } from "../scripts/retrieval-r60_10-query-weighting-report.mjs";

test("R60.10 builds weighted templates and reports before/after metrics", async () => {
  const rewrittenTasks = [
    {
      queryId: "q1",
      originalQuery: "procedural history hearing notice",
      adoptedQuery: "procedural history hearing notice",
      intent: "procedural_history",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["procedural_history"]
    },
    {
      queryId: "q2",
      originalQuery: "findings credibility evidence",
      adoptedQuery: "findings credibility evidence",
      intent: "findings",
      expectedDecisionIds: ["doc_b"],
      expectedSectionTypes: ["findings"]
    }
  ];

  const normalizedPackages = [
    {
      queryId: "q1",
      normalizedQuery: "procedural history hearing notice",
      compressedKeywordQuery: "procedural hearing notice",
      citationFocusedQuery: "",
      proceduralQuery: "procedural history hearing notice continuance due process",
      findingsCredibilityQuery: "",
      dispositionQuery: ""
    },
    {
      queryId: "q2",
      normalizedQuery: "findings credibility evidence",
      compressedKeywordQuery: "findings credibility evidence",
      citationFocusedQuery: "",
      proceduralQuery: "",
      findingsCredibilityQuery: "findings of fact credibility witness evidence weight",
      dispositionQuery: ""
    }
  ];

  const r60_8Report = {
    emptyTasksAfter: 2,
    top1DecisionHitRate: 0,
    top3DecisionHitRate: 0,
    top5DecisionHitRate: 0,
    sectionTypeHitRate: 0,
    taskEvaluations: [
      { queryId: "q1", bestDecisionHit: false },
      { queryId: "q2", bestDecisionHit: false }
    ]
  };
  const r60_7Report = {
    clusterCounts: { empty_even_after_rewrite: 2 }
  };

  global.fetch = async (_url, opts) => {
    const body = JSON.parse(opts.body);
    if (String(body.query).includes("procedural")) {
      return {
        ok: true,
        async text() {
          return JSON.stringify({ results: [{ documentId: "doc_a", sectionLabel: "procedural_history" }] });
        }
      };
    }
    if (String(body.query).includes("findings")) {
      return {
        ok: true,
        async text() {
          return JSON.stringify({ results: [{ documentId: "doc_b", sectionLabel: "FINDINGS" }] });
        }
      };
    }
    return { ok: true, async text() { return JSON.stringify({ results: [] }); } };
  };

  const out = await buildR60_10QueryWeightingReport({
    rewrittenTasks,
    normalizedPackages,
    r60_8Report,
    r60_7Report,
    trustedDecisionIds: ["doc_a", "doc_b"],
    apiBaseUrl: "http://example.test"
  });

  assert.equal(out.phase, "R60.10");
  assert.equal(out.tasksEvaluated, 2);
  assert.equal(out.tasksRecoveredByWeightingCount, 2);
  assert.equal(out.emptyTasksAfter, 0);
  assert.equal(out.top1DecisionHitRate, 1);
  assert.equal(out.proceduralIntentHitRateAfter, 1);
  assert.equal(out.weightedEntries.length, 2);
});
