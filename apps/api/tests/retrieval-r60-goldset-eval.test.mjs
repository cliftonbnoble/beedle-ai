import test from "node:test";
import assert from "node:assert/strict";
import { R60_GOLDSET_TASKS, runR60GoldsetEvaluation } from "../scripts/retrieval-r60-goldset-eval-utils.mjs";

test("R60 gold-set contains 20-30 deterministic judicial tasks with required fields", () => {
  assert.ok(R60_GOLDSET_TASKS.length >= 20 && R60_GOLDSET_TASKS.length <= 30);
  for (const row of R60_GOLDSET_TASKS) {
    assert.equal(typeof row.queryId, "string");
    assert.equal(typeof row.query, "string");
    assert.equal(typeof row.intent, "string");
    assert.ok(Array.isArray(row.expectedDecisionIds));
    assert.ok(Array.isArray(row.expectedSectionTypes));
    assert.equal(typeof row.minimumAcceptableRank, "number");
    assert.equal(typeof row.notes, "string");
  }
});

test("R60 evaluation computes required benchmark metrics deterministically", async () => {
  const tasks = [
    {
      queryId: "t1",
      query: "Rule 37.8",
      intent: "citation_direct",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["authority_discussion"],
      minimumAcceptableRank: 5,
      notes: "citation task"
    },
    {
      queryId: "t2",
      query: "findings credibility",
      intent: "findings",
      expectedDecisionIds: ["doc_b"],
      expectedSectionTypes: ["findings"],
      minimumAcceptableRank: 5,
      notes: "findings task"
    }
  ];

  const byQuery = {
    "Rule 37.8": [
      {
        documentId: "doc_a",
        title: "A",
        chunkId: "c1",
        sectionLabel: "authority_discussion",
        diagnostics: { rerankScore: 1 },
        citationAnchor: "x#1",
        sourceLink: "/a"
      }
    ],
    "findings credibility": [
      {
        documentId: "doc_b",
        title: "B",
        chunkId: "c2",
        sectionLabel: "analysis_reasoning",
        diagnostics: { rerankScore: 1 },
        citationAnchor: "y#1",
        sourceLink: "/b"
      }
    ]
  };

  const report = await runR60GoldsetEvaluation({
    apiBase: "http://example.test",
    reportsDir: new URL("../../reports/", import.meta.url).pathname,
    tasks,
    limit: 5,
    trustedDocumentIdsOverride: ["doc_a", "doc_b"],
    fetchSearchDebug: async (payload) => ({ results: byQuery[payload.query] || [], total: (byQuery[payload.query] || []).length })
  });

  assert.equal(typeof report.top1DecisionHitRate, "number");
  assert.equal(typeof report.top3DecisionHitRate, "number");
  assert.equal(typeof report.top5DecisionHitRate, "number");
  assert.equal(typeof report.sectionTypeHitRate, "number");
  assert.ok(Array.isArray(report.intentBreakdown));
  assert.ok(Array.isArray(report.falsePositiveChunkTypeCounts));
  assert.equal(typeof report.noisyChunkDominationRate, "number");
  assert.equal(report.queryResults.length, 2);
});
