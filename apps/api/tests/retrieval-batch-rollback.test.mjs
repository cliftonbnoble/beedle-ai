import test from "node:test";
import assert from "node:assert/strict";
import { buildStructuralChunkGuardReport, summarizeLowSignalPressure } from "../scripts/retrieval-batch-rollback-utils.mjs";

function qaReport(score, rows) {
  return {
    summary: {
      averageQualityScore: score
    },
    queryResults: rows
  };
}

test("low-signal structural pressure summary is deterministic", () => {
  const report = qaReport(55, [
    {
      queryId: "q1",
      query: "authority query",
      topResults: [
        { sectionLabel: "APPEARANCES" },
        { sectionLabel: "caption_title" },
        { sectionLabel: "analysis_reasoning" }
      ]
    }
  ]);

  const one = summarizeLowSignalPressure(report);
  const two = summarizeLowSignalPressure(report);
  assert.deepEqual(one, two);
  assert.equal(one.lowSignalTop10Hits, 2);
  assert.equal(one.top10Slots, 3);
});

test("structural guard report captures pre/post low-signal share deltas", () => {
  const pre = qaReport(55.37, [
    { queryId: "q1", query: "a", topResults: [{ sectionLabel: "APPEARANCES" }, { sectionLabel: "caption_title" }] },
    { queryId: "q2", query: "b", topResults: [{ sectionLabel: "issue_statement" }, { sectionLabel: "analysis_reasoning" }] }
  ]);
  const post = qaReport(65.37, [
    { queryId: "q1", query: "a", topResults: [{ sectionLabel: "analysis_reasoning" }, { sectionLabel: "authority_discussion" }] },
    { queryId: "q2", query: "b", topResults: [{ sectionLabel: "findings" }, { sectionLabel: "procedural_history" }] }
  ]);
  const rollbackReport = { summary: { rollbackVerificationPassed: true } };

  const result = buildStructuralChunkGuardReport({ preRollbackQa: pre, postRollbackQa: post, rollbackReport });
  assert.equal(result.summary.rollbackVerificationPassed, true);
  assert.ok(result.summary.preRollbackLowSignalTop10Share > result.summary.postRollbackLowSignalTop10Share);
  assert.ok(Array.isArray(result.guardLogic));
  assert.ok(result.guardLogic.length >= 3);
});

