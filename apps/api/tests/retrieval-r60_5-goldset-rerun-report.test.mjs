import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_5RerunReport } from "../scripts/retrieval-r60_5-goldset-rerun-report.mjs";

test("R60.5 aggregates rewrite adoption rerun metrics and side-by-side comparison", () => {
  const originalTasks = [
    { queryId: "q1", query: "alpha" },
    { queryId: "q2", query: "beta" }
  ];
  const rewrittenTasks = [
    {
      queryId: "q1",
      originalQuery: "alpha",
      adoptedQuery: "alpha relaxed",
      rewriteAdopted: true
    },
    {
      queryId: "q2",
      originalQuery: "beta",
      adoptedQuery: "beta",
      rewriteAdopted: false
    }
  ];
  const rerunEval = {
    top1DecisionHitRate: 0.2,
    top3DecisionHitRate: 0.3,
    top5DecisionHitRate: 0.4,
    sectionTypeHitRate: 0.5,
    noisyChunkDominationRate: 0.1,
    intentBreakdown: [{ intent: "analysis", tasks: 2, top1DecisionHitRate: 0.2 }],
    falsePositiveChunkTypeCounts: [{ chunkType: "analysis_reasoning", count: 1 }],
    queryResults: [
      { queryId: "q1", trustedResultCount: 2 },
      { queryId: "q2", trustedResultCount: 0 }
    ]
  };
  const originalEval = {
    top1DecisionHitRate: 0,
    top3DecisionHitRate: 0,
    top5DecisionHitRate: 0,
    sectionTypeHitRate: 0,
    noisyChunkDominationRate: 0
  };

  const report = buildR60_5RerunReport({
    originalTasks,
    rewrittenTasks,
    rerunEval,
    originalEval
  });

  assert.equal(report.phase, "R60.5");
  assert.equal(report.rewrittenTaskCount, 2);
  assert.equal(report.rewriteAdoptedCount, 1);
  assert.deepEqual(report.tasksRecoveredByRewrite, ["q1"]);
  assert.deepEqual(report.tasksStillFailingAfterRewrite, ["q2"]);
  assert.equal(report.sideBySideComparisonVsOriginalR60.top1DecisionHitRate.delta, 0.2);
});
