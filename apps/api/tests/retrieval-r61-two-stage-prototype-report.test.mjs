import test from "node:test";
import assert from "node:assert/strict";
import { buildR61TwoStagePrototypeReport } from "../scripts/retrieval-r61-two-stage-prototype-report.mjs";

test("R61 uses normalized query frontend and computes two-stage metrics", async () => {
  const tasks = [
    {
      queryId: "q1",
      query: "original wording",
      adoptedQuery: "original wording",
      intent: "analysis_reasoning",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["analysis_reasoning"]
    }
  ];
  const normalizedPackages = [{ queryId: "q1", normalizedQuery: "normalized wording", intent: "analysis_reasoning" }];

  const fetchImpl = async (_url, options) => {
    const payload = JSON.parse(options.body);
    if (payload.query === "normalized wording") {
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({
            query: payload.query,
            queryType: payload.queryType,
            filters: payload.filters,
            results: [
              {
                documentId: "doc_a",
                chunkId: "chunk_1",
                title: "Doc A",
                sectionLabel: "analysis_reasoning",
                score: 0.9
              }
            ]
          });
        }
      };
    }
    return {
      ok: true,
      status: 200,
      async text() {
        return JSON.stringify({ results: [] });
      }
    };
  };

  const report = await buildR61TwoStagePrototypeReport({
    tasks,
    normalizedPackages,
    trustedDecisionIds: ["doc_a"],
    baselineR60: {},
    baselineR608: { top5DecisionHitRate: 0, sectionTypeHitRate: 0, emptyTasksAfter: 1, stillEmptyTaskIds: ["q1"] },
    baselineR6010: {},
    apiBaseUrl: "http://example.test",
    fetchImpl
  });

  assert.equal(report.phase, "R61");
  assert.equal(report.tasksEvaluated, 1);
  assert.equal(report.top1DecisionHitRate, 1);
  assert.equal(report.top5DecisionHitRate, 1);
  assert.equal(report.sectionTypeHitRate, 1);
  assert.equal(report.emptyTasksAfter, 0);
  assert.equal(report.tasksRecoveredVsR60_8.count, 1);
  assert.deepEqual(report.tasksRecoveredVsR60_8.taskIds, ["q1"]);
});
