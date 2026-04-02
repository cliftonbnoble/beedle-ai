import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10_1WeightingTraceReport } from "../scripts/retrieval-r60_10_1-weighting-trace-report.mjs";

test("R60.10.1 traces weighted collapse versus normalized queries", async () => {
  const tasks = [
    { queryId: "q1", intent: "procedural_history", adoptedQuery: "hearing notice continuance" },
    { queryId: "q2", intent: "findings", adoptedQuery: "findings credibility" }
  ];
  const normalizedQueries = [
    { queryId: "q1", normalizedQuery: "hearing notice continuance" },
    { queryId: "q2", normalizedQuery: "findings credibility" }
  ];
  const weightedQueries = [
    { queryId: "q1", weightedQuery: "hearing notice continuance service filing deadline extension dismissal denial grant appearance" },
    { queryId: "q2", weightedQuery: "findings credibility evidence witness fact" }
  ];

  global.fetch = async (_url, options) => {
    const body = JSON.parse(options.body);
    const query = String(body.query || "");
    if (query.includes("service filing deadline extension")) {
      return {
        ok: true,
        async text() {
          return JSON.stringify({ results: [] });
        }
      };
    }
    if (query.includes("hearing notice continuance")) {
      return {
        ok: true,
        async text() {
          return JSON.stringify({ results: [{ documentId: "doc_a", sectionLabel: "procedural_history" }] });
        }
      };
    }
    if (query.includes("evidence witness fact")) {
      return {
        ok: true,
        async text() {
          return JSON.stringify({ results: [] });
        }
      };
    }
    if (query === "findings credibility") {
      return {
        ok: true,
        async text() {
          return JSON.stringify({ results: [{ documentId: "doc_b", sectionLabel: "FINDINGS" }] });
        }
      };
    }
    return {
      ok: true,
      async text() {
        return JSON.stringify({ results: [] });
      }
    };
  };

  const out = await buildR60_10_1WeightingTraceReport({
    tasks,
    normalizedQueries,
    weightedQueries,
    trustedDecisionIds: ["doc_a", "doc_b"],
    apiBaseUrl: "http://example.test"
  });

  assert.equal(out.phase, "R60.10.1");
  assert.equal(out.tasksEvaluated, 2);
  assert.deepEqual(out.tasksWhereWeightingReducedRawResults, ["q1", "q2"]);
  assert.deepEqual(out.tasksWhereWeightingReducedParsedResults, ["q1", "q2"]);
  assert.equal(out.rootCauseClassification, "overconstrained_query_text");
  assert.equal(out.taskRows.length, 2);
  assert.equal(out.taskRows[0].queryId, "q1");
  assert.equal(out.taskRows[0].likelyCollapseCause, "overconstrained_query_text");
});
