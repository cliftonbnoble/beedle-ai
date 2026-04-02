import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10_2EndpointContractReport } from "../scripts/retrieval-r60_10_2-endpoint-contract-report.mjs";

test("R60.10.2 detects endpoint query overwrite contract mismatch", async () => {
  const tasks = [{ queryId: "q1", intent: "procedural_history", adoptedQuery: "hearing notice" }];
  const normalizedQueries = [{ queryId: "q1", normalizedQuery: "hearing notice" }];
  const weightedQueries = [{ queryId: "q1", weightedQuery: "hearing notice continuance service filing deadline extension" }];

  global.fetch = async (_url, options) => {
    const payload = JSON.parse(options.body);
    const response = {
      query: "hearing notice",
      queryType: payload.queryType,
      filters: payload.filters,
      total: 1,
      results: [{ documentId: "doc_a", sectionLabel: "procedural_history" }]
    };
    return {
      ok: true,
      status: 200,
      async text() {
        return JSON.stringify(response);
      }
    };
  };

  const out = await buildR60_10_2EndpointContractReport({
    tasks,
    normalizedQueries,
    weightedQueries,
    apiBaseUrl: "http://example.test"
  });

  assert.equal(out.phase, "R60.10.2");
  assert.equal(out.tasksEvaluated, 1);
  assert.equal(out.rootCauseClassification, "endpoint_overwrites_query_variant");
  assert.deepEqual(out.tasksWithEndpointInputDifferences, ["q1"]);
  assert.equal(out.taskRows[0].likelyContractMismatch, "endpoint_overwrites_query_variant");
  assert.equal(out.taskRows[0].weightedEndpointInputsObserved.query, "hearing notice");
});
