import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10_4RuntimePreflightReport } from "../scripts/retrieval-r60_10_4-runtime-preflight-report.mjs";

test("R60.10.4 preflight reports healthy runtime when probes succeed", async () => {
  const fetchImpl = async (url, options) => {
    if (!options) {
      return {
        status: 200,
        async text() {
          return JSON.stringify({ ok: true });
        }
      };
    }
    const payload = JSON.parse(options.body);
    return {
      status: 200,
      async text() {
        return JSON.stringify({
          query: payload.query,
          queryType: payload.queryType,
          filters: payload.filters,
          results: [{ documentId: "doc_a", sectionLabel: "analysis_reasoning" }]
        });
      }
    };
  };

  const report = await buildR60_10_4RuntimePreflightReport({
    apiBaseUrl: "http://127.0.0.1:8787",
    fetchImpl
  });

  assert.equal(report.phase, "R60.10.4");
  assert.equal(report.baseApiReachable, true);
  assert.equal(report.healthEndpointStatus, 200);
  assert.equal(report.benchmarkEndpointReachable, true);
  assert.equal(report.minimalKnownGoodQueryWorks, true);
  assert.equal(report.preflightPassed, true);
  assert.equal(report.runtimeModeDetected, "local_http_runtime");
});
