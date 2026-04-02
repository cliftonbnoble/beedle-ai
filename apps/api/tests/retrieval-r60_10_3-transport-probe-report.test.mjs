import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10_3TransportProbeReport } from "../scripts/retrieval-r60_10_3-transport-probe-report.mjs";

test("R60.10.3 classifies transport/runtime unreachable when fetch fails", async () => {
  const normalizedRows = [{ queryId: "q1", intent: "analysis_reasoning", normalizedQuery: "analysis standard" }];
  const weightedRows = [{ queryId: "q1", intent: "analysis_reasoning", weightedQuery: "analysis authority reasoning standard" }];

  const failingFetch = async () => {
    throw new Error("fetch failed");
  };

  const report = await buildR60_10_3TransportProbeReport({
    normalizedRows,
    weightedRows,
    apiBaseUrl: "http://example.test",
    fetchImpl: failingFetch
  });

  assert.equal(report.phase, "R60.10.3");
  assert.equal(report.baseApiReachable, false);
  assert.equal(report.benchmarkEndpointReachable, false);
  assert.equal(report.transportFailureDetected, true);
  assert.equal(report.likelyRootCause, "transport_or_runtime_unreachable");
  assert.equal(report.probeRows.length, 5);
});
