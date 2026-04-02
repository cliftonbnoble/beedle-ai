import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10_5RuntimeStartupReport } from "../scripts/retrieval-r60_10_5-runtime-startup-report.mjs";

test("R60.10.5 emits canonical startup workflow and probe pass flags", async () => {
  const fetchImpl = async (_url, options) => {
    if (!options) {
      return {
        status: 200,
        async text() {
          return JSON.stringify({ ok: true });
        }
      };
    }
    return {
      status: 200,
      async text() {
        return JSON.stringify({
          query: "analysis standard",
          queryType: "keyword",
          filters: { approvedOnly: true, fileType: "decision_docx" },
          results: [{ documentId: "doc_a", sectionLabel: "analysis_reasoning" }]
        });
      }
    };
  };

  const report = await buildR60_10_5RuntimeStartupReport({
    apiBaseUrl: "http://127.0.0.1:8787",
    fetchImpl
  });

  assert.equal(report.phase, "R60.10.5");
  assert.equal(report.selectedBaseUrl, "http://127.0.0.1:8787");
  assert.equal(report.startupCommand, "pnpm dev");
  assert.equal(report.healthEndpoint, "/health");
  assert.equal(report.benchmarkEndpoint, "/admin/retrieval/debug");
  assert.equal(report.healthPassed, true);
  assert.equal(report.benchmarkEndpointPassed, true);
  assert.equal(report.smokeQueryPassed, true);
  assert.equal(report.preflightPassed, true);
});
