import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_8NormalizationReport } from "../scripts/retrieval-r60_8-query-normalization-report.mjs";

test("R60.8 builds normalized query packages and recovers tasks when variants return trusted results", async () => {
  const tasks = [
    {
      queryId: "q1",
      originalQuery: "Rule 37.8 authority discussion",
      adoptedQuery: "Rule 37.8 authority discussion",
      intent: "citation_direct",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["authority_discussion"]
    },
    {
      queryId: "q2",
      originalQuery: "findings credibility witness",
      adoptedQuery: "findings credibility witness",
      intent: "findings",
      expectedDecisionIds: ["doc_b"],
      expectedSectionTypes: ["findings"]
    }
  ];
  const baselineReport = { clusterCounts: { empty_even_after_rewrite: 2 } };

  let callCount = 0;
  global.fetch = async (_url, opts) => {
    const body = JSON.parse(opts.body);
    callCount += 1;
    if (String(body.query).includes("Rule 37.8")) {
      return {
        ok: true,
        async text() {
          return JSON.stringify({
            results: [{ documentId: "doc_a", sectionLabel: "authority_discussion" }]
          });
        }
      };
    }
    if (String(body.query).includes("findings")) {
      return {
        ok: true,
        async text() {
          return JSON.stringify({
            results: [{ documentId: "doc_b", sectionLabel: "FINDINGS" }]
          });
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

  const out = await buildR60_8NormalizationReport({
    tasks,
    baselineReport,
    trustedDecisionIds: ["doc_a", "doc_b"],
    apiBaseUrl: "http://example.test"
  });

  assert.equal(out.phase, "R60.8");
  assert.equal(out.tasksEvaluated, 2);
  assert.ok(callCount > 0);
  assert.equal(out.tasksRecoveredByNormalizationCount, 2);
  assert.equal(out.emptyTasksAfter, 0);
  assert.equal(out.top1DecisionHitRate, 1);
  assert.equal(out.sectionTypeHitRate, 1);
  assert.equal(out.normalizedPackages.length, 2);
});
