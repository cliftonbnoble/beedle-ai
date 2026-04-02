import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalRankingRegressionReport } from "../scripts/retrieval-ranking-regression-utils.mjs";

function mkQuery(queryId, query, qualityScore, expectedTypeHitRate) {
  return { queryId, query, qualityScore, expectedTypeHitRate };
}

function stripGeneratedAt(value) {
  if (Array.isArray(value)) return value.map((row) => stripGeneratedAt(row));
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [key, val] of Object.entries(value)) {
    out[key] = key === "generatedAt" ? "<ignored>" : stripGeneratedAt(val);
  }
  return out;
}

test("ranking regression report is deterministic and keeps batch active on recovered quality", () => {
  const input = {
    baselinePreBatchSummary: { averageQualityScore: 65.37 },
    preRetuneReport: {
      summary: { averageQualityScore: 57.19 },
      resultQualityByQuery: [
        mkQuery("authority_ordinance", "authority", 45, 0.2),
        mkQuery("citation_rule_direct", "rule", 80, 0.7)
      ],
      queryResults: []
    },
    postRetuneReport: {
      summary: {
        averageQualityScore: 66,
        outOfCorpusHitQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1,
        zeroTrustedResultQueryCount: 0
      },
      resultQualityByQuery: [
        mkQuery("authority_ordinance", "authority", 63, 0.5),
        mkQuery("citation_rule_direct", "rule", 82, 0.72)
      ],
      queryResults: []
    },
    batchDocIds: ["doc_a"],
    activatedBatchId: "activation_1",
    rollbackBatchId: "rollback_1"
  };

  const one = buildRetrievalRankingRegressionReport(input);
  const two = buildRetrievalRankingRegressionReport(input);

  assert.deepEqual(stripGeneratedAt(one), stripGeneratedAt(two));
  assert.equal(one.recommendation, "keep_batch_active");
  assert.equal(one.hardGuardChecks.noOutOfCorpusLeakage, true);
  assert.equal(one.citationQueryRegression.worsenedCitationQueryCount, 0);
});

test("regression report recommends rollback when citation queries worsen or hard guards fail", () => {
  const report = buildRetrievalRankingRegressionReport({
    baselinePreBatchSummary: { averageQualityScore: 65.37 },
    preRetuneReport: {
      summary: { averageQualityScore: 57.19 },
      resultQualityByQuery: [
        mkQuery("citation_rule_direct", "rule", 82, 0.8),
        mkQuery("citation_ordinance_direct", "ord", 80, 0.8)
      ],
      queryResults: []
    },
    postRetuneReport: {
      summary: {
        averageQualityScore: 52,
        outOfCorpusHitQueryCount: 1,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1,
        zeroTrustedResultQueryCount: 0
      },
      resultQualityByQuery: [
        mkQuery("citation_rule_direct", "rule", 70, 0.7),
        mkQuery("citation_ordinance_direct", "ord", 68, 0.7)
      ],
      queryResults: []
    },
    batchDocIds: ["doc_a"],
    activatedBatchId: "activation_1",
    rollbackBatchId: "rollback_1"
  });

  assert.equal(report.recommendation, "rollback_batch");
  assert.equal(report.hardGuardChecks.noOutOfCorpusLeakage, false);
  assert.equal(report.citationQueryRegression.worsenedCitationQueryCount, 2);
});
