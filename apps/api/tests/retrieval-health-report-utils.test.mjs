import test from "node:test";
import assert from "node:assert/strict";
import {
  compareHealthSummaries,
  summarizeHealthQuery,
  summarizeRetrievalHealth
} from "../scripts/retrieval-health-report-utils.mjs";

test("summarizeHealthQuery captures result count and decision diversity", () => {
  const row = summarizeHealthQuery(
    { id: "heat", query: "heat", family: "housing_conditions" },
    {
      total: 3,
      runtimeDiagnostics: { vectorMatchCount: 12 },
      results: [
        { documentId: "doc_a", chunkType: "findings_of_fact", score: 0.7, vectorScore: 0.5, lexicalScore: 0.4 },
        { documentId: "doc_a", chunkType: "order", score: 0.6, vectorScore: 0.4, lexicalScore: 0.3 },
        { documentId: "doc_b", chunkType: "findings_of_fact", score: 0.5, vectorScore: 0.2, lexicalScore: 0.2 }
      ]
    }
  );

  assert.equal(row.totalResults, 3);
  assert.equal(row.uniqueDecisionCount, 2);
  assert.equal(row.vectorMatchCount, 12);
  assert.deepEqual(row.topChunkTypes, ["findings_of_fact", "order"]);
});

test("summarizeRetrievalHealth reports family hit rates and universe size", () => {
  const summary = summarizeRetrievalHealth([
    {
      id: "heat",
      query: "heat",
      family: "housing_conditions",
      totalResults: 3,
      uniqueDecisionCount: 2,
      topDecisionIds: ["doc_a", "doc_b"],
      vectorMatchCount: 10,
      returnedAny: true
    },
    {
      id: "cooling",
      query: "cooling",
      family: "housing_conditions",
      totalResults: 0,
      uniqueDecisionCount: 0,
      topDecisionIds: [],
      vectorMatchCount: 8,
      returnedAny: false
    },
    {
      id: "notice",
      query: "notice",
      family: "procedure_notice",
      totalResults: 4,
      uniqueDecisionCount: 3,
      topDecisionIds: ["doc_c", "doc_d", "doc_e"],
      vectorMatchCount: 6,
      returnedAny: true
    }
  ]);

  assert.equal(summary.queryCount, 3);
  assert.equal(summary.returnedQueryCount, 2);
  assert.equal(summary.uniqueDecisionUniverseCount, 5);
  assert.equal(summary.familyStats.find((row) => row.family === "housing_conditions").hitRate, 0.5);
});

test("compareHealthSummaries returns deltas", () => {
  const delta = compareHealthSummaries(
    {
      overallHitRate: 0.7,
      avgResultsPerQuery: 2.1,
      avgDecisionDiversity: 1.4,
      avgVectorMatchCount: 11.2,
      uniqueDecisionUniverseCount: 9
    },
    {
      overallHitRate: 0.5,
      avgResultsPerQuery: 1.8,
      avgDecisionDiversity: 1.1,
      avgVectorMatchCount: 8.2,
      uniqueDecisionUniverseCount: 7
    }
  );

  assert.equal(delta.overallHitRateDelta, 0.2);
  assert.equal(delta.uniqueDecisionUniverseCountDelta, 2);
});
