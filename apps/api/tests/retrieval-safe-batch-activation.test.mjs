import test from "node:test";
import assert from "node:assert/strict";
import {
  computeAttainableFloorGivenUniqueDocsAtK,
  computeCitationTopDocumentShareAverage,
  computeLowSignalStructuralShare,
  evaluateSafeBatchHardGate,
  resolveCitationTopDocumentShareCeiling
} from "../scripts/retrieval-safe-batch-activation-utils.mjs";

function mkRow(queryId, topResults, overrides = {}) {
  return {
    queryId,
    metrics: { topDocumentShare: overrides.topDocumentShare ?? 0.2 },
    topResults
  };
}

test("citation concentration and low-signal share metrics are deterministic", () => {
  const queryResults = [
    mkRow("citation_rule_direct", [{ chunkType: "authority_discussion" }, { chunkType: "authority_discussion" }], { topDocumentShare: 0.3 }),
    mkRow("citation_ordinance_direct", [{ chunkType: "authority_discussion" }], { topDocumentShare: 0.1 }),
    mkRow("authority_ordinance", [{ chunkType: "APPEARANCES" }, { chunkType: "analysis_reasoning" }]),
    mkRow("findings_credibility", [{ chunkType: "findings" }, { chunkType: "caption_title" }])
  ];

  assert.equal(computeCitationTopDocumentShareAverage(queryResults), 0.2);
  assert.equal(computeLowSignalStructuralShare(queryResults), 0.5);
});

test("hard gate fails on quality or concentration regressions", () => {
  const before = [
    mkRow("citation_rule_direct", [], { topDocumentShare: 0.2 }),
    mkRow("citation_ordinance_direct", [], { topDocumentShare: 0.2 }),
    mkRow("authority_ordinance", [{ chunkType: "analysis_reasoning" }, { chunkType: "authority_discussion" }]),
    mkRow("findings_credibility", [{ chunkType: "findings" }, { chunkType: "analysis_reasoning" }])
  ];
  const after = [
    mkRow("citation_rule_direct", [], { topDocumentShare: 0.4 }),
    mkRow("citation_ordinance_direct", [], { topDocumentShare: 0.3 }),
    mkRow("authority_ordinance", [{ chunkType: "APPEARANCES" }, { chunkType: "caption_title" }]),
    mkRow("findings_credibility", [{ chunkType: "findings" }, { chunkType: "caption_title" }])
  ];

  const result = evaluateSafeBatchHardGate({
    baselineAverageQualityScore: 65.31,
    afterSummary: {
      averageQualityScore: 64.6,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    beforeQueryResults: before,
    afterQueryResults: after,
    minAllowedQualityScore: 64.81
  });

  assert.equal(result.passed, false);
  assert.ok(result.failures.includes("qualityAboveThreshold"));
  assert.ok(result.failures.includes("citationConcentrationNotWorse"));
  assert.ok(result.failures.includes("lowSignalStructuralShareNotWorse"));
});

test("citation concentration effective ceiling calibrates to attainable floor and lets stable baseline pass", () => {
  const baselineCitationRows = [
    {
      queryId: "citation_rule_direct",
      metrics: { topDocumentShare: 0.2, uniqueDocumentsInTopK: 5 },
      topResults: [{ documentId: "doc_a" }, { documentId: "doc_b" }, { documentId: "doc_c" }, { documentId: "doc_d" }, { documentId: "doc_e" }]
    },
    {
      queryId: "citation_ordinance_direct",
      metrics: { topDocumentShare: 0.2, uniqueDocumentsInTopK: 5 },
      topResults: [{ documentId: "doc_f" }, { documentId: "doc_g" }, { documentId: "doc_h" }, { documentId: "doc_i" }, { documentId: "doc_j" }]
    }
  ];

  assert.equal(computeAttainableFloorGivenUniqueDocsAtK(baselineCitationRows, 10), 0.2);
  const threshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: baselineCitationRows,
    configuredGlobalCeiling: 0.1,
    k: 10
  });
  assert.equal(threshold.configuredGlobalCeiling, 0.1);
  assert.equal(threshold.attainableFloorGivenUniqueDocsAtK, 0.2);
  assert.equal(threshold.effectiveCeiling, 0.2);
  assert.equal(computeCitationTopDocumentShareAverage(baselineCitationRows) <= threshold.effectiveCeiling, true);
});

test("citation concentration still fails when above calibrated ceiling", () => {
  const baselineCitationRows = [
    { queryId: "citation_rule_direct", metrics: { uniqueDocumentsInTopK: 5 }, topResults: [{ documentId: "a" }, { documentId: "b" }] },
    { queryId: "citation_ordinance_direct", metrics: { uniqueDocumentsInTopK: 5 }, topResults: [{ documentId: "c" }, { documentId: "d" }] }
  ];
  const threshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: baselineCitationRows,
    configuredGlobalCeiling: 0.1,
    k: 10
  });
  assert.equal(threshold.effectiveCeiling, 0.2);

  const afterCitationRows = [
    { queryId: "citation_rule_direct", metrics: { topDocumentShare: 0.3 } },
    { queryId: "citation_ordinance_direct", metrics: { topDocumentShare: 0.2 } }
  ];
  assert.equal(computeCitationTopDocumentShareAverage(afterCitationRows), 0.25);
  assert.equal(computeCitationTopDocumentShareAverage(afterCitationRows) <= threshold.effectiveCeiling, false);
});
