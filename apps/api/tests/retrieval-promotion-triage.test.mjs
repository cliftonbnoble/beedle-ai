import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalPromotionTriageReport } from "../scripts/retrieval-promotion-triage-utils.mjs";

function makeDoc({
  documentId,
  title,
  isLikelyFixture = false,
  stats,
  chunkTypeCounts,
  chunkClassificationReasonCounts,
  retrievalPriorityCounts
}) {
  return {
    document: {
      documentId,
      title,
      sourceFileRef: `/tmp/${documentId}.docx`,
      sourceLink: `https://example.test/${documentId}`
    },
    isLikelyFixture,
    stats: {
      sectionCount: stats.sectionCount ?? 6,
      chunkCount: stats.chunkCount,
      chunkTypeSpread: stats.chunkTypeSpread,
      usedFallbackChunking: stats.usedFallbackChunking,
      avgChunkLength: stats.avgChunkLength,
      maxChunkLength: stats.maxChunkLength,
      minChunkLength: stats.minChunkLength,
      headingCount: stats.headingCount,
      paragraphCount: stats.paragraphCount,
      chunksFlaggedMixedTopic: stats.chunksFlaggedMixedTopic,
      chunksFlaggedOverlong: stats.chunksFlaggedOverlong,
      chunksWithWeakHeadingSignal: stats.chunksWithWeakHeadingSignal,
      chunksWithCanonicalReferenceAlignment: stats.chunksWithCanonicalReferenceAlignment,
      referenceDensityStats: stats.referenceDensityStats,
      chunkTypeCounts,
      chunkClassificationReasonCounts,
      retrievalPriorityCounts,
      parsingGaps: stats.parsingGaps || [],
      repairApplied: Boolean(stats.repairApplied),
      repairStrategyCounts: stats.repairStrategyCounts || {},
      preRepairChunkCount: stats.preRepairChunkCount ?? stats.chunkCount,
      postRepairChunkCount: stats.postRepairChunkCount ?? stats.chunkCount,
      preRepairChunkTypeSpread: stats.preRepairChunkTypeSpread ?? stats.chunkTypeSpread,
      preRepairChunksFlaggedMixedTopic: stats.preRepairChunksFlaggedMixedTopic ?? stats.chunksFlaggedMixedTopic,
      preRepairChunksWithCanonicalReferenceAlignment:
        stats.preRepairChunksWithCanonicalReferenceAlignment ?? stats.chunksWithCanonicalReferenceAlignment
    },
    chunks: []
  };
}

function makeStrongAdmitDoc() {
  return makeDoc({
    documentId: "doc_admit",
    title: "Admit",
    stats: {
      chunkCount: 5,
      chunkTypeSpread: 4,
      usedFallbackChunking: false,
      avgChunkLength: 620,
      maxChunkLength: 900,
      minChunkLength: 280,
      headingCount: 5,
      paragraphCount: 18,
      chunksFlaggedMixedTopic: 0,
      chunksFlaggedOverlong: 0,
      chunksWithWeakHeadingSignal: 0,
      chunksWithCanonicalReferenceAlignment: 3,
      referenceDensityStats: { min: 0.2, max: 1.5, avg: 0.7 }
    },
    chunkTypeCounts: { authority_discussion: 1, findings: 1, analysis_reasoning: 2, holding_disposition: 1 },
    chunkClassificationReasonCounts: { heading_match: 4, analysis_language_match: 1 },
    retrievalPriorityCounts: { high: 3, medium: 2 }
  });
}

function makeHeldRepairableDoc() {
  return makeDoc({
    documentId: "doc_hold_high",
    title: "Hold High",
    stats: {
      chunkCount: 3,
      chunkTypeSpread: 2,
      usedFallbackChunking: true,
      avgChunkLength: 760,
      maxChunkLength: 1100,
      minChunkLength: 300,
      headingCount: 1,
      paragraphCount: 16,
      chunksFlaggedMixedTopic: 1,
      chunksFlaggedOverlong: 0,
      chunksWithWeakHeadingSignal: 2,
      chunksWithCanonicalReferenceAlignment: 1,
      referenceDensityStats: { min: 0.2, max: 1.4, avg: 0.8 },
      repairApplied: true,
      preRepairChunkCount: 2,
      postRepairChunkCount: 3,
      preRepairChunkTypeSpread: 1,
      preRepairChunksFlaggedMixedTopic: 2,
      preRepairChunksWithCanonicalReferenceAlignment: 0
    },
    chunkTypeCounts: { authority_discussion: 2, analysis_reasoning: 1 },
    chunkClassificationReasonCounts: { paragraph_window_fallback: 2, low_structure_discourse_split: 1 },
    retrievalPriorityCounts: { high: 1, medium: 2 }
  });
}

function makeExcludedMalformedDoc() {
  return makeDoc({
    documentId: "doc_bad",
    title: "Bad",
    stats: {
      chunkCount: 1,
      chunkTypeSpread: 1,
      usedFallbackChunking: true,
      avgChunkLength: 2200,
      maxChunkLength: 2500,
      minChunkLength: 2200,
      headingCount: 0,
      paragraphCount: 22,
      chunksFlaggedMixedTopic: 1,
      chunksFlaggedOverlong: 1,
      chunksWithWeakHeadingSignal: 1,
      chunksWithCanonicalReferenceAlignment: 0,
      referenceDensityStats: { min: 0.2, max: 0.7, avg: 0.4 },
      parsingGaps: ["no_sections", "low_type_diversity"]
    },
    chunkTypeCounts: { authority_discussion: 1 },
    chunkClassificationReasonCounts: { paragraph_window_fallback: 1 },
    retrievalPriorityCounts: { low: 1 }
  });
}

test("only held/excluded real docs are triaged; admit docs excluded", () => {
  const report = buildRetrievalPromotionTriageReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [makeStrongAdmitDoc(), makeHeldRepairableDoc(), makeExcludedMalformedDoc()]
  });

  const ids = report.documents.map((row) => row.documentId);
  assert.ok(!ids.includes("doc_admit"));
  assert.ok(ids.includes("doc_hold_high"));
  assert.ok(ids.includes("doc_bad"));
});

test("fixtures do not pollute triage output", () => {
  const fixture = makeHeldRepairableDoc();
  fixture.document.documentId = "doc_fixture";
  fixture.isLikelyFixture = true;

  const report = buildRetrievalPromotionTriageReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [makeHeldRepairableDoc(), fixture]
  });

  assert.equal(report.documents.length, 1);
  assert.equal(report.documents[0].documentId, "doc_hold_high");
});

test("repairable docs classify higher confidence than malformed docs", () => {
  const report = buildRetrievalPromotionTriageReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [makeHeldRepairableDoc(), makeExcludedMalformedDoc()]
  });

  const held = report.documents.find((row) => row.documentId === "doc_hold_high");
  const bad = report.documents.find((row) => row.documentId === "doc_bad");
  assert.ok(held);
  assert.ok(bad);
  assert.ok(held.promotionConfidenceScore > bad.promotionConfidenceScore);
  assert.notEqual(held.promotionClass, "not_promotable_without_manual_review");
  assert.equal(bad.promotionClass, "not_promotable_without_manual_review");
});

test("promotion simulation booleans and bundles are deterministic", () => {
  const docs = [makeHeldRepairableDoc(), makeExcludedMalformedDoc()];
  const one = buildRetrievalPromotionTriageReport({ apiBase: "x", input: { realOnly: true }, documents: docs });
  const two = buildRetrievalPromotionTriageReport({ apiBase: "x", input: { realOnly: true }, documents: docs });

  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });

  const held = one.documents.find((row) => row.documentId === "doc_hold_high");
  assert.ok(held.wouldPromoteIfCombinedNarrowRepairs || held.wouldPromoteIfHeadingRepair || held.wouldPromoteIfMixedTopicRepair);
  assert.ok(Array.isArray(one.promotionBundles.highConfidenceDocIds));
  assert.ok(Array.isArray(one.promotionBundles.mediumConfidenceDocIds));
  assert.ok(Array.isArray(one.promotionBundles.lowConfidenceDocIds));
  assert.ok(Array.isArray(one.promotionBundles.manualReviewDocIds));
});
