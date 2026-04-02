import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalPromotionRehearsalReport } from "../scripts/retrieval-promotion-rehearsal-utils.mjs";

function mkDoc({ documentId, title, isLikelyFixture = false, stats }) {
  return {
    document: {
      documentId,
      title,
      sourceFileRef: `/tmp/${documentId}.docx`,
      sourceLink: `https://example.test/${documentId}`
    },
    isLikelyFixture,
    stats,
    chunks: []
  };
}

function baseStats(overrides = {}) {
  return {
    sectionCount: 4,
    chunkCount: 4,
    chunkTypeSpread: 3,
    usedFallbackChunking: false,
    avgChunkLength: 650,
    maxChunkLength: 900,
    minChunkLength: 300,
    headingCount: 3,
    paragraphCount: 14,
    chunksFlaggedMixedTopic: 0,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: 0,
    chunksWithCanonicalReferenceAlignment: 2,
    referenceDensityStats: { min: 0.2, max: 1.1, avg: 0.6 },
    chunkTypeCounts: { authority_discussion: 1, analysis_reasoning: 2, holding_disposition: 1 },
    chunkClassificationReasonCounts: { heading_match: 4 },
    retrievalPriorityCounts: { high: 2, medium: 2 },
    parsingGaps: [],
    repairApplied: false,
    repairStrategyCounts: {},
    preRepairChunkCount: 4,
    postRepairChunkCount: 4,
    preRepairChunkTypeSpread: 3,
    preRepairChunksFlaggedMixedTopic: 0,
    preRepairChunksWithCanonicalReferenceAlignment: 2,
    ...overrides
  };
}

test("only low-confidence real docs are rehearsed; admit and fixtures excluded", () => {
  const admitDoc = mkDoc({ documentId: "doc_admit", title: "Admit", stats: baseStats() });

  const lowDoc = mkDoc({
    documentId: "doc_low",
    title: "Low",
    stats: baseStats({
      chunkCount: 5,
      chunkTypeSpread: 3,
      usedFallbackChunking: false,
      chunksWithCanonicalReferenceAlignment: 1,
      preRepairChunkCount: 4,
      preRepairChunkTypeSpread: 2,
      preRepairChunksWithCanonicalReferenceAlignment: 0,
      repairApplied: true
    })
  });

  const fixtureLow = mkDoc({
    documentId: "doc_fixture",
    title: "Fixture Low",
    isLikelyFixture: true,
    stats: baseStats({
      chunkCount: 5,
      chunkTypeSpread: 3,
      chunksWithCanonicalReferenceAlignment: 1
    })
  });

  const report = buildRetrievalPromotionRehearsalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [admitDoc, lowDoc, fixtureLow]
  });

  const ids = report.documents.map((row) => row.documentId);
  assert.ok(ids.includes("doc_low"));
  assert.ok(!ids.includes("doc_admit"));
  assert.ok(!ids.includes("doc_fixture"));
});

test("repair rehearsal deterministic and promotable docs can move to promote_after_repair", () => {
  const lowPromotable = mkDoc({
    documentId: "doc_promote",
    title: "Promotable",
    stats: baseStats({
      chunkCount: 5,
      chunkTypeSpread: 3,
      usedFallbackChunking: false,
      chunksWithCanonicalReferenceAlignment: 1,
      preRepairChunkCount: 4,
      preRepairChunkTypeSpread: 2,
      preRepairChunksWithCanonicalReferenceAlignment: 0,
      repairApplied: true,
      repairStrategyCounts: { citation_density_boundary_split: 1 }
    })
  });

  const one = buildRetrievalPromotionRehearsalReport({ apiBase: "x", input: { realOnly: true }, documents: [lowPromotable] });
  const two = buildRetrievalPromotionRehearsalReport({ apiBase: "x", input: { realOnly: true }, documents: [lowPromotable] });

  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });
  assert.ok(one.documents.length > 0);
  const row = one.documents[0];
  assert.equal(row.preRepairPromotionClass, "repair_promotable_low_confidence");
  assert.equal(row.promotionOutcome, "promote_after_repair");
  assert.equal(row.postRepairSimulatedAdmissionStatus, "admit_now");
  assert.ok(row.promotionDeltaScore >= 0);
});

test("weak/no improvement docs stay hold or exclude; malformed non-low docs are not rehearsed", () => {
  const lowHold = mkDoc({
    documentId: "doc_hold",
    title: "Hold",
    stats: baseStats({
      chunkCount: 4,
      chunkTypeSpread: 3,
      usedFallbackChunking: true,
      chunksWithWeakHeadingSignal: 1,
      chunksWithCanonicalReferenceAlignment: 2,
      preRepairChunkCount: 4,
      preRepairChunkTypeSpread: 3,
      preRepairChunksWithCanonicalReferenceAlignment: 2
    })
  });

  const malformedManual = mkDoc({
    documentId: "doc_manual",
    title: "Manual",
    stats: baseStats({
      chunkCount: 1,
      chunkTypeSpread: 1,
      usedFallbackChunking: true,
      avgChunkLength: 2300,
      maxChunkLength: 2600,
      headingCount: 0,
      paragraphCount: 20,
      chunksFlaggedMixedTopic: 1,
      chunksFlaggedOverlong: 1,
      chunksWithWeakHeadingSignal: 1,
      chunksWithCanonicalReferenceAlignment: 0,
      preRepairChunkCount: 1,
      preRepairChunkTypeSpread: 1,
      preRepairChunksFlaggedMixedTopic: 1,
      preRepairChunksWithCanonicalReferenceAlignment: 0
    })
  });

  const report = buildRetrievalPromotionRehearsalReport({
    apiBase: "x",
    input: { realOnly: true },
    documents: [lowHold, malformedManual]
  });

  const holdRow = report.documents.find((row) => row.documentId === "doc_hold");
  assert.ok(holdRow);
  assert.ok(["still_hold_after_repair", "still_exclude_after_repair"].includes(holdRow.promotionOutcome));

  const ids = report.documents.map((row) => row.documentId);
  assert.ok(!ids.includes("doc_manual"), "manual-only malformed doc should not be in low-confidence rehearsal target set");
});

test("output bundles and ordering are deterministic", () => {
  const docs = [
    mkDoc({
      documentId: "doc_b",
      title: "B",
      stats: baseStats({
        chunkCount: 5,
        chunkTypeSpread: 3,
        usedFallbackChunking: false,
        chunksWithCanonicalReferenceAlignment: 1,
        preRepairChunkCount: 4,
        preRepairChunkTypeSpread: 2,
        preRepairChunksWithCanonicalReferenceAlignment: 0
      })
    }),
    mkDoc({
      documentId: "doc_a",
      title: "A",
      stats: baseStats({
        chunkCount: 5,
        chunkTypeSpread: 3,
        usedFallbackChunking: false,
        chunksWithCanonicalReferenceAlignment: 1,
        preRepairChunkCount: 4,
        preRepairChunkTypeSpread: 2,
        preRepairChunksWithCanonicalReferenceAlignment: 0
      })
    })
  ];

  const report = buildRetrievalPromotionRehearsalReport({ apiBase: "x", input: { realOnly: true }, documents: docs });
  assert.ok(Array.isArray(report.promotionOutcomeBundles.promoteAfterRepairDocIds));
  assert.ok(Array.isArray(report.promotionOutcomeBundles.stillHoldDocIds));
  assert.ok(Array.isArray(report.promotionOutcomeBundles.stillExcludeDocIds));

  const sortedIds = [...report.documents.map((row) => row.documentId)].sort((a, b) => a.localeCompare(b));
  assert.deepEqual(
    [...report.documents.map((row) => row.documentId)].sort((a, b) => a.localeCompare(b)),
    sortedIds
  );
});
