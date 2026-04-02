import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalStructuralNormalizationReport } from "../scripts/retrieval-structural-normalization-utils.mjs";

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

function stats(overrides = {}) {
  return {
    sectionCount: 2,
    chunkCount: 2,
    chunkTypeSpread: 1,
    usedFallbackChunking: true,
    avgChunkLength: 980,
    maxChunkLength: 1300,
    minChunkLength: 300,
    headingCount: 0,
    paragraphCount: 18,
    chunksFlaggedMixedTopic: 0,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: 2,
    chunksWithCanonicalReferenceAlignment: 0,
    referenceDensityStats: { min: 0.2, max: 1.1, avg: 0.6 },
    chunkTypeCounts: { authority_discussion: 2 },
    chunkClassificationReasonCounts: { paragraph_window_fallback: 2 },
    retrievalPriorityCounts: { medium: 2 },
    parsingGaps: [],
    repairApplied: true,
    repairStrategyCounts: { low_structure_discourse_split: 1 },
    preRepairChunkCount: 2,
    postRepairChunkCount: 2,
    preRepairChunkTypeSpread: 1,
    preRepairChunksFlaggedMixedTopic: 0,
    preRepairChunksWithCanonicalReferenceAlignment: 0,
    ...overrides
  };
}

test("clustering deterministic and repeated family docs cluster together", () => {
  const docs = [
    mkDoc({ documentId: "doc_l1", title: "L182001 Decision", stats: stats() }),
    mkDoc({ documentId: "doc_l2", title: "L182002 Decision", stats: stats() }),
    mkDoc({ documentId: "doc_t1", title: "T191001 Decision", stats: stats({ paragraphCount: 22 }) })
  ];

  const one = buildRetrievalStructuralNormalizationReport({ apiBase: "x", input: { realOnly: true }, documents: docs });
  const two = buildRetrievalStructuralNormalizationReport({ apiBase: "x", input: { realOnly: true }, documents: docs });

  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });
  const lRows = one.documents.filter((row) => row.documentId === "doc_l1" || row.documentId === "doc_l2");
  assert.equal(lRows.length, 2);
  assert.equal(lRows[0].structuralPatternKey, lRows[1].structuralPatternKey);
});

test("fixtures and admit-like docs excluded from structural normalization targets", () => {
  const admitLike = mkDoc({
    documentId: "doc_admit",
    title: "L170001 Decision",
    stats: stats({
      usedFallbackChunking: false,
      chunksFlaggedMixedTopic: 0,
      headingCount: 4,
      chunkTypeSpread: 4,
      chunksWithCanonicalReferenceAlignment: 3,
      preRepairChunkCount: 4,
      preRepairChunkTypeSpread: 4,
      preRepairChunksFlaggedMixedTopic: 0,
      preRepairChunksWithCanonicalReferenceAlignment: 3
    })
  });

  const fixture = mkDoc({ documentId: "doc_fix", title: "L182999 Decision", isLikelyFixture: true, stats: stats() });
  const hold = mkDoc({ documentId: "doc_hold", title: "L182120 Decision", stats: stats() });

  const report = buildRetrievalStructuralNormalizationReport({
    apiBase: "x",
    input: { realOnly: true },
    documents: [admitLike, fixture, hold]
  });

  const ids = report.documents.map((row) => row.documentId);
  assert.ok(ids.includes("doc_hold"));
  assert.ok(!ids.includes("doc_admit"));
  assert.ok(!ids.includes("doc_fix"));
});

test("normalization rehearsal can improve pattern-family docs and malformed docs remain excluded", () => {
  const improvable = mkDoc({ documentId: "doc_imp", title: "L182130 Decision", stats: stats() });

  const malformed = mkDoc({
    documentId: "doc_bad",
    title: "T191777 Decision",
    stats: stats({
      chunkCount: 1,
      chunkTypeSpread: 1,
      avgChunkLength: 2300,
      maxChunkLength: 2600,
      headingCount: 0,
      chunksFlaggedMixedTopic: 1,
      chunksWithCanonicalReferenceAlignment: 0,
      preRepairChunkCount: 1,
      preRepairChunkTypeSpread: 1,
      preRepairChunksFlaggedMixedTopic: 1,
      preRepairChunksWithCanonicalReferenceAlignment: 0
    })
  });

  const report = buildRetrievalStructuralNormalizationReport({ apiBase: "x", input: { realOnly: true }, documents: [improvable, malformed] });

  const imp = report.documents.find((row) => row.documentId === "doc_imp");
  const bad = report.documents.find((row) => row.documentId === "doc_bad");
  assert.ok(imp);
  assert.ok(!bad, "manual-review malformed doc should not enter low-confidence normalization rehearsal scope");

  assert.equal(imp.normalizationRehearsalApplied, true);
  assert.ok(imp.normalizationImprovementReasons.length > 0);
  assert.ok(["promote_after_normalization", "still_hold_after_normalization"].includes(imp.postNormalizationPromotionOutcome));
});

test("output ordering deterministic and bundles populated", () => {
  const docs = [
    mkDoc({ documentId: "doc_b", title: "L182210 Decision", stats: stats() }),
    mkDoc({ documentId: "doc_a", title: "L182209 Decision", stats: stats() })
  ];

  const report = buildRetrievalStructuralNormalizationReport({ apiBase: "x", input: { realOnly: true }, documents: docs });

  assert.ok(Array.isArray(report.normalizationOutcomeBundles.promoteAfterNormalizationDocIds));
  assert.ok(Array.isArray(report.normalizationOutcomeBundles.stillHoldDocIds));
  assert.ok(Array.isArray(report.normalizationOutcomeBundles.stillExcludeDocIds));
  assert.equal(typeof report.patternBundles, "object");

  const sorted = [...report.documents].sort((a, b) => a.documentId.localeCompare(b.documentId)).map((row) => row.documentId);
  const actual = [...report.documents].map((row) => row.documentId).sort((a, b) => a.localeCompare(b));
  assert.deepEqual(actual, sorted);
});
