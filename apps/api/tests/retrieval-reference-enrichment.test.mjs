import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalReferenceEnrichmentReport } from "../scripts/retrieval-reference-enrichment-utils.mjs";

function mkDoc({ documentId, title, isLikelyFixture = false, stats, chunks = [] }) {
  return {
    document: {
      documentId,
      title,
      sourceFileRef: `/tmp/${documentId}.docx`,
      sourceLink: `https://example.test/${documentId}`
    },
    isLikelyFixture,
    stats,
    chunks
  };
}

function baseStats(overrides = {}) {
  return {
    sectionCount: 3,
    chunkCount: 4,
    chunkTypeSpread: 3,
    usedFallbackChunking: true,
    avgChunkLength: 840,
    maxChunkLength: 1100,
    minChunkLength: 260,
    headingCount: 1,
    paragraphCount: 16,
    chunksFlaggedMixedTopic: 0,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: 1,
    chunksWithCanonicalReferenceAlignment: 1,
    referenceDensityStats: { min: 0.05, max: 0.6, avg: 0.18 },
    chunkTypeCounts: { authority_discussion: 1, analysis_reasoning: 1, findings: 1, holding_disposition: 1 },
    chunkClassificationReasonCounts: { paragraph_window_fallback: 3, heading_match: 1 },
    retrievalPriorityCounts: { high: 1, medium: 3 },
    parsingGaps: [],
    repairApplied: true,
    repairStrategyCounts: { low_structure_discourse_split: 1 },
    preRepairChunkCount: 3,
    postRepairChunkCount: 4,
    preRepairChunkTypeSpread: 2,
    preRepairChunksFlaggedMixedTopic: 1,
    preRepairChunksWithCanonicalReferenceAlignment: 0,
    ...overrides
  };
}

function baseChunks() {
  return [
    {
      chunkId: "c1",
      chunkOrdinal: 1,
      chunkType: "authority_discussion",
      sectionCanonicalKey: "authority_discussion",
      sectionLabel: "Authority",
      headingPath: ["Authority"],
      hasCanonicalReferenceAlignment: true,
      canonicalOrdinanceReferences: ["37.2"],
      canonicalRulesReferences: ["37.8"],
      canonicalIndexCodes: ["13"],
      ordinanceReferences: ["Ordinance 37.2"],
      rulesReferences: ["Rule 37.8"],
      indexCodeReferences: ["13"],
      referenceDensity: 0.7,
      containsFindings: false,
      containsDispositionLanguage: false
    },
    {
      chunkId: "c2",
      chunkOrdinal: 2,
      chunkType: "analysis_reasoning",
      sectionCanonicalKey: "analysis_reasoning",
      sectionLabel: "Analysis",
      headingPath: ["Analysis"],
      hasCanonicalReferenceAlignment: false,
      canonicalOrdinanceReferences: [],
      canonicalRulesReferences: [],
      canonicalIndexCodes: [],
      ordinanceReferences: [],
      rulesReferences: [],
      indexCodeReferences: [],
      referenceDensity: 0.05,
      containsFindings: false,
      containsDispositionLanguage: false
    },
    {
      chunkId: "c3",
      chunkOrdinal: 3,
      chunkType: "findings",
      sectionCanonicalKey: "findings",
      sectionLabel: "Findings",
      headingPath: ["Findings"],
      hasCanonicalReferenceAlignment: false,
      canonicalOrdinanceReferences: [],
      canonicalRulesReferences: [],
      canonicalIndexCodes: [],
      ordinanceReferences: [],
      rulesReferences: [],
      indexCodeReferences: [],
      referenceDensity: 0.03,
      containsFindings: true,
      containsDispositionLanguage: false
    },
    {
      chunkId: "c4",
      chunkOrdinal: 4,
      chunkType: "holding_disposition",
      sectionCanonicalKey: "holding_disposition",
      sectionLabel: "Order",
      headingPath: ["Order"],
      hasCanonicalReferenceAlignment: false,
      canonicalOrdinanceReferences: [],
      canonicalRulesReferences: [],
      canonicalIndexCodes: [],
      ordinanceReferences: [],
      rulesReferences: [],
      indexCodeReferences: [],
      referenceDensity: 0.02,
      containsFindings: false,
      containsDispositionLanguage: true
    }
  ];
}

test("enrichment deterministic and fixtures/admit docs excluded", () => {
  const admitDoc = mkDoc({
    documentId: "doc_admit",
    title: "Admit",
    stats: baseStats({
      usedFallbackChunking: false,
      headingCount: 4,
      chunkCount: 5,
      chunkTypeSpread: 4,
      chunksWithCanonicalReferenceAlignment: 3,
      referenceDensityStats: { min: 0.1, max: 1.2, avg: 0.5 },
      preRepairChunkCount: 5,
      preRepairChunkTypeSpread: 4,
      preRepairChunksWithCanonicalReferenceAlignment: 3,
      repairApplied: false,
      repairStrategyCounts: {}
    }),
    chunks: baseChunks()
  });

  const fixture = mkDoc({ documentId: "doc_fixture", title: "Fixture", isLikelyFixture: true, stats: baseStats(), chunks: baseChunks() });
  const hold = mkDoc({ documentId: "doc_hold", title: "Hold", stats: baseStats(), chunks: baseChunks() });

  const one = buildRetrievalReferenceEnrichmentReport({ apiBase: "x", input: { realOnly: true }, documents: [admitDoc, fixture, hold] });
  const two = buildRetrievalReferenceEnrichmentReport({ apiBase: "x", input: { realOnly: true }, documents: [admitDoc, fixture, hold] });

  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });
  const ids = one.documents.map((row) => row.documentId);
  assert.ok(ids.includes("doc_hold"));
  assert.ok(!ids.includes("doc_admit"));
  assert.ok(!ids.includes("doc_fixture"));
});

test("reference propagation improves alignment when evidence is present", () => {
  const doc = mkDoc({ documentId: "doc_align", title: "L182200 Decision", stats: baseStats(), chunks: baseChunks() });
  const report = buildRetrievalReferenceEnrichmentReport({ apiBase: "x", input: { realOnly: true }, documents: [doc] });
  const row = report.documents.find((r) => r.documentId === "doc_align");

  assert.ok(row);
  assert.ok(row.postEnrichmentCanonicalAlignmentCount > row.preEnrichmentCanonicalAlignmentCount);
  assert.ok(row.referenceEnrichmentApplied);
  assert.ok((row.referenceEnrichmentStrategies || []).length > 0);

  const enriched = row.chunks.find((chunk) => chunk.chunkId === "c2");
  assert.ok(enriched);
  assert.equal(enriched.preEnrichmentCanonicalAlignment, false);
  assert.equal(enriched.postEnrichmentCanonicalAlignment, true);
});

test("weak docs can promote only with material trust-signal improvement", () => {
  const promotable = mkDoc({
    documentId: "doc_promote",
    title: "L182201 Decision",
    stats: baseStats({
      usedFallbackChunking: true,
      headingCount: 3,
      chunksWithWeakHeadingSignal: 1,
      chunkCount: 4,
      chunkTypeSpread: 3,
      chunksWithCanonicalReferenceAlignment: 1,
      referenceDensityStats: { min: 0.08, max: 0.8, avg: 0.22 }
    }),
    chunks: baseChunks()
  });

  const malformed = mkDoc({
    documentId: "doc_malformed",
    title: "L182202 Decision",
    stats: baseStats({
      chunkCount: 1,
      chunkTypeSpread: 1,
      usedFallbackChunking: true,
      avgChunkLength: 2200,
      maxChunkLength: 2600,
      headingCount: 0,
      paragraphCount: 22,
      chunksWithWeakHeadingSignal: 1,
      chunksWithCanonicalReferenceAlignment: 0,
      referenceDensityStats: { min: 0, max: 0, avg: 0 }
    }),
    chunks: [
      {
        chunkId: "mx1",
        chunkOrdinal: 1,
        chunkType: "general_body",
        sectionCanonicalKey: "body",
        sectionLabel: "Body",
        headingPath: [],
        hasCanonicalReferenceAlignment: false,
        canonicalOrdinanceReferences: [],
        canonicalRulesReferences: [],
        canonicalIndexCodes: [],
        ordinanceReferences: [],
        rulesReferences: [],
        indexCodeReferences: [],
        referenceDensity: 0,
        containsFindings: false,
        containsDispositionLanguage: false
      }
    ]
  });

  const report = buildRetrievalReferenceEnrichmentReport({
    apiBase: "x",
    input: { realOnly: true },
    documents: [promotable, malformed]
  });

  const p = report.documents.find((row) => row.documentId === "doc_promote");
  const m = report.documents.find((row) => row.documentId === "doc_malformed");
  assert.ok(p);

  assert.equal(p.postEnrichmentPromotionOutcome, "promote_after_enrichment");
  assert.equal(p.postEnrichmentCorpusAdmissionStatus, "admit_now");
  assert.ok((p.referenceEnrichmentImprovementReasons || []).includes("canonical_alignment_improved"));

  if (m) {
    assert.notEqual(m.postEnrichmentPromotionOutcome, "promote_after_enrichment");
    assert.ok((m.remainingReferenceEnrichmentBlockers || []).length > 0);
  } else {
    assert.ok(!report.enrichmentOutcomeBundles.promoteAfterEnrichmentDocIds.includes("doc_malformed"));
  }
});

test("ordering and bundles deterministic", () => {
  const a = mkDoc({ documentId: "doc_a", title: "A", stats: baseStats(), chunks: baseChunks() });
  const b = mkDoc({ documentId: "doc_b", title: "B", stats: baseStats(), chunks: baseChunks() });
  const report = buildRetrievalReferenceEnrichmentReport({ apiBase: "x", input: { realOnly: true }, documents: [b, a] });

  assert.ok(Array.isArray(report.enrichmentOutcomeBundles.promoteAfterEnrichmentDocIds));
  assert.ok(Array.isArray(report.enrichmentOutcomeBundles.stillHoldDocIds));
  assert.ok(Array.isArray(report.enrichmentOutcomeBundles.stillExcludeDocIds));

  const ids = report.documents.map((row) => row.documentId);
  const sorted = [...ids].sort((x, y) => x.localeCompare(y));
  assert.deepEqual([...ids].sort((x, y) => x.localeCompare(y)), sorted);
});
