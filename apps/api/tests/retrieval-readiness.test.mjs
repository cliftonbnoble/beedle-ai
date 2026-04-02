import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalReadinessReport } from "../scripts/retrieval-readiness-report-utils.mjs";

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
      parsingGaps: stats.parsingGaps || []
    }
  };
}

test("clearly good multi-section decision is retrieval_ready", () => {
  const report = buildRetrievalReadinessReport({
    apiBase: "http://127.0.0.1:8787",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_ready",
        title: "Ready Decision",
        stats: {
          chunkCount: 6,
          chunkTypeSpread: 5,
          usedFallbackChunking: false,
          avgChunkLength: 640,
          maxChunkLength: 910,
          minChunkLength: 280,
          headingCount: 6,
          paragraphCount: 20,
          chunksFlaggedMixedTopic: 0,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 0,
          chunksWithCanonicalReferenceAlignment: 4,
          referenceDensityStats: { min: 0.3, max: 1.8, avg: 0.95 }
        },
        chunkTypeCounts: { facts_background: 1, issue_statement: 1, authority_discussion: 1, analysis_reasoning: 2, holding_disposition: 1 },
        chunkClassificationReasonCounts: { heading_match: 5, analysis_language_match: 1 },
        retrievalPriorityCounts: { high: 3, medium: 3 }
      })
    ]
  });

  assert.equal(report.retrievalReadyDocuments.length, 1);
  assert.equal(report.retrievalReadyDocuments[0].readinessStatus, "retrieval_ready");
  assert.deepEqual(report.retrievalReadyDocuments[0].blockingReasons, []);
});

test("messy heading but usable doc is retrieval_review_needed", () => {
  const report = buildRetrievalReadinessReport({
    apiBase: "http://127.0.0.1:8787",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_review",
        title: "Review Needed Decision",
        stats: {
          chunkCount: 3,
          chunkTypeSpread: 2,
          usedFallbackChunking: true,
          avgChunkLength: 810,
          maxChunkLength: 1200,
          minChunkLength: 420,
          headingCount: 1,
          paragraphCount: 15,
          chunksFlaggedMixedTopic: 1,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 2,
          chunksWithCanonicalReferenceAlignment: 1,
          referenceDensityStats: { min: 0.2, max: 1.3, avg: 0.7 },
          parsingGaps: ["low_type_diversity"]
        },
        chunkTypeCounts: { authority_discussion: 2, analysis_reasoning: 1 },
        chunkClassificationReasonCounts: { paragraph_window_fallback: 3 },
        retrievalPriorityCounts: { high: 1, medium: 2 }
      })
    ]
  });

  assert.equal(report.retrievalReviewNeededDocuments.length, 1);
  const row = report.retrievalReviewNeededDocuments[0];
  assert.equal(row.readinessStatus, "retrieval_review_needed");
  assert.ok(row.warningReasons.includes("fallback_chunking_used"));
  assert.ok(row.warningReasons.includes("low_type_diversity"));
  assert.equal(row.blockingReasons.length, 0);
});

test("fallback-heavy mixed-topic weak-alignment doc can be blocked by severity", () => {
  const report = buildRetrievalReadinessReport({
    apiBase: "http://127.0.0.1:8787",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_block",
        title: "Blocked Decision",
        stats: {
          chunkCount: 3,
          chunkTypeSpread: 1,
          usedFallbackChunking: true,
          avgChunkLength: 1800,
          maxChunkLength: 2600,
          minChunkLength: 1500,
          headingCount: 0,
          paragraphCount: 18,
          chunksFlaggedMixedTopic: 3,
          chunksFlaggedOverlong: 3,
          chunksWithWeakHeadingSignal: 3,
          chunksWithCanonicalReferenceAlignment: 0,
          referenceDensityStats: { min: 0.4, max: 1.4, avg: 0.9 },
          parsingGaps: ["no_sections", "low_type_diversity"]
        },
        chunkTypeCounts: { authority_discussion: 3 },
        chunkClassificationReasonCounts: { paragraph_window_fallback: 3 },
        retrievalPriorityCounts: { low: 3 }
      })
    ]
  });

  assert.equal(report.retrievalBlockedDocuments.length, 1);
  const row = report.retrievalBlockedDocuments[0];
  assert.equal(row.readinessStatus, "retrieval_blocked");
  assert.ok(row.blockingReasons.includes("extreme_chunk_overlength"));
  assert.ok(row.blockingReasons.includes("severe_mixed_topic_chunking"));
});

test("report output is deterministic with stable ordering", () => {
  const docs = [
    makeDoc({
      documentId: "doc_b",
      title: "B",
      stats: {
        chunkCount: 1,
        chunkTypeSpread: 1,
        usedFallbackChunking: true,
        avgChunkLength: 1900,
        maxChunkLength: 2500,
        minChunkLength: 1900,
        headingCount: 0,
        paragraphCount: 12,
        chunksFlaggedMixedTopic: 1,
        chunksFlaggedOverlong: 1,
        chunksWithWeakHeadingSignal: 1,
        chunksWithCanonicalReferenceAlignment: 0,
        referenceDensityStats: { min: 0.2, max: 0.4, avg: 0.3 }
      },
      chunkTypeCounts: { general_body: 1 },
      chunkClassificationReasonCounts: { paragraph_window_fallback: 1 },
      retrievalPriorityCounts: { low: 1 }
    }),
    makeDoc({
      documentId: "doc_a",
      title: "A",
      stats: {
        chunkCount: 4,
        chunkTypeSpread: 3,
        usedFallbackChunking: false,
        avgChunkLength: 620,
        maxChunkLength: 900,
        minChunkLength: 300,
        headingCount: 4,
        paragraphCount: 14,
        chunksFlaggedMixedTopic: 0,
        chunksFlaggedOverlong: 0,
        chunksWithWeakHeadingSignal: 0,
        chunksWithCanonicalReferenceAlignment: 2,
        referenceDensityStats: { min: 0.1, max: 0.9, avg: 0.45 }
      },
      chunkTypeCounts: { facts_background: 1, analysis_reasoning: 2, holding_disposition: 1 },
      chunkClassificationReasonCounts: { heading_match: 4 },
      retrievalPriorityCounts: { medium: 3, high: 1 }
    })
  ];

  const one = buildRetrievalReadinessReport({ apiBase: "x", input: { realOnly: true }, documents: docs });
  const two = buildRetrievalReadinessReport({ apiBase: "x", input: { realOnly: true }, documents: docs });

  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });
  assert.equal(one.documents[0].documentId, "doc_b");
});

test("fixtures do not pollute real-only reporting", () => {
  const report = buildRetrievalReadinessReport({
    apiBase: "x",
    input: { realOnly: true },
    documents: [
      makeDoc({
        documentId: "doc_real",
        title: "Real",
        isLikelyFixture: false,
        stats: {
          chunkCount: 3,
          chunkTypeSpread: 2,
          usedFallbackChunking: true,
          avgChunkLength: 700,
          maxChunkLength: 980,
          minChunkLength: 340,
          headingCount: 1,
          paragraphCount: 10,
          chunksFlaggedMixedTopic: 0,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 1,
          chunksWithCanonicalReferenceAlignment: 1,
          referenceDensityStats: { min: 0.1, max: 0.8, avg: 0.3 }
        },
        chunkTypeCounts: { authority_discussion: 2, analysis_reasoning: 1 },
        chunkClassificationReasonCounts: { paragraph_window_fallback: 3 },
        retrievalPriorityCounts: { medium: 3 }
      }),
      makeDoc({
        documentId: "doc_fixture",
        title: "Fixture",
        isLikelyFixture: true,
        stats: {
          chunkCount: 3,
          chunkTypeSpread: 3,
          usedFallbackChunking: false,
          avgChunkLength: 500,
          maxChunkLength: 700,
          minChunkLength: 300,
          headingCount: 3,
          paragraphCount: 8,
          chunksFlaggedMixedTopic: 0,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 0,
          chunksWithCanonicalReferenceAlignment: 2,
          referenceDensityStats: { min: 0.2, max: 1.0, avg: 0.6 }
        },
        chunkTypeCounts: { facts_background: 1, analysis_reasoning: 1, holding_disposition: 1 },
        chunkClassificationReasonCounts: { heading_match: 3 },
        retrievalPriorityCounts: { medium: 2, high: 1 }
      })
    ]
  });

  assert.equal(report.summary.documentsAnalyzed, 1);
  assert.equal(report.summary.fixtureRowsExcluded, 1);
  assert.equal(report.documents.length, 1);
  assert.equal(report.documents[0].documentId, "doc_real");
});
