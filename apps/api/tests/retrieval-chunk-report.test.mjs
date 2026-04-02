import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalChunkReport } from "../scripts/retrieval-chunk-report-utils.mjs";

function doc(overrides = {}) {
  return {
    document: {
      documentId: "doc_1",
      title: "Doc One",
      citation: "CIT-1"
    },
    stats: {
      chunkCount: 3,
      chunkTypeSpread: 2,
      usedFallbackChunking: true,
      avgChunkLength: 900,
      maxChunkLength: 1200,
      minChunkLength: 300,
      headingCount: 1,
      paragraphCount: 10,
      chunkTypeCounts: { authority_discussion: 2, general_body: 1 },
      chunkClassificationReasonCounts: { paragraph_window_fallback: 3 },
      retrievalPriorityCounts: { medium: 2, low: 1 },
      chunksFlaggedOverlong: 1,
      chunksFlaggedMixedTopic: 1,
      chunksWithWeakHeadingSignal: 3,
      chunksWithCanonicalReferenceAlignment: 0,
      referenceDensityStats: { min: 0.1, max: 1.2, avg: 0.6 },
      parsingGaps: ["low_type_diversity"]
    },
    chunks: [
      { chunkType: "authority_discussion", chunkClassificationReason: "paragraph_window_fallback", retrievalPriority: "medium", textLength: 1000, referenceDensity: 0.7 },
      { chunkType: "authority_discussion", chunkClassificationReason: "paragraph_window_fallback", retrievalPriority: "medium", textLength: 800, referenceDensity: 0.6 },
      { chunkType: "general_body", chunkClassificationReason: "paragraph_window_fallback", retrievalPriority: "low", textLength: 900, referenceDensity: 0.3 }
    ],
    ...overrides
  };
}

test("report identifies weak documents and counts deterministically", () => {
  const report = buildRetrievalChunkReport({
    apiBase: "http://127.0.0.1:8787",
    input: { documentIds: ["doc_1", "doc_2"], realOnly: true, includeText: false, limit: 2 },
    documents: [
      doc(),
      doc({
        document: { documentId: "doc_2", title: "Doc Two", citation: "CIT-2" },
        stats: {
          chunkCount: 5,
          chunkTypeSpread: 4,
          usedFallbackChunking: false,
          avgChunkLength: 620,
          maxChunkLength: 900,
          minChunkLength: 280,
          headingCount: 5,
          paragraphCount: 18,
          chunkTypeCounts: { findings: 1, analysis_reasoning: 2, authority_discussion: 1, holding_disposition: 1 },
          chunkClassificationReasonCounts: { heading_match: 4, finding_section_match: 1 },
          retrievalPriorityCounts: { high: 3, medium: 2 },
          chunksFlaggedOverlong: 0,
          chunksFlaggedMixedTopic: 0,
          chunksWithWeakHeadingSignal: 0,
          chunksWithCanonicalReferenceAlignment: 4,
          referenceDensityStats: { min: 0.5, max: 2.5, avg: 1.4 },
          parsingGaps: []
        },
        chunks: [
          { chunkType: "findings", chunkClassificationReason: "finding_section_match", retrievalPriority: "high", textLength: 580, referenceDensity: 1.1 },
          { chunkType: "analysis_reasoning", chunkClassificationReason: "heading_match", retrievalPriority: "high", textLength: 640, referenceDensity: 1.5 },
          { chunkType: "analysis_reasoning", chunkClassificationReason: "heading_match", retrievalPriority: "high", textLength: 680, referenceDensity: 1.7 },
          { chunkType: "authority_discussion", chunkClassificationReason: "heading_match", retrievalPriority: "medium", textLength: 620, referenceDensity: 2.1 },
          { chunkType: "holding_disposition", chunkClassificationReason: "heading_match", retrievalPriority: "medium", textLength: 600, referenceDensity: 0.9 }
        ]
      })
    ]
  });

  assert.equal(report.summary.documentsAnalyzed, 2);
  assert.equal(report.summary.totalChunks, 8);
  assert.equal(report.summary.documentsUsingFallbackChunking, 1);
  assert.equal(report.documentsNeedingChunkReview.length, 1);
  assert.equal(report.documentsNeedingChunkReview[0].documentId, "doc_1");
  assert.ok(report.documentsNeedingChunkReview[0].reasons.includes("fallback_chunking"));
  assert.equal(report.documentsOverusingFallback.length, 1);
  assert.equal(report.documentsWithLowTypeDiversity.length, 1);
  assert.equal(report.documentsWithMixedTopicChunks.length, 1);
  assert.equal(report.documentsWithPoorReferenceAlignment.length, 1);
});
