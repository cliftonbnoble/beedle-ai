import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalCorpusAdmissionReport } from "../scripts/retrieval-corpus-admission-utils.mjs";

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
    }
  };
}

test("clearly strong doc is admit_now", () => {
  const report = buildRetrievalCorpusAdmissionReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_ready",
        title: "Ready",
        stats: {
          chunkCount: 6,
          chunkTypeSpread: 5,
          usedFallbackChunking: false,
          avgChunkLength: 620,
          maxChunkLength: 930,
          minChunkLength: 300,
          headingCount: 6,
          paragraphCount: 22,
          chunksFlaggedMixedTopic: 0,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 0,
          chunksWithCanonicalReferenceAlignment: 4,
          referenceDensityStats: { min: 0.2, max: 1.2, avg: 0.6 }
        },
        chunkTypeCounts: { facts_background: 1, issue_statement: 1, authority_discussion: 1, analysis_reasoning: 2, holding_disposition: 1 },
        chunkClassificationReasonCounts: { heading_match: 5, analysis_language_match: 1 },
        retrievalPriorityCounts: { high: 3, medium: 3 }
      })
    ]
  });

  assert.equal(report.admitNowDocuments.length, 1);
  assert.equal(report.admitNowDocuments[0].corpusAdmissionStatus, "admit_now");
  assert.equal(report.summary.initialEmbeddingCandidateCount, 1);
  assert.equal(report.summary.initialSearchExposureCandidateCount, 1);
});

test("review-needed but recoverable doc is hold_for_repair_review", () => {
  const report = buildRetrievalCorpusAdmissionReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_hold",
        title: "Hold",
        stats: {
          chunkCount: 3,
          chunkTypeSpread: 2,
          usedFallbackChunking: true,
          avgChunkLength: 760,
          maxChunkLength: 1080,
          minChunkLength: 320,
          headingCount: 1,
          paragraphCount: 15,
          chunksFlaggedMixedTopic: 1,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 2,
          chunksWithCanonicalReferenceAlignment: 1,
          referenceDensityStats: { min: 0.1, max: 1.1, avg: 0.5 }
        },
        chunkTypeCounts: { authority_discussion: 2, analysis_reasoning: 1 },
        chunkClassificationReasonCounts: { paragraph_window_fallback: 3 },
        retrievalPriorityCounts: { high: 1, medium: 2 }
      })
    ]
  });

  assert.equal(report.holdForRepairReviewDocuments.length, 1);
  const row = report.holdForRepairReviewDocuments[0];
  assert.equal(row.corpusAdmissionStatus, "hold_for_repair_review");
  assert.ok(row.corpusAdmissionWarnings.includes("fallback_chunking_detected"));
  assert.equal(row.eligibleForInitialEmbedding, false);
});

test("weak alignment and weak structure doc is exclude_from_initial_corpus", () => {
  const report = buildRetrievalCorpusAdmissionReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_exclude",
        title: "Exclude",
        stats: {
          chunkCount: 1,
          chunkTypeSpread: 1,
          usedFallbackChunking: true,
          avgChunkLength: 2100,
          maxChunkLength: 2500,
          minChunkLength: 2100,
          headingCount: 0,
          paragraphCount: 18,
          chunksFlaggedMixedTopic: 1,
          chunksFlaggedOverlong: 1,
          chunksWithWeakHeadingSignal: 1,
          chunksWithCanonicalReferenceAlignment: 0,
          referenceDensityStats: { min: 0.4, max: 0.9, avg: 0.55 },
          parsingGaps: ["no_sections", "low_type_diversity"]
        },
        chunkTypeCounts: { general_body: 1 },
        chunkClassificationReasonCounts: { paragraph_window_fallback: 1 },
        retrievalPriorityCounts: { low: 1 }
      })
    ]
  });

  assert.equal(report.excludeFromInitialCorpusDocuments.length, 1);
  const row = report.excludeFromInitialCorpusDocuments[0];
  assert.equal(row.corpusAdmissionStatus, "exclude_from_initial_corpus");
  assert.equal(row.eligibleForSearchExposure, false);
  assert.ok(row.corpusAdmissionReasons.includes("severe_structure_or_alignment_risk"));
});

test("repair-improved doc can be admitted when evidence supports it", () => {
  const report = buildRetrievalCorpusAdmissionReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_repaired",
        title: "Repair Improved",
        stats: {
          chunkCount: 4,
          chunkTypeSpread: 3,
          usedFallbackChunking: false,
          avgChunkLength: 640,
          maxChunkLength: 930,
          minChunkLength: 300,
          headingCount: 2,
          paragraphCount: 14,
          chunksFlaggedMixedTopic: 0,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 0,
          chunksWithCanonicalReferenceAlignment: 2,
          referenceDensityStats: { min: 0.1, max: 1.0, avg: 0.4 },
          repairApplied: true,
          repairStrategyCounts: { low_structure_discourse_split: 2 },
          preRepairChunkCount: 2,
          postRepairChunkCount: 4,
          preRepairChunkTypeSpread: 1,
          preRepairChunksFlaggedMixedTopic: 1,
          preRepairChunksWithCanonicalReferenceAlignment: 0
        },
        chunkTypeCounts: { facts_background: 1, analysis_reasoning: 2, holding_disposition: 1 },
        chunkClassificationReasonCounts: { low_structure_discourse_split: 2, disposition_language_match: 1, analysis_language_match: 1 },
        retrievalPriorityCounts: { high: 2, medium: 2 }
      })
    ]
  });

  assert.equal(report.admitNowDocuments.length, 1);
  assert.equal(report.summary.admitNowAfterRepairCount, 1);
  assert.equal(report.admitNowDocuments[0].readinessChangedAfterRepair, true);
});

test("fixtures do not pollute real-only admission output", () => {
  const report = buildRetrievalCorpusAdmissionReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 10 },
    documents: [
      makeDoc({
        documentId: "doc_real",
        title: "Real",
        isLikelyFixture: false,
        stats: {
          chunkCount: 3,
          chunkTypeSpread: 2,
          usedFallbackChunking: true,
          avgChunkLength: 730,
          maxChunkLength: 1000,
          minChunkLength: 320,
          headingCount: 1,
          paragraphCount: 11,
          chunksFlaggedMixedTopic: 0,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 1,
          chunksWithCanonicalReferenceAlignment: 1,
          referenceDensityStats: { min: 0.1, max: 0.9, avg: 0.35 }
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
          chunkCount: 4,
          chunkTypeSpread: 4,
          usedFallbackChunking: false,
          avgChunkLength: 580,
          maxChunkLength: 850,
          minChunkLength: 300,
          headingCount: 4,
          paragraphCount: 12,
          chunksFlaggedMixedTopic: 0,
          chunksFlaggedOverlong: 0,
          chunksWithWeakHeadingSignal: 0,
          chunksWithCanonicalReferenceAlignment: 3,
          referenceDensityStats: { min: 0.2, max: 1.2, avg: 0.6 }
        },
        chunkTypeCounts: { facts_background: 1, issue_statement: 1, analysis_reasoning: 1, holding_disposition: 1 },
        chunkClassificationReasonCounts: { heading_match: 4 },
        retrievalPriorityCounts: { high: 2, medium: 2 }
      })
    ]
  });

  assert.equal(report.summary.documentsAnalyzed, 1);
  assert.equal(report.summary.fixtureRowsExcluded, 1);
  assert.equal(report.documents.length, 1);
  assert.equal(report.documents[0].documentId, "doc_real");
});

test("report ordering deterministic and read-only flags preserved", () => {
  const docs = [
    makeDoc({
      documentId: "doc_z",
      title: "Z",
      stats: {
        chunkCount: 1,
        chunkTypeSpread: 1,
        usedFallbackChunking: true,
        avgChunkLength: 2200,
        maxChunkLength: 2400,
        minChunkLength: 2200,
        headingCount: 0,
        paragraphCount: 14,
        chunksFlaggedMixedTopic: 1,
        chunksFlaggedOverlong: 1,
        chunksWithWeakHeadingSignal: 1,
        chunksWithCanonicalReferenceAlignment: 0,
        referenceDensityStats: { min: 0.2, max: 0.8, avg: 0.4 }
      },
      chunkTypeCounts: { general_body: 1 },
      chunkClassificationReasonCounts: { paragraph_window_fallback: 1 },
      retrievalPriorityCounts: { low: 1 }
    }),
    makeDoc({
      documentId: "doc_a",
      title: "A",
      stats: {
        chunkCount: 5,
        chunkTypeSpread: 4,
        usedFallbackChunking: false,
        avgChunkLength: 620,
        maxChunkLength: 900,
        minChunkLength: 310,
        headingCount: 5,
        paragraphCount: 18,
        chunksFlaggedMixedTopic: 0,
        chunksFlaggedOverlong: 0,
        chunksWithWeakHeadingSignal: 0,
        chunksWithCanonicalReferenceAlignment: 3,
        referenceDensityStats: { min: 0.1, max: 1.0, avg: 0.5 }
      },
      chunkTypeCounts: { facts_background: 1, issue_statement: 1, analysis_reasoning: 2, holding_disposition: 1 },
      chunkClassificationReasonCounts: { heading_match: 5 },
      retrievalPriorityCounts: { high: 3, medium: 2 }
    })
  ];

  const one = buildRetrievalCorpusAdmissionReport({ apiBase: "x", input: { realOnly: true, includeText: false }, documents: docs });
  const two = buildRetrievalCorpusAdmissionReport({ apiBase: "x", input: { realOnly: true, includeText: false }, documents: docs });

  assert.equal(one.readOnly, true);
  assert.equal(one.input.includeText, false);
  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });
  assert.equal(one.documents[0].corpusAdmissionStatus, "exclude_from_initial_corpus");
});
