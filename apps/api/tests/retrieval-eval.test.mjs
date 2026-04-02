import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalEvalReport } from "../scripts/retrieval-eval-utils.mjs";

function makeDoc({
  documentId,
  title,
  isLikelyFixture = false,
  readinessShape,
  chunks
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
      sectionCount: readinessShape.sectionCount ?? 5,
      chunkCount: readinessShape.chunkCount,
      chunkTypeSpread: readinessShape.chunkTypeSpread,
      usedFallbackChunking: readinessShape.usedFallbackChunking,
      avgChunkLength: readinessShape.avgChunkLength,
      maxChunkLength: readinessShape.maxChunkLength,
      minChunkLength: readinessShape.minChunkLength,
      headingCount: readinessShape.headingCount,
      paragraphCount: readinessShape.paragraphCount,
      chunksFlaggedMixedTopic: readinessShape.chunksFlaggedMixedTopic,
      chunksFlaggedOverlong: readinessShape.chunksFlaggedOverlong,
      chunksWithWeakHeadingSignal: readinessShape.chunksWithWeakHeadingSignal,
      chunksWithCanonicalReferenceAlignment: readinessShape.chunksWithCanonicalReferenceAlignment,
      referenceDensityStats: readinessShape.referenceDensityStats,
      chunkTypeCounts: readinessShape.chunkTypeCounts,
      chunkClassificationReasonCounts: readinessShape.chunkClassificationReasonCounts,
      retrievalPriorityCounts: readinessShape.retrievalPriorityCounts,
      parsingGaps: readinessShape.parsingGaps || [],
      repairApplied: Boolean(readinessShape.repairApplied),
      repairStrategyCounts: readinessShape.repairStrategyCounts || {},
      preRepairChunkCount: readinessShape.preRepairChunkCount ?? readinessShape.chunkCount,
      postRepairChunkCount: readinessShape.postRepairChunkCount ?? readinessShape.chunkCount,
      preRepairChunkTypeSpread: readinessShape.preRepairChunkTypeSpread ?? readinessShape.chunkTypeSpread,
      preRepairChunksFlaggedMixedTopic:
        readinessShape.preRepairChunksFlaggedMixedTopic ?? readinessShape.chunksFlaggedMixedTopic,
      preRepairChunksWithCanonicalReferenceAlignment:
        readinessShape.preRepairChunksWithCanonicalReferenceAlignment ?? readinessShape.chunksWithCanonicalReferenceAlignment
    },
    chunks
  };
}

function chunk({
  documentId,
  title,
  chunkId,
  chunkType,
  sourceText,
  retrievalPriority = "medium",
  hasCanonicalReferenceAlignment = false
}) {
  return {
    documentId,
    title,
    chunkId,
    chunkType,
    sourceText,
    citationAnchorStart: `${documentId}#p1`,
    citationAnchorEnd: `${documentId}#p2`,
    retrievalPriority,
    hasCanonicalReferenceAlignment,
    sectionLabel: chunkType,
    citationFamilies: ["37.2"],
    ordinanceReferences: ["Ordinance 37.2"],
    rulesReferences: ["Rule 37.8"],
    canonicalOrdinanceReferences: hasCanonicalReferenceAlignment ? ["37.2"] : [],
    canonicalRulesReferences: hasCanonicalReferenceAlignment ? ["37.8"] : [],
    canonicalIndexCodes: [],
    tokenEstimate: 120,
    chunkRepairApplied: false,
    chunkRepairStrategy: "none"
  };
}

const admitDoc = makeDoc({
  documentId: "doc_admit",
  title: "Admit Decision",
  readinessShape: {
    chunkCount: 4,
    chunkTypeSpread: 4,
    usedFallbackChunking: false,
    avgChunkLength: 620,
    maxChunkLength: 900,
    minChunkLength: 310,
    headingCount: 4,
    paragraphCount: 16,
    chunksFlaggedMixedTopic: 0,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: 0,
    chunksWithCanonicalReferenceAlignment: 3,
    referenceDensityStats: { min: 0.2, max: 1.5, avg: 0.8 },
    chunkTypeCounts: { authority_discussion: 1, findings: 1, analysis_reasoning: 1, holding_disposition: 1 },
    chunkClassificationReasonCounts: { heading_match: 4 },
    retrievalPriorityCounts: { high: 3, medium: 1 }
  },
  chunks: [
    chunk({
      documentId: "doc_admit",
      title: "Admit Decision",
      chunkId: "drchk_a1",
      chunkType: "authority_discussion",
      sourceText: "Rule 37.8 and Ordinance 37.2 provide legal authority for analysis.",
      retrievalPriority: "high",
      hasCanonicalReferenceAlignment: true
    }),
    chunk({
      documentId: "doc_admit",
      title: "Admit Decision",
      chunkId: "drchk_a2",
      chunkType: "findings",
      sourceText: "Findings of fact establish evidence and credibility determinations.",
      retrievalPriority: "high",
      hasCanonicalReferenceAlignment: true
    })
  ]
});

const holdDoc = makeDoc({
  documentId: "doc_hold",
  title: "Hold Decision",
  readinessShape: {
    chunkCount: 2,
    chunkTypeSpread: 1,
    usedFallbackChunking: true,
    avgChunkLength: 980,
    maxChunkLength: 1300,
    minChunkLength: 600,
    headingCount: 0,
    paragraphCount: 14,
    chunksFlaggedMixedTopic: 1,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: 2,
    chunksWithCanonicalReferenceAlignment: 0,
    referenceDensityStats: { min: 0.1, max: 0.7, avg: 0.3 },
    chunkTypeCounts: { authority_discussion: 2 },
    chunkClassificationReasonCounts: { paragraph_window_fallback: 2 },
    retrievalPriorityCounts: { medium: 2 }
  },
  chunks: [
    chunk({
      documentId: "doc_hold",
      title: "Hold Decision",
      chunkId: "drchk_h1",
      chunkType: "authority_discussion",
      sourceText: "This hold doc should never be in admitted retrieval results.",
      retrievalPriority: "medium",
      hasCanonicalReferenceAlignment: false
    })
  ]
});

test("only admit_now docs are included in admitted corpus retrieval and embedding prep", () => {
  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 5 },
    documents: [admitDoc, holdDoc],
    includeText: false,
    queries: [{ id: "q1", query: "ordinance 37.2", intent: "authority" }]
  });

  assert.deepEqual(report.admittedCorpus.admittedDocumentIds, ["doc_admit"]);
  assert.equal(report.embeddingPrep.rowCount, 2);
  assert.ok(report.embeddingPrep.rows.every((row) => row.documentId === "doc_admit"));
});

test("held/excluded docs do not appear in admitted retrieval results", () => {
  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 5 },
    documents: [admitDoc, holdDoc],
    includeText: false,
    queries: [{ id: "q1", query: "hold doc never appear", intent: "analysis" }]
  });

  const allResultDocIds = report.queryResults.flatMap((row) => row.topResults.map((r) => r.documentId));
  assert.ok(allResultDocIds.every((id) => id !== "doc_hold"));
});

test("retrieval results include stable provenance and deterministic ordering", () => {
  const querySet = [
    { id: "q1", query: "Rule 37.8 Ordinance 37.2", intent: "authority" },
    { id: "q2", query: "findings of fact evidence", intent: "findings" }
  ];
  const one = buildRetrievalEvalReport({ apiBase: "x", input: { realOnly: true }, documents: [admitDoc], includeText: true, queries: querySet });
  const two = buildRetrievalEvalReport({ apiBase: "x", input: { realOnly: true }, documents: [admitDoc], includeText: true, queries: querySet });

  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });

  const top = one.queryResults[0].topResults[0];
  assert.ok(top.documentId);
  assert.ok(top.chunkId);
  assert.ok(top.citationAnchorStart);
  assert.ok(top.citationAnchorEnd);
  assert.ok(top.sourceLink);
  assert.equal(typeof top.score, "number");
});

test("evaluation report output deterministic and coverage fields populated", () => {
  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true },
    documents: [admitDoc],
    includeText: false,
    queries: [{ id: "q1", query: "procedural history", intent: "procedural" }]
  });

  assert.equal(report.readOnly, true);
  assert.ok(Array.isArray(report.topResultDocuments));
  assert.ok(Array.isArray(report.topResultChunkTypes));
  assert.ok(Array.isArray(report.retrievalCoverageByDocument));
  assert.ok(Array.isArray(report.retrievalCoverageByChunkType));
  assert.equal(typeof report.averageResultsPerQuery, "number");
});

test("includeText=0 preserves redaction in eval output rows", () => {
  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [admitDoc],
    includeText: false,
    queries: [{ id: "q1", query: "authority", intent: "authority" }]
  });

  const row = report.queryResults[0].topResults[0];
  assert.equal(row.excerpt, "");
});
