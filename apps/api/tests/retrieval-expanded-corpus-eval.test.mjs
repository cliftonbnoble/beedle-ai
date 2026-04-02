import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalExpandedCorpusEvalReport } from "../scripts/retrieval-expanded-corpus-eval-utils.mjs";

function mkDoc({ documentId, title, isLikelyFixture = false, stats, chunks }) {
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

function mkStats(overrides = {}) {
  return {
    sectionCount: 4,
    chunkCount: 4,
    chunkTypeSpread: 3,
    usedFallbackChunking: false,
    avgChunkLength: 650,
    maxChunkLength: 900,
    minChunkLength: 250,
    headingCount: 3,
    paragraphCount: 12,
    chunksFlaggedMixedTopic: 0,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: 0,
    chunksWithCanonicalReferenceAlignment: 2,
    referenceDensityStats: { min: 0.1, max: 1.1, avg: 0.5 },
    chunkTypeCounts: { authority_discussion: 1, findings: 1, analysis_reasoning: 1, holding_disposition: 1 },
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

function mkChunk({ documentId, title, chunkId, chunkType, sourceText, aligned = true, ordinal = 1 }) {
  return {
    documentId,
    title,
    chunkId,
    chunkType,
    sectionLabel: chunkType,
    sectionCanonicalKey: `sec_${chunkType}`,
    chunkOrdinal: ordinal,
    sourceText,
    citationAnchorStart: `${documentId}#p${ordinal}`,
    citationAnchorEnd: `${documentId}#p${ordinal + 1}`,
    retrievalPriority: chunkType === "authority_discussion" ? "high" : "medium",
    hasCanonicalReferenceAlignment: aligned,
    containsFindings: chunkType === "findings",
    containsProceduralHistory: chunkType === "procedural_history",
    containsDispositionLanguage: chunkType === "holding_disposition",
    citationFamilies: ["37.2"],
    ordinanceReferences: aligned ? ["Ordinance 37.2"] : [],
    rulesReferences: aligned ? ["Rule 37.8"] : [],
    canonicalOrdinanceReferences: aligned ? ["37.2"] : [],
    canonicalRulesReferences: aligned ? ["37.8"] : [],
    canonicalIndexCodes: [],
    tokenEstimate: 120,
    chunkRepairApplied: false,
    chunkRepairStrategy: "none"
  };
}

const baselineAdmit = mkDoc({
  documentId: "doc_admit",
  title: "Admit Decision",
  stats: mkStats(),
  chunks: [
    mkChunk({
      documentId: "doc_admit",
      title: "Admit Decision",
      chunkId: "a1",
      chunkType: "authority_discussion",
      sourceText: "Rule 37.8 Ordinance 37.2 legal authority analysis",
      aligned: true,
      ordinal: 1
    }),
    mkChunk({
      documentId: "doc_admit",
      title: "Admit Decision",
      chunkId: "a2",
      chunkType: "findings",
      sourceText: "Findings of fact and credibility assessment",
      aligned: true,
      ordinal: 2
    })
  ]
});

const promotedCandidate = mkDoc({
  documentId: "doc_promoted",
  title: "Promoted by Enrichment",
  stats: mkStats({
    usedFallbackChunking: true,
    chunksWithWeakHeadingSignal: 1,
    chunksWithCanonicalReferenceAlignment: 1,
    referenceDensityStats: { min: 0.05, max: 0.8, avg: 0.2 }
  }),
  chunks: [
    mkChunk({
      documentId: "doc_promoted",
      title: "Promoted by Enrichment",
      chunkId: "p1",
      chunkType: "holding_disposition",
      sourceText: "Final disposition and order granted in part after findings review",
      aligned: true,
      ordinal: 1
    }),
    mkChunk({
      documentId: "doc_promoted",
      title: "Promoted by Enrichment",
      chunkId: "p2",
      chunkType: "analysis_reasoning",
      sourceText: "Issue framing and reasoning for parenthetical prefix citation treatment",
      aligned: true,
      ordinal: 2
    })
  ]
});

const heldDoc = mkDoc({
  documentId: "doc_hold",
  title: "Held Decision",
  stats: mkStats({
    chunkCount: 2,
    chunkTypeSpread: 1,
    usedFallbackChunking: true,
    headingCount: 0,
    paragraphCount: 16,
    chunksWithCanonicalReferenceAlignment: 0,
    chunksWithWeakHeadingSignal: 2,
    chunkTypeCounts: { authority_discussion: 2 },
    chunkClassificationReasonCounts: { paragraph_window_fallback: 2 }
  }),
  chunks: [
    mkChunk({
      documentId: "doc_hold",
      title: "Held Decision",
      chunkId: "h1",
      chunkType: "authority_discussion",
      sourceText: "this held doc should not appear in expanded corpus",
      aligned: false,
      ordinal: 1
    })
  ]
});

const fixtureDoc = mkDoc({
  documentId: "doc_fixture",
  title: "Fixture Decision",
  isLikelyFixture: true,
  stats: mkStats(),
  chunks: [
    mkChunk({
      documentId: "doc_fixture",
      title: "Fixture Decision",
      chunkId: "f1",
      chunkType: "authority_discussion",
      sourceText: "fixture content",
      aligned: true,
      ordinal: 1
    })
  ]
});

const queries = [
  { id: "q_authority", query: "rule 37.8 ordinance 37.2 authority", intent: "authority" },
  { id: "q_disposition", query: "final disposition order granted", intent: "disposition" }
];

function stripGeneratedAt(value) {
  if (Array.isArray(value)) return value.map((v) => stripGeneratedAt(v));
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    out[k] = k === "generatedAt" ? "<ignored>" : stripGeneratedAt(v);
  }
  return out;
}

test("expanded corpus includes only baseline admit docs plus promoted enrichment docs", () => {
  const report = buildRetrievalExpandedCorpusEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false, limit: 20 },
    documents: [baselineAdmit, promotedCandidate, heldDoc, fixtureDoc],
    queries,
    includeText: false,
    promotedEnrichmentDocIdsOverride: ["doc_promoted"]
  });

  assert.deepEqual(report.expandedCorpusBundles.baselineAdmittedDocIds, ["doc_admit"]);
  assert.deepEqual(report.expandedCorpusBundles.promotedEnrichmentDocIds, ["doc_promoted"]);
  assert.deepEqual(report.expandedCorpusBundles.expandedAdmittedDocIds, ["doc_admit", "doc_promoted"]);

  const allExpandedTopDocIds = report.expandedQueryResults.flatMap((row) => (row.expandedTopResults || []).map((result) => result.documentId));
  assert.ok(allExpandedTopDocIds.every((id) => id !== "doc_hold" && id !== "doc_fixture"));
});

test("comparative output deterministic and can improve diversity/coverage", () => {
  const one = buildRetrievalExpandedCorpusEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [baselineAdmit, promotedCandidate, heldDoc],
    queries,
    includeText: false,
    promotedEnrichmentDocIdsOverride: ["doc_promoted"]
  });
  const two = buildRetrievalExpandedCorpusEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [baselineAdmit, promotedCandidate, heldDoc],
    queries,
    includeText: false,
    promotedEnrichmentDocIdsOverride: ["doc_promoted"]
  });

  assert.deepEqual(stripGeneratedAt(one), stripGeneratedAt(two));
  assert.equal(typeof one.comparativeDeltaSummary.admittedDocumentDelta, "number");
  assert.ok(Array.isArray(one.coverageDeltaByDocument));
  assert.ok(Array.isArray(one.coverageDeltaByChunkType));

  const dispositionRow = one.expandedQueryResults.find((row) => row.queryId === "q_disposition");
  assert.ok(dispositionRow);
  assert.ok(dispositionRow.expandedUniqueDocuments >= dispositionRow.baselineUniqueDocuments);
  assert.ok(["improved", "unchanged", "worsened"].includes(dispositionRow.netExpandedCorpusOutcome));
});

test("promoted docs preserve provenance requirements", () => {
  const report = buildRetrievalExpandedCorpusEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [baselineAdmit, promotedCandidate],
    queries,
    includeText: false,
    promotedEnrichmentDocIdsOverride: ["doc_promoted"]
  });

  const promotedHits = report.expandedQueryResults
    .flatMap((row) => row.expandedTopResults || [])
    .filter((result) => result.documentId === "doc_promoted");

  assert.ok(promotedHits.length > 0);
  for (const hit of promotedHits) {
    assert.ok(hit.documentId);
    assert.ok(hit.chunkId);
    assert.ok(hit.citationAnchorStart);
    assert.ok(hit.citationAnchorEnd);
    assert.ok(hit.sourceLink);
  }

  const contribution = report.promotedDocumentContributions.find((row) => row.documentId === "doc_promoted");
  assert.ok(contribution);
  assert.equal(typeof contribution.promotedDocContributionCount, "number");
  assert.ok(Array.isArray(contribution.chunkTypesContributed));
});
