import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalActivationBundleReport } from "../scripts/retrieval-activation-bundle-utils.mjs";

function mkDoc({ documentId, title, isLikelyFixture = false, sourceLink = null, sourceFileRef = null, stats, chunks }) {
  return {
    document: {
      documentId,
      title,
      sourceFileRef: sourceFileRef === null ? `/tmp/${documentId}.docx` : sourceFileRef,
      sourceLink: sourceLink === null ? `https://example.test/${documentId}` : sourceLink
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
    avgChunkLength: 620,
    maxChunkLength: 900,
    minChunkLength: 280,
    headingCount: 3,
    paragraphCount: 12,
    chunksFlaggedMixedTopic: 0,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: 0,
    chunksWithCanonicalReferenceAlignment: 2,
    referenceDensityStats: { min: 0.1, max: 0.9, avg: 0.4 },
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

function mkChunk({ documentId, title, chunkId, chunkType, sourceText, aligned = true, ordinal = 1, sourceLink = null }) {
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
    chunkRepairStrategy: "none",
    ...(sourceLink !== null ? { sourceLink } : {})
  };
}

const baselineDoc = mkDoc({
  documentId: "doc_baseline",
  title: "Baseline Admit",
  stats: mkStats(),
  chunks: [
    mkChunk({
      documentId: "doc_baseline",
      title: "Baseline Admit",
      chunkId: "b1",
      chunkType: "authority_discussion",
      sourceText: "Rule 37.8 ordinance 37.2 authority",
      aligned: true,
      ordinal: 1
    }),
    mkChunk({
      documentId: "doc_baseline",
      title: "Baseline Admit",
      chunkId: "b2",
      chunkType: "findings",
      sourceText: "Findings and evidence",
      aligned: true,
      ordinal: 2
    })
  ]
});

const promotedDoc = mkDoc({
  documentId: "doc_promoted",
  title: "Promoted Enrichment",
  stats: mkStats({
    usedFallbackChunking: true,
    chunksWithWeakHeadingSignal: 1,
    chunksWithCanonicalReferenceAlignment: 1,
    referenceDensityStats: { min: 0.05, max: 0.8, avg: 0.2 }
  }),
  chunks: [
    mkChunk({
      documentId: "doc_promoted",
      title: "Promoted Enrichment",
      chunkId: "p1",
      chunkType: "holding_disposition",
      sourceText: "Order and disposition",
      aligned: true,
      ordinal: 1
    }),
    mkChunk({
      documentId: "doc_promoted",
      title: "Promoted Enrichment",
      chunkId: "p2",
      chunkType: "analysis_reasoning",
      sourceText: "Reasoning analysis",
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
    paragraphCount: 14,
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
      sourceText: "should stay excluded",
      aligned: false,
      ordinal: 1
    })
  ]
});

const fixtureDoc = mkDoc({
  documentId: "doc_fixture",
  title: "Fixture",
  isLikelyFixture: true,
  stats: mkStats(),
  chunks: [
    mkChunk({
      documentId: "doc_fixture",
      title: "Fixture",
      chunkId: "f1",
      chunkType: "authority_discussion",
      sourceText: "fixture",
      aligned: true,
      ordinal: 1
    })
  ]
});

test("only trusted docs from R13 bundle are included", () => {
  const report = buildRetrievalActivationBundleReport({
    apiBase: "x",
    input: { realOnly: true, includeText: true, limit: 100 },
    documents: [baselineDoc, promotedDoc, heldDoc, fixtureDoc],
    includeText: true,
    promotedEnrichmentDocIdsOverride: ["doc_promoted"]
  });

  assert.deepEqual(report.trustedCorpusBundles.baselineAdmittedDocIds, ["doc_baseline"]);
  assert.deepEqual(report.trustedCorpusBundles.promotedEnrichmentDocIds, ["doc_promoted"]);
  assert.deepEqual(report.trustedCorpusBundles.expandedAdmittedDocIds, ["doc_baseline", "doc_promoted"]);

  const docIds = report.documents.map((row) => row.documentId);
  assert.ok(docIds.includes("doc_baseline"));
  assert.ok(docIds.includes("doc_promoted"));
  assert.ok(!docIds.includes("doc_hold"));
  assert.ok(!docIds.includes("doc_fixture"));
});

test("payload rows deterministic and manifests deterministic/reversible", () => {
  const one = buildRetrievalActivationBundleReport({
    apiBase: "x",
    input: { realOnly: true, includeText: true },
    documents: [baselineDoc, promotedDoc, heldDoc],
    includeText: true,
    promotedEnrichmentDocIdsOverride: ["doc_promoted"]
  });

  const two = buildRetrievalActivationBundleReport({
    apiBase: "x",
    input: { realOnly: true, includeText: true },
    documents: [baselineDoc, promotedDoc, heldDoc],
    includeText: true,
    promotedEnrichmentDocIdsOverride: ["doc_promoted"]
  });

  const sanitize = (obj) => {
    if (Array.isArray(obj)) return obj.map(sanitize);
    if (!obj || typeof obj !== "object") return obj;
    const out = {};
    for (const [k, v] of Object.entries(obj)) out[k] = k === "generatedAt" ? "<ignored>" : sanitize(v);
    return out;
  };

  assert.deepEqual(sanitize(one), sanitize(two));
  assert.equal(one.summary.rollbackReady, true);
  assert.ok(Array.isArray(one.payloads.activationManifest.documentsToActivate));
  assert.ok(Array.isArray(one.payloads.rollbackManifest.documentsToRemove));
  assert.deepEqual(one.payloads.activationManifest.documentsToActivate, one.payloads.rollbackManifest.documentsToRemove);
});

test("provenance completeness is enforced in eligibility", () => {
  const brokenDoc = mkDoc({
    documentId: "doc_broken",
    title: "Broken Provenance",
    sourceLink: "",
    sourceFileRef: "",
    stats: mkStats(),
    chunks: [
      mkChunk({
        documentId: "doc_broken",
        title: "Broken Provenance",
        chunkId: "x1",
        chunkType: "analysis_reasoning",
        sourceText: "analysis",
        aligned: true,
        ordinal: 1,
        sourceLink: ""
      })
    ]
  });

  const report = buildRetrievalActivationBundleReport({
    apiBase: "x",
    input: { realOnly: true, includeText: true },
    documents: [baselineDoc, brokenDoc],
    includeText: true,
    promotedEnrichmentDocIdsOverride: ["doc_broken"]
  });

  const broken = report.documents.find((row) => row.documentId === "doc_broken");
  assert.ok(broken);
  assert.equal(broken.provenanceComplete, false);
  assert.equal(broken.activationEligible, false);
  assert.ok(report.summary.documentsMissingProvenanceCount >= 1);
  assert.ok(report.summary.chunksMissingProvenanceCount >= 1);
  assert.equal(report.readOnly, true);
});

