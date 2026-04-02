import test from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalEvalReport } from "../scripts/retrieval-eval-utils.mjs";

function mkDoc(documentId, title, stats, chunks, isLikelyFixture = false) {
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

function mkStats({
  chunkCount,
  chunkTypeSpread,
  usedFallbackChunking,
  headingCount,
  paragraphCount,
  aligned
}) {
  return {
    sectionCount: Math.max(1, headingCount),
    chunkCount,
    chunkTypeSpread,
    usedFallbackChunking,
    avgChunkLength: 620,
    maxChunkLength: 900,
    minChunkLength: 300,
    headingCount,
    paragraphCount,
    chunksFlaggedMixedTopic: 0,
    chunksFlaggedOverlong: 0,
    chunksWithWeakHeadingSignal: usedFallbackChunking ? 1 : 0,
    chunksWithCanonicalReferenceAlignment: aligned,
    referenceDensityStats: { min: 0.2, max: 1.2, avg: 0.7 },
    chunkTypeCounts: { authority_discussion: 2, analysis_reasoning: 1 },
    chunkClassificationReasonCounts: { heading_match: 2, analysis_language_match: 1 },
    retrievalPriorityCounts: { high: 2, medium: 1 },
    parsingGaps: [],
    repairApplied: false,
    repairStrategyCounts: {},
    preRepairChunkCount: chunkCount,
    postRepairChunkCount: chunkCount,
    preRepairChunkTypeSpread: chunkTypeSpread,
    preRepairChunksFlaggedMixedTopic: 0,
    preRepairChunksWithCanonicalReferenceAlignment: aligned
  };
}

function mkChunk({ documentId, title, id, text, chunkType = "authority_discussion", ordinal = 0, aligned = true }) {
  return {
    documentId,
    title,
    chunkId: id,
    chunkType,
    sectionLabel: chunkType,
    sectionCanonicalKey: `sec_${chunkType}`,
    chunkOrdinal: ordinal,
    sourceText: text,
    citationAnchorStart: `${documentId}#p${ordinal + 1}`,
    citationAnchorEnd: `${documentId}#p${ordinal + 2}`,
    retrievalPriority: chunkType === "authority_discussion" ? "high" : "medium",
    hasCanonicalReferenceAlignment: aligned,
    containsFindings: chunkType === "findings",
    containsProceduralHistory: chunkType === "procedural_history",
    containsDispositionLanguage: chunkType === "holding_disposition",
    citationFamilies: ["37.2"],
    ordinanceReferences: ["Ordinance 37.2"],
    rulesReferences: ["Rule 37.8"],
    canonicalOrdinanceReferences: aligned ? ["37.2"] : [],
    canonicalRulesReferences: aligned ? ["37.8"] : [],
    canonicalIndexCodes: [],
    tokenEstimate: 120,
    chunkRepairApplied: false,
    chunkRepairStrategy: "none"
  };
}

test("deterministic rerank ordering and admitted-only scope", () => {
  const docA = mkDoc(
    "doc_a",
    "Doc A",
    mkStats({ chunkCount: 4, chunkTypeSpread: 3, usedFallbackChunking: false, headingCount: 4, paragraphCount: 12, aligned: 3 }),
    [
      mkChunk({ documentId: "doc_a", title: "Doc A", id: "a1", text: "Ordinance 37.2 Rule 37.8 authority authority authority", ordinal: 0 }),
      mkChunk({ documentId: "doc_a", title: "Doc A", id: "a2", text: "Ordinance 37.2 authority analysis reasoning", chunkType: "analysis_reasoning", ordinal: 1 }),
      mkChunk({ documentId: "doc_a", title: "Doc A", id: "a3", text: "Findings of fact and evidence", chunkType: "findings", ordinal: 2 })
    ]
  );

  const docB = mkDoc(
    "doc_b",
    "Doc B",
    mkStats({ chunkCount: 3, chunkTypeSpread: 3, usedFallbackChunking: false, headingCount: 3, paragraphCount: 10, aligned: 2 }),
    [
      mkChunk({ documentId: "doc_b", title: "Doc B", id: "b1", text: "Rule 37.8 legal authority and ordinance interpretation", ordinal: 0 }),
      mkChunk({ documentId: "doc_b", title: "Doc B", id: "b2", text: "Order and disposition granted in part", chunkType: "holding_disposition", ordinal: 1 })
    ]
  );

  const docHold = mkDoc(
    "doc_hold",
    "Doc Hold",
    mkStats({ chunkCount: 2, chunkTypeSpread: 1, usedFallbackChunking: true, headingCount: 0, paragraphCount: 14, aligned: 0 }),
    [mkChunk({ documentId: "doc_hold", title: "Doc Hold", id: "h1", text: "hold doc should not appear", aligned: false })]
  );

  const querySet = [{ id: "q1", query: "ordinance 37.2 rule 37.8 authority", intent: "authority" }];

  const one = buildRetrievalEvalReport({ apiBase: "x", input: { realOnly: true, includeText: false }, documents: [docA, docB, docHold], queries: querySet, includeText: false });
  const two = buildRetrievalEvalReport({ apiBase: "x", input: { realOnly: true, includeText: false }, documents: [docA, docB, docHold], queries: querySet, includeText: false });

  assert.deepEqual({ ...one, generatedAt: "<ignored>" }, { ...two, generatedAt: "<ignored>" });
  assert.ok(one.admittedCorpus.admittedDocumentIds.includes("doc_a"));
  assert.ok(one.admittedCorpus.admittedDocumentIds.includes("doc_b"));
  assert.ok(!one.admittedCorpus.admittedDocumentIds.includes("doc_hold"));

  const ids = one.queryResults[0].topResults.map((r) => r.documentId);
  assert.ok(ids.every((id) => id !== "doc_hold"));
});

test("diversity penalties reduce same-document flooding without dropping strongest hit", () => {
  const dominantDoc = mkDoc(
    "doc_dom",
    "Dominant",
    mkStats({ chunkCount: 6, chunkTypeSpread: 3, usedFallbackChunking: false, headingCount: 5, paragraphCount: 20, aligned: 5 }),
    Array.from({ length: 6 }, (_, idx) =>
      mkChunk({
        documentId: "doc_dom",
        title: "Dominant",
        id: `dom${idx + 1}`,
        text: `Ordinance 37.2 Rule 37.8 authority discussion repeated ${idx + 1}`,
        chunkType: idx === 5 ? "analysis_reasoning" : "authority_discussion",
        ordinal: idx
      })
    )
  );

  const altDoc = mkDoc(
    "doc_alt",
    "Alternative",
    mkStats({ chunkCount: 3, chunkTypeSpread: 3, usedFallbackChunking: false, headingCount: 3, paragraphCount: 10, aligned: 2 }),
    [
      mkChunk({ documentId: "doc_alt", title: "Alternative", id: "alt1", text: "Rule 37.8 authority with ordinance 37.2", ordinal: 0 }),
      mkChunk({ documentId: "doc_alt", title: "Alternative", id: "alt2", text: "Procedural history and analysis", chunkType: "procedural_history", ordinal: 1 })
    ]
  );

  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [dominantDoc, altDoc],
    includeText: false,
    queries: [{ id: "q1", query: "ordinance 37.2 authority rule 37.8", intent: "authority" }]
  });

  const row = report.queryResults[0];
  assert.ok(row.beforeTopResults.length > 0);
  assert.ok(row.topResults.length > 0);
  const beforeDomCount = row.beforeTopResults.slice(0, 5).filter((r) => r.documentId === "doc_dom").length;
  const afterDomCount = row.topResults.slice(0, 5).filter((r) => r.documentId === "doc_dom").length;
  assert.ok(beforeDomCount >= afterDomCount, "diversification should not increase dominant-doc saturation");
  assert.ok(row.topResults.some((r) => r.documentId === "doc_dom"), "diversity should not remove strongest-matching doc entirely");

  const beforeUnique = row.beforeDiversity.uniqueDocuments;
  const afterUnique = row.afterDiversity.uniqueDocuments;
  assert.ok(afterUnique >= beforeUnique);
  assert.ok(report.redundancyPenaltyCounts.same_document_penalty >= 1);
});

test("provenance fields preserved after rerank and redaction honored", () => {
  const doc = mkDoc(
    "doc_p",
    "Prov",
    mkStats({ chunkCount: 3, chunkTypeSpread: 3, usedFallbackChunking: false, headingCount: 3, paragraphCount: 9, aligned: 2 }),
    [mkChunk({ documentId: "doc_p", title: "Prov", id: "p1", text: "findings and ordinance 37.2", chunkType: "findings", ordinal: 0 })]
  );

  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [doc],
    includeText: false,
    queries: [{ id: "q1", query: "findings ordinance 37.2", intent: "findings" }]
  });

  const result = report.queryResults[0].topResults[0];
  assert.ok(result.chunkId);
  assert.ok(result.citationAnchorStart);
  assert.ok(result.citationAnchorEnd);
  assert.ok(result.sourceLink);
  assert.equal(result.excerpt, "");
  assert.equal(typeof result.baseScore, "number");
  assert.equal(typeof result.rerankScore, "number");
  assert.equal(typeof result.diversityAdjustment, "number");
  assert.equal(typeof result.redundancyPenalty, "number");
  assert.ok(Array.isArray(result.rankingExplanation));
});

test("citation intent applies deterministic family penalty and per-document cap", () => {
  const docA = mkDoc(
    "doc_cit_a",
    "Citation A",
    mkStats({ chunkCount: 4, chunkTypeSpread: 2, usedFallbackChunking: false, headingCount: 2, paragraphCount: 10, aligned: 2 }),
    [
      mkChunk({ documentId: "doc_cit_a", title: "Citation A", id: "ca1", text: "Rule 37.8 Ordinance 37.2 authority", ordinal: 0 }),
      mkChunk({ documentId: "doc_cit_a", title: "Citation A", id: "ca2", text: "Rule 37.8 Ordinance 37.2 authority repeat", ordinal: 1 })
    ]
  );
  const docB = mkDoc(
    "doc_cit_b",
    "Citation B",
    mkStats({ chunkCount: 4, chunkTypeSpread: 2, usedFallbackChunking: false, headingCount: 2, paragraphCount: 10, aligned: 2 }),
    [
      mkChunk({ documentId: "doc_cit_b", title: "Citation B", id: "cb1", text: "Rule 37.8 Ordinance 37.2 authority", ordinal: 0 }),
      mkChunk({ documentId: "doc_cit_b", title: "Citation B", id: "cb2", text: "Rule 37.8 Ordinance 37.2 authority repeat", ordinal: 1 })
    ]
  );
  const docC = mkDoc(
    "doc_cit_c",
    "Citation C",
    mkStats({ chunkCount: 4, chunkTypeSpread: 2, usedFallbackChunking: false, headingCount: 2, paragraphCount: 10, aligned: 2 }),
    [
      mkChunk({ documentId: "doc_cit_c", title: "Citation C", id: "cc1", text: "Rule 37.8 Ordinance 37.2 authority", ordinal: 0 }),
      mkChunk({ documentId: "doc_cit_c", title: "Citation C", id: "cc2", text: "Rule 37.8 Ordinance 37.2 authority repeat", ordinal: 1 })
    ]
  );

  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [docA, docB, docC],
    includeText: false,
    queries: [{ id: "citation_test", query: "Rule 37.8", intent: "citation" }],
    admittedDocumentIdsOverride: ["doc_cit_a", "doc_cit_b", "doc_cit_c"]
  });

  const top = report.queryResults[0].topResults.slice(0, 10);
  const byDoc = top.reduce((acc, row) => ({ ...acc, [row.documentId]: (acc[row.documentId] || 0) + 1 }), {});
  assert.ok(Object.values(byDoc).every((count) => Number(count) <= 1), "citation intent should cap one result per doc");
  assert.ok(
    top.some((row) => row.rankingExplanation.some((line) => String(line).startsWith("citation_family_repeat_penalty="))),
    "citation family repeat penalty explanation should be present"
  );
});

test("citation intent anchor-neighbor suppression removes adjacent same-doc anchors when cap allows >1", () => {
  const docA = mkDoc(
    "doc_anchor_a",
    "Anchor A",
    mkStats({ chunkCount: 4, chunkTypeSpread: 2, usedFallbackChunking: false, headingCount: 2, paragraphCount: 10, aligned: 2 }),
    [
      mkChunk({ documentId: "doc_anchor_a", title: "Anchor A", id: "aa1", text: "Rule 37.8", ordinal: 1 }),
      mkChunk({ documentId: "doc_anchor_a", title: "Anchor A", id: "aa2", text: "Rule 37.8", ordinal: 2 }),
      mkChunk({ documentId: "doc_anchor_a", title: "Anchor A", id: "aa3", text: "Rule 37.8", ordinal: 7 })
    ]
  );
  const docB = mkDoc(
    "doc_anchor_b",
    "Anchor B",
    mkStats({ chunkCount: 3, chunkTypeSpread: 2, usedFallbackChunking: false, headingCount: 2, paragraphCount: 8, aligned: 2 }),
    [mkChunk({ documentId: "doc_anchor_b", title: "Anchor B", id: "ab1", text: "Rule 37.8", ordinal: 1 })]
  );

  const report = buildRetrievalEvalReport({
    apiBase: "x",
    input: { realOnly: true, includeText: false },
    documents: [docA, docB],
    includeText: false,
    queries: [{ id: "citation_anchor_guard", query: "Rule 37.8", intent: "citation" }],
    admittedDocumentIdsOverride: ["doc_anchor_a", "doc_anchor_b"],
    rerankOptions: {
      citationIntentPerDocumentCap: 2,
      citationAnchorNeighborWindow: 1
    }
  });

  const top = report.queryResults[0].topResults;
  const docAOrdinals = top
    .filter((row) => row.documentId === "doc_anchor_a")
    .map((row) => Number((row.citationAnchorStart.match(/p(\d+)$/) || [])[1] || -1))
    .filter((n) => n >= 0)
    .sort((a, b) => a - b);

  for (let i = 1; i < docAOrdinals.length; i += 1) {
    assert.ok(Math.abs(docAOrdinals[i] - docAOrdinals[i - 1]) > 1, "adjacent anchors should not co-exist in top citation results");
  }
});
