import { buildRetrievalCorpusAdmissionReport } from "./retrieval-corpus-admission-utils.mjs";

function normalize(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s_-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenize(text) {
  const cleaned = normalize(text);
  if (!cleaned) return [];
  return cleaned.split(" ").filter(Boolean);
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(entries) {
  return Object.entries(entries)
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

function topCounts(rows, field, maxItems = 10) {
  return sortCountEntries(countBy(rows.map((row) => row[field]))).slice(0, maxItems);
}

const DEFAULT_QUERIES = [
  { id: "q_ordinance", query: "ordinance 37.2 authority discussion", intent: "authority" },
  { id: "q_findings", query: "findings of fact evidence credibility", intent: "findings" },
  { id: "q_issue_disposition", query: "issue presented and final disposition order", intent: "disposition" },
  { id: "q_procedural", query: "procedural history notice hearing continuance", intent: "procedural" },
  { id: "q_reasoning", query: "analysis reasoning legal standard application", intent: "analysis" }
];

function intentBoost(intent, chunk) {
  if (!intent) return 0;
  if (intent === "authority") {
    if (chunk.chunkType === "authority_discussion") return 1.5;
    if (chunk.hasCanonicalReferenceAlignment) return 1.0;
  }
  if (intent === "findings") {
    if (chunk.chunkType === "findings") return 1.5;
    if (chunk.containsFindings) return 1.0;
  }
  if (intent === "disposition") {
    if (chunk.chunkType === "holding_disposition") return 1.5;
    if (chunk.containsDispositionLanguage) return 1.0;
  }
  if (intent === "procedural") {
    if (chunk.chunkType === "procedural_history") return 1.5;
    if (chunk.containsProceduralHistory) return 1.0;
  }
  if (intent === "analysis") {
    if (chunk.chunkType === "analysis_reasoning") return 1.5;
  }
  return 0;
}

function priorityBoost(priority) {
  if (priority === "high") return 1.0;
  if (priority === "medium") return 0.5;
  return 0.1;
}

function sectionIntentBoost(intent, chunk) {
  const label = normalize(chunk.sectionLabel || "");
  if (!intent || !label) return 0;
  if (intent === "authority" && /authority|conclusions|law|rule|ordinance/.test(label)) return 0.6;
  if (intent === "findings" && /finding|fact/.test(label)) return 0.6;
  if (intent === "disposition" && /order|disposition|decision|holding/.test(label)) return 0.6;
  if (intent === "procedural" && /procedural|history|background/.test(label)) return 0.6;
  if (intent === "analysis" && /analysis|discussion|reasoning/.test(label)) return 0.6;
  return 0;
}

function referenceOverlapBoost(queryTokens, chunk) {
  const refs = [
    ...(chunk.citationFamilies || []),
    ...(chunk.ordinanceReferences || []),
    ...(chunk.rulesReferences || []),
    ...(chunk.canonicalOrdinanceReferences || []),
    ...(chunk.canonicalRulesReferences || []),
    ...(chunk.canonicalIndexCodes || [])
  ]
    .map((value) => normalize(value))
    .filter(Boolean);

  if (!refs.length || !queryTokens.length) return 0;
  const hits = queryTokens.filter((token) => refs.some((ref) => ref.includes(token)));
  if (!hits.length) return 0;
  return Math.min(1.1, 0.25 * hits.length);
}

function extractReferenceFamilyToken(value) {
  const normalized = normalize(value || "");
  const match = normalized.match(/\b\d+\.\d+\b/);
  if (match?.[0]) return match[0];
  return normalized;
}

function buildChunkReferenceFamilySignature(chunk) {
  const families = Array.from(new Set([
    ...(chunk.canonicalOrdinanceReferences || []),
    ...(chunk.canonicalRulesReferences || []),
    ...(chunk.canonicalIndexCodes || []),
    ...(chunk.ordinanceReferences || []),
    ...(chunk.rulesReferences || []),
    ...(chunk.citationFamilies || [])
  ]
    .map((value) => extractReferenceFamilyToken(value))
    .filter(Boolean)))
    .sort((a, b) => String(a).localeCompare(String(b)));
  if (!families.length) return "<none>";
  return families.join("|");
}

function buildCandidate(query, chunk) {
  const queryTokens = tokenize(query.query);
  const queryNormalized = normalize(query.query);
  const haystack = normalize(
    [
      chunk.title,
      chunk.chunkType,
      chunk.sectionLabel,
      chunk.sourceText,
      ...(chunk.citationFamilies || []),
      ...(chunk.ordinanceReferences || []),
      ...(chunk.rulesReferences || []),
      ...(chunk.canonicalOrdinanceReferences || []),
      ...(chunk.canonicalRulesReferences || []),
      ...(chunk.canonicalIndexCodes || [])
    ].join(" ")
  );

  const tokenHits = queryTokens.filter((token) => haystack.includes(token));
  const tokenOverlap = queryTokens.length ? tokenHits.length / queryTokens.length : 0;
  const phraseMatch = queryNormalized && haystack.includes(queryNormalized);

  const baseScore = Number((tokenOverlap * 4 + (phraseMatch ? 1.2 : 0)).toFixed(4));
  const intentAlignmentScore = Number((intentBoost(query.intent, chunk) + sectionIntentBoost(query.intent, chunk)).toFixed(4));
  const referenceAlignmentScore = Number((referenceOverlapBoost(queryTokens, chunk) + (chunk.hasCanonicalReferenceAlignment ? 0.6 : 0)).toFixed(4));
  const priorityScore = Number(priorityBoost(chunk.retrievalPriority).toFixed(4));

  const preliminaryScore = Number((baseScore + intentAlignmentScore + referenceAlignmentScore + priorityScore).toFixed(4));

  return {
    chunk,
    query,
    referenceFamilyKey: buildChunkReferenceFamilySignature(chunk),
    baseScore,
    intentAlignmentScore,
    referenceAlignmentScore,
    priorityScore,
    diversityAdjustment: 0,
    redundancyPenalty: 0,
    rerankScore: preliminaryScore,
    rankingExplanation: [
      `token_overlap=${tokenOverlap.toFixed(3)}`,
      `phrase_match=${phraseMatch}`,
      `intent_alignment=${intentAlignmentScore.toFixed(2)}`,
      `reference_alignment=${referenceAlignmentScore.toFixed(2)}`,
      `priority=${priorityScore.toFixed(2)}`
    ],
    rankingSignals: {
      tokenOverlap: Number(tokenOverlap.toFixed(4)),
      tokenHits,
      phraseMatch,
      intentAlignmentScore,
      referenceAlignmentScore,
      priorityScore
    }
  };
}

function applyCitationFamilyPenalty(query, candidates, enabled = true) {
  if (!enabled || query.intent !== "citation") return candidates;
  const familyCounts = countBy(candidates.map((candidate) => candidate.referenceFamilyKey || "<none>"));
  return candidates.map((candidate) => {
    const familyKey = candidate.referenceFamilyKey || "<none>";
    const familyCount = Number(familyCounts[familyKey] || 0);
    const familyPenalty = familyCount > 1 ? Number(Math.min(0.24, (familyCount - 1) * 0.03).toFixed(4)) : 0;
    const rerankScore = Number(Math.max(0, candidate.rerankScore - familyPenalty).toFixed(4));
    return {
      ...candidate,
      rerankScore,
      redundancyPenalty: Number((candidate.redundancyPenalty + familyPenalty).toFixed(4)),
      rankingExplanation: [
        ...candidate.rankingExplanation,
        ...(familyPenalty > 0 ? [`citation_family_repeat_penalty=${familyPenalty.toFixed(2)}:${familyKey}`] : [])
      ]
    };
  });
}

function toResultRow(candidate, includeText) {
  const chunk = candidate.chunk;
  return {
    documentId: chunk.documentId,
    title: chunk.title,
    chunkId: chunk.chunkId,
    chunkType: chunk.chunkType,
    sectionLabel: chunk.sectionLabel,
    score: candidate.rerankScore,
    baseScore: candidate.baseScore,
    rerankScore: candidate.rerankScore,
    diversityAdjustment: candidate.diversityAdjustment,
    intentAlignmentScore: candidate.intentAlignmentScore,
    referenceAlignmentScore: candidate.referenceAlignmentScore,
    priorityScore: candidate.priorityScore,
    redundancyPenalty: candidate.redundancyPenalty,
    rankingExplanation: candidate.rankingExplanation,
    rankingSignals: candidate.rankingSignals,
    citationAnchorStart: chunk.citationAnchorStart,
    citationAnchorEnd: chunk.citationAnchorEnd,
    sourceLink: chunk.sourceLink || chunk.provenance?.sourceLink || "",
    retrievalPriority: chunk.retrievalPriority,
    hasCanonicalReferenceAlignment: Boolean(chunk.hasCanonicalReferenceAlignment),
    excerpt: includeText ? String(chunk.sourceText || "").slice(0, 260) : ""
  };
}

function stableSortCandidates(candidates, scoreField = "rerankScore") {
  return [...candidates].sort((a, b) => {
    if (b[scoreField] !== a[scoreField]) return b[scoreField] - a[scoreField];
    const docCompare = String(a.chunk.documentId).localeCompare(String(b.chunk.documentId));
    if (docCompare !== 0) return docCompare;
    return String(a.chunk.chunkId).localeCompare(String(b.chunk.chunkId));
  });
}

function parseAnchorOrdinal(anchor) {
  const raw = String(anchor || "");
  if (!raw) return null;
  const match = raw.match(/(?:^|[#:_-])p(\d+)\b/i);
  if (!match) return null;
  const parsed = Number.parseInt(match[1] || "", 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function queryIntentMatchesChunkType(intent, chunkType) {
  if (!intent) return false;
  if (intent === "authority" && chunkType === "authority_discussion") return true;
  if (intent === "findings" && chunkType === "findings") return true;
  if (intent === "disposition" && chunkType === "holding_disposition") return true;
  if (intent === "procedural" && chunkType === "procedural_history") return true;
  if (intent === "analysis" && chunkType === "analysis_reasoning") return true;
  return false;
}

function applyDiversityRerank(query, candidates, limit = 20, options = {}) {
  const citationIntentPerDocumentCap = Number(options.citationIntentPerDocumentCap ?? 1);
  const citationAnchorNeighborWindow = Number(options.citationAnchorNeighborWindow ?? 1);
  const selected = [];
  const remaining = [...candidates];
  const perDocSelected = new Map();
  const selectedAnchorOrdinalsByDoc = new Map();

  const penaltyCounts = {
    same_document_penalty: 0,
    same_section_penalty: 0,
    same_anchor_penalty: 0,
    adjacent_chunk_penalty: 0,
    repeated_type_penalty: 0
  };

  while (selected.length < limit && remaining.length) {
    const rescored = remaining.map((candidate) => {
      const sameDocCount = selected.filter((row) => row.chunk.documentId === candidate.chunk.documentId).length;
      const sameSectionCount = selected.filter(
        (row) =>
          row.chunk.documentId === candidate.chunk.documentId &&
          String(row.chunk.sectionCanonicalKey || "") === String(candidate.chunk.sectionCanonicalKey || "")
      ).length;
      const sameAnchorCount = selected.filter(
        (row) =>
          row.chunk.documentId === candidate.chunk.documentId &&
          String(row.chunk.citationAnchorStart || "") === String(candidate.chunk.citationAnchorStart || "")
      ).length;

      const selectedOrdinal = selected
        .filter((row) => row.chunk.documentId === candidate.chunk.documentId)
        .map((row) => Number(row.chunk.chunkOrdinal ?? -1000));
      const candidateOrdinal = Number(candidate.chunk.chunkOrdinal ?? -1000);
      const adjacentHit = selectedOrdinal.some((ord) => Math.abs(ord - candidateOrdinal) <= 1);

      const sameTypeCount = selected.filter((row) => row.chunk.chunkType === candidate.chunk.chunkType).length;
      const intentTypeMatch = queryIntentMatchesChunkType(query.intent, candidate.chunk.chunkType);

      const documentNoveltyBoost = sameDocCount === 0 ? 0.45 : 0;
      const typeNoveltyBoost = sameTypeCount === 0 ? 0.2 : 0;
      const diversityAdjustment = Number((documentNoveltyBoost + typeNoveltyBoost).toFixed(4));

      let redundancyPenalty = 0;
      const explanation = [...candidate.rankingExplanation];

      if (sameDocCount > 0) {
        const p = Number((0.65 * sameDocCount).toFixed(4));
        redundancyPenalty += p;
        explanation.push(`same_document_penalty=${p.toFixed(2)}`);
      }
      if (sameSectionCount > 0) {
        const p = Number((0.85 * sameSectionCount).toFixed(4));
        redundancyPenalty += p;
        explanation.push(`same_section_penalty=${p.toFixed(2)}`);
      }
      if (sameAnchorCount > 0) {
        const p = Number((0.75 * sameAnchorCount).toFixed(4));
        redundancyPenalty += p;
        explanation.push(`same_anchor_penalty=${p.toFixed(2)}`);
      }
      if (adjacentHit) {
        redundancyPenalty += 0.5;
        explanation.push("adjacent_chunk_penalty=0.50");
      }
      if (sameTypeCount >= 2 && !intentTypeMatch) {
        const p = Number((0.35 * (sameTypeCount - 1)).toFixed(4));
        redundancyPenalty += p;
        explanation.push(`repeated_type_penalty=${p.toFixed(2)}`);
      }

      redundancyPenalty = Number(redundancyPenalty.toFixed(4));
      const rerankScore = Number((candidate.baseScore + candidate.intentAlignmentScore + candidate.referenceAlignmentScore + candidate.priorityScore + diversityAdjustment - redundancyPenalty).toFixed(4));

      return {
        ...candidate,
        diversityAdjustment,
        redundancyPenalty,
        rerankScore,
        rankingExplanation: explanation
      };
    });

    const ranked = stableSortCandidates(rescored, "rerankScore");
    let picked = null;
    if (query.intent === "citation") {
      for (const candidate of ranked) {
        const docId = String(candidate.chunk.documentId || "");
        const docCount = Number(perDocSelected.get(docId) || 0);
        if (docCount >= citationIntentPerDocumentCap) continue;
        const candidateOrdinal = parseAnchorOrdinal(candidate.chunk.citationAnchorStart || "");
        const selectedOrdinals = selectedAnchorOrdinalsByDoc.get(docId) || [];
        if (
          candidateOrdinal != null &&
          selectedOrdinals.some((ord) => Math.abs(Number(ord) - candidateOrdinal) <= citationAnchorNeighborWindow)
        ) {
          continue;
        }
        picked = candidate;
        break;
      }
    } else {
      picked = ranked[0];
    }
    if (!picked) break;

    if (picked.redundancyPenalty > 0) {
      if (picked.rankingExplanation.some((s) => s.startsWith("same_document_penalty="))) penaltyCounts.same_document_penalty += 1;
      if (picked.rankingExplanation.some((s) => s.startsWith("same_section_penalty="))) penaltyCounts.same_section_penalty += 1;
      if (picked.rankingExplanation.some((s) => s.startsWith("same_anchor_penalty="))) penaltyCounts.same_anchor_penalty += 1;
      if (picked.rankingExplanation.some((s) => s.startsWith("adjacent_chunk_penalty="))) penaltyCounts.adjacent_chunk_penalty += 1;
      if (picked.rankingExplanation.some((s) => s.startsWith("repeated_type_penalty="))) penaltyCounts.repeated_type_penalty += 1;
    }

    selected.push(picked);
    const pickedDocId = String(picked.chunk.documentId || "");
    perDocSelected.set(pickedDocId, Number(perDocSelected.get(pickedDocId) || 0) + 1);
    const pickedOrdinal = parseAnchorOrdinal(picked.chunk.citationAnchorStart || "");
    if (pickedOrdinal != null) {
      selectedAnchorOrdinalsByDoc.set(pickedDocId, [...(selectedAnchorOrdinalsByDoc.get(pickedDocId) || []), pickedOrdinal]);
    }
    const idx = remaining.findIndex((row) => row.chunk.chunkId === picked.chunk.chunkId);
    if (idx >= 0) remaining.splice(idx, 1);
  }

  return {
    results: selected,
    penaltyCounts
  };
}

function diversityStats(results, topN = 10) {
  const top = results.slice(0, topN);
  const docCounts = countBy(top.map((row) => row.documentId));
  const chunkTypeCounts = countBy(top.map((row) => row.chunkType));
  const uniqueDocuments = Object.keys(docCounts).length;
  const uniqueChunkTypes = Object.keys(chunkTypeCounts).length;

  const maxShare = top.length
    ? Number((Math.max(...Object.values(docCounts).map((v) => Number(v || 0))) / top.length).toFixed(4))
    : 0;

  let maxStreak = 0;
  let currentStreak = 0;
  let prevDoc = "";
  for (const row of top) {
    if (row.documentId === prevDoc) {
      currentStreak += 1;
    } else {
      currentStreak = 1;
      prevDoc = row.documentId;
    }
    if (currentStreak > maxStreak) maxStreak = currentStreak;
  }

  return {
    uniqueDocuments,
    uniqueChunkTypes,
    topDocumentShare: maxShare,
    sameDocumentTopResultStreak: maxStreak,
    documentCounts: docCounts,
    chunkTypeCounts
  };
}

export function buildAdmittedCorpus({ apiBase, input, documents, admittedDocumentIdsOverride = null }) {
  const admission = buildRetrievalCorpusAdmissionReport({ apiBase, input, documents });
  const baseAdmittedIds = admission.documentsEligibleForInitialEmbedding || [];
  const admittedSet = new Set(
    Array.isArray(admittedDocumentIdsOverride) && admittedDocumentIdsOverride.length
      ? admittedDocumentIdsOverride
      : baseAdmittedIds
  );

  const admittedDocs = (documents || []).filter((doc) => admittedSet.has(doc.document?.documentId));
  const admittedChunks = admittedDocs.flatMap((doc) =>
    (doc.chunks || []).map((chunk) => ({
      ...chunk,
      sourceLink: doc.document?.sourceLink || ""
    }))
  );

  const embeddingPrepRows = admittedChunks.map((chunk) => ({
    embeddingId: `embprep_${chunk.chunkId}`,
    documentId: chunk.documentId,
    chunkId: chunk.chunkId,
    title: chunk.title,
    chunkType: chunk.chunkType,
    retrievalPriority: chunk.retrievalPriority,
    hasCanonicalReferenceAlignment: Boolean(chunk.hasCanonicalReferenceAlignment),
    citationAnchorStart: chunk.citationAnchorStart,
    citationAnchorEnd: chunk.citationAnchorEnd,
    sourceLink: chunk.sourceLink,
    sourceText: chunk.sourceText,
    tokenEstimate: chunk.tokenEstimate,
    metadata: {
      sectionLabel: chunk.sectionLabel,
      sectionCanonicalKey: chunk.sectionCanonicalKey,
      chunkRepairApplied: Boolean(chunk.chunkRepairApplied),
      chunkRepairStrategy: chunk.chunkRepairStrategy,
      citationFamilies: chunk.citationFamilies || []
    }
  }));

  return {
    admission,
    baseAdmittedIds: [...baseAdmittedIds].sort((a, b) => String(a).localeCompare(String(b))),
    admittedDocumentIds: Array.from(admittedSet).sort((a, b) => String(a).localeCompare(String(b))),
    admittedDocs,
    admittedChunks,
    embeddingPrepRows,
    admittedDocumentIdsOverrideApplied: Array.isArray(admittedDocumentIdsOverride) && admittedDocumentIdsOverride.length > 0
  };
}

export function buildRetrievalEvalReport({
  apiBase,
  input,
  documents,
  queries = DEFAULT_QUERIES,
  includeText = false,
  admittedDocumentIdsOverride = null,
  rerankOptions = {}
}) {
  const corpus = buildAdmittedCorpus({ apiBase, input, documents, admittedDocumentIdsOverride });
  const admittedChunks = corpus.admittedChunks;

  const queryRows = queries.map((query) => {
    const baseCandidates = admittedChunks
      .map((chunk) => buildCandidate(query, chunk))
      .filter((candidate) => candidate.rerankScore > 0);
    const candidates = applyCitationFamilyPenalty(
      query,
      baseCandidates,
      rerankOptions.citationFamilyPenaltyEnabled !== false
    );

    const beforeRanked = stableSortCandidates(candidates, "rerankScore");
    const beforeResults = beforeRanked.slice(0, 20).map((candidate) => toResultRow(candidate, includeText));

    const reranked = applyDiversityRerank(query, beforeRanked, 20, {
      citationIntentPerDocumentCap:
        rerankOptions.citationIntentPerDocumentCap == null ? 1 : Number(rerankOptions.citationIntentPerDocumentCap),
      citationAnchorNeighborWindow:
        rerankOptions.citationAnchorNeighborWindow == null ? 1 : Number(rerankOptions.citationAnchorNeighborWindow)
    });
    const afterResults = reranked.results.map((candidate) => toResultRow(candidate, includeText));

    const beforeTop = beforeResults.slice(0, 10);
    const afterTop = afterResults.slice(0, 10);
    const beforeStats = diversityStats(beforeTop);
    const afterStats = diversityStats(afterTop);

    const topScore = afterResults[0]?.rerankScore || 0;
    const lowConfidence = topScore > 0 && topScore < 2.2;
    const improved =
      afterStats.uniqueDocuments > beforeStats.uniqueDocuments ||
      afterStats.uniqueChunkTypes > beforeStats.uniqueChunkTypes ||
      afterStats.topDocumentShare < beforeStats.topDocumentShare;

    return {
      queryId: query.id,
      query: query.query,
      intent: query.intent,
      resultCount: afterResults.length,
      lowConfidence,
      improvedByDiversification: improved,
      beforeTopResults: beforeResults,
      topResults: afterResults,
      beforeDiversity: beforeStats,
      afterDiversity: afterStats,
      redundancyPenaltyCounts: reranked.penaltyCounts
    };
  });

  const beforeFlattenedTop = queryRows.flatMap((row) => row.beforeTopResults.slice(0, 5));
  const afterFlattenedTop = queryRows.flatMap((row) => row.topResults.slice(0, 5));

  const queriesWithNoResults = queryRows.filter((row) => row.resultCount === 0).map((row) => row.query);
  const queriesWithLowConfidenceResults = queryRows.filter((row) => row.lowConfidence).map((row) => row.query);
  const queriesImprovedByDiversification = queryRows.filter((row) => row.improvedByDiversification).map((row) => row.query);

  const uniqueDocumentsPerQuery = queryRows.map((row) => ({ query: row.query, uniqueDocuments: row.afterDiversity.uniqueDocuments }));
  const uniqueChunkTypesPerQuery = queryRows.map((row) => ({ query: row.query, uniqueChunkTypes: row.afterDiversity.uniqueChunkTypes }));
  const sameDocumentTopResultStreaks = queryRows.map((row) => ({ query: row.query, before: row.beforeDiversity.sameDocumentTopResultStreak, after: row.afterDiversity.sameDocumentTopResultStreak }));

  const resultDiversityByQuery = queryRows.map((row) => ({
    query: row.query,
    beforeUniqueDocuments: row.beforeDiversity.uniqueDocuments,
    afterUniqueDocuments: row.afterDiversity.uniqueDocuments,
    beforeUniqueChunkTypes: row.beforeDiversity.uniqueChunkTypes,
    afterUniqueChunkTypes: row.afterDiversity.uniqueChunkTypes,
    beforeTopDocumentShare: row.beforeDiversity.topDocumentShare,
    afterTopDocumentShare: row.afterDiversity.topDocumentShare
  }));

  const topResultShareByDocument = sortCountEntries(countBy(afterFlattenedTop.map((row) => row.documentId))).map((row) => ({
    documentId: row.key,
    hitCount: row.count,
    share: afterFlattenedTop.length ? Number((row.count / afterFlattenedTop.length).toFixed(4)) : 0
  }));

  const documentDominanceStats = {
    beforeMaxTopResultShare: queryRows.length ? Number(Math.max(...queryRows.map((row) => row.beforeDiversity.topDocumentShare)).toFixed(4)) : 0,
    afterMaxTopResultShare: queryRows.length ? Number(Math.max(...queryRows.map((row) => row.afterDiversity.topDocumentShare)).toFixed(4)) : 0,
    queriesWithSingleDocumentTop5Before: queryRows.filter((row) => row.beforeDiversity.uniqueDocuments <= 1).length,
    queriesWithSingleDocumentTop5After: queryRows.filter((row) => row.afterDiversity.uniqueDocuments <= 1).length
  };

  const retrievalCoverageByDocument = sortCountEntries(countBy(afterFlattenedTop.map((row) => row.documentId))).map((row) => ({
    documentId: row.key,
    hitCount: row.count
  }));
  const retrievalCoverageByChunkType = sortCountEntries(countBy(afterFlattenedTop.map((row) => row.chunkType))).map((row) => ({
    chunkType: row.key,
    hitCount: row.count
  }));

  const redundancyPenaltyCounts = queryRows.reduce((acc, row) => {
    for (const [key, value] of Object.entries(row.redundancyPenaltyCounts || {})) {
      acc[key] = (acc[key] || 0) + Number(value || 0);
    }
    return acc;
  }, {});

  const summary = {
    documentsAnalyzed: (documents || []).length,
    admittedDocumentCount: corpus.admittedDocumentIds.length,
    admittedChunkCount: admittedChunks.length,
    heldOrExcludedDocumentCount: (documents || []).length - corpus.admittedDocumentIds.length,
    queriesEvaluated: queryRows.length,
    averageResultsPerQuery: Number((queryRows.reduce((sum, row) => sum + row.resultCount, 0) / Math.max(1, queryRows.length)).toFixed(2)),
    queriesWithLowConfidenceResults: queriesWithLowConfidenceResults.length,
    queriesWithNoResults: queriesWithNoResults.length,
    queriesImprovedByDiversification: queriesImprovedByDiversification.length
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    queriesEvaluated: queries.map((row) => ({ queryId: row.id, query: row.query, intent: row.intent })),
    beforeTopResultChunkTypes: topCounts(beforeFlattenedTop, "chunkType", 10),
    afterTopResultChunkTypes: topCounts(afterFlattenedTop, "chunkType", 10),
    beforeTopResultDocuments: topCounts(beforeFlattenedTop, "documentId", 10),
    afterTopResultDocuments: topCounts(afterFlattenedTop, "documentId", 10),
    topResultChunkTypes: topCounts(afterFlattenedTop, "chunkType", 10),
    topResultDocuments: topCounts(afterFlattenedTop, "documentId", 10),
    averageResultsPerQuery: summary.averageResultsPerQuery,
    queriesWithLowConfidenceResults,
    queriesWithNoResults,
    queriesImprovedByDiversification,
    resultDiversityByQuery,
    uniqueDocumentsPerQuery,
    uniqueChunkTypesPerQuery,
    sameDocumentTopResultStreaks,
    redundancyPenaltyCounts,
    documentDominanceStats,
    topResultShareByDocument,
    retrievalCoverageByDocument,
    retrievalCoverageByChunkType,
    queryResults: queryRows,
    admittedCorpus: {
      baseAdmittedDocumentIds: corpus.baseAdmittedIds,
      admittedDocumentIds: corpus.admittedDocumentIds,
      documentsEligibleForInitialEmbedding: corpus.admission.documentsEligibleForInitialEmbedding,
      documentsEligibleForSearchExposure: corpus.admission.documentsEligibleForSearchExposure,
      excludedDocumentIds: corpus.admission.excludeFromInitialCorpusDocuments.map((row) => row.documentId),
      heldDocumentIds: corpus.admission.holdForRepairReviewDocuments.map((row) => row.documentId),
      admittedDocumentIdsOverrideApplied: corpus.admittedDocumentIdsOverrideApplied
    },
    embeddingPrep: {
      rowCount: corpus.embeddingPrepRows.length,
      rows: corpus.embeddingPrepRows
    }
  };
}

export function formatRetrievalEvalMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Eval Report (Admitted Corpus Only)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Dominance");
  for (const [k, v] of Object.entries(report.documentDominanceStats || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Before/After Top Result Documents");
  lines.push("- Before:");
  for (const row of report.beforeTopResultDocuments || []) lines.push(`  - ${row.key}: ${row.count}`);
  if (!(report.beforeTopResultDocuments || []).length) lines.push("  - none");
  lines.push("- After:");
  for (const row of report.afterTopResultDocuments || []) lines.push(`  - ${row.key}: ${row.count}`);
  if (!(report.afterTopResultDocuments || []).length) lines.push("  - none");
  lines.push("");

  lines.push("## Before/After Top Result Chunk Types");
  lines.push("- Before:");
  for (const row of report.beforeTopResultChunkTypes || []) lines.push(`  - ${row.key}: ${row.count}`);
  if (!(report.beforeTopResultChunkTypes || []).length) lines.push("  - none");
  lines.push("- After:");
  for (const row of report.afterTopResultChunkTypes || []) lines.push(`  - ${row.key}: ${row.count}`);
  if (!(report.afterTopResultChunkTypes || []).length) lines.push("  - none");
  lines.push("");

  lines.push("## Query Results (Before/After)");
  for (const row of report.queryResults || []) {
    lines.push(`- ${row.query} | results=${row.resultCount} lowConfidence=${row.lowConfidence} improved=${row.improvedByDiversification}`);
    lines.push("  - before:");
    for (const top of (row.beforeTopResults || []).slice(0, 3)) {
      lines.push(`    - ${top.documentId} | ${top.chunkId} | ${top.chunkType} | score=${top.score}`);
    }
    lines.push("  - after:");
    for (const top of (row.topResults || []).slice(0, 3)) {
      lines.push(`    - ${top.documentId} | ${top.chunkId} | ${top.chunkType} | score=${top.score}`);
    }
  }
  lines.push("");

  lines.push("## Admitted Corpus");
  lines.push(`- admittedDocumentIds: ${(report.admittedCorpus?.admittedDocumentIds || []).length}`);
  lines.push(`- documentsEligibleForInitialEmbedding: ${(report.admittedCorpus?.documentsEligibleForInitialEmbedding || []).length}`);
  lines.push(`- documentsEligibleForSearchExposure: ${(report.admittedCorpus?.documentsEligibleForSearchExposure || []).length}`);
  lines.push(`- heldDocumentIds: ${(report.admittedCorpus?.heldDocumentIds || []).length}`);
  lines.push(`- excludedDocumentIds: ${(report.admittedCorpus?.excludedDocumentIds || []).length}`);
  lines.push("");

  lines.push("## Embedding Prep (Read-only)");
  lines.push(`- rowCount: ${report.embeddingPrep?.rowCount || 0}`);
  lines.push("");

  lines.push("- Read-only eval only. No embedding/vector/search-index writes are performed in this phase.");
  return `${lines.join("\n")}\n`;
}

export { DEFAULT_QUERIES };
