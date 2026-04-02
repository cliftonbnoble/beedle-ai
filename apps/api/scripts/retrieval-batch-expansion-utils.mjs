import { LIVE_SEARCH_QA_QUERIES, buildRetrievalLiveSearchQaReport } from "./retrieval-live-search-qa-utils.mjs";
import { buildRetrievalEvalReport } from "./retrieval-eval-utils.mjs";

function normalizeChunkType(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
}

function isLowSignalStructuralChunkType(value) {
  const t = normalizeChunkType(value);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(obj) {
  return Object.entries(obj || {})
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function expectedChunkTypesByQueryId() {
  return new Map(LIVE_SEARCH_QA_QUERIES.map((q) => [q.id, (q.expectedChunkTypes || []).map((v) => normalizeChunkType(v))]));
}

function mapByDocument(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const id = row?.documentId;
    if (!id) continue;
    map.set(String(id), row);
  }
  return map;
}

function mapByDocumentFromPreviews(previews) {
  const map = new Map();
  for (const preview of previews || []) {
    const id = String(preview?.document?.documentId || "");
    if (!id) continue;
    const chunks = Array.isArray(preview?.chunks) ? preview.chunks : [];
    const types = chunks.map((chunk) => normalizeChunkType(chunk?.chunkType || chunk?.sectionLabel || ""));
    const lowSignalCount = types.filter((type) => isLowSignalStructuralChunkType(type)).length;
    const lowSignalShare = chunks.length ? Number((lowSignalCount / chunks.length).toFixed(4)) : 0;
    map.set(id, {
      documentId: id,
      title: String(preview?.document?.title || ""),
      chunkCount: chunks.length,
      chunkTypeCounts: sortCountEntries(countBy(types)),
      lowSignalChunkCount: lowSignalCount,
      lowSignalChunkShare: lowSignalShare
    });
  }
  return map;
}

function materialBool(value) {
  return value === true || value === 1;
}

export function scoreExpansionCandidate({ admissionRow, enrichmentRow, rehearsalRow }) {
  let score = 0;
  const reasons = [];
  const blockers = [];

  const keyStats = admissionRow?.keyStats || enrichmentRow?.keyStats || {};
  const spread = Number(keyStats.chunkTypeSpread || 0);
  const mixedTopic = Number(keyStats.chunksFlaggedMixedTopic || 0);
  const fallback = materialBool(keyStats.usedFallbackChunking);

  const preAlign = Number(enrichmentRow?.preEnrichmentCanonicalAlignmentCount || admissionRow?.preEnrichmentCanonicalAlignmentCount || 0);
  const postAlign = Number(enrichmentRow?.postEnrichmentCanonicalAlignmentCount || admissionRow?.postEnrichmentCanonicalAlignmentCount || 0);
  const alignGain = Math.max(0, postAlign - preAlign);

  if (alignGain > 0) {
    score += Math.min(22, alignGain * 6);
    reasons.push(`alignment_gain_${alignGain}`);
  }

  if (postAlign >= 3) {
    score += 16;
    reasons.push("strong_post_alignment");
  } else if (postAlign >= 2) {
    score += 10;
    reasons.push("moderate_post_alignment");
  }

  if (spread >= 4) {
    score += 14;
    reasons.push("strong_chunk_type_spread");
  } else if (spread >= 3) {
    score += 10;
    reasons.push("adequate_chunk_type_spread");
  } else if (spread >= 2) {
    score += 4;
    reasons.push("limited_chunk_type_spread");
  } else {
    blockers.push("low_chunk_type_spread");
    score -= 10;
  }

  if (mixedTopic === 0) {
    score += 10;
    reasons.push("mixed_topic_absent");
  } else if (mixedTopic === 1) {
    score += 2;
    blockers.push("mixed_topic_present");
  } else {
    score -= 8;
    blockers.push("mixed_topic_high");
  }

  if (!fallback) {
    score += 10;
    reasons.push("fallback_not_used");
  } else {
    score -= 6;
    blockers.push("fallback_in_use");
  }

  const rehearsalDelta = Number(rehearsalRow?.promotionDeltaScore || 0);
  if (rehearsalDelta > 0) {
    score += Math.min(12, rehearsalDelta / 3);
    reasons.push("positive_rehearsal_delta");
  }

  if (materialBool(admissionRow?.wouldPromoteIfCombinedNarrowRepairs) || materialBool(enrichmentRow?.wouldPromoteIfCombinedNarrowRepairs)) {
    score += 12;
    reasons.push("combined_repair_promotable");
  }

  const admissionWarnings = new Set([...(admissionRow?.corpusAdmissionWarnings || []), ...(admissionRow?.warningReasons || [])]);
  if (admissionWarnings.has("reference_alignment_below_preferred_corpus_threshold")) {
    blockers.push("alignment_threshold_gap");
    score -= 4;
  }
  if (admissionWarnings.has("requires_additional_structure_review_before_corpus_admission")) {
    blockers.push("structure_review_needed");
    score -= 4;
  }

  score = Number(Math.max(0, Math.min(100, score)).toFixed(2));

  return {
    candidateScore: score,
    candidateReasons: unique(reasons),
    candidateBlockers: unique(blockers)
  };
}

export function selectNextBatchCandidates({
  corpusAdmissionRows,
  referenceEnrichmentRows,
  rehearsalRows,
  trustedDocumentIds,
  batchSize = 10,
  documentChunkProfiles,
  excludeLowSignalDominated = false,
  lowSignalShareThreshold = 0.5
}) {
  const trusted = new Set((trustedDocumentIds || []).map((id) => String(id)));
  const enrichMap = mapByDocument(referenceEnrichmentRows || []);
  const rehearsalMap = mapByDocument(rehearsalRows || []);
  const chunkProfileMap = documentChunkProfiles instanceof Map ? documentChunkProfiles : mapByDocumentFromPreviews(documentChunkProfiles || []);

  const ranked = [];
  const excluded = [];

  for (const row of corpusAdmissionRows || []) {
    const documentId = String(row?.documentId || "");
    if (!documentId) continue;

    const exclusionReasons = [];

    if (materialBool(row?.isLikelyFixture)) exclusionReasons.push("fixture_excluded");
    if (trusted.has(documentId)) exclusionReasons.push("already_trusted_excluded");
    if (String(row?.corpusAdmissionStatus || "") !== "hold_for_repair_review") exclusionReasons.push("not_hold_status_excluded");
    if (!materialBool(row?.include ?? true)) exclusionReasons.push("not_included_row_excluded");

    const enrichmentRow = enrichMap.get(documentId) || row;
    const rehearsalRow = rehearsalMap.get(documentId) || null;
    const chunkProfile = chunkProfileMap.get(documentId) || null;

    if (!enrichmentRow) exclusionReasons.push("missing_enrichment_context");
    if (/retrieval messy headings/i.test(String(row?.title || enrichmentRow?.title || ""))) {
      exclusionReasons.push("previous_regression_family_excluded");
    }
    if (excludeLowSignalDominated && chunkProfile && Number(chunkProfile.lowSignalChunkShare || 0) > Number(lowSignalShareThreshold || 0.5)) {
      exclusionReasons.push("low_signal_structural_dominated");
    }

    if (exclusionReasons.length) {
      excluded.push({
        documentId,
        title: row?.title || "",
        exclusionReasons
      });
      continue;
    }

    const scored = scoreExpansionCandidate({ admissionRow: row, enrichmentRow, rehearsalRow });
    ranked.push({
      documentId,
      title: row?.title || enrichmentRow?.title || "",
      corpusAdmissionStatus: row?.corpusAdmissionStatus || "hold_for_repair_review",
      candidateScore: scored.candidateScore,
      candidateReasons: scored.candidateReasons,
      candidateBlockers: scored.candidateBlockers,
      keyStats: row?.keyStats || enrichmentRow?.keyStats || {},
      chunkProfile,
      sourceLink: row?.sourceLink || enrichmentRow?.sourceLink || "",
      sourceFileRef: row?.sourceFileRef || enrichmentRow?.sourceFileRef || "",
      referenceEnrichmentStrategies: enrichmentRow?.referenceEnrichmentStrategies || [],
      promotionDeltaScore: Number(rehearsalRow?.promotionDeltaScore || row?.promotionDeltaScore || 0)
    });
  }

  ranked.sort((a, b) => {
    if (b.candidateScore !== a.candidateScore) return b.candidateScore - a.candidateScore;
    if (b.promotionDeltaScore !== a.promotionDeltaScore) return b.promotionDeltaScore - a.promotionDeltaScore;
    return String(a.documentId).localeCompare(String(b.documentId));
  });

  const selected = ranked.slice(0, Math.max(1, Number(batchSize || 10)));

  return {
    selected,
    ranked,
    excluded,
    exclusionReasonCounts: sortCountEntries(countBy(excluded.flatMap((row) => row.exclusionReasons || [])))
  };
}

function computeQualityForEvalQuery(queryRow, expectedChunkTypesNormalized) {
  const top = (queryRow?.topResults || []).slice(0, 10);
  const count = top.length;
  const normChunkTypes = top.map((r) => normalizeChunkType(r.chunkType));
  const expectedHits = expectedChunkTypesNormalized.length
    ? normChunkTypes.filter((type) => expectedChunkTypesNormalized.includes(type)).length
    : 0;
  const expectedTypeHitRate = count ? Number((expectedHits / count).toFixed(4)) : 0;

  const rerankScores = top.map((r) => Number(r.rerankScore || r.score || 0));
  const max = rerankScores.length ? Math.max(...rerankScores) : 0;
  const avg = rerankScores.length ? Number((rerankScores.reduce((s, n) => s + n, 0) / rerankScores.length).toFixed(4)) : 0;
  const rerankNorm = max > 0 ? Number(Math.min(1, avg / max).toFixed(4)) : 0;

  const provenanceHits = top.filter((r) => r.sourceLink && r.documentId && r.chunkId && r.citationAnchorStart && r.citationAnchorEnd).length;
  const provenanceCompleteness = count ? Number((provenanceHits / count).toFixed(4)) : 0;
  const anchorHits = top.filter((r) => r.citationAnchorStart && r.citationAnchorEnd).length;
  const citationAnchorCoverage = count ? Number((anchorHits / count).toFixed(4)) : 0;

  const uniqueDocs = unique(top.map((r) => r.documentId)).length;
  const uniqueTypes = unique(normChunkTypes).length;
  const docDiversityRatio = count ? Number((uniqueDocs / count).toFixed(4)) : 0;
  const typeDiversityRatio = count ? Number((uniqueTypes / count).toFixed(4)) : 0;

  const topShare = count ? Number((Math.max(...Object.values(countBy(top.map((r) => r.documentId))).map((v) => Number(v || 0))) / count).toFixed(4)) : 0;
  let streak = 0;
  let maxStreak = 0;
  let lastDoc = "";
  for (const row of top) {
    if (row.documentId === lastDoc) streak += 1;
    else {
      streak = 1;
      lastDoc = row.documentId;
    }
    if (streak > maxStreak) maxStreak = streak;
  }
  const duplicatePressure = Number(((topShare + Math.min(1, maxStreak / 5)) / 2).toFixed(4));

  const qualityScore = Number(
    (
      expectedTypeHitRate * 30 +
      rerankNorm * 15 +
      provenanceCompleteness * 15 +
      citationAnchorCoverage * 10 +
      docDiversityRatio * 10 +
      typeDiversityRatio * 10 +
      (1 - duplicatePressure) * 10
    ).toFixed(2)
  );

  return {
    qualityScore,
    expectedTypeHitRate,
    provenanceCompleteness,
    citationAnchorCoverage,
    uniqueDocuments: uniqueDocs,
    uniqueChunkTypes: uniqueTypes,
    topDocumentShare: topShare,
    duplicatePressure,
    resultCount: Number(queryRow?.resultCount || 0)
  };
}

export function summarizeEvalAsQa(evalReport) {
  const expectedMap = expectedChunkTypesByQueryId();
  const rows = (evalReport?.queryResults || []).map((row) => {
    const expected = expectedMap.get(String(row.queryId || "")) || [];
    const metrics = computeQualityForEvalQuery(row, expected);
    return {
      queryId: row.queryId,
      query: row.query,
      metrics
    };
  });

  const summary = {
    queriesEvaluated: rows.length,
    totalApiResultsAcrossQueries: rows.reduce((sum, row) => sum + Number(row.metrics.resultCount || 0), 0),
    zeroTrustedResultQueryCount: rows.filter((row) => row.metrics.resultCount === 0).length,
    averageQualityScore: Number((rows.reduce((sum, row) => sum + Number(row.metrics.qualityScore || 0), 0) / Math.max(1, rows.length)).toFixed(2)),
    outOfCorpusHitQueryCount: 0,
    duplicateFloodingQueryCount: rows.filter((row) => row.metrics.duplicatePressure > 0.7).length,
    provenanceCompletenessAverage: Number((rows.reduce((sum, row) => sum + Number(row.metrics.provenanceCompleteness || 0), 0) / Math.max(1, rows.length)).toFixed(4)),
    citationAnchorCoverageAverage: Number((rows.reduce((sum, row) => sum + Number(row.metrics.citationAnchorCoverage || 0), 0) / Math.max(1, rows.length)).toFixed(4)),
    uniqueDocumentsPerQueryAvg: Number((rows.reduce((sum, row) => sum + Number(row.metrics.uniqueDocuments || 0), 0) / Math.max(1, rows.length)).toFixed(4)),
    uniqueChunkTypesPerQueryAvg: Number((rows.reduce((sum, row) => sum + Number(row.metrics.uniqueChunkTypes || 0), 0) / Math.max(1, rows.length)).toFixed(4)),
    topDocumentShareAverage: Number((rows.reduce((sum, row) => sum + Number(row.metrics.topDocumentShare || 0), 0) / Math.max(1, rows.length)).toFixed(4))
  };

  return {
    summary,
    byQuery: rows
  };
}

function computeLowSignalShareByIntent(evalReport) {
  const queryRows = Array.isArray(evalReport?.queryResults) ? evalReport.queryResults : [];
  const nonStructural = queryRows.filter((row) => {
    const queryId = String(row?.queryId || "");
    return !/citation_/.test(queryId);
  });

  let total = 0;
  let low = 0;
  for (const row of nonStructural) {
    const top = Array.isArray(row?.topResults) ? row.topResults.slice(0, 10) : [];
    total += top.length;
    low += top.filter((item) => isLowSignalStructuralChunkType(item?.chunkType || item?.sectionLabel || "")).length;
  }
  return total ? Number((low / total).toFixed(4)) : 0;
}

function computeCitationTopDocumentShareAverage(byQueryRows) {
  const rows = (byQueryRows || []).filter((row) => /citation_/.test(String(row?.queryId || "")));
  if (!rows.length) return 0;
  return Number((rows.reduce((sum, row) => sum + Number(row?.metrics?.topDocumentShare || 0), 0) / rows.length).toFixed(4));
}

export function evaluateExpansionGate({ baselineSummary, expandedSummary, maxQualityRegression = 3 }) {
  const checks = {
    noOutOfCorpusLeakage: Number(expandedSummary?.outOfCorpusHitQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(expandedSummary?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(expandedSummary?.citationAnchorCoverageAverage || 0) === 1,
    zeroTrustedResultStillZero: Number(expandedSummary?.zeroTrustedResultQueryCount || 0) === 0,
    noMaterialQualityRegression:
      Number(expandedSummary?.averageQualityScore || 0) >= Number(baselineSummary?.averageQualityScore || 0) - Number(maxQualityRegression || 0)
  };

  const passed = Object.values(checks).every(Boolean);
  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);

  return { passed, checks, failures };
}

export function evaluateNextSafeBatchGate({
  expandedSimSummary,
  baselineTargetScore = 65.37,
  baselineCitationTopDocumentShareAvg = 0,
  expandedCitationTopDocumentShareAvg = 0,
  baselineLowSignalShare = 0,
  expandedLowSignalShare = 0,
  maxBaselineRegression = 0.5
}) {
  const checks = {
    qualityWithinBaselineTolerance:
      Number(expandedSimSummary?.averageQualityScore || 0) >= Number(baselineTargetScore || 65.37) - Number(maxBaselineRegression || 0.5),
    noCitationConcentrationIncrease:
      Number(expandedCitationTopDocumentShareAvg || 0) <= Number(baselineCitationTopDocumentShareAvg || 0),
    noLowSignalStructuralIncrease: Number(expandedLowSignalShare || 0) <= Number(baselineLowSignalShare || 0),
    provenanceCompletenessOne: Number(expandedSimSummary?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(expandedSimSummary?.citationAnchorCoverageAverage || 0) === 1,
    zeroTrustedResultStillZero: Number(expandedSimSummary?.zeroTrustedResultQueryCount || 0) === 0,
    outOfCorpusStillZero: Number(expandedSimSummary?.outOfCorpusHitQueryCount || 0) === 0
  };
  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);
  return { passed: failures.length === 0, checks, failures };
}

function delta(before, after) {
  return Number((Number(after || 0) - Number(before || 0)).toFixed(4));
}

export async function buildRetrievalBatchExpansionReport({
  apiBase,
  documents,
  corpusAdmissionReport,
  referenceEnrichmentReport,
  promotionRehearsalReport,
  trustedDocumentIds,
  batchSize = 10,
  fetchSearchDebug,
  liveLimit = 20,
  maxQualityRegression = 3,
  strictSafeMode = false,
  baselineTargetScore = 65.37,
  maxBaselineRegression = 0.5
}) {
  const chunkProfiles = mapByDocumentFromPreviews(documents || []);
  const selection = selectNextBatchCandidates({
    corpusAdmissionRows: corpusAdmissionReport?.documents || [],
    referenceEnrichmentRows: referenceEnrichmentReport?.documents || [],
    rehearsalRows: promotionRehearsalReport?.documents || [],
    trustedDocumentIds,
    batchSize,
    documentChunkProfiles: chunkProfiles,
    excludeLowSignalDominated: strictSafeMode,
    lowSignalShareThreshold: 0.45
  });

  const baselineTrustedIds = unique((trustedDocumentIds || []).map(String)).sort((a, b) => String(a).localeCompare(String(b)));
  const nextBatchDocIds = selection.selected.map((row) => row.documentId);
  const expandedTrustedIds = unique([...baselineTrustedIds, ...nextBatchDocIds]).sort((a, b) => String(a).localeCompare(String(b)));

  const qaQueries = LIVE_SEARCH_QA_QUERIES.map((row) => ({ id: row.id, query: row.query, intent: row.intent }));

  const baselineEval = buildRetrievalEvalReport({
    apiBase,
    input: { mode: "batch_expansion_baseline", trustedDocumentIds: baselineTrustedIds },
    documents,
    queries: qaQueries,
    includeText: false,
    admittedDocumentIdsOverride: baselineTrustedIds
  });

  const expandedEval = buildRetrievalEvalReport({
    apiBase,
    input: { mode: "batch_expansion_expanded", trustedDocumentIds: expandedTrustedIds },
    documents,
    queries: qaQueries,
    includeText: false,
    admittedDocumentIdsOverride: expandedTrustedIds
  });

  const baselineSimQa = summarizeEvalAsQa(baselineEval);
  const expandedSimQa = summarizeEvalAsQa(expandedEval);

  const baselineLiveQa = await buildRetrievalLiveSearchQaReport({
    apiBase,
    trustedDocumentIds: baselineTrustedIds,
    queries: LIVE_SEARCH_QA_QUERIES,
    fetchSearchDebug,
    limit: liveLimit,
    realOnly: true
  });

  const expandedGate = evaluateExpansionGate({
    baselineSummary: baselineLiveQa.summary,
    expandedSummary: expandedSimQa.summary,
    maxQualityRegression
  });

  const baselineCitationTopDocumentShareAvg = computeCitationTopDocumentShareAverage(baselineSimQa.byQuery);
  const expandedCitationTopDocumentShareAvg = computeCitationTopDocumentShareAverage(expandedSimQa.byQuery);
  const baselineLowSignalShare = computeLowSignalShareByIntent(baselineEval);
  const expandedLowSignalShare = computeLowSignalShareByIntent(expandedEval);

  const strictGate = evaluateNextSafeBatchGate({
    expandedSimSummary: expandedSimQa.summary,
    baselineTargetScore,
    baselineCitationTopDocumentShareAvg,
    expandedCitationTopDocumentShareAvg,
    baselineLowSignalShare,
    expandedLowSignalShare,
    maxBaselineRegression
  });

  const comparison = {
    baseline: {
      liveSummary: baselineLiveQa.summary,
      simulatedSummary: baselineSimQa.summary
    },
    expanded: {
      simulatedSummary: expandedSimQa.summary
    },
    deltas: {
      totalApiResultsAcrossQueries: delta(baselineSimQa.summary.totalApiResultsAcrossQueries, expandedSimQa.summary.totalApiResultsAcrossQueries),
      averageQualityScore: delta(baselineSimQa.summary.averageQualityScore, expandedSimQa.summary.averageQualityScore),
      uniqueDocumentsPerQueryAvg: delta(baselineSimQa.summary.uniqueDocumentsPerQueryAvg, expandedSimQa.summary.uniqueDocumentsPerQueryAvg),
      uniqueChunkTypesPerQueryAvg: delta(baselineSimQa.summary.uniqueChunkTypesPerQueryAvg, expandedSimQa.summary.uniqueChunkTypesPerQueryAvg),
      topDocumentShareAverage: delta(baselineSimQa.summary.topDocumentShareAverage, expandedSimQa.summary.topDocumentShareAverage)
    }
  };

  const effectiveGate = strictSafeMode ? strictGate : expandedGate;
  const recommended = effectiveGate.passed ? "yes" : "no";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    summary: {
      trustedDocumentCount: baselineTrustedIds.length,
      candidatePoolCount: selection.ranked.length,
      excludedCandidateCount: selection.excluded.length,
      proposedBatchSize: nextBatchDocIds.length,
      activationWorthy: effectiveGate.passed,
      activationRecommendation: recommended
    },
    batchSelectionLogic: {
      batchSize,
      rankingSignals: [
        "reference_enrichment_gains",
        "chunk_type_spread",
        "fallback_dependence",
        "canonical_alignment",
        "mixed_topic_reduction",
        "promotion_rehearsal_delta"
      ],
      strictFilters: [
        "real_docs_only",
        "hold_for_repair_review_only",
        "exclude_already_trusted",
        "exclude_fixtures"
      ]
    },
    beforeVsAfterQa: comparison,
    regressionGate: effectiveGate,
    strictSafeGate: strictGate,
    strictSafeMode,
    concentrationDeltas: {
      baselineCitationTopDocumentShareAvg,
      expandedCitationTopDocumentShareAvg,
      deltaCitationTopDocumentShareAvg: delta(baselineCitationTopDocumentShareAvg, expandedCitationTopDocumentShareAvg),
      baselineLowSignalStructuralShare: baselineLowSignalShare,
      expandedLowSignalStructuralShare: expandedLowSignalShare,
      deltaLowSignalStructuralShare: delta(baselineLowSignalShare, expandedLowSignalShare)
    },
    proposedNextBatch: selection.selected,
    topIncludedReasons: sortCountEntries(countBy(selection.selected.flatMap((row) => row.candidateReasons || []))).slice(0, 12),
    topExcludedReasons: selection.exclusionReasonCounts,
    candidateRanking: selection.ranked,
    candidateExclusions: selection.excluded,
    liveQaBaseline: baselineLiveQa,
    simulatedExpandedQa: {
      summary: expandedSimQa.summary,
      byQuery: expandedSimQa.byQuery
    },
    manifests: {
      baselineTrustedDocIds: baselineTrustedIds,
      nextBatchDocIds,
      expandedTrustedDocIds: expandedTrustedIds
    }
  };
}

export function formatRetrievalBatchExpansionMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Batch Expansion Report (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Gate Result");
  lines.push(`- passed: ${report.regressionGate?.passed}`);
  for (const [k, v] of Object.entries(report.regressionGate?.checks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.regressionGate?.failures || []).length) {
    lines.push(`- failures: ${(report.regressionGate.failures || []).join(", ")}`);
  }
  lines.push("");

  if (report.concentrationDeltas) {
    lines.push("## Concentration Deltas");
    for (const [k, v] of Object.entries(report.concentrationDeltas || {})) lines.push(`- ${k}: ${v}`);
    lines.push("");
  }

  lines.push("## Proposed Next Batch");
  for (const row of report.proposedNextBatch || []) {
    lines.push(`- ${row.documentId} | score=${row.candidateScore} | ${row.title}`);
    lines.push(`  - reasons: ${(row.candidateReasons || []).join(", ") || "<none>"}`);
    lines.push(`  - blockers: ${(row.candidateBlockers || []).join(", ") || "<none>"}`);
  }
  if (!(report.proposedNextBatch || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Before vs After (Simulated)");
  const deltas = report.beforeVsAfterQa?.deltas || {};
  for (const [k, v] of Object.entries(deltas)) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Top Included Reasons");
  for (const row of report.topIncludedReasons || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.topIncludedReasons || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Top Excluded Reasons");
  for (const row of report.topExcludedReasons || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.topExcludedReasons || []).length) lines.push("- none");
  lines.push("");

  lines.push("- Dry-run only. No activation, embedding writes, search-index writes, or trust-gate mutations.");
  return `${lines.join("\n")}\n`;
}
