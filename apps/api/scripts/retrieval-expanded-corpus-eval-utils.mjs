import { buildRetrievalEvalReport, DEFAULT_QUERIES } from "./retrieval-eval-utils.mjs";
import { buildRetrievalReferenceEnrichmentReport } from "./retrieval-reference-enrichment-utils.mjs";

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function mapCountEntries(arr, keyField, valueField = "hitCount") {
  const map = new Map();
  for (const row of arr || []) {
    map.set(String(row?.[keyField] || ""), Number(row?.[valueField] || 0));
  }
  return map;
}

function sortByDeltaThenKey(rows, keyField) {
  return [...rows].sort((a, b) => {
    if (b.delta !== a.delta) return b.delta - a.delta;
    return String(a[keyField] || "").localeCompare(String(b[keyField] || ""));
  });
}

function buildCoverageDelta(baselineRows, expandedRows, keyField) {
  const baseMap = mapCountEntries(baselineRows, keyField);
  const expMap = mapCountEntries(expandedRows, keyField);
  const keys = uniqueSorted([...baseMap.keys(), ...expMap.keys()]);
  const rows = keys.map((key) => {
    const baselineHits = Number(baseMap.get(key) || 0);
    const expandedHits = Number(expMap.get(key) || 0);
    return {
      [keyField]: key,
      baselineHits,
      expandedHits,
      delta: expandedHits - baselineHits
    };
  });
  return sortByDeltaThenKey(rows, keyField);
}

function classifyQueryOutcome(base, exp) {
  const improvementSignals = [];
  const regressionSignals = [];

  if ((exp?.afterDiversity?.uniqueDocuments || 0) > (base?.afterDiversity?.uniqueDocuments || 0)) {
    improvementSignals.push("unique_documents_increased");
  }
  if ((exp?.afterDiversity?.uniqueChunkTypes || 0) > (base?.afterDiversity?.uniqueChunkTypes || 0)) {
    improvementSignals.push("unique_chunk_types_increased");
  }
  if ((exp?.afterDiversity?.topDocumentShare || 0) < (base?.afterDiversity?.topDocumentShare || 0)) {
    improvementSignals.push("top_document_share_reduced");
  }
  if ((exp?.resultCount || 0) > (base?.resultCount || 0)) {
    improvementSignals.push("result_count_increased");
  }

  if ((exp?.resultCount || 0) < (base?.resultCount || 0)) {
    regressionSignals.push("result_count_decreased");
  }
  if ((exp?.afterDiversity?.topDocumentShare || 0) > (base?.afterDiversity?.topDocumentShare || 0) + 0.05) {
    regressionSignals.push("top_document_share_increased");
  }
  if ((exp?.afterDiversity?.sameDocumentTopResultStreak || 0) > (base?.afterDiversity?.sameDocumentTopResultStreak || 0)) {
    regressionSignals.push("same_document_streak_increased");
  }

  let netExpandedCorpusOutcome = "unchanged";
  if (improvementSignals.length && !regressionSignals.length) netExpandedCorpusOutcome = "improved";
  else if (!improvementSignals.length && regressionSignals.length) netExpandedCorpusOutcome = "worsened";

  return {
    netExpandedCorpusOutcome,
    improvementSignals,
    regressionSignals
  };
}

function topResultShareByDoc(rows, keyField = "documentId") {
  const total = Number((rows || []).length);
  const counts = countBy((rows || []).map((row) => row?.[keyField]));
  return Object.entries(counts)
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({
      [keyField]: key,
      count,
      share: total > 0 ? Number((Number(count) / total).toFixed(4)) : 0
    }));
}

function contributionConfidence(row) {
  const topAppear = Number(row.topResultAppearances || 0);
  const queryAppear = Number((row.queriesWherePromotedDocAppears || []).length);
  if (topAppear >= 3 || queryAppear >= 3) return "high";
  if (topAppear >= 1 || queryAppear >= 1) return "medium";
  return "low";
}

export function buildRetrievalExpandedCorpusEvalReport({
  apiBase,
  input,
  documents,
  queries = DEFAULT_QUERIES,
  includeText = false,
  promotedEnrichmentDocIdsOverride = null
}) {
  const baselineReport = buildRetrievalEvalReport({ apiBase, input, documents, queries, includeText });
  const baselineAdmittedDocIds = baselineReport.admittedCorpus.admittedDocumentIds || [];

  const enrichment = Array.isArray(promotedEnrichmentDocIdsOverride)
    ? {
        enrichmentOutcomeBundles: {
          promoteAfterEnrichmentDocIds: promotedEnrichmentDocIdsOverride
        }
      }
    : buildRetrievalReferenceEnrichmentReport({ apiBase, input, documents });

  const promotedEnrichmentDocIds = uniqueSorted(enrichment.enrichmentOutcomeBundles?.promoteAfterEnrichmentDocIds || []);
  const realDocIds = new Set((documents || []).filter((doc) => !doc?.isLikelyFixture).map((doc) => doc?.document?.documentId).filter(Boolean));

  const expandedAdmittedDocIds = uniqueSorted([...baselineAdmittedDocIds, ...promotedEnrichmentDocIds]).filter((id) => realDocIds.has(id));

  const expandedReport = buildRetrievalEvalReport({
    apiBase,
    input,
    documents,
    queries,
    includeText,
    admittedDocumentIdsOverride: expandedAdmittedDocIds
  });

  const baselineByQueryId = new Map((baselineReport.queryResults || []).map((row) => [String(row.queryId), row]));
  const expandedByQueryId = new Map((expandedReport.queryResults || []).map((row) => [String(row.queryId), row]));

  const expandedQueryResults = (queries || []).map((q) => {
    const base = baselineByQueryId.get(String(q.id)) || {
      queryId: q.id,
      query: q.query,
      intent: q.intent,
      resultCount: 0,
      beforeTopResults: [],
      topResults: [],
      afterDiversity: { uniqueDocuments: 0, uniqueChunkTypes: 0, topDocumentShare: 0, sameDocumentTopResultStreak: 0 }
    };
    const exp = expandedByQueryId.get(String(q.id)) || {
      queryId: q.id,
      query: q.query,
      intent: q.intent,
      resultCount: 0,
      beforeTopResults: [],
      topResults: [],
      afterDiversity: { uniqueDocuments: 0, uniqueChunkTypes: 0, topDocumentShare: 0, sameDocumentTopResultStreak: 0 }
    };

    const classified = classifyQueryOutcome(base, exp);

    return {
      queryId: q.id,
      query: q.query,
      intent: q.intent,
      baselineUniqueDocuments: Number(base.afterDiversity?.uniqueDocuments || 0),
      expandedUniqueDocuments: Number(exp.afterDiversity?.uniqueDocuments || 0),
      baselineUniqueChunkTypes: Number(base.afterDiversity?.uniqueChunkTypes || 0),
      expandedUniqueChunkTypes: Number(exp.afterDiversity?.uniqueChunkTypes || 0),
      baselineTopDocumentShare: Number(base.afterDiversity?.topDocumentShare || 0),
      expandedTopDocumentShare: Number(exp.afterDiversity?.topDocumentShare || 0),
      baselineTopResults: base.topResults || [],
      expandedTopResults: exp.topResults || [],
      improvementSignals: classified.improvementSignals,
      regressionSignals: classified.regressionSignals,
      netExpandedCorpusOutcome: classified.netExpandedCorpusOutcome
    };
  });

  const queriesImprovedByExpandedCorpus = expandedQueryResults
    .filter((row) => row.netExpandedCorpusOutcome === "improved")
    .map((row) => row.query);
  const queriesUnchangedByExpandedCorpus = expandedQueryResults
    .filter((row) => row.netExpandedCorpusOutcome === "unchanged")
    .map((row) => row.query);
  const queriesWorsenedByExpandedCorpus = expandedQueryResults
    .filter((row) => row.netExpandedCorpusOutcome === "worsened")
    .map((row) => row.query);

  const promotedSet = new Set(promotedEnrichmentDocIds);
  const promotedDocumentContributions = promotedEnrichmentDocIds
    .map((documentId) => {
      const perQuery = (expandedReport.queryResults || []).map((row) => {
        const topResults = row.topResults || [];
        const appears = topResults.some((result) => result.documentId === documentId);
        const topAppear = topResults.slice(0, 5).filter((result) => result.documentId === documentId).length;
        return {
          query: row.query,
          appears,
          topAppear,
          chunkTypes: uniqueSorted(topResults.filter((result) => result.documentId === documentId).map((result) => result.chunkType))
        };
      });

      const queriesWherePromotedDocAppears = perQuery.filter((row) => row.appears).map((row) => row.query);
      const topResultAppearances = perQuery.reduce((sum, row) => sum + Number(row.topAppear || 0), 0);
      const chunkTypesContributed = uniqueSorted(perQuery.flatMap((row) => row.chunkTypes || []));

      return {
        documentId,
        promotedDocContributionCount: queriesWherePromotedDocAppears.length,
        queriesWherePromotedDocAppears,
        topResultAppearances,
        chunkTypesContributed,
        contributionConfidence: contributionConfidence({
          queriesWherePromotedDocAppears,
          topResultAppearances
        })
      };
    })
    .sort((a, b) => {
      if (b.promotedDocContributionCount !== a.promotedDocContributionCount) return b.promotedDocContributionCount - a.promotedDocContributionCount;
      if (b.topResultAppearances !== a.topResultAppearances) return b.topResultAppearances - a.topResultAppearances;
      return String(a.documentId).localeCompare(String(b.documentId));
    });

  const coverageDeltaByDocument = buildCoverageDelta(
    baselineReport.retrievalCoverageByDocument || [],
    expandedReport.retrievalCoverageByDocument || [],
    "documentId"
  );
  const coverageDeltaByChunkType = buildCoverageDelta(
    baselineReport.retrievalCoverageByChunkType || [],
    expandedReport.retrievalCoverageByChunkType || [],
    "chunkType"
  );

  const baselineCorpusSummary = {
    admittedDocumentCount: Number(baselineReport.summary?.admittedDocumentCount || 0),
    admittedChunkCount: Number(baselineReport.summary?.admittedChunkCount || 0),
    averageResultsPerQuery: Number(baselineReport.summary?.averageResultsPerQuery || 0),
    queriesWithNoResults: Number(baselineReport.summary?.queriesWithNoResults || 0),
    queriesWithLowConfidenceResults: Number(baselineReport.summary?.queriesWithLowConfidenceResults || 0),
    uniqueDocumentsPerQuery: baselineReport.uniqueDocumentsPerQuery || [],
    uniqueChunkTypesPerQuery: baselineReport.uniqueChunkTypesPerQuery || [],
    topResultShareByDocument: baselineReport.topResultShareByDocument || [],
    retrievalCoverageByDocument: baselineReport.retrievalCoverageByDocument || [],
    retrievalCoverageByChunkType: baselineReport.retrievalCoverageByChunkType || []
  };

  const expandedCorpusSummary = {
    admittedDocumentCount: Number(expandedReport.summary?.admittedDocumentCount || 0),
    admittedChunkCount: Number(expandedReport.summary?.admittedChunkCount || 0),
    averageResultsPerQuery: Number(expandedReport.summary?.averageResultsPerQuery || 0),
    queriesWithNoResults: Number(expandedReport.summary?.queriesWithNoResults || 0),
    queriesWithLowConfidenceResults: Number(expandedReport.summary?.queriesWithLowConfidenceResults || 0),
    uniqueDocumentsPerQuery: expandedReport.uniqueDocumentsPerQuery || [],
    uniqueChunkTypesPerQuery: expandedReport.uniqueChunkTypesPerQuery || [],
    topResultShareByDocument: expandedReport.topResultShareByDocument || [],
    retrievalCoverageByDocument: expandedReport.retrievalCoverageByDocument || [],
    retrievalCoverageByChunkType: expandedReport.retrievalCoverageByChunkType || []
  };

  const comparativeDeltaSummary = {
    admittedDocumentDelta: expandedCorpusSummary.admittedDocumentCount - baselineCorpusSummary.admittedDocumentCount,
    admittedChunkDelta: expandedCorpusSummary.admittedChunkCount - baselineCorpusSummary.admittedChunkCount,
    averageResultsPerQueryDelta: Number((expandedCorpusSummary.averageResultsPerQuery - baselineCorpusSummary.averageResultsPerQuery).toFixed(4)),
    queriesWithNoResultsDelta: expandedCorpusSummary.queriesWithNoResults - baselineCorpusSummary.queriesWithNoResults,
    queriesWithLowConfidenceResultsDelta:
      expandedCorpusSummary.queriesWithLowConfidenceResults - baselineCorpusSummary.queriesWithLowConfidenceResults,
    improvedQueryCount: queriesImprovedByExpandedCorpus.length,
    unchangedQueryCount: queriesUnchangedByExpandedCorpus.length,
    worsenedQueryCount: queriesWorsenedByExpandedCorpus.length
  };

  const summary = {
    documentsAnalyzed: Number((documents || []).length),
    queriesEvaluated: Number((queries || []).length),
    baselineAdmittedDocumentCount: baselineCorpusSummary.admittedDocumentCount,
    expandedAdmittedDocumentCount: expandedCorpusSummary.admittedDocumentCount,
    promotedEnrichmentDocumentCount: promotedEnrichmentDocIds.length,
    improvedQueryCount: queriesImprovedByExpandedCorpus.length,
    unchangedQueryCount: queriesUnchangedByExpandedCorpus.length,
    worsenedQueryCount: queriesWorsenedByExpandedCorpus.length,
    provenanceCompletenessOk:
      (expandedReport.queryResults || []).flatMap((row) => row.topResults || []).every((result) =>
        Boolean(result.documentId && result.chunkId && result.citationAnchorStart && result.citationAnchorEnd && result.sourceLink)
      )
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    baselineCorpusSummary,
    expandedCorpusSummary,
    comparativeDeltaSummary,
    queriesImprovedByExpandedCorpus,
    queriesUnchangedByExpandedCorpus,
    queriesWorsenedByExpandedCorpus,
    baselineQueryResults: baselineReport.queryResults || [],
    expandedQueryResults,
    coverageDeltaByDocument,
    coverageDeltaByChunkType,
    promotedDocumentContributions,
    expandedCorpusBundles: {
      baselineAdmittedDocIds,
      promotedEnrichmentDocIds,
      expandedAdmittedDocIds
    },
    baselineEvalReport: baselineReport,
    expandedEvalReport: expandedReport
  };
}

export function formatRetrievalExpandedCorpusEvalMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Expanded Corpus Comparative Eval Report");
  lines.push("");

  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Baseline Corpus Summary");
  for (const [k, v] of Object.entries(report.baselineCorpusSummary || {})) {
    if (Array.isArray(v)) continue;
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");

  lines.push("## Expanded Corpus Summary");
  for (const [k, v] of Object.entries(report.expandedCorpusSummary || {})) {
    if (Array.isArray(v)) continue;
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");

  lines.push("## Comparative Delta Summary");
  for (const [k, v] of Object.entries(report.comparativeDeltaSummary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Query Outcome Split");
  lines.push(`- improved: ${(report.queriesImprovedByExpandedCorpus || []).length}`);
  lines.push(`- unchanged: ${(report.queriesUnchangedByExpandedCorpus || []).length}`);
  lines.push(`- worsened: ${(report.queriesWorsenedByExpandedCorpus || []).length}`);
  lines.push("");

  lines.push("## Promoted Document Contributions");
  for (const row of report.promotedDocumentContributions || []) {
    lines.push(
      `- ${row.documentId} | queries=${row.promotedDocContributionCount} top5=${row.topResultAppearances} confidence=${row.contributionConfidence} | chunkTypes=${(row.chunkTypesContributed || []).join(",") || "<none>"}`
    );
  }
  if (!(report.promotedDocumentContributions || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Expanded Corpus Bundles");
  lines.push(`- baselineAdmittedDocIds: ${(report.expandedCorpusBundles?.baselineAdmittedDocIds || []).length}`);
  lines.push(`- promotedEnrichmentDocIds: ${(report.expandedCorpusBundles?.promotedEnrichmentDocIds || []).length}`);
  lines.push(`- expandedAdmittedDocIds: ${(report.expandedCorpusBundles?.expandedAdmittedDocIds || []).length}`);
  lines.push("");

  lines.push("- Read-only comparative rehearsal only. No admission-state mutation, no embedding/vector/index writes.");
  return `${lines.join("\n")}\n`;
}
