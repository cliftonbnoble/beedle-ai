import fs from "node:fs/promises";
import path from "node:path";

function normalize(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s_-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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

function topDocShare(rows) {
  if (!rows.length) return 0;
  const counts = countBy(rows.map((row) => row.documentId));
  const max = Math.max(...Object.values(counts).map((n) => Number(n || 0)));
  return Number((max / rows.length).toFixed(4));
}

function sameDocStreak(rows) {
  let max = 0;
  let streak = 0;
  let last = "";
  for (const row of rows) {
    const current = String(row.documentId || "");
    if (current === last) streak += 1;
    else {
      streak = 1;
      last = current;
    }
    if (streak > max) max = streak;
  }
  return max;
}

export const LIVE_SEARCH_QA_QUERIES = [
  {
    id: "authority_ordinance",
    group: "ordinance_rule_authority_lookup",
    query: "ordinance 37.2 rule 37.8 authority discussion",
    queryType: "rules_ordinance",
    expectedChunkTypes: ["authority_discussion", "analysis_reasoning"],
    intent: "authority"
  },
  {
    id: "findings_credibility",
    group: "findings_credibility_evidence",
    query: "findings of fact credibility witness evidence",
    queryType: "keyword",
    expectedChunkTypes: ["findings", "analysis_reasoning"],
    intent: "findings"
  },
  {
    id: "procedural_history",
    group: "procedural_history_hearing_notice_continuance",
    query: "procedural history hearing notice continuance",
    queryType: "keyword",
    expectedChunkTypes: ["procedural_history", "facts_background"],
    intent: "procedural"
  },
  {
    id: "issue_holding",
    group: "issue_holding_disposition_order",
    query: "issue presented holding disposition order",
    queryType: "keyword",
    expectedChunkTypes: ["holding_disposition", "analysis_reasoning"],
    intent: "holding"
  },
  {
    id: "legal_standard",
    group: "legal_standard_analysis_reasoning_application",
    query: "legal standard analysis reasoning application",
    queryType: "keyword",
    expectedChunkTypes: ["analysis_reasoning", "authority_discussion"],
    intent: "analysis"
  },
  {
    id: "comparative_reasoning",
    group: "comparative_multi_decision_reasoning",
    query: "compare how prior decisions applied ordinance 37.2 evidence standard",
    queryType: "keyword",
    expectedChunkTypes: ["analysis_reasoning", "authority_discussion", "findings"],
    intent: "comparative"
  },
  {
    id: "citation_rule_direct",
    group: "citation_style_direct_reference",
    query: "Rule 37.8",
    queryType: "citation_lookup",
    expectedChunkTypes: ["authority_discussion"],
    intent: "citation"
  },
  {
    id: "citation_ordinance_direct",
    group: "citation_style_direct_reference",
    query: "Ordinance 37.2",
    queryType: "citation_lookup",
    expectedChunkTypes: ["authority_discussion"],
    intent: "citation"
  }
];

export async function loadTrustedActivatedDocumentIds({ reportsDir, reportName, manifestName }) {
  const writePath = path.resolve(reportsDir, reportName || "retrieval-activation-write-report.json");
  const manifestPath = path.resolve(reportsDir, manifestName || "retrieval-trusted-activation-manifest.json");

  const sources = [];
  const ids = new Set();

  let rollbackApplied = false;
  try {
    const rollback = JSON.parse(await fs.readFile(path.resolve(reportsDir, "retrieval-batch-rollback-report.json"), "utf8"));
    rollbackApplied = Boolean(rollback?.summary?.rollbackVerificationPassed) && Number(rollback?.summary?.removedDocumentCount || 0) > 0;
  } catch {
    // ignore
  }

  if (!rollbackApplied) {
    try {
      const batchActivation = JSON.parse(
        await fs.readFile(path.resolve(reportsDir, "retrieval-batch-activation-report.json"), "utf8")
      );
      for (const id of batchActivation?.manifests?.trustedAfterDocIds || []) {
        if (id) ids.add(String(id));
      }
      if ((batchActivation?.manifests?.trustedAfterDocIds || []).length > 0) sources.push("batch_activation_report");
    } catch {
      // ignore
    }
  }

  try {
    const writeReport = JSON.parse(await fs.readFile(writePath, "utf8"));
    for (const row of writeReport.documentsActivated || []) {
      if (row?.documentId) ids.add(String(row.documentId));
    }
    if ((writeReport.summary?.activatedDocumentCount || 0) > 0) sources.push("activation_write_report");
  } catch {
    // ignore; fallback to manifest
  }

  try {
    const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
    for (const id of manifest.documentIdsToActivate || []) {
      if (id) ids.add(String(id));
    }
    if ((manifest.documentIdsToActivate || []).length > 0) sources.push("activation_manifest");
  } catch {
    // ignore
  }

  return {
    trustedDocumentIds: Array.from(ids).sort((a, b) => String(a).localeCompare(String(b))),
    sources: unique(sources)
  };
}

function classifyQueryQuality({ querySpec, trustedRows, allRows }) {
  const top = trustedRows.slice(0, 10);
  const expectedSet = new Set((querySpec.expectedChunkTypes || []).map((v) => String(v)));
  const expectedHits = top.filter((row) => expectedSet.has(String(row.sectionLabel || ""))).length;
  const expectedTypeHitRate = top.length ? Number((expectedHits / top.length).toFixed(4)) : 0;

  const rerankScores = top.map((row) => Number(row?.diagnostics?.rerankScore || 0));
  const maxRerank = rerankScores.length ? Math.max(...rerankScores) : 0;
  const avgRerank = rerankScores.length
    ? Number((rerankScores.reduce((sum, n) => sum + n, 0) / rerankScores.length).toFixed(4))
    : 0;
  const rerankNormalized = maxRerank > 0 ? Number(Math.min(1, avgRerank / maxRerank).toFixed(4)) : 0;

  const provenanceCompleteHits = top.filter((row) => row.sourceLink && row.sourceFileRef && row.citationAnchor).length;
  const provenanceCompleteness = top.length ? Number((provenanceCompleteHits / top.length).toFixed(4)) : 0;

  const citationAnchors = top.filter((row) => row.citationAnchor).length;
  const citationAnchorCoverage = top.length ? Number((citationAnchors / top.length).toFixed(4)) : 0;

  const uniqueDocuments = unique(top.map((row) => row.documentId)).length;
  const uniqueChunkTypes = unique(top.map((row) => row.sectionLabel)).length;
  const documentDiversityRatio = top.length ? Number((uniqueDocuments / top.length).toFixed(4)) : 0;
  const chunkDiversityRatio = top.length ? Number((uniqueChunkTypes / top.length).toFixed(4)) : 0;

  const topDocumentShare = topDocShare(top);
  const duplicateStreak = sameDocStreak(top);
  const duplicatePressure = Number(((topDocumentShare + Math.min(1, duplicateStreak / 5)) / 2).toFixed(4));

  const outOfCorpusHits = allRows.filter((row) => !row.inTrustedCorpus).length;
  const outOfCorpusRate = allRows.length ? Number((outOfCorpusHits / allRows.length).toFixed(4)) : 0;

  const qualityScore = Number(
    (
      expectedTypeHitRate * 30 +
      rerankNormalized * 15 +
      provenanceCompleteness * 15 +
      citationAnchorCoverage * 10 +
      documentDiversityRatio * 10 +
      chunkDiversityRatio * 10 +
      (1 - duplicatePressure) * 10
    ).toFixed(2)
  );

  const weaknessSignals = [];
  if (!trustedRows.length) weaknessSignals.push("no_trusted_corpus_results");
  if (expectedTypeHitRate < 0.35) weaknessSignals.push("weak_chunk_type_targeting");
  if (provenanceCompleteness < 1) weaknessSignals.push("provenance_incomplete_results");
  if (citationAnchorCoverage < 0.8) weaknessSignals.push("low_citation_anchor_coverage");
  if (duplicatePressure > 0.7) weaknessSignals.push("duplicate_flooding_pressure");
  if (documentDiversityRatio < 0.2) weaknessSignals.push("low_document_diversity");
  if (outOfCorpusRate > 0) weaknessSignals.push("out_of_corpus_hits_present");

  return {
    qualityScore,
    expectedTypeHitRate,
    rerankNormalized,
    provenanceCompleteness,
    citationAnchorCoverage,
    uniqueDocuments,
    uniqueChunkTypes,
    documentDiversityRatio,
    chunkDiversityRatio,
    topDocumentShare,
    duplicateStreak,
    duplicatePressure,
    outOfCorpusHits,
    outOfCorpusRate,
    weaknessSignals
  };
}

function computeRecommendedTuningActions(queryRows) {
  const actions = [];
  const noTrustedResults = queryRows.filter((row) => row.trustedResultCount === 0).length;
  const weakType = queryRows.filter((row) => row.metrics.expectedTypeHitRate < 0.35).length;
  const dupPressure = queryRows.filter((row) => row.metrics.duplicatePressure > 0.7).length;
  const anchorWeak = queryRows.filter((row) => row.metrics.citationAnchorCoverage < 0.8).length;
  const outOfCorpus = queryRows.filter((row) => row.metrics.outOfCorpusHits > 0).length;
  const diversityWeak = queryRows.filter((row) => row.metrics.documentDiversityRatio < 0.2).length;

  if (noTrustedResults > 0) {
    actions.push({
      action: "verify_activation_queryability_guards_for_trusted_docs",
      rationale: `${noTrustedResults} queries returned zero trusted results; verify runtime search eligibility gates for activated documents.`
    });
  }
  if (weakType > 0) {
    actions.push({
      action: "boost_query_intent_to_chunk_type_alignment",
      rationale: `${weakType} queries show weak section/type targeting in top trusted hits.`
    });
  }
  if (dupPressure > 0) {
    actions.push({
      action: "increase_same_document_redundancy_penalty",
      rationale: `${dupPressure} queries still show duplicate flooding pressure.`
    });
  }
  if (anchorWeak > 0) {
    actions.push({
      action: "increase_citation_anchor_preference_in_rerank",
      rationale: `${anchorWeak} queries have low citation-anchor coverage in top trusted hits.`
    });
  }
  if (outOfCorpus > 0) {
    actions.push({
      action: "enforce_activated_corpus_filter_at_query_time",
      rationale: `${outOfCorpus} queries returned out-of-corpus hits before trusted filtering.`
    });
  }
  if (diversityWeak > 0) {
    actions.push({
      action: "promote_cross_document_diversity_in_top_results",
      rationale: `${diversityWeak} queries have low unique-document ratio in top trusted results.`
    });
  }

  if (!actions.length) {
    actions.push({
      action: "maintain_current_ranking_profile",
      rationale: "Current live QA shows strong relevance/provenance/diversity across seeded judge-style queries."
    });
  }

  return actions;
}

export async function buildRetrievalLiveSearchQaReport({
  apiBase,
  trustedDocumentIds,
  queries = LIVE_SEARCH_QA_QUERIES,
  fetchSearchDebug,
  limit = 20,
  realOnly = true
}) {
  const trustedSet = new Set((trustedDocumentIds || []).map((id) => String(id)));
  const queryRows = [];

  for (const spec of queries) {
    const payload = {
      query: spec.query,
      queryType: spec.queryType || "keyword",
      limit,
      filters: {
        approvedOnly: true,
        fileType: "decision_docx"
      }
    };

    const response = await fetchSearchDebug(payload);
    const results = (response.results || []).map((row) => ({
      ...row,
      inTrustedCorpus: trustedSet.has(String(row.documentId || ""))
    }));

    const trustedResults = results.filter((row) => row.inTrustedCorpus);
    const metrics = classifyQueryQuality({ querySpec: spec, trustedRows: trustedResults, allRows: results });

    queryRows.push({
      queryId: spec.id,
      group: spec.group,
      intent: spec.intent,
      query: spec.query,
      expectedChunkTypes: spec.expectedChunkTypes || [],
      totalApiResults: Number(response.total || results.length || 0),
      trustedResultCount: trustedResults.length,
      metrics,
      topResults: trustedResults.slice(0, 10).map((row) => ({
        documentId: row.documentId,
        title: row.title,
        chunkId: row.chunkId,
        chunkType: row.sectionLabel,
        score: row.score,
        lexicalScore: row.lexicalScore,
        vectorScore: row.vectorScore,
        rerankScore: Number(row?.diagnostics?.rerankScore || 0),
        citationAnchor: row.citationAnchor,
        sourceLink: row.sourceLink,
        sourceFileRef: row.sourceFileRef,
        snippet: row.snippet
      })),
      outOfCorpusTopResults: results
        .filter((row) => !row.inTrustedCorpus)
        .slice(0, 5)
        .map((row) => ({
          documentId: row.documentId,
          title: row.title,
          chunkId: row.chunkId,
          chunkType: row.sectionLabel,
          score: row.score
        }))
    });
  }

  const chunkTypeCounts = countBy(queryRows.flatMap((row) => row.topResults.map((result) => result.chunkType)));
  const groupedExpected = queryRows.map((row) => ({
    query: row.query,
    group: row.group,
    expectedTypeHitRate: row.metrics.expectedTypeHitRate,
    expectedChunkTypes: row.expectedChunkTypes
  }));

  const qualityByQuery = queryRows.map((row) => ({
    queryId: row.queryId,
    group: row.group,
    query: row.query,
    qualityScore: row.metrics.qualityScore,
    expectedTypeHitRate: row.metrics.expectedTypeHitRate,
    weaknessSignals: row.metrics.weaknessSignals
  }));

  const documentDiversityByQuery = queryRows.map((row) => ({
    queryId: row.queryId,
    query: row.query,
    uniqueDocuments: row.metrics.uniqueDocuments,
    documentDiversityRatio: row.metrics.documentDiversityRatio,
    topDocumentShare: row.metrics.topDocumentShare
  }));

  const duplicatePressureByQuery = queryRows.map((row) => ({
    queryId: row.queryId,
    query: row.query,
    duplicatePressure: row.metrics.duplicatePressure,
    duplicateStreak: row.metrics.duplicateStreak,
    topDocumentShare: row.metrics.topDocumentShare
  }));

  const provenanceCompletenessByQuery = queryRows.map((row) => ({
    queryId: row.queryId,
    query: row.query,
    provenanceCompleteness: row.metrics.provenanceCompleteness
  }));

  const citationAnchorCoverageByQuery = queryRows.map((row) => ({
    queryId: row.queryId,
    query: row.query,
    citationAnchorCoverage: row.metrics.citationAnchorCoverage
  }));

  const topMissesOrWeakQueries = [...queryRows]
    .filter((row) => row.metrics.qualityScore < 65 || row.metrics.weaknessSignals.length > 0)
    .sort((a, b) => {
      if (a.metrics.qualityScore !== b.metrics.qualityScore) return a.metrics.qualityScore - b.metrics.qualityScore;
      return String(a.queryId).localeCompare(String(b.queryId));
    })
    .map((row) => ({
      queryId: row.queryId,
      group: row.group,
      query: row.query,
      qualityScore: row.metrics.qualityScore,
      weaknessSignals: row.metrics.weaknessSignals,
      trustedResultCount: row.trustedResultCount,
      outOfCorpusHits: row.metrics.outOfCorpusHits
    }));

  const summary = {
    readOnly: true,
    queriesEvaluated: queryRows.length,
    trustedDocumentCount: trustedSet.size,
    averageQualityScore: Number(
      (
        queryRows.reduce((sum, row) => sum + Number(row.metrics.qualityScore || 0), 0) /
        Math.max(1, queryRows.length)
      ).toFixed(2)
    ),
    weakQueryCount: topMissesOrWeakQueries.length,
    totalApiResultsAcrossQueries: queryRows.reduce((sum, row) => sum + Number(row.totalApiResults || 0), 0),
    outOfCorpusHitQueryCount: queryRows.filter((row) => row.metrics.outOfCorpusHits > 0).length,
    zeroTrustedResultQueryCount: queryRows.filter((row) => row.trustedResultCount === 0).length,
    realOnly,
    duplicateFloodingQueryCount: queryRows.filter((row) => row.metrics.duplicatePressure > 0.7).length,
    provenanceCompletenessAverage: Number(
      (
        queryRows.reduce((sum, row) => sum + Number(row.metrics.provenanceCompleteness || 0), 0) /
        Math.max(1, queryRows.length)
      ).toFixed(4)
    ),
    citationAnchorCoverageAverage: Number(
      (
        queryRows.reduce((sum, row) => sum + Number(row.metrics.citationAnchorCoverage || 0), 0) /
        Math.max(1, queryRows.length)
      ).toFixed(4)
    )
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    summary,
    queriesEvaluated: queries.map((row) => ({ id: row.id, group: row.group, query: row.query, queryType: row.queryType })),
    resultQualityByQuery: qualityByQuery,
    chunkTypeHitRates: {
      counts: sortCountEntries(chunkTypeCounts),
      expectedTargetingByQuery: groupedExpected
    },
    documentDiversityByQuery,
    duplicatePressureByQuery,
    provenanceCompletenessByQuery,
    citationAnchorCoverageByQuery,
    topMissesOrWeakQueries,
    recommendedTuningActions: computeRecommendedTuningActions(queryRows),
    queryResults: queryRows,
    trustedCorpus: {
      trustedDocumentIds: Array.from(trustedSet).sort((a, b) => String(a).localeCompare(String(b)))
    }
  };
}

export function formatRetrievalLiveSearchQaMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Live Search QA Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Strongest Queries");
  const strongest = [...(report.resultQualityByQuery || [])]
    .sort((a, b) => {
      if (b.qualityScore !== a.qualityScore) return b.qualityScore - a.qualityScore;
      return String(a.queryId).localeCompare(String(b.queryId));
    })
    .slice(0, 5);
  for (const row of strongest) {
    lines.push(`- ${row.query} | quality=${row.qualityScore} | expectedTypeHitRate=${row.expectedTypeHitRate}`);
  }
  if (!strongest.length) lines.push("- none");
  lines.push("");

  lines.push("## Weakest Queries");
  for (const row of report.topMissesOrWeakQueries || []) {
    lines.push(`- ${row.query} | quality=${row.qualityScore} | weaknesses=${(row.weaknessSignals || []).join(", ") || "<none>"}`);
  }
  if (!(report.topMissesOrWeakQueries || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Recommended Tuning Actions");
  for (const row of report.recommendedTuningActions || []) {
    lines.push(`- ${row.action}: ${row.rationale}`);
  }
  if (!(report.recommendedTuningActions || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Query Detail");
  for (const row of report.queryResults || []) {
    lines.push(`- ${row.query} | trustedResults=${row.trustedResultCount} | quality=${row.metrics?.qualityScore}`);
    lines.push(`  - weaknessSignals: ${(row.metrics?.weaknessSignals || []).join(", ") || "<none>"}`);
    for (const top of (row.topResults || []).slice(0, 3)) {
      lines.push(`  - ${top.documentId} | ${top.chunkId} | ${top.chunkType} | rerank=${top.rerankScore}`);
    }
  }
  lines.push("");
  lines.push("- Live QA only. Read-only diagnostics; no embedding/index/admission mutations.");
  return `${lines.join("\n")}\n`;
}
