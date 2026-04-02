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

function toMapByQueryId(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const id = String(row?.queryId || "");
    if (!id) continue;
    map.set(id, row);
  }
  return map;
}

function summarizeTopContribution(queryRows, batchDocIds) {
  const batchSet = new Set((batchDocIds || []).map(String));
  const perQuery = [];
  const allTop5 = [];
  const allTop10 = [];

  for (const row of queryRows || []) {
    const top10 = (row?.topResults || []).slice(0, 10);
    const top5 = top10.slice(0, 5);
    const top5Hits = top5.filter((result) => batchSet.has(String(result.documentId || "")));
    const top10Hits = top10.filter((result) => batchSet.has(String(result.documentId || "")));

    allTop5.push(...top5Hits);
    allTop10.push(...top10Hits);

    perQuery.push({
      queryId: row?.queryId,
      query: row?.query,
      batchTop5Hits: top5Hits.length,
      batchTop10Hits: top10Hits.length,
      batchTop5Share: top5.length ? Number((top5Hits.length / top5.length).toFixed(4)) : 0,
      batchTop10Share: top10.length ? Number((top10Hits.length / top10.length).toFixed(4)) : 0,
      dominantBatchChunkTypes: sortCountEntries(countBy(top10Hits.map((result) => result.chunkType))).slice(0, 5)
    });
  }

  return {
    perQuery,
    totalTop5BatchHits: allTop5.length,
    totalTop10BatchHits: allTop10.length,
    top5BatchShare: (queryRows || []).length ? Number((allTop5.length / ((queryRows || []).length * 5)).toFixed(4)) : 0,
    top10BatchShare: (queryRows || []).length ? Number((allTop10.length / ((queryRows || []).length * 10)).toFixed(4)) : 0,
    batchChunkTypeCountsTop10: sortCountEntries(countBy(allTop10.map((result) => result.chunkType))),
    batchDocCountsTop10: sortCountEntries(countBy(allTop10.map((result) => result.documentId)))
  };
}

function compareQueries(preRows, postRows) {
  const preMap = toMapByQueryId(preRows);
  const postMap = toMapByQueryId(postRows);
  const queryIds = Array.from(new Set([...preMap.keys(), ...postMap.keys()])).sort((a, b) => a.localeCompare(b));

  const outcomes = [];
  for (const queryId of queryIds) {
    const pre = preMap.get(queryId) || {};
    const post = postMap.get(queryId) || {};

    const preScore = Number(pre?.qualityScore || 0);
    const postScore = Number(post?.qualityScore || 0);
    const deltaScore = Number((postScore - preScore).toFixed(4));

    const preExpected = Number(pre?.expectedTypeHitRate || 0);
    const postExpected = Number(post?.expectedTypeHitRate || 0);

    const outcome = deltaScore > 0.2 ? "improved" : deltaScore < -0.2 ? "worsened" : "unchanged";

    outcomes.push({
      queryId,
      query: String(post?.query || pre?.query || ""),
      preScore,
      postScore,
      deltaScore,
      preExpectedTypeHitRate: preExpected,
      postExpectedTypeHitRate: postExpected,
      deltaExpectedTypeHitRate: Number((postExpected - preExpected).toFixed(4)),
      outcome
    });
  }

  return {
    rows: outcomes,
    counts: {
      improved: outcomes.filter((row) => row.outcome === "improved").length,
      unchanged: outcomes.filter((row) => row.outcome === "unchanged").length,
      worsened: outcomes.filter((row) => row.outcome === "worsened").length
    }
  };
}

function deriveQualityRowsFromQueryResults(queryResults) {
  return (queryResults || []).map((row) => {
    const top = (row?.topResults || []).slice(0, 10);
    const avgScore = top.length
      ? Number((top.reduce((sum, item) => sum + Number(item?.score || 0), 0) / top.length).toFixed(4))
      : 0;
    const typeCounts = countBy(top.map((item) => String(item?.chunkType || "").toLowerCase()));
    const dominantTypeCount = Math.max(0, ...Object.values(typeCounts).map((v) => Number(v || 0)));
    const expectedTypeHitRate = top.length ? Number((dominantTypeCount / top.length).toFixed(4)) : 0;
    return {
      queryId: row?.queryId,
      query: row?.query,
      qualityScore: Number((avgScore * 100).toFixed(2)),
      expectedTypeHitRate
    };
  });
}

function evaluateHardGuards(summary) {
  return {
    noOutOfCorpusLeakage: Number(summary?.outOfCorpusHitQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(summary?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(summary?.citationAnchorCoverageAverage || 0) === 1,
    zeroResultQueryCountZero: Number(summary?.zeroTrustedResultQueryCount || 0) === 0
  };
}

function evaluateCitationQueryRegression(perQueryRows) {
  const citationRows = (perQueryRows || []).filter((row) => String(row.queryId || "").startsWith("citation_"));
  const worsened = citationRows.filter((row) => Number(row.deltaScore || 0) < -0.2);
  return {
    citationQueryCount: citationRows.length,
    worsenedCitationQueryCount: worsened.length,
    citationQueriesWorsened: worsened.map((row) => row.queryId)
  };
}

export function buildRetrievalRankingRegressionReport({
  baselinePreBatchSummary,
  preRetuneReport,
  postRetuneReport,
  batchDocIds,
  activatedBatchId,
  rollbackBatchId
}) {
  const hardGuards = evaluateHardGuards(postRetuneReport?.summary || {});
  const preQualityRows = (preRetuneReport?.resultQualityByQuery || []).length
    ? preRetuneReport.resultQualityByQuery
    : deriveQualityRowsFromQueryResults(preRetuneReport?.queryResults || []);
  const postQualityRows = (postRetuneReport?.resultQualityByQuery || []).length
    ? postRetuneReport.resultQualityByQuery
    : deriveQualityRowsFromQueryResults(postRetuneReport?.queryResults || []);
  const queryComparison = compareQueries(preQualityRows, postQualityRows);
  const citationRegression = evaluateCitationQueryRegression(queryComparison.rows);

  const contributionBreakdown = summarizeTopContribution(preRetuneReport?.queryResults || [], batchDocIds || []);

  const baselineQuality = Number(baselinePreBatchSummary?.averageQualityScore || 0);
  const preRetuneQuality = Number(preRetuneReport?.summary?.averageQualityScore || 0);
  const postRetuneQuality = Number(postRetuneReport?.summary?.averageQualityScore || 0);

  const qualityRecovered = postRetuneQuality >= Math.max(baselineQuality, preRetuneQuality);
  const qualityMateriallyImproved = postRetuneQuality >= preRetuneQuality + 5;

  const keepBatchActive =
    Object.values(hardGuards).every(Boolean) &&
    citationRegression.worsenedCitationQueryCount === 0 &&
    (qualityRecovered || qualityMateriallyImproved || postRetuneQuality >= baselineQuality);

  const recommendation = keepBatchActive ? "keep_batch_active" : "rollback_batch";

  const rootCauseFindings = [
    {
      code: "batch_chunk_type_distribution_shift",
      detail: "Newly activated docs increased caption_title / issue_statement presence in top-ranked results for intent-specific queries.",
      evidence: contributionBreakdown.batchChunkTypeCountsTop10.slice(0, 6)
    },
    {
      code: "intent_chunktype_mismatch_penalties_previously_insufficient",
      detail: "Authority/findings/procedural intents were under-penalizing low-signal structural chunks in pre-retune ranking.",
      evidence: queryComparison.rows
        .filter((row) => ["authority_ordinance", "findings_credibility", "procedural_history", "legal_standard"].includes(String(row.queryId || "")))
        .map((row) => ({ queryId: row.queryId, deltaScore: row.deltaScore, deltaExpectedTypeHitRate: row.deltaExpectedTypeHitRate }))
    }
  ];

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      activatedBatchId,
      rollbackBatchId,
      baselinePreBatchAverageQualityScore: baselineQuality,
      preRetuneAverageQualityScore: preRetuneQuality,
      postRetuneAverageQualityScore: postRetuneQuality,
      qualityDeltaPreToPostRetune: Number((postRetuneQuality - preRetuneQuality).toFixed(4)),
      qualityDeltaBaselineToPostRetune: Number((postRetuneQuality - baselineQuality).toFixed(4)),
      recommendation
    },
    rootCauseFindings,
    contributionBreakdownForActivatedBatch: contributionBreakdown,
    queryOutcomeTable: queryComparison,
    hardGuardChecks: hardGuards,
    citationQueryRegression: citationRegression,
    recommendation,
    rollbackManifestRef: "reports/retrieval-batch-rollback-manifest.json"
  };
}

export function formatRetrievalRankingRegressionMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Ranking Regression Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Root Cause Findings");
  for (const row of report.rootCauseFindings || []) {
    lines.push(`- ${row.code}: ${row.detail}`);
  }
  lines.push("");

  lines.push("## Query Outcomes");
  for (const row of report.queryOutcomeTable?.rows || []) {
    lines.push(`- ${row.queryId} | outcome=${row.outcome} | pre=${row.preScore} post=${row.postScore} delta=${row.deltaScore}`);
  }
  lines.push("");

  lines.push("## Activated Batch Contribution (Pre-retune)");
  lines.push(`- totalTop5BatchHits: ${report.contributionBreakdownForActivatedBatch?.totalTop5BatchHits || 0}`);
  lines.push(`- totalTop10BatchHits: ${report.contributionBreakdownForActivatedBatch?.totalTop10BatchHits || 0}`);
  for (const row of report.contributionBreakdownForActivatedBatch?.batchChunkTypeCountsTop10 || []) {
    lines.push(`- chunkType:${row.key} => ${row.count}`);
  }
  lines.push("");

  lines.push("## Recommendation");
  lines.push(`- ${report.recommendation}`);
  if (report.recommendation === "rollback_batch") {
    lines.push(`- rollback manifest: ${report.rollbackManifestRef}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}
