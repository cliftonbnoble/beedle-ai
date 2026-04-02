export const RETRIEVAL_HEALTH_QUERIES = [
  { id: "heat", query: "heat", family: "housing_conditions" },
  { id: "hot_water", query: "hot water", family: "housing_conditions" },
  { id: "cooling", query: "cooling", family: "housing_conditions" },
  { id: "ventilation", query: "ventilation", family: "housing_conditions" },
  { id: "mold", query: "mold", family: "housing_conditions" },
  { id: "leak", query: "leak", family: "housing_conditions" },
  { id: "notice", query: "notice", family: "procedure_notice" },
  { id: "repair", query: "repair", family: "housing_conditions" },
  { id: "tenant_petition", query: "tenant petition", family: "procedure_notice" }
];

function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(4));
}

function sum(values) {
  return values.reduce((total, value) => total + Number(value || 0), 0);
}

export function summarizeHealthQuery(queryConfig, response) {
  const results = Array.isArray(response?.results) ? response.results : [];
  const uniqueDocumentIds = Array.from(new Set(results.map((row) => row.documentId).filter(Boolean))).sort();
  const topChunkTypes = Array.from(
    results.reduce((map, row) => {
      const key = String(row?.chunkType || "<none>");
      map.set(key, (map.get(key) || 0) + 1);
      return map;
    }, new Map()).entries()
  )
    .sort((a, b) => (b[1] - a[1]) || String(a[0]).localeCompare(String(b[0])))
    .slice(0, 3)
    .map(([chunkType]) => chunkType);

  return {
    id: queryConfig.id,
    query: queryConfig.query,
    family: queryConfig.family,
    totalResults: Number(response?.total || results.length || 0),
    uniqueDecisionCount: uniqueDocumentIds.length,
    topDecisionIds: uniqueDocumentIds.slice(0, 5),
    topChunkTypes,
    avgScore: avg(results.map((row) => Number(row?.score || 0))),
    avgVectorScore: avg(results.map((row) => Number(row?.vectorScore || 0))),
    avgLexicalScore: avg(results.map((row) => Number(row?.lexicalScore || 0))),
    vectorMatchCount: Number(response?.runtimeDiagnostics?.vectorMatchCount || 0),
    returnedAny: results.length > 0
  };
}

export function summarizeRetrievalHealth(reportRows) {
  const rows = Array.isArray(reportRows) ? reportRows : [];
  const returned = rows.filter((row) => row.returnedAny);
  const uniqueDecisionUniverse = Array.from(new Set(returned.flatMap((row) => row.topDecisionIds || []))).sort();

  const familyStats = Array.from(
    rows.reduce((map, row) => {
      const existing = map.get(row.family) || {
        family: row.family,
        queryCount: 0,
        returnedCount: 0,
        totalResults: 0,
        uniqueDecisionCountSum: 0,
        vectorMatchCountSum: 0
      };
      existing.queryCount += 1;
      existing.returnedCount += row.returnedAny ? 1 : 0;
      existing.totalResults += Number(row.totalResults || 0);
      existing.uniqueDecisionCountSum += Number(row.uniqueDecisionCount || 0);
      existing.vectorMatchCountSum += Number(row.vectorMatchCount || 0);
      map.set(row.family, existing);
      return map;
    }, new Map()).values()
  ).map((entry) => ({
    family: entry.family,
    queryCount: entry.queryCount,
    returnedCount: entry.returnedCount,
    hitRate: entry.queryCount > 0 ? Number((entry.returnedCount / entry.queryCount).toFixed(4)) : 0,
    avgResults: entry.queryCount > 0 ? Number((entry.totalResults / entry.queryCount).toFixed(4)) : 0,
    avgDecisionDiversity: entry.queryCount > 0 ? Number((entry.uniqueDecisionCountSum / entry.queryCount).toFixed(4)) : 0,
    avgVectorMatchCount: entry.queryCount > 0 ? Number((entry.vectorMatchCountSum / entry.queryCount).toFixed(4)) : 0
  }));

  return {
    queryCount: rows.length,
    returnedQueryCount: returned.length,
    overallHitRate: rows.length > 0 ? Number((returned.length / rows.length).toFixed(4)) : 0,
    avgResultsPerQuery: avg(rows.map((row) => Number(row.totalResults || 0))),
    avgDecisionDiversity: avg(rows.map((row) => Number(row.uniqueDecisionCount || 0))),
    avgVectorMatchCount: avg(rows.map((row) => Number(row.vectorMatchCount || 0))),
    uniqueDecisionUniverseCount: uniqueDecisionUniverse.length,
    uniqueDecisionUniverse,
    familyStats
  };
}

export function compareHealthSummaries(current, baseline) {
  if (!baseline) return null;
  return {
    overallHitRateDelta: Number((Number(current.overallHitRate || 0) - Number(baseline.overallHitRate || 0)).toFixed(4)),
    avgResultsPerQueryDelta: Number((Number(current.avgResultsPerQuery || 0) - Number(baseline.avgResultsPerQuery || 0)).toFixed(4)),
    avgDecisionDiversityDelta: Number((Number(current.avgDecisionDiversity || 0) - Number(baseline.avgDecisionDiversity || 0)).toFixed(4)),
    avgVectorMatchCountDelta: Number((Number(current.avgVectorMatchCount || 0) - Number(baseline.avgVectorMatchCount || 0)).toFixed(4)),
    uniqueDecisionUniverseCountDelta: Number(current.uniqueDecisionUniverseCount || 0) - Number(baseline.uniqueDecisionUniverseCount || 0)
  };
}

export function formatRetrievalHealthMarkdown(report) {
  const lines = [
    "# Retrieval Health Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Corpus mode: \`${report.corpusMode}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Returned query count: \`${report.summary.returnedQueryCount}\``,
    `- Overall hit rate: \`${report.summary.overallHitRate}\``,
    `- Avg results/query: \`${report.summary.avgResultsPerQuery}\``,
    `- Avg decision diversity/query: \`${report.summary.avgDecisionDiversity}\``,
    `- Unique decision universe: \`${report.summary.uniqueDecisionUniverseCount}\``,
    ""
  ];

  if (report.deltaVsBaseline) {
    lines.push("## Delta vs Baseline");
    lines.push("");
    lines.push(`- Hit rate delta: \`${report.deltaVsBaseline.overallHitRateDelta}\``);
    lines.push(`- Avg results/query delta: \`${report.deltaVsBaseline.avgResultsPerQueryDelta}\``);
    lines.push(`- Avg decision diversity delta: \`${report.deltaVsBaseline.avgDecisionDiversityDelta}\``);
    lines.push(`- Avg vector match count delta: \`${report.deltaVsBaseline.avgVectorMatchCountDelta}\``);
    lines.push(`- Unique decision universe delta: \`${report.deltaVsBaseline.uniqueDecisionUniverseCountDelta}\``);
    lines.push("");
  }

  lines.push("## Family Coverage");
  lines.push("");
  for (const family of report.summary.familyStats || []) {
    lines.push(
      `- \`${family.family}\` | hitRate=${family.hitRate} | avgResults=${family.avgResults} | avgDecisionDiversity=${family.avgDecisionDiversity} | avgVectorMatchCount=${family.avgVectorMatchCount}`
    );
  }
  lines.push("");

  lines.push("## Query Detail");
  lines.push("");
  for (const row of report.queries || []) {
    lines.push(
      `- \`${row.query}\` | total=${row.totalResults} | uniqueDecisions=${row.uniqueDecisionCount} | topChunkTypes=${(row.topChunkTypes || []).join(", ") || "<none>"} | vectorMatchCount=${row.vectorMatchCount}`
    );
  }
  lines.push("");

  return `${lines.join("\n")}\n`;
}
