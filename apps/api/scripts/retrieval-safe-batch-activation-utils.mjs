function normalizeChunkType(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function isLowSignalStructuralChunkType(value) {
  const t = normalizeChunkType(value);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function mean(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, n) => sum + Number(n || 0), 0) / values.length).toFixed(4));
}

function isCitationIntentQueryId(queryId) {
  return /citation_/.test(String(queryId || ""));
}

function uniqueDocumentCountFromTopResults(topResults) {
  const docs = new Set();
  for (const row of Array.isArray(topResults) ? topResults : []) {
    const docId = String(row?.documentId || "").trim();
    if (docId) docs.add(docId);
  }
  return docs.size;
}

export function computeAttainableFloorGivenUniqueDocsAtK(queryResults, k = 10) {
  const safeK = Math.max(1, Number.parseInt(String(k || 10), 10) || 10);
  const floors = [];
  for (const row of queryResults || []) {
    if (!isCitationIntentQueryId(row?.queryId)) continue;
    const explicitUnique = Number(row?.metrics?.uniqueDocumentsInTopK || 0);
    const inferredUnique = uniqueDocumentCountFromTopResults(row?.topResults || []);
    const uniqueDocs = Math.max(explicitUnique, inferredUnique);
    if (!uniqueDocs) continue;
    floors.push(Number((1 / Math.min(safeK, uniqueDocs)).toFixed(4)));
  }
  return mean(floors);
}

export function resolveCitationTopDocumentShareCeiling({
  baselineCitationQueryResults = [],
  configuredGlobalCeiling = 0.1,
  k = 10
} = {}) {
  const configured = Number(configuredGlobalCeiling || 0.1);
  const attainableFloor = computeAttainableFloorGivenUniqueDocsAtK(baselineCitationQueryResults, k);
  const effectiveCeiling = Number(Math.max(configured, attainableFloor).toFixed(4));
  return {
    configuredGlobalCeiling: configured,
    attainableFloorGivenUniqueDocsAtK: attainableFloor,
    effectiveCeiling,
    k
  };
}

export function computeCitationTopDocumentShareAverage(queryResults) {
  const rows = (queryResults || []).filter((row) => isCitationIntentQueryId(row?.queryId));
  return mean(rows.map((row) => Number(row?.metrics?.topDocumentShare || 0)));
}

export function computeLowSignalStructuralShare(queryResults) {
  const nonStructuralRows = (queryResults || []).filter((row) => !/citation_/.test(String(row?.queryId || "")));
  let total = 0;
  let lowSignal = 0;
  for (const row of nonStructuralRows) {
    const top = Array.isArray(row?.topResults) ? row.topResults.slice(0, 10) : [];
    total += top.length;
    lowSignal += top.filter((result) => isLowSignalStructuralChunkType(result?.chunkType || result?.sectionLabel || "")).length;
  }
  return total ? Number((lowSignal / total).toFixed(4)) : 0;
}

export function evaluateSafeBatchHardGate({
  baselineAverageQualityScore = 65.31,
  afterSummary = {},
  beforeQueryResults = [],
  afterQueryResults = [],
  minAllowedQualityScore = 64.81
}) {
  const beforeCitationTopDocumentShareAvg = computeCitationTopDocumentShareAverage(beforeQueryResults);
  const afterCitationTopDocumentShareAvg = computeCitationTopDocumentShareAverage(afterQueryResults);
  const beforeLowSignalStructuralShare = computeLowSignalStructuralShare(beforeQueryResults);
  const afterLowSignalStructuralShare = computeLowSignalStructuralShare(afterQueryResults);

  const checks = {
    outOfCorpusHitQueryCountZero: Number(afterSummary?.outOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(afterSummary?.zeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(afterSummary?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(afterSummary?.citationAnchorCoverageAverage || 0) === 1,
    qualityAboveThreshold: Number(afterSummary?.averageQualityScore || 0) >= Number(minAllowedQualityScore || 64.81),
    citationConcentrationNotWorse: Number(afterCitationTopDocumentShareAvg || 0) <= Number(beforeCitationTopDocumentShareAvg || 0),
    lowSignalStructuralShareNotWorse: Number(afterLowSignalStructuralShare || 0) <= Number(beforeLowSignalStructuralShare || 0)
  };

  const failures = Object.entries(checks)
    .filter(([, passed]) => !passed)
    .map(([name]) => name);

  return {
    passed: failures.length === 0,
    checks,
    failures,
    baselineAverageQualityScore: Number(baselineAverageQualityScore || 0),
    minAllowedQualityScore: Number(minAllowedQualityScore || 64.81),
    afterAverageQualityScore: Number(afterSummary?.averageQualityScore || 0),
    beforeCitationTopDocumentShareAvg,
    afterCitationTopDocumentShareAvg,
    deltaCitationTopDocumentShare: Number((afterCitationTopDocumentShareAvg - beforeCitationTopDocumentShareAvg).toFixed(4)),
    beforeLowSignalStructuralShare,
    afterLowSignalStructuralShare,
    deltaLowSignalStructuralShare: Number((afterLowSignalStructuralShare - beforeLowSignalStructuralShare).toFixed(4))
  };
}

export function buildSafeBatchActivationMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Safe Batch Activation Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Hard Gate");
  for (const [k, v] of Object.entries(report.hardGate?.checks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.hardGate?.failures || []).length) lines.push(`- failures: ${(report.hardGate.failures || []).join(", ")}`);
  lines.push("");
  lines.push("## Activated Docs");
  for (const row of report.documentsActivated || []) lines.push(`- ${row.documentId} | ${row.title || ""}`);
  if (!(report.documentsActivated || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Recommendation");
  lines.push(`- keep_or_rollback: ${report.summary?.keepOrRollbackRecommendation || "rollback_batch"}`);
  lines.push(`- rollbackManifest: ${report.rollbackManifestFile || "retrieval-safe-batch-rollback-manifest.json"}`);
  return `${lines.join("\n")}\n`;
}
