import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.RETRIEVAL_BASELINE_RESTORATION_REPORT_NAME || "retrieval-baseline-restoration-report.json";
const markdownName = process.env.RETRIEVAL_BASELINE_RESTORATION_MARKDOWN_NAME || "retrieval-baseline-restoration-report.md";

async function readJson(name) {
  const filePath = path.resolve(reportsDir, name);
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function mapByQuery(report) {
  return new Map((report?.queryResults || []).map((row) => [row.queryId, row]));
}

function topRows(row, count = 5) {
  return (row?.topResults || []).slice(0, count).map((r) => ({
    documentId: r.documentId,
    title: r.title,
    chunkId: r.chunkId,
    chunkType: r.chunkType,
    score: Number(r.score || 0)
  }));
}

function differenceIds(a, b) {
  const setB = new Set((b || []).map((row) => row.chunkId));
  return (a || []).filter((row) => !setB.has(row.chunkId));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortedCounts(map) {
  return Object.entries(map || {})
    .sort((a, b) => (b[1] === a[1] ? String(a[0]).localeCompare(String(b[0])) : b[1] - a[1]))
    .map(([key, count]) => ({ key, count }));
}

function fmt(n) {
  return Number(n || 0);
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Baseline Restoration Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Root Cause");
  for (const finding of report.rootCauseFindings || []) {
    lines.push(`- ${finding.code}: ${finding.detail}`);
  }
  lines.push("");
  lines.push("## Per Query Delta");
  for (const row of report.perQueryDiagnosis || []) {
    lines.push(
      `- ${row.queryId}: baseline=${row.baselineQualityScore}, current=${row.currentQualityScore}, delta=${row.deltaQualityScore}, outcome=${row.outcome}`
    );
  }
  lines.push("");
  lines.push("## Tuning Adjustment");
  lines.push(`- ${report.tuningAdjustment?.change || "<none>"}`);
  lines.push(`- ${report.tuningAdjustment?.rationale || "<none>"}`);
  lines.push("");
  lines.push("## Readiness Recommendation");
  lines.push(`- ${report.nextRecommendation || "no_recommendation"}`);
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const baseline = await readJson("retrieval-live-search-qa-report.json");
  const badPostBatch = await readJson("retrieval-batch-live-qa-report.json");
  const restored = await readJson("retrieval-live-search-qa-restored-report.json");
  const rollback = await readJson("retrieval-batch-rollback-report.json");

  const baselineByQuery = mapByQuery(baseline);
  const badByQuery = mapByQuery({
    queryResults: (badPostBatch.afterQueryResults || []).map((row) => ({
      ...row,
      queryId: row.queryId || row.id
    }))
  });
  const restoredByQuery = mapByQuery(restored);

  const queryIds = Array.from(
    new Set([...(baseline.queryResults || []).map((q) => q.queryId), ...(restored.queryResults || []).map((q) => q.queryId)])
  ).sort((a, b) => a.localeCompare(b));

  const perQueryDiagnosis = [];
  const lossChunkTypes = [];
  const introducedChunkTypes = [];

  for (const queryId of queryIds) {
    const b = baselineByQuery.get(queryId) || {};
    const g = restoredByQuery.get(queryId) || {};
    const baselineTop = topRows(b);
    const currentTop = topRows(g);
    const missingGoodHits = differenceIds(baselineTop, currentTop);
    const newlyIntroducedHits = differenceIds(currentTop, baselineTop);

    for (const row of missingGoodHits) lossChunkTypes.push(row.chunkType || "<none>");
    for (const row of newlyIntroducedHits) introducedChunkTypes.push(row.chunkType || "<none>");

    const baselineQualityScore = fmt(b?.metrics?.qualityScore);
    const currentQualityScore = fmt(g?.metrics?.qualityScore);
    const deltaQualityScore = Number((currentQualityScore - baselineQualityScore).toFixed(2));
    const badScore = fmt((badByQuery.get(queryId) || {}).metrics?.qualityScore);

    perQueryDiagnosis.push({
      queryId,
      query: String(g?.query || b?.query || ""),
      baselineQualityScore,
      badPostBatchQualityScore: badScore,
      currentQualityScore,
      deltaQualityScore,
      outcome: deltaQualityScore > 0 ? "improved" : deltaQualityScore < 0 ? "worsened" : "unchanged",
      baselineTopResults: baselineTop,
      currentTopResults: currentTop,
      missingFormerlyGoodHits: missingGoodHits,
      newlyIntroducedLowerQualityHits: newlyIntroducedHits,
      metricDeltas: {
        topDocumentShare: Number((fmt(g?.metrics?.topDocumentShare) - fmt(b?.metrics?.topDocumentShare)).toFixed(4)),
        uniqueDocuments: fmt(g?.metrics?.uniqueDocuments) - fmt(b?.metrics?.uniqueDocuments),
        uniqueChunkTypes: fmt(g?.metrics?.uniqueChunkTypes) - fmt(b?.metrics?.uniqueChunkTypes),
        duplicatePressure: Number((fmt(g?.metrics?.duplicatePressure) - fmt(b?.metrics?.duplicatePressure)).toFixed(4)),
        expectedTypeHitRate: Number((fmt(g?.metrics?.expectedTypeHitRate) - fmt(b?.metrics?.expectedTypeHitRate)).toFixed(4))
      }
    });
  }

  const worsened = perQueryDiagnosis.filter((row) => row.outcome === "worsened");
  const citationWorsened = worsened.filter((row) => /citation_/.test(row.queryId));
  const averageGap = Number((fmt(restored.summary?.averageQualityScore) - fmt(baseline.summary?.averageQualityScore)).toFixed(2));

  const rootCauseFindings = [
    {
      code: "citation_query_diversity_regression",
      detail:
        "Direct citation queries still show higher document concentration than baseline, reducing quality score despite safe corpus scope.",
      evidence: {
        citationWorsenedQueryCount: citationWorsened.length,
        citationQueries: citationWorsened.map((row) => row.queryId),
        currentCitationTopShare: citationWorsened.map((row) => ({
          queryId: row.queryId,
          topDocumentShare: row.currentTopResults.length
            ? Number(
                (
                  Math.max(
                    ...Object.values(
                      countBy((row.currentTopResults || []).map((item) => item.documentId))
                    ).map((n) => Number(n || 0))
                  ) / row.currentTopResults.length
                ).toFixed(4)
              )
            : 0
        }))
      }
    },
    {
      code: "residual_query_level_deltas_after_rollback",
      detail: "Rollback removed noisy batch docs, but a small set of baseline query top-hit compositions did not fully restore.",
      evidence: {
        worsenedQueryCount: worsened.length,
        improvedQueryCount: perQueryDiagnosis.filter((row) => row.outcome === "improved").length,
        unchangedQueryCount: perQueryDiagnosis.filter((row) => row.outcome === "unchanged").length
      }
    }
  ];

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      baselineAverageQualityScore: fmt(baseline.summary?.averageQualityScore),
      badPostBatchAverageQualityScore: fmt(badPostBatch?.summary?.after?.averageQualityScore),
      currentPostRollbackAverageQualityScore: fmt(restored.summary?.averageQualityScore),
      deltaFromBaseline: averageGap,
      rollbackVerificationPassed: Boolean(rollback.summary?.rollbackVerificationPassed),
      perQueryImprovedCount: perQueryDiagnosis.filter((row) => row.outcome === "improved").length,
      perQueryUnchangedCount: perQueryDiagnosis.filter((row) => row.outcome === "unchanged").length,
      perQueryWorsenedCount: perQueryDiagnosis.filter((row) => row.outcome === "worsened").length
    },
    rootCauseFindings,
    lossSourceQuantification: {
      missingTopChunkTypeCounts: sortedCounts(countBy(lossChunkTypes)),
      introducedTopChunkTypeCounts: sortedCounts(countBy(introducedChunkTypes)),
      worsenedQueries: worsened.map((row) => ({
        queryId: row.queryId,
        deltaQualityScore: row.deltaQualityScore,
        metricDeltas: row.metricDeltas
      }))
    },
    tuningAdjustment: {
      change: "citation_lookup per-document cap tightened from 4 to 2 in diversify()",
      rationale:
        "Reduces citation-query concentration and duplicate pressure without changing trust/admission/provenance gates."
    },
    perQueryDiagnosis,
    nextRecommendation:
      averageGap >= 0
        ? "ready_for_next_batch_simulation"
        : "run_one_more_narrow_citation_diversity_tune_before_next_batch_simulation"
  };

  const reportPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, buildMarkdown(report));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval baseline restoration report written to ${reportPath}`);
  console.log(`Retrieval baseline restoration markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});

