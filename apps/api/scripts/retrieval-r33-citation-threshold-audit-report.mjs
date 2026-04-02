import fs from "node:fs/promises";
import path from "node:path";
import { LIVE_SEARCH_QA_QUERIES, loadTrustedActivatedDocumentIds } from "./retrieval-live-search-qa-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const outputJsonName = "retrieval-r33-citation-threshold-audit-report.json";
const outputMdName = "retrieval-r33-citation-threshold-audit-report.md";
const configuredCeiling = Number(process.env.RETRIEVAL_CITATION_TOP_DOC_SHARE_CEILING || "0.1");
const kValues = [5, 10, 20];

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function mean(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, n) => sum + Number(n || 0), 0) / values.length).toFixed(4));
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) return Number(((sorted[mid - 1] + sorted[mid]) / 2).toFixed(4));
  return Number(sorted[mid].toFixed(4));
}

function min(values) {
  return values.length ? Number(Math.min(...values).toFixed(4)) : 0;
}

function max(values) {
  return values.length ? Number(Math.max(...values).toFixed(4)) : 0;
}

function shareFor(rows, denominator) {
  if (!rows.length || denominator <= 0) return 0;
  const counts = Object.values(countBy(rows.map((row) => row.documentId))).map((n) => Number(n || 0));
  const top = counts.length ? Math.max(...counts) : 0;
  return Number((top / denominator).toFixed(4));
}

function quantizedCeiling(v) {
  return Number(v.toFixed(4));
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON`);
  }
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

function summarizeDistribution(rows, field) {
  const values = rows.map((row) => Number(row[field] || 0));
  return {
    min: min(values),
    max: max(values),
    avg: mean(values),
    median: median(values)
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# R33 Citation Threshold Audit");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Recommendation");
  lines.push(`- ${report.recommendation.decision}`);
  lines.push(`- rationale: ${report.recommendation.rationale}`);
  lines.push("");
  lines.push("## Threshold Candidates");
  for (const row of report.thresholdCandidates || []) {
    lines.push(`- ${row.policy}: ${row.value}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const trusted = await loadTrustedActivatedDocumentIds({
    reportsDir,
    reportName: "retrieval-activation-write-report.json",
    manifestName: "retrieval-trusted-activation-manifest.json"
  });
  let trustedIds = (trusted?.trustedDocumentIds || []).map(String);
  try {
    const stableBaselineManifest = await readJson(path.resolve(reportsDir, "retrieval-r27-next-manifest.json"));
    const baselineIds = (stableBaselineManifest?.baselineTrustedDocIds || []).map(String);
    if (baselineIds.length) trustedIds = baselineIds;
  } catch {
    // fallback to loader output
  }
  if (!trustedIds.length) throw new Error("No trusted baseline IDs found for R33.");
  const citationQueries = LIVE_SEARCH_QA_QUERIES.filter((q) => String(q.queryType || "").toLowerCase() === "citation_lookup");
  let perQueryRaw = [];
  try {
    const r32Baseline = await readJson(path.resolve(reportsDir, "retrieval-r32-single-live-qa-report.json"));
    const beforeRows = (r32Baseline?.beforeQueryResults || []).filter((row) => /^citation_/.test(String(row?.queryId || "")));
    perQueryRaw = beforeRows.map((row) => ({
      queryId: row.queryId,
      query: row.query,
      queryType: "citation_lookup",
      trustedResultCount: (row.topResults || []).length,
      trustedResults: row.topResults || [],
      source: "r32_before_query_results"
    }));
  } catch {
    const trustedSet = new Set(trustedIds);
    for (const query of citationQueries) {
      const payload = {
        query: query.query,
        queryType: query.queryType || "citation_lookup",
        limit: 40,
        filters: {
          approvedOnly: true,
          fileType: "decision_docx"
        }
      };
      const response = await fetchJson(`${apiBase}/admin/retrieval/debug`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      const trustedResults = (response.results || []).filter((row) => trustedSet.has(String(row.documentId || "")));
      perQueryRaw.push({
        queryId: query.id,
        query: query.query,
        queryType: query.queryType,
        trustedResultCount: trustedResults.length,
        trustedResults,
        source: "live_api"
      });
    }
  }

  const byK = {};
  for (const k of kValues) {
    const rows = perQueryRaw.map((queryRow) => {
      const top = (queryRow.trustedResults || []).slice(0, k);
      const denominatorTopK = Math.min(k, top.length);
      const denominatorTotalTrusted = queryRow.trustedResultCount;
      const uniqueDocsTopK = new Set(top.map((row) => row.documentId)).size;
      const shareTopK = shareFor(top, denominatorTopK);
      const shareTotalTrusted = shareFor(top, denominatorTotalTrusted);
      const shareUniqueDocDenominator = shareFor(top, uniqueDocsTopK);
      const attainableFloorGivenUniqueDocs = uniqueDocsTopK > 0 ? Number((1 / uniqueDocsTopK).toFixed(4)) : 0;

      return {
        queryId: queryRow.queryId,
        query: queryRow.query,
        k,
        trustedResultCount: queryRow.trustedResultCount,
        uniqueDocsTopK,
        shareTopK,
        shareTotalTrusted,
        shareUniqueDocDenominator,
        attainableFloorGivenUniqueDocs,
        belowConfiguredCeiling: shareTopK <= configuredCeiling
      };
    });

    byK[String(k)] = {
      rows,
      distribution: {
        shareTopK: summarizeDistribution(rows, "shareTopK"),
        shareTotalTrusted: summarizeDistribution(rows, "shareTotalTrusted"),
        shareUniqueDocDenominator: summarizeDistribution(rows, "shareUniqueDocDenominator"),
        attainableFloorGivenUniqueDocs: summarizeDistribution(rows, "attainableFloorGivenUniqueDocs")
      }
    };
  }

  const focusK = byK["10"];
  const observedFloor = focusK?.distribution?.shareTopK?.min || 0;
  const attainableFloor = focusK?.distribution?.attainableFloorGivenUniqueDocs?.avg || 0;
  const ceilingOverConstrained = observedFloor > configuredCeiling || attainableFloor > configuredCeiling;

  const dynamicCeilingAtK10 = quantizedCeiling(
    Math.max(
      configuredCeiling,
      ...(focusK?.rows || []).map((row) => row.attainableFloorGivenUniqueDocs)
    )
  );
  const medianBasedCandidate = quantizedCeiling(focusK?.distribution?.shareTopK?.median || configuredCeiling);
  const conservativeCandidate = quantizedCeiling(Math.max(dynamicCeilingAtK10, medianBasedCandidate));

  const recommendation = ceilingOverConstrained
    ? {
        decision: "revise_current_ceiling",
        rationale:
          `Configured ceiling ${configuredCeiling} is below current observed/attainable floor at K=10 (observed min ${observedFloor}, attainable avg ${attainableFloor}).`,
        policy: {
          type: "queryType_and_corpus_size_aware",
          details:
            "For citation_lookup, set effective ceiling = max(global_ceiling, 1 / min(K, trusted_result_count_for_query)). Keep global ceiling unchanged for non-citation intents."
        }
      }
    : {
        decision: "keep_current_ceiling",
        rationale: `Configured ceiling ${configuredCeiling} is attainable at current K/query distribution.`,
        policy: {
          type: "fixed_global",
          details: "Keep current threshold policy."
        }
      };

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      configuredCitationTopDocumentShareCeiling: configuredCeiling,
      citationQueriesAnalyzed: citationQueries.length,
      trustedDocumentCount: trustedIds.length,
      dataSource: perQueryRaw[0]?.source || "unknown",
      kValues,
      ceilingOverConstrained,
      observedStableFloorK10: observedFloor,
      attainableFloorK10Avg: attainableFloor
    },
    concentrationByQueryAndK: byK,
    citationConcentrationDistribution: {
      k5: byK["5"]?.distribution || {},
      k10: byK["10"]?.distribution || {},
      k20: byK["20"]?.distribution || {}
    },
    thresholdCandidates: [
      { policy: "current_fixed_ceiling", value: configuredCeiling },
      { policy: "dynamic_unique_doc_floor_k10", value: dynamicCeilingAtK10 },
      { policy: "median_based_k10", value: medianBasedCandidate },
      { policy: "conservative_dynamic_candidate", value: conservativeCandidate }
    ],
    recommendation
  };

  const outJson = path.resolve(reportsDir, outputJsonName);
  const outMd = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(outJson, JSON.stringify(report, null, 2)),
    fs.writeFile(outMd, buildMarkdown(report))
  ]);

  console.log(
    JSON.stringify(
      {
        configuredCeiling,
        ceilingOverConstrained,
        observedStableFloorK10: observedFloor,
        attainableFloorK10Avg: attainableFloor,
        recommendation: recommendation.decision
      },
      null,
      2
    )
  );
  console.log(`R33 threshold audit report written to ${outJson}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
