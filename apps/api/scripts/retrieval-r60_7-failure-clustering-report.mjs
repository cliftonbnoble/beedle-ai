import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const traceReportName = process.env.RETRIEVAL_R60_7_TRACE_REPORT_NAME || "retrieval-r60_6-endpoint-trace-report.json";
const outputJsonName = process.env.RETRIEVAL_R60_7_REPORT_NAME || "retrieval-r60_7-failure-clustering-report.json";
const outputMdName = process.env.RETRIEVAL_R60_7_MARKDOWN_NAME || "retrieval-r60_7-failure-clustering-report.md";

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function countBy(values = []) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCounts(obj = {}) {
  return Object.entries(obj)
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([key, count]) => ({ key, count }));
}

function inferIntent(queryId = "") {
  const id = String(queryId || "");
  if (id.includes("authority")) return "authority_lookup";
  if (id.includes("findings") || id.includes("evidence")) return "findings";
  if (id.includes("procedural")) return "procedural_history";
  if (id.includes("issue") || id.includes("disposition")) return "issue_holding_disposition";
  if (id.includes("analysis")) return "analysis_reasoning";
  if (id.includes("comparative")) return "comparative_reasoning";
  if (id.includes("citation")) return "citation_direct";
  return "unknown";
}

function classifyTask(row) {
  const expectedDecisionIds = unique(row.expectedDecisionIds || []);
  const returnedDecisionIds = unique(row.topReturnedDecisionIds || []);
  const expectedSectionTypes = unique((row.expectedSectionTypes || []).map(String));
  const returnedSectionTypes = unique((row.topReturnedSectionTypes || []).map(String));

  const decisionMatched = expectedDecisionIds.some((id) => returnedDecisionIds.includes(id));
  const sectionMatched = expectedSectionTypes.some((type) => returnedSectionTypes.includes(type));

  let failureCluster = "recovered_and_correct";
  if (row.taskClassifiedEmpty || (row.parsedResultCount || 0) === 0) failureCluster = "empty_even_after_rewrite";
  else if (!decisionMatched) failureCluster = "recovered_but_wrong_decision";
  else if (!sectionMatched) failureCluster = "recovered_but_wrong_section_type";

  let likelyPrimaryCause = "none";
  let likelySecondaryCause = "none";
  if (failureCluster === "empty_even_after_rewrite") {
    likelyPrimaryCause = "query_formulation_gap";
    likelySecondaryCause = "runtime_scope_filtering_or_index_coverage_gap";
  } else if (failureCluster === "recovered_but_wrong_decision") {
    likelyPrimaryCause = "decision_retrieval_gap";
    likelySecondaryCause = "ranking_relevance_drift";
  } else if (failureCluster === "recovered_but_wrong_section_type") {
    likelyPrimaryCause = "section_routing_gap";
    likelySecondaryCause = "chunk_type_rerank_misalignment";
  }

  return {
    queryId: String(row.queryId || ""),
    adoptedQuery: String(row.adoptedQuery || ""),
    intent: inferIntent(row.queryId),
    expectedDecisionIds,
    returnedDecisionIds,
    expectedSectionTypes,
    returnedSectionTypes,
    failureCluster,
    likelyPrimaryCause,
    likelySecondaryCause
  };
}

function classifyBottleneck(clusterCounts) {
  const empty = Number(clusterCounts.empty_even_after_rewrite || 0);
  const wrongDecision = Number(clusterCounts.recovered_but_wrong_decision || 0);
  const wrongSection = Number(clusterCounts.recovered_but_wrong_section_type || 0);
  if (empty > wrongDecision + wrongSection) return "query_formulation_gap";
  if (wrongDecision > 0 && wrongSection === 0) return "decision_retrieval_gap";
  if (wrongSection > 0 && wrongDecision === 0) return "section_routing_gap";
  if (wrongDecision > 0 && wrongSection > 0) return "mixed_retrieval_and_section_gap";
  return "query_formulation_gap";
}

export function buildR60_7FailureClusteringReport(traceReport) {
  const rows = (traceReport?.perTaskRows || []).map(classifyTask).sort((a, b) => a.queryId.localeCompare(b.queryId));
  const clusterCounts = countBy(rows.map((r) => r.failureCluster));

  const emptyByIntent = sortCounts(
    countBy(rows.filter((r) => r.failureCluster === "empty_even_after_rewrite").map((r) => r.intent))
  );
  const wrongDecisionByIntent = sortCounts(
    countBy(rows.filter((r) => r.failureCluster === "recovered_but_wrong_decision").map((r) => r.intent))
  );
  const wrongSectionTypeByIntent = sortCounts(
    countBy(rows.filter((r) => r.failureCluster === "recovered_but_wrong_section_type").map((r) => r.intent))
  );

  const dominantFalsePositiveChunkTypes = sortCounts(
    countBy(
      rows
        .filter((r) => r.failureCluster !== "recovered_and_correct" && r.failureCluster !== "empty_even_after_rewrite")
        .flatMap((r) => r.returnedSectionTypes.filter((type) => !r.expectedSectionTypes.includes(type)))
    )
  );

  const dominantReturnedSectionTypesForMisses = sortCounts(
    countBy(
      rows
        .filter((r) => r.failureCluster !== "recovered_and_correct")
        .flatMap((r) => r.returnedSectionTypes)
    )
  );

  const recommendedFixOrder = [
    "stabilize_query_templates_for_empty_even_after_rewrite_cluster",
    "improve_decision_ranking_for_recovered_but_wrong_decision_cluster",
    "tighten_section_type_routing_for_recovered_but_wrong_section_type_cluster",
    "re-run_r60_goldset_with_cluster_specific_fixes"
  ];

  const primarySystemBottleneck = classifyBottleneck(clusterCounts);

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.7",
    clusterCounts,
    emptyByIntent,
    wrongDecisionByIntent,
    wrongSectionTypeByIntent,
    dominantFalsePositiveChunkTypes,
    dominantReturnedSectionTypesForMisses,
    recommendedFixOrder,
    primarySystemBottleneck,
    perTaskRows: rows
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.7 Recovered-Task Failure Clustering (Dry Run)");
  lines.push("");
  lines.push("## Cluster Counts");
  for (const [cluster, count] of Object.entries(report.clusterCounts || {})) lines.push(`- ${cluster}: ${count}`);
  lines.push(`- primarySystemBottleneck: ${report.primarySystemBottleneck}`);
  lines.push("");

  lines.push("## Empty By Intent");
  for (const row of report.emptyByIntent || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.emptyByIntent || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Wrong Decision By Intent");
  for (const row of report.wrongDecisionByIntent || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.wrongDecisionByIntent || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Wrong Section Type By Intent");
  for (const row of report.wrongSectionTypeByIntent || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.wrongSectionTypeByIntent || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Recommended Fix Order");
  for (const step of report.recommendedFixOrder || []) lines.push(`- ${step}`);
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const traceReport = JSON.parse(await fs.readFile(path.resolve(reportsDir, traceReportName), "utf8"));
  const report = buildR60_7FailureClusteringReport(traceReport);

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        clusterCounts: report.clusterCounts,
        primarySystemBottleneck: report.primarySystemBottleneck
      },
      null,
      2
    )
  );
  console.log(`R60.7 failure clustering report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
