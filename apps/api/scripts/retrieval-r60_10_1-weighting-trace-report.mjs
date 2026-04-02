import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  BENCHMARK_INTENT_TO_QUERY_TYPE,
  buildScoringInputs,
  buildBenchmarkDebugPayload,
  callBenchmarkDebug,
  normalizeSectionTypeRuntime
} from "./retrieval-benchmark-contract-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const tasksName = process.env.RETRIEVAL_R60_10_1_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const normalizedName = process.env.RETRIEVAL_R60_10_1_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const weightedName = process.env.RETRIEVAL_R60_10_1_WEIGHTED_NAME || "retrieval-r60_10-weighted-queries.json";
const evalName = process.env.RETRIEVAL_R60_10_1_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const outputReportName = process.env.RETRIEVAL_R60_10_1_REPORT_NAME || "retrieval-r60_10_1-weighting-trace-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_1_MARKDOWN_NAME || "retrieval-r60_10_1-weighting-trace-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_10_1_LIMIT || "10", 10);

const INTENT_TO_QUERY_TYPE = BENCHMARK_INTENT_TO_QUERY_TYPE;

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

function tokenize(query) {
  return String(query || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
}

const normalizeSectionType = normalizeSectionTypeRuntime;

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchDebugDetailed(apiBaseUrl, payload) {
  const contract = await callBenchmarkDebug({ apiBaseUrl, payload });
  return {
    endpointOk: Boolean(contract.fetchSucceeded && contract.responseOk),
    parseOk: Boolean(contract.parseSucceeded),
    rawResultCount: Number(contract.parsedResults?.length || 0),
    results: contract.parsedResults || []
  };
}

function classifyCollapseCause(row) {
  if (!row.normalizedEndpointOk || !row.weightedEndpointOk || !row.normalizedParseOk || !row.weightedParseOk) {
    return "runtime_endpoint_interpretation_mismatch";
  }

  if (!row.weightedQuery || tokenize(row.weightedQuery).length === 0) {
    return "malformed_weight_template";
  }

  if (row.weightedRawResultCount < row.normalizedRawResultCount) {
    const weightedTokens = tokenize(row.weightedQuery).length;
    const normalizedTokens = tokenize(row.normalizedQuery).length;
    if (weightedTokens >= normalizedTokens + 3) return "overconstrained_query_text";
    return "lexical_match_collapse";
  }

  if (row.weightedEmpty && !row.normalizedEmpty) {
    return "lexical_match_collapse";
  }

  if (row.weightedRawResultCount === row.normalizedRawResultCount && row.weightedParsedResultCount < row.normalizedParsedResultCount) {
    return "lexical_match_collapse";
  }

  if (
    row.weightedRawResultCount === row.normalizedRawResultCount &&
    row.weightedParsedResultCount === row.normalizedParsedResultCount &&
    row.normalizedTopReturnedDecisionIds.join("|") !== row.weightedTopReturnedDecisionIds.join("|")
  ) {
    return "runtime_endpoint_interpretation_mismatch";
  }

  return "mixed_causes";
}

function chooseRootCause(rows) {
  const counts = countBy(rows.map((row) => row.likelyCollapseCause));
  const ordered = Object.entries(counts).sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return a[0].localeCompare(b[0]);
  });
  return {
    rootCauseClassification: ordered[0]?.[0] || "mixed_causes",
    dominantCollapsePatterns: ordered.map(([pattern, count]) => ({ pattern, count }))
  };
}

function recommendRollbackScope(rows) {
  const reducedParsed = rows.filter((row) => row.weightedParsedResultCount < row.normalizedParsedResultCount);
  const reducedRate = reducedParsed.length / Math.max(1, rows.length);
  const proceduralReduced = reducedParsed.filter((row) => row.intent === "procedural_history").length;

  if (reducedRate >= 0.7) return "rollback_all_weight_templates";
  if (proceduralReduced >= Math.max(1, Math.floor(reducedParsed.length / 2))) return "rollback_procedural_weight_template_first";
  if (reducedParsed.length > 0) return "rollback_templates_for_reduced_tasks_only";
  return "no_rollback_scope_detected";
}

export async function buildR60_10_1WeightingTraceReport({
  tasks,
  normalizedQueries,
  weightedQueries,
  trustedDecisionIds,
  apiBaseUrl
}) {
  const normalizedById = new Map((normalizedQueries || []).map((row) => [String(row?.queryId || ""), row]));
  const weightedById = new Map((weightedQueries || []).map((row) => [String(row?.queryId || ""), row]));
  const trustedSet = new Set((trustedDecisionIds || []).map(String));

  const rows = [];
  for (const task of (tasks || []).slice().sort((a, b) => String(a?.queryId || "").localeCompare(String(b?.queryId || "")))) {
    const queryId = String(task?.queryId || "");
    const intent = String(task?.intent || "");
    const normalizedQuery = String(normalizedById.get(queryId)?.normalizedQuery || task?.adoptedQuery || task?.originalQuery || "");
    const weightedQuery = String(weightedById.get(queryId)?.weightedQuery || "");
    const queryType = INTENT_TO_QUERY_TYPE[intent] || "keyword";

    const normalizedRuntime = await fetchDebugDetailed(
      apiBaseUrl,
      buildBenchmarkDebugPayload({
        query: normalizedQuery,
        queryType,
        limit
      })
    );
    const weightedRuntime = await fetchDebugDetailed(
      apiBaseUrl,
      buildBenchmarkDebugPayload({
        query: weightedQuery,
        queryType,
        limit
      })
    );

    const normalizedTrusted = (normalizedRuntime.results || [])
      .filter((row) => trustedSet.has(String(row?.documentId || "")))
      .slice(0, limit);
    const weightedTrusted = (weightedRuntime.results || [])
      .filter((row) => trustedSet.has(String(row?.documentId || "")))
      .slice(0, limit);

    const row = {
      queryId,
      intent,
      normalizedQuery,
      weightedQuery,
      normalizedEndpointOk: normalizedRuntime.endpointOk,
      weightedEndpointOk: weightedRuntime.endpointOk,
      normalizedParseOk: normalizedRuntime.parseOk,
      weightedParseOk: weightedRuntime.parseOk,
      normalizedRawResultCount: normalizedRuntime.rawResultCount,
      weightedRawResultCount: weightedRuntime.rawResultCount,
      normalizedParsedResultCount: normalizedTrusted.length,
      weightedParsedResultCount: weightedTrusted.length,
      normalizedTopReturnedDecisionIds: unique(normalizedTrusted.slice(0, 5).map((r) => r.documentId)),
      weightedTopReturnedDecisionIds: unique(weightedTrusted.slice(0, 5).map((r) => r.documentId)),
      normalizedTopReturnedSectionTypes: unique(
        normalizedTrusted.slice(0, 5).map((r) => normalizeSectionType(r.sectionLabel || r.chunkType || ""))
      ),
      weightedTopReturnedSectionTypes: unique(
        weightedTrusted.slice(0, 5).map((r) => normalizeSectionType(r.sectionLabel || r.chunkType || ""))
      )
    };
    const normalizedScoring = buildScoringInputs({
      task: { expectedDecisionIds: [], expectedSectionTypes: [] },
      trustedRows: normalizedTrusted.map((r) => ({
        documentId: String(r?.documentId || ""),
        sectionType: normalizeSectionType(r.sectionLabel || r.chunkType || "")
      })),
      topK: 5
    });
    const weightedScoring = buildScoringInputs({
      task: { expectedDecisionIds: [], expectedSectionTypes: [] },
      trustedRows: weightedTrusted.map((r) => ({
        documentId: String(r?.documentId || ""),
        sectionType: normalizeSectionType(r.sectionLabel || r.chunkType || "")
      })),
      topK: 5
    });
    row.normalizedEmpty = Boolean(normalizedScoring.isEmpty);
    row.weightedEmpty = Boolean(weightedScoring.isEmpty);
    row.likelyCollapseCause = classifyCollapseCause(row);
    rows.push(row);
  }

  const tasksWhereWeightingReducedRawResults = rows
    .filter((row) => row.weightedRawResultCount < row.normalizedRawResultCount)
    .map((row) => row.queryId);
  const tasksWhereWeightingReducedParsedResults = rows
    .filter((row) => row.weightedParsedResultCount < row.normalizedParsedResultCount)
    .map((row) => row.queryId);
  const tasksWhereWeightingChangedEndpointBehavior = rows
    .filter(
      (row) =>
        row.normalizedEndpointOk !== row.weightedEndpointOk ||
        row.normalizedParseOk !== row.weightedParseOk
    )
    .map((row) => row.queryId);
  const tasksWhereWeightingOnlyChangedMatching = rows
    .filter(
      (row) =>
        row.normalizedEndpointOk === row.weightedEndpointOk &&
        row.normalizedParseOk === row.weightedParseOk &&
        row.normalizedRawResultCount === row.weightedRawResultCount &&
        (row.normalizedParsedResultCount !== row.weightedParsedResultCount ||
          row.normalizedTopReturnedDecisionIds.join("|") !== row.weightedTopReturnedDecisionIds.join("|"))
    )
    .map((row) => row.queryId);

  const root = chooseRootCause(rows);
  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.1",
    tasksEvaluated: rows.length,
    tasksWhereWeightingReducedRawResults: unique(tasksWhereWeightingReducedRawResults),
    tasksWhereWeightingReducedParsedResults: unique(tasksWhereWeightingReducedParsedResults),
    tasksWhereWeightingChangedEndpointBehavior: unique(tasksWhereWeightingChangedEndpointBehavior),
    tasksWhereWeightingOnlyChangedMatching: unique(tasksWhereWeightingOnlyChangedMatching),
    dominantCollapsePatterns: root.dominantCollapsePatterns,
    rootCauseClassification: root.rootCauseClassification,
    recommendedQueryTemplateRollbackScope: recommendRollbackScope(rows),
    recommendedNextStep:
      root.rootCauseClassification === "overconstrained_query_text"
        ? "rollback_or_lighten_weight_templates_then_rerun_r60_10"
        : "trace_weight_template_generation_and_endpoint_interpretation_before_reuse",
    taskRows: rows
  };
  return report;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.1 Weighted-vs-Normalized Query Trace Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- tasksEvaluated: ${report.tasksEvaluated}`);
  lines.push(`- tasksWhereWeightingReducedRawResults: ${report.tasksWhereWeightingReducedRawResults.length}`);
  lines.push(`- tasksWhereWeightingReducedParsedResults: ${report.tasksWhereWeightingReducedParsedResults.length}`);
  lines.push(`- tasksWhereWeightingChangedEndpointBehavior: ${report.tasksWhereWeightingChangedEndpointBehavior.length}`);
  lines.push(`- tasksWhereWeightingOnlyChangedMatching: ${report.tasksWhereWeightingOnlyChangedMatching.length}`);
  lines.push(`- rootCauseClassification: ${report.rootCauseClassification}`);
  lines.push(`- recommendedQueryTemplateRollbackScope: ${report.recommendedQueryTemplateRollbackScope}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Dominant Collapse Patterns");
  for (const row of report.dominantCollapsePatterns || []) lines.push(`- ${row.pattern}: ${row.count}`);
  if (!(report.dominantCollapsePatterns || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [tasks, normalizedQueries, weightedQueries, evalReport] = await Promise.all([
    readJson(path.resolve(reportsDir, tasksName)),
    readJson(path.resolve(reportsDir, normalizedName)),
    readJson(path.resolve(reportsDir, weightedName)),
    readJson(path.resolve(reportsDir, evalName))
  ]);

  const report = await buildR60_10_1WeightingTraceReport({
    tasks,
    normalizedQueries,
    weightedQueries,
    trustedDecisionIds: evalReport?.trustedCorpus?.trustedDocumentIds || [],
    apiBaseUrl: apiBase
  });

  const reportPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, toMarkdown(report))
  ]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.tasksEvaluated,
        tasksWhereWeightingReducedRawResults: report.tasksWhereWeightingReducedRawResults.length,
        tasksWhereWeightingReducedParsedResults: report.tasksWhereWeightingReducedParsedResults.length,
        tasksWhereWeightingChangedEndpointBehavior: report.tasksWhereWeightingChangedEndpointBehavior.length,
        tasksWhereWeightingOnlyChangedMatching: report.tasksWhereWeightingOnlyChangedMatching.length,
        rootCauseClassification: report.rootCauseClassification,
        recommendedQueryTemplateRollbackScope: report.recommendedQueryTemplateRollbackScope,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.10.1 report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
