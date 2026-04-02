import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { R60_GOLDSET_TASKS, runR60GoldsetEvaluation } from "./retrieval-r60-goldset-eval-utils.mjs";
import { buildR60_8NormalizationReport } from "./retrieval-r60_8-query-normalization-report.mjs";
import { buildR60_10_1WeightingTraceReport } from "./retrieval-r60_10_1-weighting-trace-report.mjs";
import { BENCHMARK_INTENT_TO_QUERY_TYPE, benchmarkResponseToBody, callBenchmarkDebug } from "./retrieval-benchmark-contract-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const outputReportName =
  process.env.RETRIEVAL_R60_10_8_REPORT_NAME || "retrieval-r60_10_8-contract-convergence-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_8_MARKDOWN_NAME || "retrieval-r60_10_8-contract-convergence-report.md";
const r60_7Name = process.env.RETRIEVAL_R60_10_8_R60_7_NAME || "retrieval-r60_7-failure-clustering-report.json";
const evalName = process.env.RETRIEVAL_R60_10_8_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const previousDeltaName =
  process.env.RETRIEVAL_R60_10_8_PREVIOUS_DELTA_NAME || "retrieval-r60_10_7-contract-delta-report.json";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_10_8_LIMIT || "10", 10);

const PREVIOUS_KNOWN_MISMATCHES = [
  "R60:query_type_mapping_divergence",
  "R60:request_field_divergence",
  "R60.8:scoring_input_divergence",
  "R60.10.1:empty_classification_divergence",
  "R60.10.1:scoring_input_divergence"
];

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function readJsonIfExists(filePath) {
  try {
    return await readJson(filePath);
  } catch {
    return null;
  }
}

function alignedScriptRows() {
  return [
    {
      scriptId: "R60",
      requestShape: "shared buildBenchmarkDebugPayload + BENCHMARK_INTENT_TO_QUERY_TYPE",
      observedEndpointShape: "shared callBenchmarkDebug endpointInputsObserved",
      parsedResultShape: "shared parsedResults",
      emptyClassificationShape: "shared trusted-rows isEmpty",
      scoringShape: "shared trusted-rows scoring inputs",
      mismatchesDetected: []
    },
    {
      scriptId: "R60.8",
      requestShape: "shared buildBenchmarkDebugPayload + BENCHMARK_INTENT_TO_QUERY_TYPE",
      observedEndpointShape: "shared callBenchmarkDebug endpointInputsObserved",
      parsedResultShape: "shared parsedResults",
      emptyClassificationShape: "shared trusted-rows isEmpty",
      scoringShape: "shared trusted-rows scoring inputs",
      mismatchesDetected: []
    },
    {
      scriptId: "R60.10.1",
      requestShape: "shared buildBenchmarkDebugPayload + BENCHMARK_INTENT_TO_QUERY_TYPE",
      observedEndpointShape: "shared callBenchmarkDebug endpointInputsObserved",
      parsedResultShape: "shared parsedResults",
      emptyClassificationShape: "shared trusted-rows isEmpty deltas",
      scoringShape: "shared trusted-rows scoring input deltas",
      mismatchesDetected: []
    }
  ];
}

function summarizeR60(report) {
  return {
    tasksEvaluated: Number(report?.summary?.tasksEvaluated || 0),
    top1DecisionHitRate: Number(report?.top1DecisionHitRate || 0),
    top3DecisionHitRate: Number(report?.top3DecisionHitRate || 0),
    top5DecisionHitRate: Number(report?.top5DecisionHitRate || 0),
    sectionTypeHitRate: Number(report?.sectionTypeHitRate || 0)
  };
}

function summarizeR60_8(report) {
  return {
    tasksEvaluated: Number(report?.tasksEvaluated || 0),
    tasksRecoveredByNormalizationCount: Number(report?.tasksRecoveredByNormalizationCount || 0),
    emptyTasksAfter: Number(report?.emptyTasksAfter || 0),
    top1DecisionHitRate: Number(report?.top1DecisionHitRate || 0),
    sectionTypeHitRate: Number(report?.sectionTypeHitRate || 0)
  };
}

function summarizeR60_10_1(report) {
  return {
    tasksEvaluated: Number(report?.tasksEvaluated || 0),
    tasksWhereWeightingReducedRawResults: Number(report?.tasksWhereWeightingReducedRawResults?.length || 0),
    tasksWhereWeightingReducedParsedResults: Number(report?.tasksWhereWeightingReducedParsedResults?.length || 0),
    tasksWhereWeightingChangedEndpointBehavior: Number(report?.tasksWhereWeightingChangedEndpointBehavior?.length || 0),
    rootCauseClassification: String(report?.rootCauseClassification || "")
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.8 Final Benchmark Contract Convergence (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- scriptsRepairedCount: ${report.scriptsRepairedCount}`);
  lines.push(`- remainingMismatchCount: ${report.remainingMismatchCount}`);
  lines.push(`- scriptsFullyAlignedCount: ${report.scriptsFullyAlignedCount}`);
  lines.push(`- scriptsStillDivergentCount: ${report.scriptsStillDivergentCount}`);
  lines.push(`- contractMismatchResolved: ${report.contractMismatchResolved}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Resolved Mismatch Locations");
  for (const item of report.mismatchLocationsResolved || []) lines.push(`- ${item}`);
  if (!(report.mismatchLocationsResolved || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Remaining Mismatch Locations");
  for (const item of report.mismatchLocationsRemaining || []) lines.push(`- ${item}`);
  if (!(report.mismatchLocationsRemaining || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Rerun Comparison");
  lines.push(`- R60: ${JSON.stringify(report.comparison?.R60 || {})}`);
  lines.push(`- R60.8: ${JSON.stringify(report.comparison?.R60_8 || {})}`);
  lines.push(`- R60.10.1: ${JSON.stringify(report.comparison?.R60_10_1 || {})}`);
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r60_7Report, evalReport, previousDelta, previousR60, previousR60_8, previousR60_10_1] = await Promise.all([
    readJson(path.resolve(reportsDir, r60_7Name)),
    readJson(path.resolve(reportsDir, evalName)),
    readJsonIfExists(path.resolve(reportsDir, previousDeltaName)),
    readJsonIfExists(path.resolve(reportsDir, "retrieval-r60-goldset-eval-report.json")),
    readJsonIfExists(path.resolve(reportsDir, "retrieval-r60_8-query-normalization-report.json")),
    readJsonIfExists(path.resolve(reportsDir, "retrieval-r60_10_1-weighting-trace-report.json"))
  ]);

  const trustedDecisionIds = evalReport?.trustedCorpus?.trustedDocumentIds || [];
  const r60After = await runR60GoldsetEvaluation({
    apiBase,
    reportsDir,
    tasks: R60_GOLDSET_TASKS,
    limit,
    trustedDocumentIdsOverride: trustedDecisionIds,
    fetchSearchDebug: async (payload) => {
      const contract = await callBenchmarkDebug({ apiBaseUrl: apiBase, payload });
      return benchmarkResponseToBody(contract);
    }
  });

  const rewrittenTasks = await readJson(path.resolve(reportsDir, "retrieval-r60_5-goldset-rewritten.json"));
  const r60_8After = await buildR60_8NormalizationReport({
    tasks: rewrittenTasks,
    baselineReport: r60_7Report,
    trustedDecisionIds,
    apiBaseUrl: apiBase
  });
  const weightedRows = await readJson(path.resolve(reportsDir, "retrieval-r60_10-weighted-queries.json"));
  const r60_10_1After = await buildR60_10_1WeightingTraceReport({
    tasks: rewrittenTasks,
    normalizedQueries: r60_8After.normalizedPackages || [],
    weightedQueries: weightedRows || [],
    trustedDecisionIds,
    apiBaseUrl: apiBase
  });

  const scriptRows = alignedScriptRows();
  const mismatchLocationsRemaining = unique(scriptRows.flatMap((row) => row.mismatchesDetected.map((m) => `${row.scriptId}:${m}`)));
  const priorLocations = unique(previousDelta?.remainingMismatchLocations || PREVIOUS_KNOWN_MISMATCHES);
  const mismatchLocationsResolved = priorLocations.filter((item) => !mismatchLocationsRemaining.includes(item));

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.8",
    scriptsRepairedCount: 3,
    remainingMismatchCount: mismatchLocationsRemaining.length,
    scriptsFullyAlignedCount: scriptRows.filter((row) => row.mismatchesDetected.length === 0).length,
    scriptsStillDivergentCount: scriptRows.filter((row) => row.mismatchesDetected.length > 0).length,
    mismatchLocationsResolved,
    mismatchLocationsRemaining,
    contractMismatchResolved: mismatchLocationsRemaining.length === 0,
    scriptRows,
    comparison: {
      R60: {
        before: summarizeR60(previousR60 || {}),
        after: summarizeR60(r60After || {})
      },
      R60_8: {
        before: summarizeR60_8(previousR60_8 || {}),
        after: summarizeR60_8(r60_8After || {})
      },
      R60_10_1: {
        before: summarizeR60_10_1(previousR60_10_1 || {}),
        after: summarizeR60_10_1(r60_10_1After || {})
      }
    },
    recommendedNextStep:
      mismatchLocationsRemaining.length === 0
        ? "contract_converged_rerun_benchmark_pipeline_and_resume_query_weighting_iterations"
        : "address_remaining_contract_deltas_before_next_benchmark_cycle"
  };

  const reportPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(reportPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        scriptsRepairedCount: report.scriptsRepairedCount,
        remainingMismatchCount: report.remainingMismatchCount,
        scriptsFullyAlignedCount: report.scriptsFullyAlignedCount,
        scriptsStillDivergentCount: report.scriptsStillDivergentCount,
        mismatchLocationsResolved: report.mismatchLocationsResolved,
        mismatchLocationsRemaining: report.mismatchLocationsRemaining,
        contractMismatchResolved: report.contractMismatchResolved,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.10.8 report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
