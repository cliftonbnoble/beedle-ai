import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildR60_8NormalizationReport } from "./retrieval-r60_8-query-normalization-report.mjs";
import { buildR60_10QueryWeightingReport } from "./retrieval-r60_10-query-weighting-report.mjs";
import { buildR60_10_1WeightingTraceReport } from "./retrieval-r60_10_1-weighting-trace-report.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const tasksName = process.env.RETRIEVAL_R60_10_6_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const r60_7Name = process.env.RETRIEVAL_R60_10_6_R60_7_NAME || "retrieval-r60_7-failure-clustering-report.json";
const evalName = process.env.RETRIEVAL_R60_10_6_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const outputReportName = process.env.RETRIEVAL_R60_10_6_REPORT_NAME || "retrieval-r60_10_6-contract-repair-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_6_MARKDOWN_NAME || "retrieval-r60_10_6-contract-repair-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

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

function metricsFromR60_8(report) {
  return {
    tasksRecoveredByNormalizationCount: Number(report?.tasksRecoveredByNormalizationCount || 0),
    emptyTasksAfter: Number(report?.emptyTasksAfter || 0),
    top1DecisionHitRate: Number(report?.top1DecisionHitRate || 0),
    sectionTypeHitRate: Number(report?.sectionTypeHitRate || 0)
  };
}

function metricsFromR60_10(report) {
  return {
    tasksRecoveredByWeightingCount: Number(report?.tasksRecoveredByWeightingCount || 0),
    emptyTasksAfter: Number(report?.emptyTasksAfter || 0),
    top1DecisionHitRate: Number(report?.top1DecisionHitRate || 0),
    top3DecisionHitRate: Number(report?.top3DecisionHitRate || 0),
    top5DecisionHitRate: Number(report?.top5DecisionHitRate || 0),
    sectionTypeHitRate: Number(report?.sectionTypeHitRate || 0)
  };
}

function metricsFromR60_10_1(report) {
  return {
    tasksWhereWeightingReducedRawResults: Number(report?.tasksWhereWeightingReducedRawResults?.length || 0),
    tasksWhereWeightingReducedParsedResults: Number(report?.tasksWhereWeightingReducedParsedResults?.length || 0),
    tasksWhereWeightingChangedEndpointBehavior: Number(report?.tasksWhereWeightingChangedEndpointBehavior?.length || 0),
    rootCauseClassification: String(report?.rootCauseClassification || "")
  };
}

export function buildR60_10_6ContractRepairSummary({
  beforeR60_8,
  beforeR60_10,
  beforeR60_10_1,
  afterR60_8,
  afterR60_10,
  afterR60_10_1
}) {
  const requestShapeBefore = {
    r60: "inline fetchJson + script-local payload",
    r60_8: "script-local payload + script-local parse",
    r60_10: "script-local payload + script-local parse",
    r60_10_1: "script-local payload + script-local parse"
  };
  const requestShapeAfter = {
    all: "shared buildBenchmarkDebugPayload() from retrieval-benchmark-contract-utils.mjs"
  };
  const responseSchemaBefore = {
    r60: "script-local JSON parse",
    r60_8: "{ok,results} local parse contract",
    r60_10: "{ok,results} local parse contract",
    r60_10_1: "custom endpoint/parse flags"
  };
  const responseSchemaAfter = {
    all: "shared callBenchmarkDebug() contract + parsedResults/endpointInputsObserved/responseShape"
  };

  const comparison = {
    r60_8: {
      before: metricsFromR60_8(beforeR60_8 || {}),
      after: metricsFromR60_8(afterR60_8 || {})
    },
    r60_10: {
      before: metricsFromR60_10(beforeR60_10 || {}),
      after: metricsFromR60_10(afterR60_10 || {})
    },
    r60_10_1: {
      before: metricsFromR60_10_1(beforeR60_10_1 || {}),
      after: metricsFromR60_10_1(afterR60_10_1 || {})
    }
  };

  const contractMismatchResolved =
    comparison.r60_10_1.after.tasksWhereWeightingChangedEndpointBehavior === 0 &&
    comparison.r60_10_1.after.rootCauseClassification !== "runtime_endpoint_interpretation_mismatch";

  return {
    scriptsRewiredCount: 4,
    requestShapeBefore,
    requestShapeAfter,
    responseSchemaBefore,
    responseSchemaAfter,
    contractMismatchResolved,
    comparison,
    recommendedNextStep: contractMismatchResolved
      ? "rerun_r60_series_and_resume_weighting_experiments"
      : "inspect_remaining_runtime_or_contract_drift_using_r60_10_1_and_r60_10_2"
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.6 Benchmark Runner Contract Repair (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- scriptsRewiredCount: ${report.scriptsRewiredCount}`);
  lines.push(`- contractMismatchResolved: ${report.contractMismatchResolved}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Request Shape");
  lines.push(`- before: ${JSON.stringify(report.requestShapeBefore)}`);
  lines.push(`- after: ${JSON.stringify(report.requestShapeAfter)}`);
  lines.push("");
  lines.push("## Response Schema");
  lines.push(`- before: ${JSON.stringify(report.responseSchemaBefore)}`);
  lines.push(`- after: ${JSON.stringify(report.responseSchemaAfter)}`);
  lines.push("");
  lines.push("## Rerun Comparison");
  lines.push(`- R60.8: ${JSON.stringify(report.comparison?.r60_8)}`);
  lines.push(`- R60.10: ${JSON.stringify(report.comparison?.r60_10)}`);
  lines.push(`- R60.10.1: ${JSON.stringify(report.comparison?.r60_10_1)}`);
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [tasks, r60_7Report, evalReport, beforeR60_8, beforeR60_10, beforeR60_10_1] = await Promise.all([
    readJson(path.resolve(reportsDir, tasksName)),
    readJson(path.resolve(reportsDir, r60_7Name)),
    readJson(path.resolve(reportsDir, evalName)),
    readJsonIfExists(path.resolve(reportsDir, "retrieval-r60_8-query-normalization-report.json")),
    readJsonIfExists(path.resolve(reportsDir, "retrieval-r60_10-query-weighting-report.json")),
    readJsonIfExists(path.resolve(reportsDir, "retrieval-r60_10_1-weighting-trace-report.json"))
  ]);

  const trustedDecisionIds = evalReport?.trustedCorpus?.trustedDocumentIds || [];

  const afterR60_8 = await buildR60_8NormalizationReport({
    tasks,
    baselineReport: r60_7Report,
    trustedDecisionIds,
    apiBaseUrl: apiBase
  });
  const afterR60_10 = await buildR60_10QueryWeightingReport({
    rewrittenTasks: tasks,
    normalizedPackages: afterR60_8.normalizedPackages || [],
    r60_8Report: afterR60_8,
    r60_7Report,
    trustedDecisionIds,
    apiBaseUrl: apiBase
  });
  const afterR60_10_1 = await buildR60_10_1WeightingTraceReport({
    tasks,
    normalizedQueries: afterR60_8.normalizedPackages || [],
    weightedQueries: afterR60_10.weightedEntries || [],
    trustedDecisionIds,
    apiBaseUrl: apiBase
  });

  await Promise.all([
    fs.writeFile(path.resolve(reportsDir, "retrieval-r60_8-query-normalization-report.json"), JSON.stringify(afterR60_8, null, 2)),
    fs.writeFile(path.resolve(reportsDir, "retrieval-r60_8-normalized-queries.json"), JSON.stringify(afterR60_8.normalizedPackages || [], null, 2)),
    fs.writeFile(path.resolve(reportsDir, "retrieval-r60_10-query-weighting-report.json"), JSON.stringify(afterR60_10, null, 2)),
    fs.writeFile(path.resolve(reportsDir, "retrieval-r60_10-weighted-queries.json"), JSON.stringify(afterR60_10.weightedEntries || [], null, 2)),
    fs.writeFile(path.resolve(reportsDir, "retrieval-r60_10_1-weighting-trace-report.json"), JSON.stringify(afterR60_10_1, null, 2))
  ]);

  const summary = buildR60_10_6ContractRepairSummary({
    beforeR60_8,
    beforeR60_10,
    beforeR60_10_1,
    afterR60_8,
    afterR60_10,
    afterR60_10_1
  });

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.6",
    ...summary
  };

  const reportPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(reportPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        scriptsRewiredCount: report.scriptsRewiredCount,
        contractMismatchResolved: report.contractMismatchResolved,
        recommendedNextStep: report.recommendedNextStep,
        r60_8: report.comparison.r60_8,
        r60_10: report.comparison.r60_10,
        r60_10_1: report.comparison.r60_10_1
      },
      null,
      2
    )
  );
  console.log(`R60.10.6 report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
