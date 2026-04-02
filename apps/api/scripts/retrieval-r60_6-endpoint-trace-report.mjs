import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const rewrittenTasksName = process.env.RETRIEVAL_R60_6_REWRITTEN_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const r60EvalName = process.env.RETRIEVAL_R60_6_BASELINE_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const r60_5ReportName = process.env.RETRIEVAL_R60_6_RERUN_REPORT_NAME || "retrieval-r60_5-goldset-rerun-report.json";
const outputJsonName = process.env.RETRIEVAL_R60_6_REPORT_NAME || "retrieval-r60_6-endpoint-trace-report.json";
const outputMdName = process.env.RETRIEVAL_R60_6_MARKDOWN_NAME || "retrieval-r60_6-endpoint-trace-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_6_LIMIT || "10", 10);

const INTENT_TO_QUERY_TYPE = {
  authority_lookup: "rules_ordinance",
  findings: "keyword",
  procedural_history: "keyword",
  issue_holding_disposition: "keyword",
  analysis_reasoning: "keyword",
  comparative_reasoning: "keyword",
  citation_direct: "citation_lookup"
};

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function normalizeSectionType(value) {
  const s = String(value || "");
  const lower = s.toLowerCase();
  if (lower === "analysis" || lower === "body") return "analysis_reasoning";
  if (lower === "order") return "holding_disposition";
  if (lower === "findings") return "findings";
  return lower.replace(/\s+/g, "_");
}

async function fetchDebug({ query, queryType }) {
  const endpointCalled = `${apiBase}/admin/retrieval/debug`;
  const payload = {
    query,
    queryType,
    limit,
    filters: {
      approvedOnly: true,
      fileType: "decision_docx"
    }
  };

  try {
    const response = await fetch(endpointCalled, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const rawText = await response.text();
    let body = null;
    try {
      body = JSON.parse(rawText);
    } catch {
      return {
        ok: false,
        endpointCalled,
        rawResultCount: 0,
        parsedResults: [],
        responseShape: "non_json"
      };
    }
    const rawResultCount = toNumber(body?.total, Array.isArray(body?.results) ? body.results.length : 0);
    const parsedResults = Array.isArray(body?.results) ? body.results : [];
    return {
      ok: response.ok,
      endpointCalled,
      rawResultCount,
      parsedResults,
      responseShape: Array.isArray(body?.results) ? "results_array" : "results_not_array"
    };
  } catch {
    return {
      ok: false,
      endpointCalled,
      rawResultCount: 0,
      parsedResults: [],
      responseShape: "fetch_failed"
    };
  }
}

function likelyInconsistencyCause({ rawResultCount, parsedResultCount, topHit, taskClassifiedEmpty, responseShape }) {
  if (responseShape === "non_json" || responseShape === "results_not_array") return "endpoint_response_shape_bug";
  if (rawResultCount > 0 && parsedResultCount === 0) return "parser_or_mapping_bug";
  if (topHit && taskClassifiedEmpty) return "task_empty_classification_bug";
  return "none";
}

function classifyRootCause({
  endpointShapeBugCount,
  parserBugCount,
  hitButEmptyCount,
  priorVsReconciledMismatch
}) {
  const active = [
    endpointShapeBugCount > 0,
    parserBugCount > 0,
    hitButEmptyCount > 0,
    priorVsReconciledMismatch
  ].filter(Boolean).length;
  if (active === 0) return "no_inconsistency_detected";
  if (active > 1) return "mixed_benchmark_pipeline_bug";
  if (endpointShapeBugCount > 0) return "endpoint_response_shape_bug";
  if (parserBugCount > 0) return "parser_or_mapping_bug";
  if (hitButEmptyCount > 0) return "task_empty_classification_bug";
  if (priorVsReconciledMismatch) return "hit_rate_aggregation_bug";
  return "no_inconsistency_detected";
}

export async function buildR60_6EndpointTraceReport({ rewrittenTasks, trustedDecisionIds, priorRerunReport }) {
  const trustedSet = new Set((trustedDecisionIds || []).map(String));
  const perTaskRows = [];

  for (const task of rewrittenTasks || []) {
    const queryType = INTENT_TO_QUERY_TYPE[String(task?.intent || "")] || "keyword";
    const runtime = await fetchDebug({ query: task.adoptedQuery || task.originalQuery, queryType });

    const parsedResults = (runtime.parsedResults || []).filter((row) => trustedSet.has(String(row?.documentId || "")));
    const topRows = parsedResults.slice(0, limit);
    const topReturnedDecisionIds = unique(topRows.map((row) => row.documentId));
    const topReturnedSectionTypes = unique(topRows.map((row) => normalizeSectionType(row.sectionLabel || row.chunkType || "")));
    const expectedDecisionIds = (task.expectedDecisionIds || []).map(String);
    const expectedSectionTypes = (task.expectedSectionTypes || []).map(String);

    const top1Hit = expectedDecisionIds.includes(String(topRows[0]?.documentId || ""));
    const top3Hit = topRows.slice(0, 3).some((row) => expectedDecisionIds.includes(String(row?.documentId || "")));
    const top5Hit = topRows.slice(0, 5).some((row) => expectedDecisionIds.includes(String(row?.documentId || "")));
    const sectionTypeHit = topRows
      .slice(0, 5)
      .some((row) => expectedSectionTypes.map(normalizeSectionType).includes(normalizeSectionType(row.sectionLabel || row.chunkType || "")));

    const taskClassifiedEmpty = topRows.length === 0;
    const row = {
      queryId: String(task.queryId || ""),
      adoptedQuery: String(task.adoptedQuery || task.originalQuery || ""),
      endpointCalled: runtime.endpointCalled,
      rawResultCount: runtime.rawResultCount,
      parsedResultCount: topRows.length,
      topReturnedDecisionIds,
      topReturnedSectionTypes,
      top1Hit,
      top3Hit,
      top5Hit,
      sectionTypeHit,
      taskClassifiedEmpty,
      expectedDecisionIds,
      expectedSectionTypes
    };
    row.likelyInconsistencyCause = likelyInconsistencyCause({
      rawResultCount: row.rawResultCount,
      parsedResultCount: row.parsedResultCount,
      topHit: row.top1Hit || row.top3Hit || row.top5Hit,
      taskClassifiedEmpty: row.taskClassifiedEmpty,
      responseShape: runtime.responseShape
    });
    perTaskRows.push(row);
  }

  const tasksEvaluated = perTaskRows.length;
  const tasksClassifiedEmptyCount = perTaskRows.filter((row) => row.taskClassifiedEmpty).length;
  const tasksWithParsedResultsCount = perTaskRows.filter((row) => row.parsedResultCount > 0).length;
  const tasksWithHitButMarkedEmptyCount = perTaskRows.filter(
    (row) => row.taskClassifiedEmpty && (row.top1Hit || row.top3Hit || row.top5Hit)
  ).length;
  const reconciledTop1DecisionHitRate = Number(
    (perTaskRows.filter((row) => row.top1Hit).length / Math.max(1, tasksEvaluated)).toFixed(4)
  );
  const reconciledTop3DecisionHitRate = Number(
    (perTaskRows.filter((row) => row.top3Hit).length / Math.max(1, tasksEvaluated)).toFixed(4)
  );
  const reconciledTop5DecisionHitRate = Number(
    (perTaskRows.filter((row) => row.top5Hit).length / Math.max(1, tasksEvaluated)).toFixed(4)
  );
  const reconciledSectionTypeHitRate = Number(
    (perTaskRows.filter((row) => row.sectionTypeHit).length / Math.max(1, tasksEvaluated)).toFixed(4)
  );

  const endpointShapeBugCount = perTaskRows.filter((row) => row.likelyInconsistencyCause === "endpoint_response_shape_bug").length;
  const parserBugCount = perTaskRows.filter((row) => row.likelyInconsistencyCause === "parser_or_mapping_bug").length;
  const priorVsReconciledMismatch =
    Math.abs(toNumber(priorRerunReport?.top1DecisionHitRate, 0) - reconciledTop1DecisionHitRate) > 0.0001 ||
    Math.abs(toNumber(priorRerunReport?.top3DecisionHitRate, 0) - reconciledTop3DecisionHitRate) > 0.0001 ||
    Math.abs(toNumber(priorRerunReport?.top5DecisionHitRate, 0) - reconciledTop5DecisionHitRate) > 0.0001 ||
    Math.abs(toNumber(priorRerunReport?.sectionTypeHitRate, 0) - reconciledSectionTypeHitRate) > 0.0001 ||
    toNumber(priorRerunReport?.tasksStillEmptyCount, 0) !== tasksClassifiedEmptyCount;

  const rootCauseClassification = classifyRootCause({
    endpointShapeBugCount,
    parserBugCount,
    hitButEmptyCount: tasksWithHitButMarkedEmptyCount,
    priorVsReconciledMismatch
  });

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.6",
    tasksEvaluated,
    tasksClassifiedEmptyCount,
    tasksWithParsedResultsCount,
    tasksWithHitButMarkedEmptyCount,
    reconciledTop1DecisionHitRate,
    reconciledTop3DecisionHitRate,
    reconciledTop5DecisionHitRate,
    reconciledSectionTypeHitRate,
    rootCauseClassification,
    priorR60_5Summary: {
      tasksStillEmptyCount: toNumber(priorRerunReport?.tasksStillEmptyCount, 0),
      top1DecisionHitRate: toNumber(priorRerunReport?.top1DecisionHitRate, 0),
      top3DecisionHitRate: toNumber(priorRerunReport?.top3DecisionHitRate, 0),
      top5DecisionHitRate: toNumber(priorRerunReport?.top5DecisionHitRate, 0),
      sectionTypeHitRate: toNumber(priorRerunReport?.sectionTypeHitRate, 0)
    },
    priorVsReconciledMismatch,
    perTaskRows: perTaskRows.sort((a, b) => String(a.queryId).localeCompare(String(b.queryId)))
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.6 Endpoint Trace + Report Consistency Audit (Dry Run)");
  lines.push("");
  lines.push("## Reconciliation");
  lines.push(`- tasksEvaluated: ${report.tasksEvaluated}`);
  lines.push(`- tasksClassifiedEmptyCount: ${report.tasksClassifiedEmptyCount}`);
  lines.push(`- tasksWithParsedResultsCount: ${report.tasksWithParsedResultsCount}`);
  lines.push(`- tasksWithHitButMarkedEmptyCount: ${report.tasksWithHitButMarkedEmptyCount}`);
  lines.push(`- reconciledTop1DecisionHitRate: ${report.reconciledTop1DecisionHitRate}`);
  lines.push(`- reconciledTop3DecisionHitRate: ${report.reconciledTop3DecisionHitRate}`);
  lines.push(`- reconciledTop5DecisionHitRate: ${report.reconciledTop5DecisionHitRate}`);
  lines.push(`- reconciledSectionTypeHitRate: ${report.reconciledSectionTypeHitRate}`);
  lines.push(`- rootCauseClassification: ${report.rootCauseClassification}`);
  lines.push(`- priorVsReconciledMismatch: ${report.priorVsReconciledMismatch}`);
  lines.push("");

  lines.push("## Per Task Trace");
  for (const row of report.perTaskRows || []) {
    lines.push(
      `- ${row.queryId}: raw=${row.rawResultCount}, parsed=${row.parsedResultCount}, empty=${row.taskClassifiedEmpty}, top1=${row.top1Hit}, top3=${row.top3Hit}, top5=${row.top5Hit}, cause=${row.likelyInconsistencyCause}`
    );
  }
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [rewrittenTasks, baselineEval, priorRerunReport] = await Promise.all([
    readJson(path.resolve(reportsDir, rewrittenTasksName)),
    readJson(path.resolve(reportsDir, r60EvalName)),
    readJson(path.resolve(reportsDir, r60_5ReportName))
  ]);

  const report = await buildR60_6EndpointTraceReport({
    rewrittenTasks,
    trustedDecisionIds: baselineEval?.trustedCorpus?.trustedDocumentIds || [],
    priorRerunReport
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.tasksEvaluated,
        tasksClassifiedEmptyCount: report.tasksClassifiedEmptyCount,
        tasksWithParsedResultsCount: report.tasksWithParsedResultsCount,
        tasksWithHitButMarkedEmptyCount: report.tasksWithHitButMarkedEmptyCount,
        reconciledTop1DecisionHitRate: report.reconciledTop1DecisionHitRate,
        reconciledTop3DecisionHitRate: report.reconciledTop3DecisionHitRate,
        reconciledTop5DecisionHitRate: report.reconciledTop5DecisionHitRate,
        reconciledSectionTypeHitRate: report.reconciledSectionTypeHitRate,
        rootCauseClassification: report.rootCauseClassification
      },
      null,
      2
    )
  );
  console.log(`R60.6 endpoint trace report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
