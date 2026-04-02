import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  BENCHMARK_INTENT_TO_QUERY_TYPE,
  buildBenchmarkDebugPayload,
  callBenchmarkDebug,
  normalizeSectionTypeRuntime,
  toTrustedRows
} from "./retrieval-benchmark-contract-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const tasksName = process.env.RETRIEVAL_R61_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const normalizedName = process.env.RETRIEVAL_R61_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const baselineR60Name = process.env.RETRIEVAL_R61_BASELINE_R60_NAME || "retrieval-r60-goldset-eval-report.json";
const baselineR608Name = process.env.RETRIEVAL_R61_BASELINE_R60_8_NAME || "retrieval-r60_8-query-normalization-report.json";
const baselineR6010Name = process.env.RETRIEVAL_R61_BASELINE_R60_10_NAME || "retrieval-r60_10-query-weighting-report.json";
const evalName = process.env.RETRIEVAL_R61_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const outputReportName =
  process.env.RETRIEVAL_R61_REPORT_NAME || "retrieval-r61-two-stage-prototype-report.json";
const outputMdName = process.env.RETRIEVAL_R61_MARKDOWN_NAME || "retrieval-r61-two-stage-prototype-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const retrievalLimit = Number.parseInt(process.env.RETRIEVAL_R61_LIMIT || "40", 10);
const stage1DecisionK = Number.parseInt(process.env.RETRIEVAL_R61_STAGE1_K || "5", 10);
const stage2ResultK = Number.parseInt(process.env.RETRIEVAL_R61_STAGE2_K || "10", 10);

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

function summarizeBaseline(report, type) {
  if (!report) return {};
  if (type === "r60") {
    return {
      tasksEvaluated: Number(report?.summary?.tasksEvaluated || 0),
      top1DecisionHitRate: Number(report?.top1DecisionHitRate || 0),
      top3DecisionHitRate: Number(report?.top3DecisionHitRate || 0),
      top5DecisionHitRate: Number(report?.top5DecisionHitRate || 0),
      sectionTypeHitRate: Number(report?.sectionTypeHitRate || 0)
    };
  }
  return {
    tasksEvaluated: Number(report?.tasksEvaluated || 0),
    top1DecisionHitRate: Number(report?.top1DecisionHitRate || 0),
    top3DecisionHitRate: Number(report?.top3DecisionHitRate || 0),
    top5DecisionHitRate: Number(report?.top5DecisionHitRate || 0),
    sectionTypeHitRate: Number(report?.sectionTypeHitRate || 0),
    emptyTasksAfter: Number(report?.emptyTasksAfter || 0)
  };
}

function toDecisionCandidates(rows, topK) {
  const grouped = new Map();
  for (const row of rows || []) {
    const id = String(row.documentId || "");
    if (!id) continue;
    const current = grouped.get(id) || { documentId: id, maxScore: -Infinity, rowCount: 0, title: "" };
    current.maxScore = Math.max(current.maxScore, Number(row.score || 0));
    current.rowCount += 1;
    if (!current.title && row.title) current.title = String(row.title);
    grouped.set(id, current);
  }
  return Array.from(grouped.values())
    .sort((a, b) => {
      if (b.maxScore !== a.maxScore) return b.maxScore - a.maxScore;
      if (b.rowCount !== a.rowCount) return b.rowCount - a.rowCount;
      return String(a.documentId).localeCompare(String(b.documentId));
    })
    .slice(0, Math.max(1, topK));
}

function inferVariantQueryType(variantType, intent) {
  const v = String(variantType || "");
  if (v === "citation_focused") return "citation_lookup";
  if (v === "compressed_keyword" || v === "procedural" || v === "findings_credibility" || v === "disposition") return "keyword";
  return BENCHMARK_INTENT_TO_QUERY_TYPE[String(intent || "")] || "keyword";
}

function bestR608VariantRow(taskEval) {
  const rows = Array.isArray(taskEval?.variantRows) ? taskEval.variantRows : [];
  const bestType = String(taskEval?.bestVariantType || "");
  const preferred = rows.find((row) => String(row?.variantType || "") === bestType);
  if (preferred) return preferred;
  return rows[0] || null;
}

function computeTaskMetrics(task, stage1Candidates, stage2Rows) {
  const expectedDecisionIds = new Set((task.expectedDecisionIds || []).map(String));
  const expectedSectionTypes = new Set((task.expectedSectionTypes || []).map((value) => normalizeSectionTypeRuntime(value)));
  const topRows = (stage2Rows || []).slice(0, stage2ResultK);
  const topDecisionIds = unique(topRows.map((row) => row.documentId));
  const topSectionTypes = unique(topRows.map((row) => row.sectionType));
  const top1 = topRows.slice(0, 1).some((row) => expectedDecisionIds.has(String(row.documentId || "")));
  const top3 = topRows.slice(0, 3).some((row) => expectedDecisionIds.has(String(row.documentId || "")));
  const top5 = topRows.slice(0, 5).some((row) => expectedDecisionIds.has(String(row.documentId || "")));
  const sectionHit = topRows.slice(0, 5).some((row) => expectedSectionTypes.has(String(row.sectionType || "")));
  const decisionCandidateRecall = stage1Candidates.some((row) => expectedDecisionIds.has(String(row.documentId || "")));
  const sectionSelectionPrecision = Number(
    (
      topRows.filter((row) => expectedSectionTypes.has(String(row.sectionType || ""))).length /
      Math.max(1, topRows.length)
    ).toFixed(4)
  );
  const isEmpty = topRows.length === 0;

  let failurePattern = "";
  if (isEmpty) failurePattern = "empty_after_stage_two";
  else if (!decisionCandidateRecall) failurePattern = "decision_candidate_miss";
  else if (!top5) failurePattern = "decision_rank_miss";
  else if (!sectionHit) failurePattern = "section_type_miss";
  else failurePattern = "recovered";

  return {
    top1DecisionHit: top1,
    top3DecisionHit: top3,
    top5DecisionHit: top5,
    sectionTypeHit: sectionHit,
    decisionCandidateRecall,
    sectionSelectionPrecision,
    emptyAfter: isEmpty,
    topDecisionIds,
    topSectionTypes,
    failurePattern
  };
}

export async function buildR61TwoStagePrototypeReport({
  tasks,
  normalizedPackages,
  trustedDecisionIds,
  r608Report = {},
  baselineR60,
  baselineR608,
  baselineR6010,
  apiBaseUrl,
  fetchImpl = fetch
}) {
  const normalizedById = new Map((normalizedPackages || []).map((row) => [String(row?.queryId || ""), row]));
  const r608ById = new Map((r608Report?.taskEvaluations || []).map((row) => [String(row?.queryId || ""), row]));
  const trustedSet = new Set((trustedDecisionIds || []).map(String));

  const taskRows = [];
  for (const task of (tasks || []).slice().sort((a, b) => String(a?.queryId || "").localeCompare(String(b?.queryId || "")))) {
    const queryId = String(task?.queryId || "");
    const normalizedQuery = String(normalizedById.get(queryId)?.normalizedQuery || task?.adoptedQuery || task?.query || "");
    const r608Task = r608ById.get(queryId);
    const bestVariant = bestR608VariantRow(r608Task);
    let trustedRows = [];
    let stage1DecisionSeedIds = [];
    let stage1Source = "r60_8_best_variant";
    if (
      bestVariant &&
      Number(bestVariant.returnedCount || 0) > 0 &&
      Array.isArray(bestVariant.trustedRows) &&
      bestVariant.trustedRows.length > 0
    ) {
      trustedRows = bestVariant.trustedRows
        .map((row, idx) => ({
          documentId: String(row?.documentId || ""),
          chunkId: `r608_${queryId}_${idx}`,
          title: "",
          sectionType: normalizeSectionTypeRuntime(row?.sectionType || ""),
          score: 1,
          sourceLink: "",
          citationAnchor: ""
        }))
        .filter((row) => trustedSet.has(String(row.documentId || "")));
      stage1DecisionSeedIds = unique(trustedRows.map((row) => row.documentId));
    } else if (bestVariant && Array.isArray(bestVariant.topReturnedDecisionIds) && bestVariant.topReturnedDecisionIds.length > 0) {
      stage1Source = "r60_8_best_variant_decision_ids";
      stage1DecisionSeedIds = unique(bestVariant.topReturnedDecisionIds).filter((id) => trustedSet.has(String(id)));
    } else {
      stage1Source = "live_normalized_fallback";
      const payload = buildBenchmarkDebugPayload({
        query: normalizedQuery,
        queryType: inferVariantQueryType(bestVariant?.variantType, task?.intent),
        limit: retrievalLimit
      });
      const runtime = await callBenchmarkDebug({ apiBaseUrl, payload, fetchImpl });
      trustedRows = toTrustedRows(runtime.parsedResults || [], trustedSet, retrievalLimit);
      stage1DecisionSeedIds = unique(trustedRows.map((row) => row.documentId));
    }

    let stage1Candidates = toDecisionCandidates(trustedRows, stage1DecisionK);
    if (stage1Candidates.length === 0 && stage1DecisionSeedIds.length > 0) {
      stage1Candidates = stage1DecisionSeedIds.slice(0, Math.max(1, stage1DecisionK)).map((documentId, idx) => ({
        documentId,
        maxScore: 1 - idx * 0.0001,
        rowCount: 0,
        title: ""
      }));
    }
    const stage1DecisionIds = new Set(stage1Candidates.map((row) => String(row.documentId)));
    let stage2Rows = trustedRows
      .filter((row) => stage1DecisionIds.has(String(row.documentId)))
      .sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        if (String(a.documentId) !== String(b.documentId)) return String(a.documentId).localeCompare(String(b.documentId));
        return String(a.chunkId).localeCompare(String(b.chunkId));
      })
      .slice(0, stage2ResultK);
    if (stage2Rows.length === 0 && stage1DecisionIds.size > 0) {
      const payload = buildBenchmarkDebugPayload({
        query: normalizedQuery,
        queryType: inferVariantQueryType(bestVariant?.variantType, task?.intent),
        limit: retrievalLimit
      });
      const runtime = await callBenchmarkDebug({ apiBaseUrl, payload, fetchImpl });
      const runtimeRows = toTrustedRows(runtime.parsedResults || [], trustedSet, retrievalLimit);
      stage2Rows = runtimeRows
        .filter((row) => stage1DecisionIds.has(String(row.documentId)))
        .sort((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          if (String(a.documentId) !== String(b.documentId)) return String(a.documentId).localeCompare(String(b.documentId));
          return String(a.chunkId).localeCompare(String(b.chunkId));
        })
        .slice(0, stage2ResultK);
    }

    const metrics = computeTaskMetrics(task, stage1Candidates, stage2Rows);
    taskRows.push({
      queryId,
      intent: String(task?.intent || ""),
      normalizedQuery,
      stage1Source,
      stage1DecisionCandidates: stage1Candidates,
      stage2TopRows: stage2Rows,
      ...metrics
    });
  }

  const total = Math.max(1, taskRows.length);
  const top1DecisionHitRate = Number((taskRows.filter((row) => row.top1DecisionHit).length / total).toFixed(4));
  const top3DecisionHitRate = Number((taskRows.filter((row) => row.top3DecisionHit).length / total).toFixed(4));
  const top5DecisionHitRate = Number((taskRows.filter((row) => row.top5DecisionHit).length / total).toFixed(4));
  const sectionTypeHitRate = Number((taskRows.filter((row) => row.sectionTypeHit).length / total).toFixed(4));
  const decisionCandidateRecall = Number((taskRows.filter((row) => row.decisionCandidateRecall).length / total).toFixed(4));
  const sectionSelectionPrecision = Number(
    (
      taskRows.reduce((acc, row) => acc + Number(row.sectionSelectionPrecision || 0), 0) /
      Math.max(1, taskRows.length)
    ).toFixed(4)
  );
  const emptyTasksAfter = taskRows.filter((row) => row.emptyAfter).length;
  const recoveredVsR608Ids = taskRows
    .filter((row) => !row.emptyAfter)
    .map((row) => row.queryId)
    .filter((id) => new Set(baselineR608?.stillEmptyTaskIds || []).has(id))
    .sort((a, b) => a.localeCompare(b));
  const tasksStillFailing = taskRows
    .filter((row) => !row.top5DecisionHit || !row.sectionTypeHit || row.emptyAfter)
    .map((row) => row.queryId)
    .sort((a, b) => a.localeCompare(b));
  const dominantFailurePatterns = Object.entries(countBy(taskRows.map((row) => row.failurePattern)))
    .map(([pattern, count]) => ({ pattern, count }))
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.pattern.localeCompare(b.pattern);
    });

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R61",
    tasksEvaluated: taskRows.length,
    top1DecisionHitRate,
    top3DecisionHitRate,
    top5DecisionHitRate,
    sectionTypeHitRate,
    decisionCandidateRecall,
    sectionSelectionPrecision,
    emptyTasksAfter,
    comparisonVsR60: summarizeBaseline(baselineR60, "r60"),
    comparisonVsR60_8: summarizeBaseline(baselineR608, "r60_8"),
    comparisonVsR60_10: summarizeBaseline(baselineR6010, "r60_10"),
    tasksRecoveredVsR60_8: {
      count: recoveredVsR608Ids.length,
      taskIds: recoveredVsR608Ids
    },
    tasksStillFailing,
    dominantFailurePatterns,
    briefImprovementComparison: {
      decisionHitRateDeltaVsR60_8: Number((top5DecisionHitRate - Number(baselineR608?.top5DecisionHitRate || 0)).toFixed(4)),
      sectionHitRateDeltaVsR60_8: Number((sectionTypeHitRate - Number(baselineR608?.sectionTypeHitRate || 0)).toFixed(4)),
      emptyTaskDeltaVsR60_8: Number((emptyTasksAfter - Number(baselineR608?.emptyTasksAfter || 0)).toFixed(0))
    },
    recommendedNextStep:
      top5DecisionHitRate >= Number(baselineR608?.top5DecisionHitRate || 0) &&
      sectionTypeHitRate >= Number(baselineR608?.sectionTypeHitRate || 0)
        ? "promote_two_stage_normalized_frontend_for_next_benchmark_iteration"
        : "refine_stage1_decision_candidate_strategy_and_stage2_section_ranking",
    taskRows
  };

  return report;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R61 Two-Stage Prototype (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- tasksEvaluated: ${report.tasksEvaluated}`);
  lines.push(`- top1DecisionHitRate: ${report.top1DecisionHitRate}`);
  lines.push(`- top3DecisionHitRate: ${report.top3DecisionHitRate}`);
  lines.push(`- top5DecisionHitRate: ${report.top5DecisionHitRate}`);
  lines.push(`- sectionTypeHitRate: ${report.sectionTypeHitRate}`);
  lines.push(`- decisionCandidateRecall: ${report.decisionCandidateRecall}`);
  lines.push(`- sectionSelectionPrecision: ${report.sectionSelectionPrecision}`);
  lines.push(`- emptyTasksAfter: ${report.emptyTasksAfter}`);
  lines.push(`- tasksRecoveredVsR60_8: ${report.tasksRecoveredVsR60_8?.count || 0}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Comparison");
  lines.push(`- comparisonVsR60: ${JSON.stringify(report.comparisonVsR60 || {})}`);
  lines.push(`- comparisonVsR60_8: ${JSON.stringify(report.comparisonVsR60_8 || {})}`);
  lines.push(`- comparisonVsR60_10: ${JSON.stringify(report.comparisonVsR60_10 || {})}`);
  lines.push(`- briefImprovementComparison: ${JSON.stringify(report.briefImprovementComparison || {})}`);
  lines.push("");
  lines.push("## Dominant Failure Patterns");
  for (const row of report.dominantFailurePatterns || []) lines.push(`- ${row.pattern}: ${row.count}`);
  if (!(report.dominantFailurePatterns || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [tasks, normalizedPackages, baselineR60, baselineR608, baselineR6010, evalReport] = await Promise.all([
    readJson(path.resolve(reportsDir, tasksName)),
    readJson(path.resolve(reportsDir, normalizedName)),
    readJsonIfExists(path.resolve(reportsDir, baselineR60Name)),
    readJsonIfExists(path.resolve(reportsDir, baselineR608Name)),
    readJsonIfExists(path.resolve(reportsDir, baselineR6010Name)),
    readJson(path.resolve(reportsDir, evalName))
  ]);
  const trustedDecisionIds = evalReport?.trustedCorpus?.trustedDocumentIds || [];

  const report = await buildR61TwoStagePrototypeReport({
    tasks,
    normalizedPackages,
    trustedDecisionIds,
    r608Report: baselineR608,
    baselineR60,
    baselineR608,
    baselineR6010,
    apiBaseUrl: apiBase
  });

  const reportPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(reportPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.tasksEvaluated,
        top1DecisionHitRate: report.top1DecisionHitRate,
        top3DecisionHitRate: report.top3DecisionHitRate,
        top5DecisionHitRate: report.top5DecisionHitRate,
        sectionTypeHitRate: report.sectionTypeHitRate,
        decisionCandidateRecall: report.decisionCandidateRecall,
        sectionSelectionPrecision: report.sectionSelectionPrecision,
        emptyTasksAfter: report.emptyTasksAfter,
        tasksRecoveredVsR60_8: report.tasksRecoveredVsR60_8,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R61 report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
