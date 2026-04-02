import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { runR60GoldsetEvaluation } from "./retrieval-r60-goldset-eval-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const originalTasksName = process.env.RETRIEVAL_R60_5_ORIGINAL_TASKS_NAME || "retrieval-r60-goldset-tasks.json";
const repairedTasksName = process.env.RETRIEVAL_R60_5_REPAIRED_TASKS_NAME || "retrieval-r60_2-goldset-repaired.json";
const relaxationName = process.env.RETRIEVAL_R60_5_RELAXATION_REPORT_NAME || "retrieval-r60_4-query-relaxation-report.json";
const originalEvalName = process.env.RETRIEVAL_R60_5_ORIGINAL_EVAL_REPORT_NAME || "retrieval-r60-goldset-eval-report.json";
const rewrittenTasksName = process.env.RETRIEVAL_R60_5_REWRITTEN_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const outputJsonName = process.env.RETRIEVAL_R60_5_REPORT_NAME || "retrieval-r60_5-goldset-rerun-report.json";
const outputMdName = process.env.RETRIEVAL_R60_5_MARKDOWN_NAME || "retrieval-r60_5-goldset-rerun-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_5_LIMIT || "10", 10);

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function readJsonIfExists(filePath) {
  try {
    return await readJson(filePath);
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") return null;
    throw error;
  }
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function buildRewrittenTasks({ originalTasks, repairedTasks, relaxationReport }) {
  const originalById = new Map((originalTasks || []).map((row) => [String(row?.queryId || ""), row]));
  const rewriteById = new Map(
    (relaxationReport?.candidateBenchmarkRewrites || []).map((row) => [String(row?.queryId || ""), row])
  );

  const rewrittenTasks = (repairedTasks || [])
    .map((row) => {
      const queryId = String(row?.queryId || "");
      const original = originalById.get(queryId) || {};
      const rewrite = rewriteById.get(queryId) || null;
      const originalQuery = String(original?.query || row?.query || "");
      const adoptedQuery = String(rewrite?.recommendedQuery || row?.query || originalQuery);
      const rewriteAdopted = Boolean(rewrite && adoptedQuery && adoptedQuery !== originalQuery);
      return {
        queryId,
        originalQuery,
        adoptedQuery,
        intent: String(row?.intent || ""),
        rewriteAdopted,
        rewriteSourceVariantType: rewriteAdopted ? String(rewrite?.recommendedVariantType || "") : "",
        expectedDecisionIds: (row?.expectedDecisionIds || []).map(String),
        expectedSectionTypes: (row?.expectedSectionTypes || []).map(String),
        minimumAcceptableRank: Number(row?.minimumAcceptableRank || 5),
        notes: String(row?.notes || ""),
        sourceOfExpectation: String(row?.sourceOfExpectation || "")
      };
    })
    .sort((a, b) => String(a.queryId).localeCompare(String(b.queryId)));

  return rewrittenTasks;
}

function toEvalTask(row) {
  return {
    queryId: row.queryId,
    query: row.adoptedQuery,
    intent: row.intent,
    expectedDecisionIds: row.expectedDecisionIds,
    expectedSectionTypes: row.expectedSectionTypes,
    minimumAcceptableRank: row.minimumAcceptableRank,
    notes: row.notes
  };
}

function buildComparison({ originalEval, rerunEval }) {
  if (!originalEval) return null;
  const fields = [
    "top1DecisionHitRate",
    "top3DecisionHitRate",
    "top5DecisionHitRate",
    "sectionTypeHitRate",
    "noisyChunkDominationRate"
  ];
  const out = {};
  for (const key of fields) {
    const before = toNumber(originalEval?.[key], 0);
    const after = toNumber(rerunEval?.[key], 0);
    out[key] = {
      original: before,
      rewritten: after,
      delta: Number((after - before).toFixed(4))
    };
  }
  return out;
}

export function buildR60_5RerunReport({ originalTasks, rewrittenTasks, rerunEval, originalEval }) {
  const queryById = new Map((rerunEval?.queryResults || []).map((row) => [String(row?.queryId || ""), row]));

  const tasksRecoveredByRewrite = rewrittenTasks
    .filter((task) => task.rewriteAdopted)
    .filter((task) => toNumber(queryById.get(task.queryId)?.trustedResultCount, 0) > 0)
    .map((task) => task.queryId)
    .sort((a, b) => a.localeCompare(b));

  const tasksStillFailingAfterRewrite = rewrittenTasks
    .filter((task) => toNumber(queryById.get(task.queryId)?.trustedResultCount, 0) === 0)
    .map((task) => task.queryId)
    .sort((a, b) => a.localeCompare(b));

  const rewrittenTaskCount = rewrittenTasks.length;
  const rewriteAdoptedCount = rewrittenTasks.filter((task) => task.rewriteAdopted).length;
  const tasksStillEmptyCount = tasksStillFailingAfterRewrite.length;

  let recommendedNextStep = "partial_recovery_retain_rewrites_and_continue_targeted_query_calibration";
  if (tasksStillEmptyCount === rewrittenTaskCount) {
    recommendedNextStep = "no_recovery_after_rewrite_runtime_scope_or_endpoint_debug_required";
  } else if (tasksRecoveredByRewrite.length > 0) {
    recommendedNextStep = "adopt_rewrites_and_rerun_goldset_then_focus_on_remaining_empty_tasks";
  }

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.5",
    originalTaskCount: (originalTasks || []).length,
    rewrittenTaskCount,
    rewriteAdoptedCount,
    tasksStillEmptyCount,
    top1DecisionHitRate: toNumber(rerunEval?.top1DecisionHitRate, 0),
    top3DecisionHitRate: toNumber(rerunEval?.top3DecisionHitRate, 0),
    top5DecisionHitRate: toNumber(rerunEval?.top5DecisionHitRate, 0),
    sectionTypeHitRate: toNumber(rerunEval?.sectionTypeHitRate, 0),
    intentBreakdown: rerunEval?.intentBreakdown || [],
    falsePositiveChunkTypeCounts: rerunEval?.falsePositiveChunkTypeCounts || [],
    noisyChunkDominationRate: toNumber(rerunEval?.noisyChunkDominationRate, 0),
    tasksRecoveredByRewrite,
    tasksStillFailingAfterRewrite,
    sideBySideComparisonVsOriginalR60: buildComparison({ originalEval, rerunEval }),
    recommendedNextStep
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.5 Gold-Set Rewrite Adoption + Rerun (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- originalTaskCount: ${report.originalTaskCount}`);
  lines.push(`- rewrittenTaskCount: ${report.rewrittenTaskCount}`);
  lines.push(`- rewriteAdoptedCount: ${report.rewriteAdoptedCount}`);
  lines.push(`- tasksStillEmptyCount: ${report.tasksStillEmptyCount}`);
  lines.push(`- top1DecisionHitRate: ${report.top1DecisionHitRate}`);
  lines.push(`- top3DecisionHitRate: ${report.top3DecisionHitRate}`);
  lines.push(`- top5DecisionHitRate: ${report.top5DecisionHitRate}`);
  lines.push(`- sectionTypeHitRate: ${report.sectionTypeHitRate}`);
  lines.push(`- noisyChunkDominationRate: ${report.noisyChunkDominationRate}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Tasks Recovered By Rewrite");
  for (const id of report.tasksRecoveredByRewrite || []) lines.push(`- ${id}`);
  if (!(report.tasksRecoveredByRewrite || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Tasks Still Failing After Rewrite");
  for (const id of report.tasksStillFailingAfterRewrite || []) lines.push(`- ${id}`);
  if (!(report.tasksStillFailingAfterRewrite || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Intent Breakdown");
  for (const row of report.intentBreakdown || []) {
    lines.push(
      `- ${row.intent}: tasks=${row.tasks}, top1=${row.top1DecisionHitRate}, top3=${row.top3DecisionHitRate}, top5=${row.top5DecisionHitRate}, sectionType=${row.sectionTypeHitRate}`
    );
  }
  if (!(report.intentBreakdown || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Side-by-Side vs Original R60");
  if (report.sideBySideComparisonVsOriginalR60) {
    for (const [metric, values] of Object.entries(report.sideBySideComparisonVsOriginalR60)) {
      lines.push(`- ${metric}: original=${values.original}, rewritten=${values.rewritten}, delta=${values.delta}`);
    }
  } else {
    lines.push("- unavailable");
  }
  lines.push("");
  lines.push("- Dry-run only. No activation writes, rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function fetchJson(url, payload) {
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const raw = await response.text();
    let body;
    try {
      body = JSON.parse(raw);
    } catch {
      return { results: [], total: 0 };
    }
    if (!response.ok) return { results: [], total: 0 };
    return body;
  } catch {
    return { results: [], total: 0 };
  }
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [originalTasks, repairedTasks, relaxationReport, originalEval] = await Promise.all([
    readJson(path.resolve(reportsDir, originalTasksName)),
    readJson(path.resolve(reportsDir, repairedTasksName)),
    readJson(path.resolve(reportsDir, relaxationName)),
    readJsonIfExists(path.resolve(reportsDir, originalEvalName))
  ]);

  const rewrittenTasks = buildRewrittenTasks({
    originalTasks,
    repairedTasks,
    relaxationReport
  });
  const evalTasks = rewrittenTasks.map(toEvalTask);
  const rerunEval = await runR60GoldsetEvaluation({
    apiBase,
    reportsDir,
    tasks: evalTasks,
    limit,
    fetchSearchDebug: (payload) => fetchJson(`${apiBase}/admin/retrieval/debug`, payload)
  });
  const report = buildR60_5RerunReport({
    originalTasks,
    rewrittenTasks,
    rerunEval,
    originalEval
  });

  const rewrittenPath = path.resolve(reportsDir, rewrittenTasksName);
  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(rewrittenPath, JSON.stringify(rewrittenTasks, null, 2)),
    fs.writeFile(jsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, toMarkdown(report))
  ]);

  console.log(
    JSON.stringify(
      {
        originalTaskCount: report.originalTaskCount,
        rewrittenTaskCount: report.rewrittenTaskCount,
        rewriteAdoptedCount: report.rewriteAdoptedCount,
        tasksStillEmptyCount: report.tasksStillEmptyCount,
        top1DecisionHitRate: report.top1DecisionHitRate,
        top3DecisionHitRate: report.top3DecisionHitRate,
        top5DecisionHitRate: report.top5DecisionHitRate,
        sectionTypeHitRate: report.sectionTypeHitRate,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.5 rewritten gold-set written to ${rewrittenPath}`);
  console.log(`R60.5 rerun report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
