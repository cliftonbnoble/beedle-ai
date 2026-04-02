import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const tasksName = process.env.RETRIEVAL_R61_1_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const normalizedName = process.env.RETRIEVAL_R61_1_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const r608Name = process.env.RETRIEVAL_R61_1_R60_8_NAME || "retrieval-r60_8-query-normalization-report.json";
const r61Name = process.env.RETRIEVAL_R61_1_R61_NAME || "retrieval-r61-two-stage-prototype-report.json";
const outputReportName =
  process.env.RETRIEVAL_R61_1_REPORT_NAME || "retrieval-r61_1-stage1-candidate-audit-report.json";
const outputMdName = process.env.RETRIEVAL_R61_1_MARKDOWN_NAME || "retrieval-r61_1-stage1-candidate-audit-report.md";

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

function pickR608DecisionIds(taskEval) {
  if (!taskEval) return [];
  const rows = Array.isArray(taskEval.variantRows) ? taskEval.variantRows : [];
  const best = rows.find((row) => String(row?.variantType || "") === String(taskEval.bestVariantType || ""));
  const selected = best || rows[0] || null;
  if (!selected) return [];
  return unique(selected.topReturnedDecisionIds || []);
}

function classifyStage1Failure(row) {
  if (row.r608DecisionIds.length > 0 && row.r61Stage1CandidateDecisionIds.length === 0) return "trusted_filter_drop_or_candidate_mapping_gap";
  if (row.r61Stage1CandidateDecisionIds.length > 0 && !row.expectedDecisionAppearedInR61Stage1) return "candidate_scoring_or_topk_selection_gap";
  if (row.r608DecisionIds.length === 0 && row.r61Stage1CandidateDecisionIds.length === 0) return "no_candidate_signal_in_either_path";
  if (row.expectedDecisionAppearedInR608 && !row.expectedDecisionAppearedInR61Stage1) return "stage1_candidate_recall_regression";
  return "none";
}

function classifyRootCause(summary) {
  if (summary.tasksWhereR60_8WorkedButR61Stage1Failed > 0 && summary.tasksWhereR61Stage1HadCandidates === 0) {
    return "stage1_candidate_mapping_bug";
  }
  if (summary.tasksWhereR61Stage1HadCandidates === 0 && summary.tasksWhereR60_8HadDecisionResults === 0) {
    return "stage1_candidate_index_gap";
  }
  if (summary.tasksWhereR61Stage1HadCandidates > 0 && summary.tasksWhereR60_8WorkedButR61Stage1Failed > 0) {
    return "stage1_candidate_scoring_bug";
  }
  if (summary.tasksWhereR61Stage1HadCandidates > 0 && summary.tasksWhereR60_8WorkedButR61Stage1Failed === 0) {
    return "stage1_candidate_query_too_strict";
  }
  return "mixed_stage1_failure";
}

export function buildR61_1Stage1AuditReport({ tasks, normalizedRows, r608Report, r61Report }) {
  const normalizedById = new Map((normalizedRows || []).map((row) => [String(row?.queryId || ""), row]));
  const r608ById = new Map((r608Report?.taskEvaluations || []).map((row) => [String(row?.queryId || ""), row]));
  const r61ById = new Map((r61Report?.taskRows || []).map((row) => [String(row?.queryId || ""), row]));

  const taskRows = (tasks || [])
    .slice()
    .sort((a, b) => String(a?.queryId || "").localeCompare(String(b?.queryId || "")))
    .map((task) => {
      const queryId = String(task?.queryId || "");
      const expectedDecisionIds = unique(task?.expectedDecisionIds || []);
      const normalizedQueryUsed = String(normalizedById.get(queryId)?.normalizedQuery || task?.adoptedQuery || task?.query || "");
      const r608Task = r608ById.get(queryId);
      const r61Task = r61ById.get(queryId);

      const r608DecisionIds = pickR608DecisionIds(r608Task);
      const r61Stage1CandidateDecisionIds = unique((r61Task?.stage1DecisionCandidates || []).map((row) => row?.documentId));
      const stage1CandidateCount = r61Stage1CandidateDecisionIds.length;
      const expectedSet = new Set(expectedDecisionIds.map(String));
      const expectedDecisionAppearedInR608 = r608DecisionIds.some((id) => expectedSet.has(String(id)));
      const expectedDecisionAppearedInR61Stage1 = r61Stage1CandidateDecisionIds.some((id) => expectedSet.has(String(id)));

      const row = {
        queryId,
        normalizedQueryUsed,
        r608DecisionIds,
        r61Stage1CandidateDecisionIds,
        stage1CandidateCount,
        expectedDecisionIds,
        expectedDecisionAppearedInR608,
        expectedDecisionAppearedInR61Stage1
      };
      row.likelyStage1FailureCause = classifyStage1Failure(row);
      return row;
    });

  const tasksEvaluated = taskRows.length;
  const tasksWhereR60_8HadDecisionResults = taskRows.filter((row) => row.r608DecisionIds.length > 0).length;
  const tasksWhereR61Stage1HadCandidates = taskRows.filter((row) => row.stage1CandidateCount > 0).length;
  const tasksWhereR60_8WorkedButR61Stage1Failed = taskRows.filter(
    (row) => row.expectedDecisionAppearedInR608 && !row.expectedDecisionAppearedInR61Stage1
  ).length;
  const stage1CandidateCountDistribution = Object.entries(countBy(taskRows.map((row) => String(row.stage1CandidateCount))))
    .map(([candidateCount, count]) => ({ candidateCount: Number(candidateCount), count }))
    .sort((a, b) => a.candidateCount - b.candidateCount);
  const dominantStage1FailurePatterns = Object.entries(countBy(taskRows.map((row) => row.likelyStage1FailureCause)))
    .map(([pattern, count]) => ({ pattern, count }))
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.pattern.localeCompare(b.pattern);
    });

  const singleStageCandidateRecall = Number((tasksWhereR60_8HadDecisionResults / Math.max(1, tasksEvaluated)).toFixed(4));
  const stage1OnlyCandidateRecall = Number((tasksWhereR61Stage1HadCandidates / Math.max(1, tasksEvaluated)).toFixed(4));
  const singleStageEmptyCount = Number(r608Report?.emptyTasksAfter ?? tasksEvaluated - tasksWhereR60_8HadDecisionResults);
  const stage1EmptyCount = Number(taskRows.filter((row) => row.stage1CandidateCount === 0).length);

  const rootSummary = {
    tasksEvaluated,
    tasksWhereR60_8HadDecisionResults,
    tasksWhereR61Stage1HadCandidates,
    tasksWhereR60_8WorkedButR61Stage1Failed
  };
  const rootCauseClassification = classifyRootCause(rootSummary);

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R61.1",
    ...rootSummary,
    stage1CandidateCountDistribution,
    dominantStage1FailurePatterns,
    rootCauseClassification,
    comparisonSingleStageVsStage1: {
      singleStageNormalizedPath: {
        candidateRecall: singleStageCandidateRecall,
        emptyTaskCount: singleStageEmptyCount
      },
      stage1OnlyPath: {
        candidateRecall: stage1OnlyCandidateRecall,
        emptyTaskCount: stage1EmptyCount
      },
      candidateRecallDelta: Number((stage1OnlyCandidateRecall - singleStageCandidateRecall).toFixed(4)),
      emptyTaskDelta: Number((stage1EmptyCount - singleStageEmptyCount).toFixed(0))
    },
    recommendedNextStep:
      rootCauseClassification === "stage1_candidate_mapping_bug"
        ? "align_stage1_candidate_filtering_and_trusted_set_loading_with_r60_8_working_path"
        : "refine_stage1_candidate_query_or_scoring_after_mapping_alignment",
    taskRows
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R61.1 Stage-1 Candidate Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- tasksEvaluated: ${report.tasksEvaluated}`);
  lines.push(`- tasksWhereR60_8HadDecisionResults: ${report.tasksWhereR60_8HadDecisionResults}`);
  lines.push(`- tasksWhereR61Stage1HadCandidates: ${report.tasksWhereR61Stage1HadCandidates}`);
  lines.push(`- tasksWhereR60_8WorkedButR61Stage1Failed: ${report.tasksWhereR60_8WorkedButR61Stage1Failed}`);
  lines.push(`- rootCauseClassification: ${report.rootCauseClassification}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Candidate Count Distribution");
  for (const row of report.stage1CandidateCountDistribution || []) lines.push(`- count=${row.candidateCount}: ${row.count}`);
  if (!(report.stage1CandidateCountDistribution || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Dominant Failure Patterns");
  for (const row of report.dominantStage1FailurePatterns || []) lines.push(`- ${row.pattern}: ${row.count}`);
  if (!(report.dominantStage1FailurePatterns || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Single-Stage vs Stage-1 Comparison");
  lines.push(`- ${JSON.stringify(report.comparisonSingleStageVsStage1)}`);
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [tasks, normalizedRows, r608Report, r61Report] = await Promise.all([
    readJson(path.resolve(reportsDir, tasksName)),
    readJson(path.resolve(reportsDir, normalizedName)),
    readJson(path.resolve(reportsDir, r608Name)),
    readJson(path.resolve(reportsDir, r61Name))
  ]);

  const report = buildR61_1Stage1AuditReport({
    tasks,
    normalizedRows,
    r608Report,
    r61Report
  });

  const jsonPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.tasksEvaluated,
        tasksWhereR60_8HadDecisionResults: report.tasksWhereR60_8HadDecisionResults,
        tasksWhereR61Stage1HadCandidates: report.tasksWhereR61Stage1HadCandidates,
        tasksWhereR60_8WorkedButR61Stage1Failed: report.tasksWhereR60_8WorkedButR61Stage1Failed,
        rootCauseClassification: report.rootCauseClassification,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R61.1 report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
