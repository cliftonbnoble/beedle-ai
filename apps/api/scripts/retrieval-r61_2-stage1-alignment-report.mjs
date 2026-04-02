import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildR61TwoStagePrototypeReport } from "./retrieval-r61-two-stage-prototype-report.mjs";
import { buildR61_1Stage1AuditReport } from "./retrieval-r61_1-stage1-candidate-audit-report.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");

const tasksName = process.env.RETRIEVAL_R61_2_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const normalizedName = process.env.RETRIEVAL_R61_2_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const baselineR60Name = process.env.RETRIEVAL_R61_2_BASELINE_R60_NAME || "retrieval-r60-goldset-eval-report.json";
const baselineR608Name = process.env.RETRIEVAL_R61_2_BASELINE_R60_8_NAME || "retrieval-r60_8-query-normalization-report.json";
const baselineR6010Name = process.env.RETRIEVAL_R61_2_BASELINE_R60_10_NAME || "retrieval-r60_10-query-weighting-report.json";
const evalName = process.env.RETRIEVAL_R61_2_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";

const beforeStage1Name = process.env.RETRIEVAL_R61_2_BEFORE_STAGE1_NAME || "retrieval-r61_1-stage1-candidate-audit-report.json";
const beforeTwoStageName = process.env.RETRIEVAL_R61_2_BEFORE_TWO_STAGE_NAME || "retrieval-r61-two-stage-prototype-report.json";

const alignmentReportName =
  process.env.RETRIEVAL_R61_2_REPORT_NAME || "retrieval-r61_2-stage1-alignment-report.json";
const alignmentMarkdownName =
  process.env.RETRIEVAL_R61_2_MARKDOWN_NAME || "retrieval-r61_2-stage1-alignment-report.md";
const rerunStage1Name =
  process.env.RETRIEVAL_R61_2_STAGE1_RERUN_NAME || "retrieval-r61_2-stage1-audit-rerun-report.json";
const rerunTwoStageName =
  process.env.RETRIEVAL_R61_2_TWO_STAGE_RERUN_NAME || "retrieval-r61_2-two-stage-rerun-report.json";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
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

function summarizeStage1(report) {
  if (!report) {
    return {
      tasksWhereR61Stage1HadCandidates: 0,
      candidateRecall: 0
    };
  }
  return {
    tasksWhereR61Stage1HadCandidates: toNumber(report.tasksWhereR61Stage1HadCandidates),
    candidateRecall: toNumber(report?.comparisonSingleStageVsStage1?.stage1OnlyPath?.candidateRecall)
  };
}

function summarizeTwoStage(report) {
  if (!report) {
    return {
      top1DecisionHitRate: 0,
      top3DecisionHitRate: 0,
      top5DecisionHitRate: 0,
      sectionTypeHitRate: 0,
      emptyTasksAfter: 0
    };
  }
  return {
    top1DecisionHitRate: toNumber(report.top1DecisionHitRate),
    top3DecisionHitRate: toNumber(report.top3DecisionHitRate),
    top5DecisionHitRate: toNumber(report.top5DecisionHitRate),
    sectionTypeHitRate: toNumber(report.sectionTypeHitRate),
    emptyTasksAfter: toNumber(report.emptyTasksAfter)
  };
}

export function buildR61_2Stage1AlignmentReport({
  beforeStage1,
  beforeTwoStage,
  afterStage1,
  afterTwoStage
}) {
  const beforeStage1Summary = summarizeStage1(beforeStage1);
  const afterStage1Summary = summarizeStage1(afterStage1);
  const beforeTwoStageSummary = summarizeTwoStage(beforeTwoStage);
  const afterTwoStageSummary = summarizeTwoStage(afterTwoStage);

  const stage1AlignmentResolved =
    afterStage1Summary.tasksWhereR61Stage1HadCandidates > beforeStage1Summary.tasksWhereR61Stage1HadCandidates ||
    (String(afterStage1?.rootCauseClassification || "") !== "stage1_candidate_mapping_bug" &&
      toNumber(afterStage1?.tasksWhereR60_8WorkedButR61Stage1Failed, 0) <=
        toNumber(beforeStage1?.tasksWhereR60_8WorkedButR61Stage1Failed, 0));

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R61.2",
    mappingFixesApplied: [
      "stage1_uses_r60_8_best_variant_rows_when_available",
      "stage1_falls_back_to_r60_8_best_variant_decision_ids_when_rows_absent",
      "stage1_decision_ids_are_filtered_by_trusted_set_before_candidate_ranking",
      "stage2_runtime_backfill_runs_when_stage1_ids_exist_but_stage2_rows_are_empty"
    ],
    trustedSetSourceBefore:
      "trusted_ids_from_r60_eval_report_with_live_normalized_fallback_stage1_inference",
    trustedSetSourceAfter:
      "trusted_ids_from_r60_eval_report_with_r60_8_best_variant_alignment_and_live_fallback",
    candidateFilterBefore:
      "stage1_candidates_derived_from_live_normalized_rows_only_then_trusted_filter_applied",
    candidateFilterAfter:
      "stage1_candidates_derived_from_r60_8_aligned_candidates_or_live_rows_then_trusted_filter_applied",
    decisionIdMappingBefore:
      "stage1_candidate_mapping_depended_on_live_row_documentId_extraction_only",
    decisionIdMappingAfter:
      "stage1_candidate_mapping_uses_r60_8_topReturnedDecisionIds_when_trustedRows_are_unavailable",
    stage1AlignmentResolved,
    tasksWhereR61Stage1HadCandidatesBefore: beforeStage1Summary.tasksWhereR61Stage1HadCandidates,
    tasksWhereR61Stage1HadCandidatesAfter: afterStage1Summary.tasksWhereR61Stage1HadCandidates,
    candidateRecallBefore: beforeStage1Summary.candidateRecall,
    candidateRecallAfter: afterStage1Summary.candidateRecall,
    top1DecisionHitRateBefore: beforeTwoStageSummary.top1DecisionHitRate,
    top1DecisionHitRateAfter: afterTwoStageSummary.top1DecisionHitRate,
    top3DecisionHitRateBefore: beforeTwoStageSummary.top3DecisionHitRate,
    top3DecisionHitRateAfter: afterTwoStageSummary.top3DecisionHitRate,
    top5DecisionHitRateBefore: beforeTwoStageSummary.top5DecisionHitRate,
    top5DecisionHitRateAfter: afterTwoStageSummary.top5DecisionHitRate,
    sectionTypeHitRateBefore: beforeTwoStageSummary.sectionTypeHitRate,
    sectionTypeHitRateAfter: afterTwoStageSummary.sectionTypeHitRate,
    emptyTasksAfterBefore: beforeTwoStageSummary.emptyTasksAfter,
    emptyTasksAfterAfter: afterTwoStageSummary.emptyTasksAfter,
    recommendedNextStep:
      stage1AlignmentResolved && afterStage1Summary.tasksWhereR61Stage1HadCandidates > 0
        ? "refine_stage2_section_selection_after_stage1_alignment"
        : "continue_stage1_candidate_alignment_using_r60_8_trusted_decision_mapping",
    beforeStage1Summary,
    afterStage1Summary,
    beforeTwoStageSummary,
    afterTwoStageSummary
  };

  return report;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R61.2 Stage-1 Candidate Mapping Alignment (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- stage1AlignmentResolved: ${report.stage1AlignmentResolved}`);
  lines.push(`- tasksWhereR61Stage1HadCandidatesBefore: ${report.tasksWhereR61Stage1HadCandidatesBefore}`);
  lines.push(`- tasksWhereR61Stage1HadCandidatesAfter: ${report.tasksWhereR61Stage1HadCandidatesAfter}`);
  lines.push(`- candidateRecallBefore: ${report.candidateRecallBefore}`);
  lines.push(`- candidateRecallAfter: ${report.candidateRecallAfter}`);
  lines.push(`- top1DecisionHitRateBefore: ${report.top1DecisionHitRateBefore}`);
  lines.push(`- top1DecisionHitRateAfter: ${report.top1DecisionHitRateAfter}`);
  lines.push(`- top3DecisionHitRateBefore: ${report.top3DecisionHitRateBefore}`);
  lines.push(`- top3DecisionHitRateAfter: ${report.top3DecisionHitRateAfter}`);
  lines.push(`- top5DecisionHitRateBefore: ${report.top5DecisionHitRateBefore}`);
  lines.push(`- top5DecisionHitRateAfter: ${report.top5DecisionHitRateAfter}`);
  lines.push(`- sectionTypeHitRateBefore: ${report.sectionTypeHitRateBefore}`);
  lines.push(`- sectionTypeHitRateAfter: ${report.sectionTypeHitRateAfter}`);
  lines.push(`- emptyTasksAfterBefore: ${report.emptyTasksAfterBefore}`);
  lines.push(`- emptyTasksAfterAfter: ${report.emptyTasksAfterAfter}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Mapping Fixes Applied");
  for (const item of report.mappingFixesApplied || []) lines.push(`- ${item}`);
  if (!(report.mappingFixesApplied || []).length) lines.push("- none");
  lines.push("");
  lines.push(`- trustedSetSourceBefore: ${report.trustedSetSourceBefore}`);
  lines.push(`- trustedSetSourceAfter: ${report.trustedSetSourceAfter}`);
  lines.push(`- candidateFilterBefore: ${report.candidateFilterBefore}`);
  lines.push(`- candidateFilterAfter: ${report.candidateFilterAfter}`);
  lines.push(`- decisionIdMappingBefore: ${report.decisionIdMappingBefore}`);
  lines.push(`- decisionIdMappingAfter: ${report.decisionIdMappingAfter}`);
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [
    tasks,
    normalizedPackages,
    baselineR60,
    baselineR608,
    baselineR6010,
    evalReport,
    beforeStage1,
    beforeTwoStage
  ] = await Promise.all([
    readJson(path.resolve(reportsDir, tasksName)),
    readJson(path.resolve(reportsDir, normalizedName)),
    readJsonIfExists(path.resolve(reportsDir, baselineR60Name)),
    readJson(path.resolve(reportsDir, baselineR608Name)),
    readJsonIfExists(path.resolve(reportsDir, baselineR6010Name)),
    readJson(path.resolve(reportsDir, evalName)),
    readJsonIfExists(path.resolve(reportsDir, beforeStage1Name)),
    readJsonIfExists(path.resolve(reportsDir, beforeTwoStageName))
  ]);

  const trustedDecisionIds = evalReport?.trustedCorpus?.trustedDocumentIds || [];

  const afterTwoStage = await buildR61TwoStagePrototypeReport({
    tasks,
    normalizedPackages,
    trustedDecisionIds,
    r608Report: baselineR608,
    baselineR60,
    baselineR608,
    baselineR6010,
    apiBaseUrl: apiBase
  });

  const afterStage1 = buildR61_1Stage1AuditReport({
    tasks,
    normalizedRows: normalizedPackages,
    r608Report: baselineR608,
    r61Report: afterTwoStage
  });

  const alignment = buildR61_2Stage1AlignmentReport({
    beforeStage1,
    beforeTwoStage,
    afterStage1,
    afterTwoStage
  });

  const paths = {
    alignmentJson: path.resolve(reportsDir, alignmentReportName),
    alignmentMd: path.resolve(reportsDir, alignmentMarkdownName),
    stage1Rerun: path.resolve(reportsDir, rerunStage1Name),
    twoStageRerun: path.resolve(reportsDir, rerunTwoStageName)
  };

  await Promise.all([
    fs.writeFile(paths.alignmentJson, JSON.stringify(alignment, null, 2)),
    fs.writeFile(paths.alignmentMd, toMarkdown(alignment)),
    fs.writeFile(paths.stage1Rerun, JSON.stringify(afterStage1, null, 2)),
    fs.writeFile(paths.twoStageRerun, JSON.stringify(afterTwoStage, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        stage1AlignmentResolved: alignment.stage1AlignmentResolved,
        tasksWhereR61Stage1HadCandidatesBefore: alignment.tasksWhereR61Stage1HadCandidatesBefore,
        tasksWhereR61Stage1HadCandidatesAfter: alignment.tasksWhereR61Stage1HadCandidatesAfter,
        candidateRecallBefore: alignment.candidateRecallBefore,
        candidateRecallAfter: alignment.candidateRecallAfter,
        top1DecisionHitRateBefore: alignment.top1DecisionHitRateBefore,
        top1DecisionHitRateAfter: alignment.top1DecisionHitRateAfter,
        top3DecisionHitRateBefore: alignment.top3DecisionHitRateBefore,
        top3DecisionHitRateAfter: alignment.top3DecisionHitRateAfter,
        top5DecisionHitRateBefore: alignment.top5DecisionHitRateBefore,
        top5DecisionHitRateAfter: alignment.top5DecisionHitRateAfter,
        sectionTypeHitRateBefore: alignment.sectionTypeHitRateBefore,
        sectionTypeHitRateAfter: alignment.sectionTypeHitRateAfter,
        emptyTasksAfterBefore: alignment.emptyTasksAfterBefore,
        emptyTasksAfterAfter: alignment.emptyTasksAfterAfter,
        recommendedNextStep: alignment.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R61.2 alignment report written to ${paths.alignmentJson}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
