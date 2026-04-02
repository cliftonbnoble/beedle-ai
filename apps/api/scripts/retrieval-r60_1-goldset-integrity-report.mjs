import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const evalReportName = process.env.RETRIEVAL_R60_1_EVAL_REPORT_NAME || "retrieval-r60-goldset-eval-report.json";
const tasksName = process.env.RETRIEVAL_R60_1_TASKS_NAME || "retrieval-r60-goldset-tasks.json";
const outputJsonName = process.env.RETRIEVAL_R60_1_REPORT_NAME || "retrieval-r60_1-goldset-integrity-report.json";
const outputMdName = process.env.RETRIEVAL_R60_1_MARKDOWN_NAME || "retrieval-r60_1-goldset-integrity-report.md";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function classifyTask({
  task,
  queryRow,
  trustedSet
}) {
  const expectedDecisionIds = (task?.expectedDecisionIds || []).map(String);
  const expectedSectionTypes = (task?.expectedSectionTypes || []).map(String);
  const expectedDecisionIdsPresentInTrustedCorpus = expectedDecisionIds.filter((id) => trustedSet.has(id));
  const missingExpectedDecisionIds = expectedDecisionIds.filter((id) => !trustedSet.has(id));

  const returnedRows = (queryRow?.topResults || []).map((row) => ({
    documentId: String(row?.documentId || ""),
    chunkType: String(row?.chunkType || row?.sectionLabel || "")
  }));
  const trustedResultsReturned = returnedRows.length;
  const returnedDecisionIds = unique(returnedRows.map((row) => row.documentId));
  const returnedSectionTypes = unique(returnedRows.map((row) => row.chunkType));

  const hasExpectedIdMatch = expectedDecisionIds.some((id) => returnedDecisionIds.includes(id));
  const hasSectionTypeMatch = expectedSectionTypes.some((sectionType) => returnedSectionTypes.includes(sectionType));

  let idMatchStatus = "no_expected_id_match";
  if (!trustedResultsReturned) idMatchStatus = "no_results";
  else if (!expectedDecisionIdsPresentInTrustedCorpus.length) idMatchStatus = "expected_ids_missing_from_trusted_corpus";
  else if (hasExpectedIdMatch) idMatchStatus = "expected_id_matched";

  let sectionTypeMatchStatus = "no_section_type_match";
  if (!trustedResultsReturned) sectionTypeMatchStatus = "no_results";
  else if (hasSectionTypeMatch) sectionTypeMatchStatus = "section_type_matched";
  else if (hasExpectedIdMatch) sectionTypeMatchStatus = "expected_id_match_but_section_type_mismatch";

  const looksPlaceholderId = expectedDecisionIds.some((id) => !/^doc_[0-9a-f-]{36}$/i.test(id));

  let likelyFailureMode = "benchmark_corpus_mismatch";
  if (!trustedResultsReturned) likelyFailureMode = "empty_runtime_results";
  else if (expectedDecisionIdsPresentInTrustedCorpus.length === 0 && !looksPlaceholderId) likelyFailureMode = "benchmark_corpus_mismatch";
  else if (expectedDecisionIdsPresentInTrustedCorpus.length === 0 && looksPlaceholderId) likelyFailureMode = "benchmark_placeholder_or_corpus_mismatch";
  else if (hasExpectedIdMatch && !hasSectionTypeMatch) likelyFailureMode = "section_type_mapping_bug";
  else if (!hasExpectedIdMatch && expectedDecisionIdsPresentInTrustedCorpus.length > 0) likelyFailureMode = "result_mapping_bug";
  else if (hasExpectedIdMatch && hasSectionTypeMatch) likelyFailureMode = "none";

  return {
    queryId: String(task?.queryId || ""),
    query: String(task?.query || ""),
    expectedDecisionIds,
    expectedSectionTypes,
    expectedDecisionIdsPresentInTrustedCorpus,
    missingExpectedDecisionIds,
    trustedResultsReturned,
    returnedDecisionIds,
    returnedSectionTypes,
    idMatchStatus,
    sectionTypeMatchStatus,
    likelyFailureMode
  };
}

function classifyRootCause(report) {
  const total = Math.max(1, report.taskRows.length);
  const noResults = report.tasksWithNoReturnedResults.length;
  const missingExpected = report.tasksWithMissingExpectedIds.length;
  const mappingNoMatch = report.tasksWithReturnedResultsButNoExpectedIdMatch.length;
  const sectionMismatch = report.tasksWithExpectedIdMatchButNoSectionTypeMatch.length;

  if (noResults === total) return "empty_runtime_results";
  if (missingExpected === total && mappingNoMatch === 0 && sectionMismatch === 0) return "benchmark_corpus_mismatch";
  if (mappingNoMatch > 0 && sectionMismatch === 0 && missingExpected === 0) return "result_mapping_bug";
  if (sectionMismatch > 0 && mappingNoMatch === 0) return "section_type_mapping_bug";
  return "mixed_causes";
}

export function buildR60_1GoldsetIntegrityReport({ evalReport, tasks }) {
  const trustedSet = new Set((evalReport?.trustedCorpus?.trustedDocumentIds || []).map(String));
  const queryById = new Map((evalReport?.queryResults || []).map((row) => [String(row?.queryId || ""), row]));

  const taskRows = (tasks || [])
    .map((task) => classifyTask({ task, queryRow: queryById.get(String(task?.queryId || "")) || {}, trustedSet }))
    .sort((a, b) => String(a.queryId).localeCompare(String(b.queryId)));

  const tasksWithMissingExpectedIds = taskRows.filter((row) => row.missingExpectedDecisionIds.length > 0).map((row) => row.queryId);
  const tasksWithNoReturnedResults = taskRows.filter((row) => row.trustedResultsReturned === 0).map((row) => row.queryId);
  const tasksWithReturnedResultsButNoExpectedIdMatch = taskRows
    .filter((row) => row.trustedResultsReturned > 0 && row.idMatchStatus !== "expected_id_matched")
    .map((row) => row.queryId);
  const tasksWithExpectedIdMatchButNoSectionTypeMatch = taskRows
    .filter((row) => row.idMatchStatus === "expected_id_matched" && row.sectionTypeMatchStatus !== "section_type_matched")
    .map((row) => row.queryId);
  const tasksLikelyUsingPlaceholderExpectations = taskRows
    .filter((row) => row.expectedDecisionIds.some((id) => !/^doc_[0-9a-f-]{36}$/i.test(id)))
    .map((row) => row.queryId);

  const base = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.1",
    summary: {
      tasksEvaluated: taskRows.length,
      trustedDocumentCount: trustedSet.size
    },
    tasksWithMissingExpectedIds,
    tasksWithNoReturnedResults,
    tasksWithReturnedResultsButNoExpectedIdMatch,
    tasksWithExpectedIdMatchButNoSectionTypeMatch,
    tasksLikelyUsingPlaceholderExpectations,
    taskRows
  };

  const rootCauseClassification = classifyRootCause(base);
  return {
    ...base,
    rootCauseClassification
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.1 Gold-Set Integrity Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push(`- rootCauseClassification: ${report.rootCauseClassification}`);
  lines.push(`- tasksWithMissingExpectedIds: ${report.tasksWithMissingExpectedIds.length}`);
  lines.push(`- tasksWithNoReturnedResults: ${report.tasksWithNoReturnedResults.length}`);
  lines.push(`- tasksWithReturnedResultsButNoExpectedIdMatch: ${report.tasksWithReturnedResultsButNoExpectedIdMatch.length}`);
  lines.push(`- tasksWithExpectedIdMatchButNoSectionTypeMatch: ${report.tasksWithExpectedIdMatchButNoSectionTypeMatch.length}`);
  lines.push(`- tasksLikelyUsingPlaceholderExpectations: ${report.tasksLikelyUsingPlaceholderExpectations.length}`);
  lines.push("");

  lines.push("## Task Diagnostics");
  for (const row of report.taskRows || []) {
    lines.push(
      `- ${row.queryId} | idMatch=${row.idMatchStatus} | sectionMatch=${row.sectionTypeMatchStatus} | failure=${row.likelyFailureMode} | trustedResults=${row.trustedResultsReturned}`
    );
    lines.push(`  - missingExpectedDecisionIds: ${(row.missingExpectedDecisionIds || []).join(", ") || "<none>"}`);
    lines.push(`  - returnedDecisionIds: ${(row.returnedDecisionIds || []).join(", ") || "<none>"}`);
    lines.push(`  - returnedSectionTypes: ${(row.returnedSectionTypes || []).join(", ") || "<none>"}`);
  }
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes or runtime mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [evalReport, tasks] = await Promise.all([
    readJson(path.resolve(reportsDir, evalReportName)),
    readJson(path.resolve(reportsDir, tasksName))
  ]);
  const report = buildR60_1GoldsetIntegrityReport({ evalReport, tasks });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.summary.tasksEvaluated,
        tasksWithMissingExpectedIds: report.tasksWithMissingExpectedIds.length,
        tasksWithNoReturnedResults: report.tasksWithNoReturnedResults.length,
        rootCauseClassification: report.rootCauseClassification
      },
      null,
      2
    )
  );
  console.log(`R60.1 gold-set integrity report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
