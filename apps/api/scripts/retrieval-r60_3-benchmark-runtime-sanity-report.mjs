import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const repairedTasksName = process.env.RETRIEVAL_R60_3_TASKS_NAME || "retrieval-r60_2-goldset-repaired.json";
const r60EvalName = process.env.RETRIEVAL_R60_3_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const outputJsonName = process.env.RETRIEVAL_R60_3_REPORT_NAME || "retrieval-r60_3-benchmark-runtime-sanity-report.json";
const outputMdName = process.env.RETRIEVAL_R60_3_MARKDOWN_NAME || "retrieval-r60_3-benchmark-runtime-sanity-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_3_LIMIT || "10", 10);

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

function normalizeSectionType(sectionType) {
  const s = String(sectionType || "");
  const lower = s.toLowerCase();
  if (lower === "analysis" || lower === "body") return "analysis_reasoning";
  if (lower === "order") return "holding_disposition";
  if (lower === "findings") return "findings";
  if (lower === "procedural_history") return "procedural_history";
  return lower.replace(/\s+/g, "_");
}

async function fetchDebugQuery({ apiBaseUrl, task }) {
  const payload = {
    query: task.query,
    queryType: INTENT_TO_QUERY_TYPE[String(task.intent || "")] || "keyword",
    limit,
    filters: {
      approvedOnly: true,
      fileType: "decision_docx"
    }
  };

  const response = await fetch(`${apiBaseUrl}/admin/retrieval/debug`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });

  const raw = await response.text();
  let json;
  try {
    json = JSON.parse(raw);
  } catch {
    return { ok: false, status: response.status, errorCode: "non_json_response", results: [] };
  }
  if (!response.ok) {
    return { ok: false, status: response.status, errorCode: "http_error", results: [], body: json };
  }
  return { ok: true, status: response.status, results: json.results || [] };
}

function inferEmptyCause({ fetchOk, trustedResults, expectedIdsPresentCount, query, intent }) {
  if (!fetchOk) return "endpoint_unreachable_or_error";
  if (!trustedResults.length && expectedIdsPresentCount === 0) return "expected_ids_not_in_trusted_scope";
  if (!trustedResults.length && /rule\s*\d|ordinance\s*\d|index code/i.test(String(query || ""))) return "citation_query_not_matching_runtime_index_terms";
  if (!trustedResults.length && String(intent) === "comparative_reasoning") return "comparative_query_too_broad_or_semantically_weak";
  if (!trustedResults.length) return "query_formulation_or_index_coverage_gap";
  return "none";
}

function inferMismatchCause({ trustedResults, expectedDecisionIds, expectedSectionTypes }) {
  if (!trustedResults.length) return "none";
  const returnedIds = new Set(trustedResults.map((row) => String(row.documentId || "")));
  const returnedTypes = new Set(trustedResults.map((row) => normalizeSectionType(row.sectionLabel || row.chunkType || "")));
  const hasExpectedId = (expectedDecisionIds || []).some((id) => returnedIds.has(String(id)));
  const hasExpectedType = (expectedSectionTypes || []).map(normalizeSectionType).some((t) => returnedTypes.has(String(t)));
  if (!hasExpectedId && hasExpectedType) return "decision_expectation_mismatch";
  if (hasExpectedId && !hasExpectedType) return "section_type_expectation_mismatch";
  if (!hasExpectedId && !hasExpectedType) return "decision_and_section_mismatch";
  return "none";
}

export async function buildR60_3RuntimeSanityReport({ repairedTasks, trustedDocumentIds, apiBaseUrl, fetchFn = fetchDebugQuery }) {
  const trustedSet = new Set((trustedDocumentIds || []).map(String));
  const rows = [];
  let endpointErrorCount = 0;

  for (const task of repairedTasks || []) {
    let runtime;
    try {
      runtime = await fetchFn({ apiBaseUrl, task });
    } catch {
      runtime = { ok: false, status: 0, errorCode: "fetch_failed", results: [] };
    }
    if (!runtime.ok) endpointErrorCount += 1;

    const runtimeResults = (runtime.results || []).filter((row) => trustedSet.has(String(row?.documentId || "")));
    const top = runtimeResults.slice(0, limit);
    const returnedDecisionIds = unique(top.map((row) => row.documentId));
    const returnedSectionTypes = unique(top.map((row) => normalizeSectionType(row.sectionLabel || row.chunkType || "")));
    const expectedDecisionIds = (task.expectedDecisionIds || []).map(String);
    const expectedSectionTypes = (task.expectedSectionTypes || []).map(String);
    const expectedDecisionIdsPresentInTrustedCorpus = expectedDecisionIds.filter((id) => trustedSet.has(String(id)));
    const idMatch = expectedDecisionIds.some((id) => returnedDecisionIds.includes(String(id)));
    const typeMatch = expectedSectionTypes.map(normalizeSectionType).some((type) => returnedSectionTypes.includes(String(type)));

    const likelyEmptyResultCause = inferEmptyCause({
      fetchOk: runtime.ok,
      trustedResults: top,
      expectedIdsPresentCount: expectedDecisionIdsPresentInTrustedCorpus.length,
      query: task.query,
      intent: task.intent
    });
    const likelyMismatchCause = inferMismatchCause({
      trustedResults: top,
      expectedDecisionIds,
      expectedSectionTypes
    });

    rows.push({
      queryId: String(task.queryId || ""),
      query: String(task.query || ""),
      intent: String(task.intent || ""),
      runtimeReturnedCount: top.length,
      topReturnedDecisionIds: returnedDecisionIds,
      topReturnedSectionTypes: returnedSectionTypes,
      expectedDecisionIds,
      expectedSectionTypes,
      likelyEmptyResultCause,
      likelyMismatchCause
    });
  }

  const tasksWithRuntimeResults = rows.filter((row) => row.runtimeReturnedCount > 0).map((row) => row.queryId);
  const tasksStillEmpty = rows.filter((row) => row.runtimeReturnedCount === 0).map((row) => row.queryId);
  const emptyByIntent = Object.entries(countBy(rows.filter((row) => row.runtimeReturnedCount === 0).map((row) => row.intent)))
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([intent, count]) => ({ intent, count }));
  const likelyQueryFormulationIssues = rows
    .filter((row) => row.likelyEmptyResultCause === "query_formulation_or_index_coverage_gap" || row.likelyEmptyResultCause === "comparative_query_too_broad_or_semantically_weak")
    .map((row) => row.queryId);
  const likelyEndpointOrScopeIssues = rows
    .filter((row) => row.likelyEmptyResultCause === "endpoint_unreachable_or_error" || row.likelyEmptyResultCause === "expected_ids_not_in_trusted_scope")
    .map((row) => row.queryId);

  let overallCauseClassification = "no_issue_detected";
  const total = Math.max(1, rows.length);
  const emptyRatio = tasksStillEmpty.length / total;
  if (tasksStillEmpty.length === 0) overallCauseClassification = "no_issue_detected";
  else if (endpointErrorCount === rows.length) overallCauseClassification = "endpoint_behavior_mismatch";
  else if (emptyRatio >= 0.8 && likelyEndpointOrScopeIssues.length >= likelyQueryFormulationIssues.length) overallCauseClassification = "runtime_scope_mismatch";
  else if (emptyRatio >= 0.6 && likelyQueryFormulationIssues.length > likelyEndpointOrScopeIssues.length) overallCauseClassification = "benchmark_queries_too_strict";
  else overallCauseClassification = "mixed_causes";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.3",
    summary: {
      tasksEvaluated: rows.length,
      trustedDocumentCount: trustedSet.size,
      tasksWithRuntimeResultsCount: tasksWithRuntimeResults.length,
      tasksStillEmptyCount: tasksStillEmpty.length
    },
    tasksWithRuntimeResults,
    tasksStillEmpty,
    emptyByIntent,
    likelyQueryFormulationIssues,
    likelyEndpointOrScopeIssues,
    perTaskRows: rows.sort((a, b) => String(a.queryId).localeCompare(String(b.queryId))),
    overallCauseClassification
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.3 Benchmark Runtime Sanity Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push(`- overallCauseClassification: ${report.overallCauseClassification}`);
  lines.push("");

  lines.push("## Tasks With Runtime Results");
  lines.push(`- count: ${(report.tasksWithRuntimeResults || []).length}`);
  for (const id of report.tasksWithRuntimeResults || []) lines.push(`- ${id}`);
  if (!(report.tasksWithRuntimeResults || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Tasks Still Empty");
  lines.push(`- count: ${(report.tasksStillEmpty || []).length}`);
  for (const id of report.tasksStillEmpty || []) lines.push(`- ${id}`);
  if (!(report.tasksStillEmpty || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Empty By Intent");
  for (const row of report.emptyByIntent || []) lines.push(`- ${row.intent}: ${row.count}`);
  if (!(report.emptyByIntent || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Per Task Rows");
  for (const row of report.perTaskRows || []) {
    lines.push(
      `- ${row.queryId} | returned=${row.runtimeReturnedCount} | emptyCause=${row.likelyEmptyResultCause} | mismatchCause=${row.likelyMismatchCause}`
    );
  }
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes or runtime mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [repairedTasks, r60Eval] = await Promise.all([
    readJson(path.resolve(reportsDir, repairedTasksName)),
    readJson(path.resolve(reportsDir, r60EvalName))
  ]);
  const trustedIds = r60Eval?.trustedCorpus?.trustedDocumentIds || [];

  const report = await buildR60_3RuntimeSanityReport({
    repairedTasks,
    trustedDocumentIds: trustedIds,
    apiBaseUrl: apiBase
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.summary.tasksEvaluated,
        tasksWithRuntimeResultsCount: report.summary.tasksWithRuntimeResultsCount,
        tasksStillEmptyCount: report.summary.tasksStillEmptyCount,
        overallCauseClassification: report.overallCauseClassification
      },
      null,
      2
    )
  );
  console.log(`R60.3 runtime sanity report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
