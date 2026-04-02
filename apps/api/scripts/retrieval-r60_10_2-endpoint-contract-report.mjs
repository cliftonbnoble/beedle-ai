import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const tasksName = process.env.RETRIEVAL_R60_10_2_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const normalizedName = process.env.RETRIEVAL_R60_10_2_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const weightedName = process.env.RETRIEVAL_R60_10_2_WEIGHTED_NAME || "retrieval-r60_10-weighted-queries.json";
const outputReportName =
  process.env.RETRIEVAL_R60_10_2_REPORT_NAME || "retrieval-r60_10_2-endpoint-contract-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_2_MARKDOWN_NAME || "retrieval-r60_10_2-endpoint-contract-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_10_2_LIMIT || "10", 10);

const INTENT_TO_QUERY_TYPE = {
  authority_lookup: "rules_ordinance",
  findings: "keyword",
  procedural_history: "keyword",
  issue_holding_disposition: "keyword",
  analysis_reasoning: "keyword",
  comparative_reasoning: "keyword",
  citation_direct: "citation_lookup"
};

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

function stableStringify(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  const keys = Object.keys(value).sort((a, b) => a.localeCompare(b));
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(",")}}`;
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function normalizeSectionType(value) {
  const s = String(value || "");
  const lower = s.toLowerCase();
  if (lower === "analysis" || lower === "body") return "analysis_reasoning";
  if (lower === "order") return "holding_disposition";
  if (lower === "findings") return "findings";
  return lower.replace(/\s+/g, "_");
}

function buildPayload(query, intent) {
  return {
    query: String(query || ""),
    queryType: INTENT_TO_QUERY_TYPE[String(intent || "")] || "keyword",
    limit,
    filters: {
      approvedOnly: true,
      fileType: "decision_docx"
    }
  };
}

function summarizeResponseShape(status, parseOk, parsed, textSnippet) {
  const hasResultsArray = Boolean(parseOk && Array.isArray(parsed?.results));
  const topLevelKeys = parseOk && parsed && typeof parsed === "object" ? Object.keys(parsed).sort((a, b) => a.localeCompare(b)) : [];
  const first = hasResultsArray && parsed.results.length ? parsed.results[0] : {};
  const firstResultKeys = first && typeof first === "object" ? Object.keys(first).sort((a, b) => a.localeCompare(b)) : [];
  return {
    status,
    parseOk,
    hasResultsArray,
    topLevelKeys,
    firstResultKeys,
    errorTextSnippet: textSnippet
  };
}

function extractObservedInputs(parseOk, parsed) {
  if (!parseOk || !parsed || typeof parsed !== "object") {
    return {
      query: "",
      queryType: "",
      filters: {}
    };
  }
  return {
    query: String(parsed.query || ""),
    queryType: String(parsed.queryType || ""),
    filters: parsed.filters && typeof parsed.filters === "object" ? parsed.filters : {}
  };
}

async function fetchDebugContract(apiBaseUrl, payload) {
  try {
    const response = await fetch(`${apiBaseUrl}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const raw = await response.text();
    let parsed = null;
    let parseOk = true;
    try {
      parsed = JSON.parse(raw);
    } catch {
      parseOk = false;
    }
    const results = parseOk && Array.isArray(parsed?.results) ? parsed.results : [];
    return {
      status: response.status,
      ok: response.ok,
      parseOk,
      parsed,
      rawResultCount: results.length,
      parsedResultCount: results.length,
      topReturnedDecisionIds: unique(results.slice(0, 5).map((row) => row?.documentId)),
      topReturnedSectionTypes: unique(results.slice(0, 5).map((row) => normalizeSectionType(row?.sectionLabel || row?.chunkType || ""))),
      observedInputs: extractObservedInputs(parseOk, parsed),
      responseShape: summarizeResponseShape(response.status, parseOk, parsed, raw.slice(0, 200))
    };
  } catch (error) {
    return {
      status: 0,
      ok: false,
      parseOk: false,
      parsed: null,
      rawResultCount: 0,
      parsedResultCount: 0,
      topReturnedDecisionIds: [],
      topReturnedSectionTypes: [],
      observedInputs: { query: "", queryType: "", filters: {} },
      responseShape: summarizeResponseShape(0, false, null, String(error || "fetch_error").slice(0, 200))
    };
  }
}

function classifyMismatch(row) {
  const payloadShapeDiff = stableStringify({
    queryType: row.normalizedRequestPayload.queryType,
    limit: row.normalizedRequestPayload.limit,
    filters: row.normalizedRequestPayload.filters
  }) !==
    stableStringify({
      queryType: row.weightedRequestPayload.queryType,
      limit: row.weightedRequestPayload.limit,
      filters: row.weightedRequestPayload.filters
    });

  if (payloadShapeDiff) return "payload_field_mismatch";

  const nObserved = row.normalizedEndpointInputsObserved;
  const wObserved = row.weightedEndpointInputsObserved;
  if (nObserved.query && !wObserved.query) return "endpoint_ignores_weighted_form";
  if (wObserved.query && row.weightedRequestPayload.query && wObserved.query !== row.weightedRequestPayload.query) {
    if (wObserved.query === row.normalizedRequestPayload.query) return "endpoint_overwrites_query_variant";
    return "benchmark_runner_contract_bug";
  }
  if (row.normalizedResponseShape.parseOk !== row.weightedResponseShape.parseOk) return "benchmark_runner_contract_bug";
  if (
    row.normalizedResponseShape.status !== row.weightedResponseShape.status ||
    stableStringify(row.normalizedResponseShape.topLevelKeys) !== stableStringify(row.weightedResponseShape.topLevelKeys)
  ) {
    return "mixed_contract_bug";
  }
  return "benchmark_runner_contract_bug";
}

function diffFields(row) {
  const diffs = [];
  const nPayload = row.normalizedRequestPayload;
  const wPayload = row.weightedRequestPayload;
  if (nPayload.queryType !== wPayload.queryType) diffs.push("queryType");
  if (nPayload.limit !== wPayload.limit) diffs.push("limit");
  if (stableStringify(nPayload.filters) !== stableStringify(wPayload.filters)) diffs.push("filters");
  if (nPayload.query !== wPayload.query) diffs.push("query");
  const nObserved = row.normalizedEndpointInputsObserved;
  const wObserved = row.weightedEndpointInputsObserved;
  if (nObserved.query !== wObserved.query) diffs.push("observed.query");
  if (nObserved.queryType !== wObserved.queryType) diffs.push("observed.queryType");
  if (stableStringify(nObserved.filters) !== stableStringify(wObserved.filters)) diffs.push("observed.filters");
  if (row.normalizedResponseShape.status !== row.weightedResponseShape.status) diffs.push("response.status");
  if (row.normalizedResponseShape.parseOk !== row.weightedResponseShape.parseOk) diffs.push("response.parseOk");
  if (stableStringify(row.normalizedResponseShape.topLevelKeys) !== stableStringify(row.weightedResponseShape.topLevelKeys)) {
    diffs.push("response.topLevelKeys");
  }
  if (
    stableStringify(row.normalizedResponseShape.firstResultKeys) !== stableStringify(row.weightedResponseShape.firstResultKeys)
  ) {
    diffs.push("response.firstResultKeys");
  }
  return unique(diffs);
}

function summarizeRecommendedFixScope(rows, rootCause) {
  if (rootCause === "payload_field_mismatch") return "normalize_payload_builder_between_normalized_and_weighted_flows";
  if (rootCause === "endpoint_ignores_weighted_form") return "ensure_weighted_query_is_bound_to_query_field_at_endpoint_boundary";
  if (rootCause === "endpoint_overwrites_query_variant") return "remove_query_variant_overwrite_in_runner_or_request_adapter";
  if (rootCause === "benchmark_runner_contract_bug") return "repair_benchmark_runner_request_response_contract_and_parsing";
  return "audit_request_adapter_and_response_parser_contract_together";
}

export async function buildR60_10_2EndpointContractReport({
  tasks,
  normalizedQueries,
  weightedQueries,
  apiBaseUrl
}) {
  const normalizedById = new Map((normalizedQueries || []).map((row) => [String(row?.queryId || ""), row]));
  const weightedById = new Map((weightedQueries || []).map((row) => [String(row?.queryId || ""), row]));

  const rows = [];
  for (const task of (tasks || []).slice().sort((a, b) => String(a?.queryId || "").localeCompare(String(b?.queryId || "")))) {
    const queryId = String(task?.queryId || "");
    const intent = String(task?.intent || "");
    const normalizedQuery = String(normalizedById.get(queryId)?.normalizedQuery || task?.adoptedQuery || task?.originalQuery || "");
    const weightedQuery = String(weightedById.get(queryId)?.weightedQuery || "");
    const normalizedRequestPayload = buildPayload(normalizedQuery, intent);
    const weightedRequestPayload = buildPayload(weightedQuery, intent);
    const normalizedRuntime = await fetchDebugContract(apiBaseUrl, normalizedRequestPayload);
    const weightedRuntime = await fetchDebugContract(apiBaseUrl, weightedRequestPayload);

    const row = {
      queryId,
      intent,
      normalizedQuery,
      weightedQuery,
      normalizedRequestPayload,
      weightedRequestPayload,
      normalizedEndpointInputsObserved: normalizedRuntime.observedInputs,
      weightedEndpointInputsObserved: weightedRuntime.observedInputs,
      normalizedResponseShape: normalizedRuntime.responseShape,
      weightedResponseShape: weightedRuntime.responseShape
    };
    row.likelyContractMismatch = classifyMismatch(row);
    row._diffFields = diffFields(row);
    rows.push(row);
  }

  const tasksWithPayloadDifferences = rows.filter((row) => row._diffFields.some((f) => f.startsWith("query") || f === "filters" || f === "limit")).map((row) => row.queryId);
  const tasksWithEndpointInputDifferences = rows
    .filter((row) => {
      if (row._diffFields.some((f) => f.startsWith("observed."))) return true;
      const nObserved = row.normalizedEndpointInputsObserved || {};
      const wObserved = row.weightedEndpointInputsObserved || {};
      return (
        String(nObserved.query || "") !== String(row.normalizedRequestPayload?.query || "") ||
        String(wObserved.query || "") !== String(row.weightedRequestPayload?.query || "") ||
        String(nObserved.queryType || "") !== String(row.normalizedRequestPayload?.queryType || "") ||
        String(wObserved.queryType || "") !== String(row.weightedRequestPayload?.queryType || "") ||
        stableStringify(nObserved.filters || {}) !== stableStringify(row.normalizedRequestPayload?.filters || {}) ||
        stableStringify(wObserved.filters || {}) !== stableStringify(row.weightedRequestPayload?.filters || {})
      );
    })
    .map((row) => row.queryId);
  const tasksWithResponseShapeDifferences = rows.filter((row) => row._diffFields.some((f) => f.startsWith("response."))).map((row) => row.queryId);
  const dominantContractMismatchPatterns = Object.entries(countBy(rows.map((row) => row.likelyContractMismatch)))
    .map(([pattern, count]) => ({ pattern, count }))
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.pattern.localeCompare(b.pattern);
    });
  const rootCauseClassification = dominantContractMismatchPatterns[0]?.pattern || "mixed_contract_bug";
  const likelyBrokenFields = Object.entries(countBy(rows.flatMap((row) => row._diffFields)))
    .map(([field, count]) => ({ field, count }))
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.field.localeCompare(b.field);
    });

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.2",
    tasksEvaluated: rows.length,
    tasksWithPayloadDifferences: unique(tasksWithPayloadDifferences),
    tasksWithEndpointInputDifferences: unique(tasksWithEndpointInputDifferences),
    tasksWithResponseShapeDifferences: unique(tasksWithResponseShapeDifferences),
    dominantContractMismatchPatterns,
    likelyBrokenFields,
    rootCauseClassification,
    recommendedFixScope: summarizeRecommendedFixScope(rows, rootCauseClassification),
    recommendedNextStep:
      rootCauseClassification === "benchmark_runner_contract_bug" || rootCauseClassification === "mixed_contract_bug"
        ? "fix_runner_contract_and_rerun_r60_10_and_r60_10_1"
        : "fix_endpoint_variant_binding_then_rerun_contract_audit",
    taskRows: rows.map((row) => {
      const { _diffFields, ...clean } = row;
      return clean;
    })
  };
  return report;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.2 Query Payload and Endpoint Contract Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- tasksEvaluated: ${report.tasksEvaluated}`);
  lines.push(`- tasksWithPayloadDifferences: ${report.tasksWithPayloadDifferences.length}`);
  lines.push(`- tasksWithEndpointInputDifferences: ${report.tasksWithEndpointInputDifferences.length}`);
  lines.push(`- tasksWithResponseShapeDifferences: ${report.tasksWithResponseShapeDifferences.length}`);
  lines.push(`- rootCauseClassification: ${report.rootCauseClassification}`);
  lines.push(`- recommendedFixScope: ${report.recommendedFixScope}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Dominant Contract Mismatch Patterns");
  for (const row of report.dominantContractMismatchPatterns || []) lines.push(`- ${row.pattern}: ${row.count}`);
  if (!(report.dominantContractMismatchPatterns || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Likely Broken Fields");
  for (const row of report.likelyBrokenFields || []) lines.push(`- ${row.field}: ${row.count}`);
  if (!(report.likelyBrokenFields || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [tasks, normalizedQueries, weightedQueries] = await Promise.all([
    readJson(path.resolve(reportsDir, tasksName)),
    readJson(path.resolve(reportsDir, normalizedName)),
    readJson(path.resolve(reportsDir, weightedName))
  ]);

  const report = await buildR60_10_2EndpointContractReport({
    tasks,
    normalizedQueries,
    weightedQueries,
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
        tasksWithPayloadDifferences: report.tasksWithPayloadDifferences.length,
        tasksWithEndpointInputDifferences: report.tasksWithEndpointInputDifferences.length,
        tasksWithResponseShapeDifferences: report.tasksWithResponseShapeDifferences.length,
        rootCauseClassification: report.rootCauseClassification,
        recommendedFixScope: report.recommendedFixScope,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.10.2 report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
