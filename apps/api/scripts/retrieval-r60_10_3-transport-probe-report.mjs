import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const normalizedName = process.env.RETRIEVAL_R60_10_3_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const weightedName = process.env.RETRIEVAL_R60_10_3_WEIGHTED_NAME || "retrieval-r60_10-weighted-queries.json";
const outputReportName =
  process.env.RETRIEVAL_R60_10_3_REPORT_NAME || "retrieval-r60_10_3-transport-probe-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_3_MARKDOWN_NAME || "retrieval-r60_10_3-transport-probe-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

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

function safeShape(parsed) {
  const topLevelKeys = parsed && typeof parsed === "object" ? Object.keys(parsed).sort((a, b) => a.localeCompare(b)) : [];
  const hasResultsArray = Array.isArray(parsed?.results);
  const first = hasResultsArray && parsed.results.length ? parsed.results[0] : null;
  const firstResultKeys = first && typeof first === "object" ? Object.keys(first).sort((a, b) => a.localeCompare(b)) : [];
  return { topLevelKeys, hasResultsArray, firstResultKeys };
}

async function runGetProbe(fetchImpl, url) {
  try {
    const response = await fetchImpl(url);
    const text = await response.text();
    let parsed = null;
    let parseSucceeded = true;
    try {
      parsed = JSON.parse(text);
    } catch {
      parseSucceeded = false;
    }
    return {
      httpStatus: response.status,
      fetchSucceeded: true,
      parseSucceeded,
      responseShape: safeShape(parsed),
      errorTextSnippet: parseSucceeded ? "" : String(text || "").slice(0, 200)
    };
  } catch (error) {
    return {
      httpStatus: 0,
      fetchSucceeded: false,
      parseSucceeded: false,
      responseShape: safeShape(null),
      errorTextSnippet: String(error instanceof Error ? error.message : error).slice(0, 200)
    };
  }
}

async function runPostProbe(fetchImpl, url, payload) {
  try {
    const response = await fetchImpl(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const text = await response.text();
    let parsed = null;
    let parseSucceeded = true;
    try {
      parsed = JSON.parse(text);
    } catch {
      parseSucceeded = false;
    }
    return {
      httpStatus: response.status,
      fetchSucceeded: true,
      parseSucceeded,
      responseShape: safeShape(parsed),
      errorTextSnippet: parseSucceeded ? "" : String(text || "").slice(0, 200)
    };
  } catch (error) {
    return {
      httpStatus: 0,
      fetchSucceeded: false,
      parseSucceeded: false,
      responseShape: safeShape(null),
      errorTextSnippet: String(error instanceof Error ? error.message : error).slice(0, 200)
    };
  }
}

function pickProbeQueries(normalizedRows, weightedRows) {
  const normalized = (normalizedRows || []).find((row) => row?.normalizedQuery) || {};
  const weighted = (weightedRows || []).find((row) => row?.weightedQuery) || {};
  const normalizedQuery = String(normalized.normalizedQuery || "analysis standard");
  const weightedQuery = String(weighted.weightedQuery || normalizedQuery);
  const normalizedIntent = String(normalized.intent || "analysis_reasoning");
  const weightedIntent = String(weighted.intent || normalizedIntent || "analysis_reasoning");
  return {
    normalizedQuery,
    weightedQuery,
    normalizedType: INTENT_TO_QUERY_TYPE[normalizedIntent] || "keyword",
    weightedType: INTENT_TO_QUERY_TYPE[weightedIntent] || "keyword"
  };
}

function classifyRootCause(summary, probes) {
  const anyFetchFail = probes.some((probe) => !probe.fetchSucceeded);
  const anyStatusZero = probes.some((probe) => probe.httpStatus === 0);
  const anyParseFail = probes.some((probe) => !probe.parseSucceeded);
  const endpointWorks = summary.benchmarkEndpointReachable;
  const normWorks = summary.normalizedProbeWorks;
  const weightedWorks = summary.weightedProbeWorks;

  if (anyStatusZero && !summary.baseApiReachable) return "transport_or_runtime_unreachable";
  if (anyFetchFail && summary.baseApiReachable) return "intermittent_local_runtime_failure";
  if (!endpointWorks && summary.baseApiReachable) return "endpoint_request_shape_bug";
  if (endpointWorks && normWorks && !weightedWorks) return "benchmark_runner_contract_bug";
  if (!anyParseFail && endpointWorks) return "no_transport_issue_detected";
  return "intermittent_local_runtime_failure";
}

function recommendedStep(rootCause) {
  if (rootCause === "transport_or_runtime_unreachable") return "start_or_stabilize_local_runtime_then_rerun_r60_10_series";
  if (rootCause === "intermittent_local_runtime_failure") return "stabilize_local_runtime_transport_and_repeat_probe_before_contract_debug";
  if (rootCause === "endpoint_request_shape_bug") return "fix_endpoint_acceptance_for_debug_payload_and_rerun_probe";
  if (rootCause === "benchmark_runner_contract_bug") return "fix_query_variant_binding_in_benchmark_runner_then_rerun_r60_10_2";
  return "proceed_to_r60_10_2_contract_fix_with_transport_cleared";
}

export async function buildR60_10_3TransportProbeReport({
  normalizedRows,
  weightedRows,
  apiBaseUrl,
  fetchImpl = fetch
}) {
  const endpoint = `${apiBaseUrl}/admin/retrieval/debug`;
  const { normalizedQuery, weightedQuery, normalizedType, weightedType } = pickProbeQueries(normalizedRows, weightedRows);

  const minimalPayload = {
    query: "analysis standard",
    queryType: "keyword",
    limit: 5,
    filters: { approvedOnly: true, fileType: "decision_docx" }
  };
  const normalizedPayload = {
    query: normalizedQuery,
    queryType: normalizedType,
    limit: 5,
    filters: { approvedOnly: true, fileType: "decision_docx" }
  };
  const weightedPayload = {
    query: weightedQuery,
    queryType: weightedType,
    limit: 5,
    filters: { approvedOnly: true, fileType: "decision_docx" }
  };

  const baseProbe = await runGetProbe(fetchImpl, apiBaseUrl);
  const endpointProbe = await runPostProbe(fetchImpl, endpoint, { query: "", queryType: "keyword", limit: 1, filters: {} });
  const minimalProbe = await runPostProbe(fetchImpl, endpoint, minimalPayload);
  const normalizedProbe = await runPostProbe(fetchImpl, endpoint, normalizedPayload);
  const weightedProbe = await runPostProbe(fetchImpl, endpoint, weightedPayload);

  const probeRows = [
    { probeName: "base_api_reachability", requestPayload: null, ...baseProbe },
    { probeName: "benchmark_endpoint_reachability", requestPayload: { query: "", queryType: "keyword", limit: 1, filters: {} }, ...endpointProbe },
    { probeName: "minimal_known_good_query", requestPayload: minimalPayload, ...minimalProbe },
    { probeName: "normalized_query_probe", requestPayload: normalizedPayload, ...normalizedProbe },
    { probeName: "weighted_query_probe", requestPayload: weightedPayload, ...weightedProbe }
  ];

  const summary = {
    baseApiReachable: baseProbe.fetchSucceeded && baseProbe.httpStatus > 0,
    benchmarkEndpointReachable: endpointProbe.fetchSucceeded && endpointProbe.httpStatus > 0,
    minimalKnownGoodQueryWorks:
      minimalProbe.fetchSucceeded &&
      minimalProbe.httpStatus > 0 &&
      minimalProbe.parseSucceeded &&
      minimalProbe.responseShape.hasResultsArray,
    normalizedProbeWorks:
      normalizedProbe.fetchSucceeded &&
      normalizedProbe.httpStatus > 0 &&
      normalizedProbe.parseSucceeded &&
      normalizedProbe.responseShape.hasResultsArray,
    weightedProbeWorks:
      weightedProbe.fetchSucceeded &&
      weightedProbe.httpStatus > 0 &&
      weightedProbe.parseSucceeded &&
      weightedProbe.responseShape.hasResultsArray
  };

  const transportFailureDetected = probeRows.some((probe) => !probe.fetchSucceeded || probe.httpStatus === 0);
  const contractMismatchDetected =
    summary.baseApiReachable &&
    summary.benchmarkEndpointReachable &&
    summary.normalizedProbeWorks &&
    !summary.weightedProbeWorks;
  const likelyRootCause = classifyRootCause(
    {
      ...summary,
      transportFailureDetected,
      contractMismatchDetected
    },
    probeRows
  );

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.3",
    probeRows,
    baseApiReachable: summary.baseApiReachable,
    benchmarkEndpointReachable: summary.benchmarkEndpointReachable,
    minimalKnownGoodQueryWorks: summary.minimalKnownGoodQueryWorks,
    normalizedProbeWorks: summary.normalizedProbeWorks,
    weightedProbeWorks: summary.weightedProbeWorks,
    transportFailureDetected,
    contractMismatchDetected,
    likelyRootCause,
    recommendedNextStep: recommendedStep(likelyRootCause)
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.3 Transport Reliability and Local Runtime Contract Probe (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- baseApiReachable: ${report.baseApiReachable}`);
  lines.push(`- benchmarkEndpointReachable: ${report.benchmarkEndpointReachable}`);
  lines.push(`- minimalKnownGoodQueryWorks: ${report.minimalKnownGoodQueryWorks}`);
  lines.push(`- normalizedProbeWorks: ${report.normalizedProbeWorks}`);
  lines.push(`- weightedProbeWorks: ${report.weightedProbeWorks}`);
  lines.push(`- transportFailureDetected: ${report.transportFailureDetected}`);
  lines.push(`- contractMismatchDetected: ${report.contractMismatchDetected}`);
  lines.push(`- likelyRootCause: ${report.likelyRootCause}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Probes");
  for (const row of report.probeRows || []) {
    lines.push(
      `- ${row.probeName}: status=${row.httpStatus}, fetchSucceeded=${row.fetchSucceeded}, parseSucceeded=${row.parseSucceeded}, hasResultsArray=${row.responseShape?.hasResultsArray}`
    );
  }
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [normalizedRows, weightedRows] = await Promise.all([
    readJson(path.resolve(reportsDir, normalizedName)),
    readJson(path.resolve(reportsDir, weightedName))
  ]);
  const report = await buildR60_10_3TransportProbeReport({
    normalizedRows,
    weightedRows,
    apiBaseUrl: apiBase
  });

  const reportPath = path.resolve(reportsDir, outputReportName);
  const markdownPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(reportPath, JSON.stringify(report, null, 2)), fs.writeFile(markdownPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        baseApiReachable: report.baseApiReachable,
        benchmarkEndpointReachable: report.benchmarkEndpointReachable,
        minimalKnownGoodQueryWorks: report.minimalKnownGoodQueryWorks,
        normalizedProbeWorks: report.normalizedProbeWorks,
        weightedProbeWorks: report.weightedProbeWorks,
        transportFailureDetected: report.transportFailureDetected,
        contractMismatchDetected: report.contractMismatchDetected,
        likelyRootCause: report.likelyRootCause,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.10.3 report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
