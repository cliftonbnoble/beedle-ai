import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { runRuntimePreflight } from "./retrieval-runtime-preflight-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const outputReportName =
  process.env.RETRIEVAL_R60_10_5_REPORT_NAME || "retrieval-r60_10_5-runtime-startup-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_5_MARKDOWN_NAME || "retrieval-r60_10_5-runtime-startup-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

const STARTUP_COMMAND = "pnpm dev";
const HEALTH_ENDPOINT = "/health";
const BENCHMARK_ENDPOINT = "/admin/retrieval/debug";
const MINIMAL_KNOWN_GOOD_QUERY = {
  query: "analysis standard",
  queryType: "keyword",
  limit: 5,
  filters: {
    approvedOnly: true,
    fileType: "decision_docx"
  }
};

export async function buildR60_10_5RuntimeStartupReport({ apiBaseUrl, fetchImpl = fetch }) {
  const preflight = await runRuntimePreflight({
    apiBaseUrl,
    fetchImpl,
    minimalQueryPayload: MINIMAL_KNOWN_GOOD_QUERY
  });

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.5",
    selectedBaseUrl: preflight.selectedBaseUrl,
    startupCommand: STARTUP_COMMAND,
    healthEndpoint: HEALTH_ENDPOINT,
    benchmarkEndpoint: BENCHMARK_ENDPOINT,
    minimalKnownGoodQuery: MINIMAL_KNOWN_GOOD_QUERY,
    healthPassed: Boolean(preflight.baseApiReachable && Number(preflight.healthEndpointStatus || 0) > 0),
    benchmarkEndpointPassed: Boolean(preflight.benchmarkEndpointReachable),
    smokeQueryPassed: Boolean(preflight.minimalKnownGoodQueryWorks),
    runtimeModeDetected: preflight.runtimeModeDetected,
    recommendedBenchmarkRunCommand: preflight.recommendedRunCommand,
    preflightPassed: Boolean(preflight.preflightPassed),
    probes: preflight.probes || []
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.5 Local Runtime Startup and Health Standardization (Dry Run)");
  lines.push("");
  lines.push("## Canonical Local Startup Path");
  lines.push(`- startupCommand: \`${report.startupCommand}\``);
  lines.push(`- selectedBaseUrl: ${report.selectedBaseUrl}`);
  lines.push(`- runtimeModeDetected: ${report.runtimeModeDetected}`);
  lines.push("");
  lines.push("## Endpoints");
  lines.push(`- healthEndpoint: ${report.healthEndpoint}`);
  lines.push(`- benchmarkEndpoint: ${report.benchmarkEndpoint}`);
  lines.push("");
  lines.push("## Smoke Query");
  lines.push(`- minimalKnownGoodQuery: \`${JSON.stringify(report.minimalKnownGoodQuery)}\``);
  lines.push("");
  lines.push("## Preflight Results");
  lines.push(`- healthPassed: ${report.healthPassed}`);
  lines.push(`- benchmarkEndpointPassed: ${report.benchmarkEndpointPassed}`);
  lines.push(`- smokeQueryPassed: ${report.smokeQueryPassed}`);
  lines.push(`- preflightPassed: ${report.preflightPassed}`);
  lines.push(`- recommendedBenchmarkRunCommand: ${report.recommendedBenchmarkRunCommand}`);
  lines.push("");
  lines.push("## Probe Rows");
  for (const row of report.probes || []) {
    lines.push(
      `- ${row.probeName}: status=${row.httpStatus}, fetchSucceeded=${row.fetchSucceeded}, parseSucceeded=${row.parseSucceeded}, hasResultsArray=${row.responseShape?.hasResultsArray}`
    );
  }
  lines.push("");
  lines.push("- Dry-run only. No retrieval/ranking/trust/corpus mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const report = await buildR60_10_5RuntimeStartupReport({ apiBaseUrl: apiBase });

  const jsonPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        selectedBaseUrl: report.selectedBaseUrl,
        startupCommand: report.startupCommand,
        healthEndpoint: report.healthEndpoint,
        benchmarkEndpoint: report.benchmarkEndpoint,
        minimalKnownGoodQuery: report.minimalKnownGoodQuery,
        healthPassed: report.healthPassed,
        benchmarkEndpointPassed: report.benchmarkEndpointPassed,
        smokeQueryPassed: report.smokeQueryPassed,
        recommendedBenchmarkRunCommand: report.recommendedBenchmarkRunCommand
      },
      null,
      2
    )
  );
  console.log(`R60.10.5 report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
