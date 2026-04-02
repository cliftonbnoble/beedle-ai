import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { assertPreflightOrThrow, runRuntimePreflight, shouldFailFast } from "./retrieval-runtime-preflight-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const outputReportName =
  process.env.RETRIEVAL_R60_10_4_REPORT_NAME || "retrieval-r60_10_4-runtime-preflight-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_4_MARKDOWN_NAME || "retrieval-r60_10_4-runtime-preflight-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

export async function buildR60_10_4RuntimePreflightReport({ apiBaseUrl, fetchImpl = fetch }) {
  const preflight = await runRuntimePreflight({ apiBaseUrl, fetchImpl });
  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.4",
    ...preflight
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.4 Local Runtime Stabilization Preflight (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- baseApiReachable: ${report.baseApiReachable}`);
  lines.push(`- healthEndpointStatus: ${report.healthEndpointStatus}`);
  lines.push(`- benchmarkEndpointReachable: ${report.benchmarkEndpointReachable}`);
  lines.push(`- minimalKnownGoodQueryWorks: ${report.minimalKnownGoodQueryWorks}`);
  lines.push(`- selectedBaseUrl: ${report.selectedBaseUrl}`);
  lines.push(`- runtimeModeDetected: ${report.runtimeModeDetected}`);
  lines.push(`- preflightPassed: ${report.preflightPassed}`);
  lines.push(`- recommendedRunCommand: ${report.recommendedRunCommand}`);
  lines.push("");
  lines.push("## Probes");
  for (const row of report.probes || []) {
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
  const report = await buildR60_10_4RuntimePreflightReport({ apiBaseUrl: apiBase });

  const reportPath = path.resolve(reportsDir, outputReportName);
  const markdownPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(reportPath, JSON.stringify(report, null, 2)), fs.writeFile(markdownPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        baseApiReachable: report.baseApiReachable,
        healthEndpointStatus: report.healthEndpointStatus,
        benchmarkEndpointReachable: report.benchmarkEndpointReachable,
        minimalKnownGoodQueryWorks: report.minimalKnownGoodQueryWorks,
        selectedBaseUrl: report.selectedBaseUrl,
        runtimeModeDetected: report.runtimeModeDetected,
        recommendedRunCommand: report.recommendedRunCommand,
        preflightPassed: report.preflightPassed
      },
      null,
      2
    )
  );
  console.log(`R60.10.4 report written to ${reportPath}`);

  if (shouldFailFast()) {
    assertPreflightOrThrow(report, "retrieval-r60_10_4-runtime-preflight-report");
  }
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
