import fs from "node:fs/promises";
import path from "node:path";
import {
  RETRIEVAL_HEALTH_QUERIES,
  compareHealthSummaries,
  formatRetrievalHealthMarkdown,
  summarizeHealthQuery,
  summarizeRetrievalHealth
} from "./retrieval-health-report-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const corpusMode = process.env.RETRIEVAL_HEALTH_CORPUS_MODE || "trusted_plus_provisional";
const limit = Number.parseInt(process.env.RETRIEVAL_HEALTH_LIMIT || "8", 10);
const reportName = process.env.RETRIEVAL_HEALTH_REPORT_NAME || "retrieval-health-report.json";
const markdownName = process.env.RETRIEVAL_HEALTH_MARKDOWN_NAME || "retrieval-health-report.md";
const baselineReportName = process.env.RETRIEVAL_HEALTH_BASELINE_REPORT || "";

async function fetchJson(url, payload) {
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
    throw new Error(`Expected JSON response from ${url}; received non-JSON.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const queryRows = [];
  for (const queryConfig of RETRIEVAL_HEALTH_QUERIES) {
    const response = await fetchJson(`${apiBase}/admin/retrieval/debug`, {
      query: queryConfig.query,
      queryType: "keyword",
      limit,
      corpusMode,
      filters: { approvedOnly: false }
    });
    queryRows.push(summarizeHealthQuery(queryConfig, response));
  }

  const summary = summarizeRetrievalHealth(queryRows);

  let deltaVsBaseline = null;
  if (baselineReportName) {
    const baselinePath = path.resolve(reportsDir, baselineReportName);
    const baseline = await readJson(baselinePath).catch(() => null);
    deltaVsBaseline = baseline?.summary ? compareHealthSummaries(summary, baseline.summary) : null;
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    limit,
    queries: queryRows,
    summary,
    deltaVsBaseline
  };

  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatRetrievalHealthMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval health JSON report written to ${jsonPath}`);
  console.log(`Retrieval health Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
