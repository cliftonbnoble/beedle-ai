import fs from "node:fs/promises";
import path from "node:path";
import {
  LIVE_SEARCH_QA_QUERIES,
  buildRetrievalLiveSearchQaReport,
  formatRetrievalLiveSearchQaMarkdown
} from "./retrieval-live-search-qa-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const reportName = process.env.RETRIEVAL_LIVE_QA_RESTORED_REPORT_NAME || "retrieval-live-search-qa-restored-report.json";
const markdownName = process.env.RETRIEVAL_LIVE_QA_RESTORED_MARKDOWN_NAME || "retrieval-live-search-qa-restored-report.md";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  const body = JSON.parse(raw);
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const activationWriteReport = await readJson(path.resolve(reportsDir, "retrieval-activation-write-report.json"));
  const trustedDocumentIds = (activationWriteReport.documentsActivated || [])
    .map((row) => String(row.documentId || ""))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  const fetchSearchDebug = (payload) =>
    fetchJson(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

  const report = await buildRetrievalLiveSearchQaReport({
    apiBase,
    trustedDocumentIds,
    queries: LIVE_SEARCH_QA_QUERIES,
    fetchSearchDebug,
    limit: 20,
    realOnly
  });

  const baseline = await readJson(path.resolve(reportsDir, "retrieval-live-search-qa-report.json")).catch(() => null);
  if (baseline?.summary?.averageQualityScore != null) {
    report.summary.baselineAverageQualityScore = Number(baseline.summary.averageQualityScore);
    report.summary.deltaVsBaseline = Number(
      (Number(report.summary.averageQualityScore || 0) - Number(baseline.summary.averageQualityScore || 0)).toFixed(2)
    );
  }

  const reportPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatRetrievalLiveSearchQaMarkdown(report));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval live search QA restored JSON report written to ${reportPath}`);
  console.log(`Retrieval live search QA restored Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});

