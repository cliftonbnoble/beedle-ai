import fs from "node:fs/promises";
import path from "node:path";
import {
  LIVE_SEARCH_QA_QUERIES,
  buildRetrievalLiveSearchQaReport,
  formatRetrievalLiveSearchQaMarkdown,
  loadTrustedActivatedDocumentIds
} from "./retrieval-live-search-qa-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.RETRIEVAL_LIVE_SEARCH_QA_RETUNED_REPORT_NAME || "retrieval-live-search-qa-retuned-report.json";
const markdownName = process.env.RETRIEVAL_LIVE_SEARCH_QA_RETUNED_MARKDOWN_NAME || "retrieval-live-search-qa-retuned-report.md";
const queryLimit = Number.parseInt(process.env.RETRIEVAL_LIVE_SEARCH_QA_LIMIT || "20", 10);
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";

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

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const trusted = await loadTrustedActivatedDocumentIds({ reportsDir });
  if (!trusted.trustedDocumentIds.length) {
    throw new Error("No trusted activated document IDs found. Run retrieval activation write first.");
  }

  const report = await buildRetrievalLiveSearchQaReport({
    apiBase,
    trustedDocumentIds: trusted.trustedDocumentIds,
    queries: LIVE_SEARCH_QA_QUERIES,
    limit: queryLimit,
    realOnly,
    fetchSearchDebug: (payload) => fetchJson(`${apiBase}/admin/retrieval/debug`, payload)
  });

  report.trustedCorpus.source = trusted.sources;

  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(mdPath, formatRetrievalLiveSearchQaMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval live search QA retuned JSON report written to ${jsonPath}`);
  console.log(`Retrieval live search QA retuned Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
