import fs from "node:fs/promises";
import path from "node:path";
import {
  buildRetrievalRankingRegressionReport,
  formatRetrievalRankingRegressionMarkdown
} from "./retrieval-ranking-regression-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.RETRIEVAL_RANKING_REGRESSION_REPORT_NAME || "retrieval-ranking-regression-report.json";
const markdownName = process.env.RETRIEVAL_RANKING_REGRESSION_MARKDOWN_NAME || "retrieval-ranking-regression-report.md";

async function readJson(fileName) {
  const fullPath = path.resolve(reportsDir, fileName);
  const raw = await fs.readFile(fullPath, "utf8");
  return JSON.parse(raw);
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [expansionReport, batchActivationReport, retunedLiveQaReport, rollbackManifest] = await Promise.all([
    readJson("retrieval-batch-expansion-report.json"),
    readJson("retrieval-batch-activation-report.json"),
    readJson("retrieval-live-search-qa-retuned-report.json"),
    readJson("retrieval-batch-rollback-manifest.json")
  ]);

  const baselinePreBatchSummary = expansionReport?.beforeVsAfterQa?.baseline?.liveSummary || {};
  const preRetuneReport = {
    summary: batchActivationReport?.beforeVsAfterLiveQa?.after || {},
    resultQualityByQuery: batchActivationReport?.beforeLiveQa?.resultQualityByQuery || batchActivationReport?.liveQaBefore?.resultQualityByQuery || [],
    queryResults: batchActivationReport?.beforeLiveQa?.queryResults || batchActivationReport?.liveQaBefore?.queryResults || []
  };

  if (!preRetuneReport.resultQualityByQuery.length) {
    const batchLiveQa = await readJson("retrieval-batch-live-qa-report.json");
    preRetuneReport.resultQualityByQuery = batchLiveQa?.afterResultQualityByQuery || [];
    preRetuneReport.queryResults = batchLiveQa?.afterQueryResults || [];
    if (!preRetuneReport.summary || Object.keys(preRetuneReport.summary).length === 0) {
      preRetuneReport.summary = batchLiveQa?.summary?.after || {};
    }
  }

  const report = buildRetrievalRankingRegressionReport({
    baselinePreBatchSummary,
    preRetuneReport,
    postRetuneReport: retunedLiveQaReport,
    batchDocIds: batchActivationReport?.manifests?.batchDocIds || [],
    activatedBatchId: batchActivationReport?.summary?.activationBatchId || "",
    rollbackBatchId: rollbackManifest?.rollbackBatchIds?.[0] || ""
  });

  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatRetrievalRankingRegressionMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval ranking regression JSON report written to ${jsonPath}`);
  console.log(`Retrieval ranking regression Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
