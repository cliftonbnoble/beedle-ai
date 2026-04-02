import fs from "node:fs/promises";
import path from "node:path";
import {
  LIVE_SEARCH_QA_QUERIES,
  buildRetrievalLiveSearchQaReport,
  formatRetrievalLiveSearchQaMarkdown
} from "./retrieval-live-search-qa-utils.mjs";
import {
  buildRollbackMarkdown,
  buildStructuralChunkGuardReport,
  buildStructuralGuardMarkdown
} from "./retrieval-batch-rollback-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";

const rollbackManifestName = process.env.RETRIEVAL_BATCH_ROLLBACK_MANIFEST_NAME || "retrieval-batch-rollback-manifest.json";
const rollbackReportName = process.env.RETRIEVAL_BATCH_ROLLBACK_REPORT_NAME || "retrieval-batch-rollback-report.json";
const rollbackMarkdownName = process.env.RETRIEVAL_BATCH_ROLLBACK_MARKDOWN_NAME || "retrieval-batch-rollback-report.md";
const postQaReportName = process.env.RETRIEVAL_POST_ROLLBACK_LIVE_QA_REPORT_NAME || "retrieval-live-search-qa-post-rollback-report.json";
const postQaMarkdownName =
  process.env.RETRIEVAL_POST_ROLLBACK_LIVE_QA_MARKDOWN_NAME || "retrieval-live-search-qa-post-rollback-report.md";
const structuralGuardReportName =
  process.env.RETRIEVAL_STRUCTURAL_CHUNK_GUARD_REPORT_NAME || "retrieval-structural-chunk-guard-report.json";
const structuralGuardMarkdownName =
  process.env.RETRIEVAL_STRUCTURAL_CHUNK_GUARD_MARKDOWN_NAME || "retrieval-structural-chunk-guard-report.md";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const rollbackManifestPath = path.resolve(reportsDir, rollbackManifestName);
  const rollbackManifest = await readJson(rollbackManifestPath);
  const batchActivationReport = await readJson(path.resolve(reportsDir, "retrieval-batch-activation-report.json"));
  const baselineQa = await readJson(path.resolve(reportsDir, "retrieval-live-search-qa-report.json"));
  const preRollbackQa = await readJson(path.resolve(reportsDir, "retrieval-live-search-qa-retuned-report.json"));
  const trustedBaselineIds = (await readJson(path.resolve(reportsDir, "retrieval-activation-write-report.json")))
    .documentsActivated.map((row) => String(row.documentId))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  const rollbackPayload = {
    rollbackBatchId: rollbackManifest.rollbackBatchIds?.[0] || "",
    rollbackManifest
  };

  const rollbackResponse = await fetchJson(`${apiBase}/admin/retrieval/activation/rollback`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(rollbackPayload)
  });

  const fetchSearchDebug = (payload) =>
    fetchJson(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

  const postRollbackQa = await buildRetrievalLiveSearchQaReport({
    apiBase,
    trustedDocumentIds: trustedBaselineIds,
    queries: LIVE_SEARCH_QA_QUERIES,
    fetchSearchDebug,
    limit: 20,
    realOnly
  });

  const rollbackReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      rollbackBatchId: rollbackResponse.summary?.rollbackBatchId || rollbackManifest.rollbackBatchIds?.[0] || "",
      attemptedDocumentCount: Number(rollbackResponse.summary?.attemptedDocumentCount || 0),
      attemptedChunkCount: Number(rollbackResponse.summary?.attemptedChunkCount || 0),
      removedDocumentCount: Number(rollbackResponse.summary?.removedDocumentCount || 0),
      removedChunkCount: Number(rollbackResponse.summary?.removedChunkCount || 0),
      rollbackVerificationPassed: Boolean(rollbackResponse.summary?.rollbackVerificationPassed)
    },
    verification: {
      removedExactlyRequestedDocs:
        Number(rollbackResponse.summary?.attemptedDocumentCount || 0) === Number(rollbackResponse.summary?.removedDocumentCount || 0),
      removedExactlyRequestedChunks:
        Number(rollbackResponse.summary?.attemptedChunkCount || 0) === Number(rollbackResponse.summary?.removedChunkCount || 0),
      docsMissingFromRollbackTargetCount: Number(rollbackResponse.summary?.docsMissingFromRollbackTargetCount || 0),
      chunksMissingFromRollbackTargetCount: Number(rollbackResponse.summary?.chunksMissingFromRollbackTargetCount || 0)
    },
    rollbackManifestSummary: rollbackResponse.rollbackManifestSummary || {},
    removalDetails: rollbackResponse.removalDetails || {},
    baselineReference: {
      preBatchAverageQualityScore: Number(baselineQa.summary?.averageQualityScore || 0),
      preRetuneAverageQualityScore: Number(preRollbackQa.summary?.averageQualityScore || 0),
      baselineTrustedDocumentCount: trustedBaselineIds.length,
      batchActivatedDocumentCount: Number(batchActivationReport.summary?.activatedDocumentCount || 0)
    }
  };

  const postRollbackQaReport = {
    ...postRollbackQa,
    generatedAt: new Date().toISOString(),
    summary: {
      ...postRollbackQa.summary,
      baselineAverageQualityScore: Number(baselineQa.summary?.averageQualityScore || 0),
      preRetuneAverageQualityScore: Number(preRollbackQa.summary?.averageQualityScore || 0),
      deltaVsBaseline: Number((Number(postRollbackQa.summary?.averageQualityScore || 0) - Number(baselineQa.summary?.averageQualityScore || 0)).toFixed(2)),
      deltaVsPreRetune: Number((Number(postRollbackQa.summary?.averageQualityScore || 0) - Number(preRollbackQa.summary?.averageQualityScore || 0)).toFixed(2))
    }
  };

  const structuralGuardReport = buildStructuralChunkGuardReport({
    preRollbackQa,
    postRollbackQa: postRollbackQaReport,
    rollbackReport
  });

  const rollbackReportPath = path.resolve(reportsDir, rollbackReportName);
  const rollbackMarkdownPath = path.resolve(reportsDir, rollbackMarkdownName);
  const postQaReportPath = path.resolve(reportsDir, postQaReportName);
  const postQaMarkdownPath = path.resolve(reportsDir, postQaMarkdownName);
  const structuralGuardPath = path.resolve(reportsDir, structuralGuardReportName);
  const structuralGuardMdPath = path.resolve(reportsDir, structuralGuardMarkdownName);

  await Promise.all([
    fs.writeFile(rollbackReportPath, JSON.stringify(rollbackReport, null, 2)),
    fs.writeFile(rollbackMarkdownPath, buildRollbackMarkdown(rollbackReport)),
    fs.writeFile(postQaReportPath, JSON.stringify(postRollbackQaReport, null, 2)),
    fs.writeFile(postQaMarkdownPath, formatRetrievalLiveSearchQaMarkdown(postRollbackQaReport)),
    fs.writeFile(structuralGuardPath, JSON.stringify(structuralGuardReport, null, 2)),
    fs.writeFile(structuralGuardMdPath, buildStructuralGuardMarkdown(structuralGuardReport))
  ]);

  console.log(
    JSON.stringify(
      {
        rollbackBatchId: rollbackReport.summary.rollbackBatchId,
        removedDocumentCount: rollbackReport.summary.removedDocumentCount,
        removedChunkCount: rollbackReport.summary.removedChunkCount,
        rollbackVerificationPassed: rollbackReport.summary.rollbackVerificationPassed,
        postRollbackAverageQualityScore: postRollbackQaReport.summary.averageQualityScore,
        baselineAverageQualityScore: postRollbackQaReport.summary.baselineAverageQualityScore,
        outOfCorpusHitQueryCount: postRollbackQaReport.summary.outOfCorpusHitQueryCount,
        zeroTrustedResultQueryCount: postRollbackQaReport.summary.zeroTrustedResultQueryCount
      },
      null,
      2
    )
  );
  console.log(`Retrieval batch rollback report written to ${rollbackReportPath}`);
  console.log(`Retrieval post-rollback live QA report written to ${postQaReportPath}`);
  console.log(`Retrieval structural chunk guard report written to ${structuralGuardPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});

