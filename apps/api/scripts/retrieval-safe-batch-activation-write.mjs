import fs from "node:fs/promises";
import path from "node:path";
import {
  buildBatchActivationArtifacts,
  buildBatchLiveQaMarkdown,
  compareLiveQa,
  runLiveQa
} from "./retrieval-batch-activation-utils.mjs";
import { buildSafeBatchActivationMarkdown, evaluateSafeBatchHardGate } from "./retrieval-safe-batch-activation-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const manifestName = process.env.RETRIEVAL_NEXT_SAFE_BATCH_MANIFEST_NAME || "retrieval-next-safe-batch-manifest.json";
const activationReportName = process.env.RETRIEVAL_SAFE_BATCH_ACTIVATION_REPORT_NAME || "retrieval-safe-batch-activation-report.json";
const activationMarkdownName = process.env.RETRIEVAL_SAFE_BATCH_ACTIVATION_MARKDOWN_NAME || "retrieval-safe-batch-activation-report.md";
const liveQaReportName = process.env.RETRIEVAL_SAFE_BATCH_LIVE_QA_REPORT_NAME || "retrieval-safe-batch-live-qa-report.json";
const liveQaMarkdownName = process.env.RETRIEVAL_SAFE_BATCH_LIVE_QA_MARKDOWN_NAME || "retrieval-safe-batch-live-qa-report.md";
const rollbackManifestName = process.env.RETRIEVAL_SAFE_BATCH_ROLLBACK_MANIFEST_NAME || "retrieval-safe-batch-rollback-manifest.json";

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
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

async function resolveDocs() {
  const url = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${docLimit}`;
  const payload = await fetchJson(url);
  return (payload.documents || []).map((row) => ({ id: row.id, isLikelyFixture: Boolean(row.isLikelyFixture) })).filter((row) => row.id);
}

async function loadPreviews(docRows) {
  const previews = [];
  for (const row of docRows || []) {
    const preview = await fetchJson(`${apiBase}/admin/retrieval/documents/${row.id}/chunks?includeText=1`);
    previews.push({ ...preview, isLikelyFixture: row.isLikelyFixture });
  }
  return previews;
}

async function postActivationWrite(payload) {
  return fetchJson(`${apiBase}/admin/retrieval/activation/write`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [manifest, activationWriteReport, baselineRestoredReport] = await Promise.all([
    readJson(path.resolve(reportsDir, manifestName)),
    readJson(path.resolve(reportsDir, "retrieval-activation-write-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-live-search-qa-restored-report.json")).catch(() => null)
  ]);

  const nextBatchDocIds = (manifest?.nextBatchDocIds || []).map(String);
  if (!nextBatchDocIds.length) throw new Error(`No nextBatchDocIds in ${manifestName}`);

  const baselineTrustedDocIds = (manifest?.baselineTrustedDocIds || []).map(String).filter(Boolean);
  const trustedBefore = baselineTrustedDocIds.length
    ? baselineTrustedDocIds
    : (activationWriteReport?.documentsActivated || []).map((row) => String(row.documentId)).filter(Boolean);
  if (!trustedBefore.length) throw new Error("No baseline trusted docs available.");

  const baselineTarget = Number(baselineRestoredReport?.summary?.currentPostRollbackAverageQualityScore || 65.31);
  const minAllowedQuality = Number(process.env.RETRIEVAL_SAFE_BATCH_MIN_QUALITY || (baselineTarget - 0.5).toFixed(2));

  const previews = await loadPreviews(await resolveDocs());
  const artifacts = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds,
    existingTrustedDocIds: trustedBefore,
    activationManifestSource: manifestName
  });
  if (artifacts.docsMissingPreview.length) {
    throw new Error(`Missing retrieval previews for batch docs: ${artifacts.docsMissingPreview.join(", ")}`);
  }

  const fetchSearchDebug = (payload) =>
    fetchJson(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

  const beforeLiveQa =
    baselineRestoredReport && Array.isArray(baselineRestoredReport.queryResults)
      ? baselineRestoredReport
      : await runLiveQa({
          apiBase,
          trustedDocumentIds: trustedBefore,
          fetchSearchDebug,
          limit: 20
        });

  const writeReport = await postActivationWrite(artifacts.payload);
  const afterTrustedDocIds = Array.from(
    new Set([...(trustedBefore || []), ...(writeReport.documentsActivated || []).map((row) => row.documentId)])
  );

  const afterLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: afterTrustedDocIds,
    fetchSearchDebug,
    limit: 20
  });

  const hardGate = evaluateSafeBatchHardGate({
    baselineAverageQualityScore: baselineTarget,
    afterSummary: afterLiveQa.summary,
    beforeQueryResults: beforeLiveQa.queryResults,
    afterQueryResults: afterLiveQa.queryResults,
    minAllowedQualityScore: minAllowedQuality
  });

  const keepOrRollbackRecommendation = hardGate.passed ? "keep_batch_active" : "rollback_batch";
  const anomalyFlags = [];
  if (!hardGate.passed) anomalyFlags.push("hard_gate_failed");
  if (hardGate.deltaCitationTopDocumentShare > 0) anomalyFlags.push("citation_concentration_worsened");
  if (hardGate.deltaLowSignalStructuralShare > 0) anomalyFlags.push("low_signal_structural_share_increased");

  const activationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      requestedBatchSize: artifacts.nextBatchDocIds.length,
      activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
      activatedDocumentCount: Number(writeReport.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport.summary?.activatedChunkCount || 0),
      baselineAverageQualityScore: baselineTarget,
      minAllowedQualityScore: minAllowedQuality,
      postActivationAverageQualityScore: Number(afterLiveQa.summary?.averageQualityScore || 0),
      keepOrRollbackRecommendation,
      hardGatePassed: hardGate.passed
    },
    docsActivatedExact: artifacts.nextBatchDocIds,
    documentsActivated: writeReport.documentsActivated || [],
    chunksActivatedSample: (writeReport.chunksActivated || []).slice(0, 50),
    beforeVsAfterLiveQa: compareLiveQa(beforeLiveQa, afterLiveQa),
    hardGate,
    anomalyFlags,
    writeValidation: {
      heldDocsWrittenCount: Number(writeReport.summary?.heldDocsWrittenCount || 0),
      excludedDocsWrittenCount: Number(writeReport.summary?.excludedDocsWrittenCount || 0),
      fixtureDocsWrittenCount: Number(writeReport.summary?.fixtureDocsWrittenCount || 0),
      provenanceFailuresCount: Number(writeReport.summary?.provenanceFailuresCount || 0)
    },
    rollbackManifest: artifacts.payload.rollbackManifest,
    rollbackManifestFile: rollbackManifestName
  };

  const liveQaReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      before: beforeLiveQa.summary,
      after: afterLiveQa.summary,
      deltas: compareLiveQa(beforeLiveQa, afterLiveQa).deltas
    },
    beforeQueryResults: beforeLiveQa.queryResults,
    afterQueryResults: afterLiveQa.queryResults
  };

  const activationReportPath = path.resolve(reportsDir, activationReportName);
  const activationMdPath = path.resolve(reportsDir, activationMarkdownName);
  const liveQaPath = path.resolve(reportsDir, liveQaReportName);
  const liveQaMdPath = path.resolve(reportsDir, liveQaMarkdownName);
  const rollbackPath = path.resolve(reportsDir, rollbackManifestName);

  await Promise.all([
    fs.writeFile(activationReportPath, JSON.stringify(activationReport, null, 2)),
    fs.writeFile(activationMdPath, buildSafeBatchActivationMarkdown(activationReport)),
    fs.writeFile(liveQaPath, JSON.stringify(liveQaReport, null, 2)),
    fs.writeFile(liveQaMdPath, buildBatchLiveQaMarkdown(afterLiveQa)),
    fs.writeFile(rollbackPath, JSON.stringify(artifacts.payload.rollbackManifest, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        activationBatchId: activationReport.summary.activationBatchId,
        activatedDocumentCount: activationReport.summary.activatedDocumentCount,
        activatedChunkCount: activationReport.summary.activatedChunkCount,
        postActivationAverageQualityScore: activationReport.summary.postActivationAverageQualityScore,
        baselineAverageQualityScore: activationReport.summary.baselineAverageQualityScore,
        minAllowedQualityScore: activationReport.summary.minAllowedQualityScore,
        keepOrRollbackRecommendation: activationReport.summary.keepOrRollbackRecommendation,
        anomalyFlags
      },
      null,
      2
    )
  );
  console.log(`Retrieval safe batch activation report written to ${activationReportPath}`);
  console.log(`Retrieval safe batch live QA report written to ${liveQaPath}`);
  console.log(`Retrieval safe batch rollback manifest written to ${rollbackPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
