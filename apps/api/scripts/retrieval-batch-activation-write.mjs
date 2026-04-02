import fs from "node:fs/promises";
import path from "node:path";
import {
  aggregateReasonCounts,
  buildBatchActivationArtifacts,
  buildBatchActivationMarkdown,
  buildBatchLiveQaMarkdown,
  buildCorpusAdmissionMap,
  compareLiveQa,
  runLiveQa,
  validateBatchActivationOutcome
} from "./retrieval-batch-activation-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const activationReportName = process.env.RETRIEVAL_BATCH_ACTIVATION_REPORT_NAME || "retrieval-batch-activation-report.json";
const activationMarkdownName = process.env.RETRIEVAL_BATCH_ACTIVATION_MARKDOWN_NAME || "retrieval-batch-activation-report.md";
const liveQaReportName = process.env.RETRIEVAL_BATCH_LIVE_QA_REPORT_NAME || "retrieval-batch-live-qa-report.json";
const liveQaMarkdownName = process.env.RETRIEVAL_BATCH_LIVE_QA_MARKDOWN_NAME || "retrieval-batch-live-qa-report.md";
const rollbackManifestName = process.env.RETRIEVAL_BATCH_ROLLBACK_MANIFEST_NAME || "retrieval-batch-rollback-manifest.json";

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

  const [nextBatchManifest, activationWriteReport, corpusAdmissionReport, expansionReport, priorBatchActivationReport] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-next-batch-manifest.json")),
    readJson(path.resolve(reportsDir, "retrieval-activation-write-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-corpus-admission-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-batch-expansion-report.json")).catch(() => null),
    readJson(path.resolve(reportsDir, activationReportName)).catch(() => null)
  ]);

  const nextBatchDocIds = (nextBatchManifest?.nextBatchDocIds || []).map(String);
  if (!nextBatchDocIds.length) {
    throw new Error("No nextBatchDocIds in retrieval-next-batch-manifest.json");
  }

  const existingTrustedDocIds = (activationWriteReport?.documentsActivated || []).map((row) => String(row.documentId)).filter(Boolean);
  if (!existingTrustedDocIds.length) {
    throw new Error("No existing trusted activated docs found in retrieval-activation-write-report.json");
  }

  const previews = await loadPreviews(await resolveDocs());

  const artifacts = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds,
    existingTrustedDocIds
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

  const replayingSameBatch =
    String(priorBatchActivationReport?.summary?.activationBatchId || "") === String(artifacts.activationBatchId || "") &&
    Number(priorBatchActivationReport?.summary?.activatedDocumentCount || 0) > 0;

  const trustedBeforeForQa = replayingSameBatch ? artifacts.trustedAfter : artifacts.trustedBefore;

  const beforeLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: trustedBeforeForQa,
    fetchSearchDebug,
    limit: 20
  });

  const writeReport = await postActivationWrite(artifacts.payload);

  const afterTrustedDocIds = Array.from(
    new Set([...(trustedBeforeForQa || []), ...(writeReport.documentsActivated || []).map((row) => row.documentId)])
  );

  const afterLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: afterTrustedDocIds,
    fetchSearchDebug,
    limit: 20
  });

  const corpusAdmissionById = buildCorpusAdmissionMap(corpusAdmissionReport);
  const validation = validateBatchActivationOutcome({
    batchDocIds: artifacts.nextBatchDocIds,
    activationWriteReport: writeReport,
    corpusAdmissionById,
    trustedBeforeIds: artifacts.trustedBefore,
    trustedAfterIds: afterTrustedDocIds,
    beforeLiveQa,
    afterLiveQa
  });

  const liveQaComparison = compareLiveQa(beforeLiveQa, afterLiveQa);
  const simulatedAfterAvg = Number(expansionReport?.beforeVsAfterQa?.expanded?.simulatedSummary?.averageQualityScore || 0);
  const actualAfterAvg = Number(liveQaComparison.after?.averageQualityScore || 0);
  const simulationDelta = Number((actualAfterAvg - simulatedAfterAvg).toFixed(4));
  const simulationMatch = simulatedAfterAvg > 0 ? Math.abs(simulationDelta) <= 3 : null;
  const simulationAnomalies = [];
  if (simulationMatch === false) {
    simulationAnomalies.push("average_quality_score_below_simulated_expectation");
  }

  const activatedRows = (writeReport.documentsActivated || []).map((row) => ({
    documentId: row.documentId,
    title: row.title,
    trustSource: row.trustSource,
    corpusAdmissionStatus: String(corpusAdmissionById.get(row.documentId)?.corpusAdmissionStatus || "unknown"),
    isLikelyFixture: Boolean(corpusAdmissionById.get(row.documentId)?.isLikelyFixture)
  }));

  const activationResult = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      requestedBatchSize: artifacts.nextBatchDocIds.length,
      activatedDocumentCount: Number(writeReport.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport.summary?.activatedChunkCount || 0),
      documentsRejectedCount: Number(writeReport.summary?.documentsRejectedCount || 0),
      chunksRejectedCount: Number(writeReport.summary?.chunksRejectedCount || 0),
      activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
      rollbackVerificationPassed: Boolean(writeReport.summary?.rollbackVerificationPassed),
      activationVerificationPassed: Boolean(writeReport.summary?.activationVerificationPassed),
      validationPassed: validation.passed,
      simulationMatch
    },
    activationWriteSummary: writeReport.summary,
    documentsActivated: activatedRows,
    chunksActivatedSample: (writeReport.chunksActivated || []).slice(0, 50),
    writePathValidationStatus: writeReport.writePathValidationStatus,
    rejectionReasonCounts: writeReport.rejectionReasonCounts,
    topRejectedDocumentReasons: aggregateReasonCounts((writeReport.documentsRejectedFromWrite || []).map((row) => ({ reasons: [row.reason] })), "reasons"),
    topRejectedChunkReasons: aggregateReasonCounts((writeReport.chunksRejectedFromWrite || []).map((row) => ({ reasons: [row.reason] })), "reasons"),
    beforeVsAfterLiveQa: liveQaComparison,
    simulationComparison: {
      simulatedAfterAverageQualityScore: simulatedAfterAvg,
      actualAfterAverageQualityScore: actualAfterAvg,
      deltaActualVsSimulated: simulationDelta,
      simulationMatch,
      anomalies: simulationAnomalies
    },
    validation,
    rollbackManifest: artifacts.payload.rollbackManifest,
    rollbackManifestFile: rollbackManifestName,
    manifests: {
      baselineTrustedDocIds: trustedBeforeForQa,
      batchDocIds: artifacts.nextBatchDocIds,
      batchChunkIds: artifacts.nextBatchChunkIds,
      trustedAfterDocIds: artifacts.trustedAfter
    }
  };

  const liveQaReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      before: beforeLiveQa.summary,
      after: afterLiveQa.summary,
      deltas: liveQaComparison.deltas
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
    fs.writeFile(activationReportPath, JSON.stringify(activationResult, null, 2)),
    fs.writeFile(activationMdPath, buildBatchActivationMarkdown(activationResult)),
    fs.writeFile(liveQaPath, JSON.stringify(liveQaReport, null, 2)),
    fs.writeFile(liveQaMdPath, buildBatchLiveQaMarkdown(afterLiveQa)),
    fs.writeFile(rollbackPath, JSON.stringify(artifacts.payload.rollbackManifest, null, 2))
  ]);

  console.log(JSON.stringify(activationResult.summary, null, 2));
  console.log(`Retrieval batch activation JSON report written to ${activationReportPath}`);
  console.log(`Retrieval batch activation Markdown report written to ${activationMdPath}`);
  console.log(`Retrieval batch live QA JSON report written to ${liveQaPath}`);
  console.log(`Retrieval batch live QA Markdown report written to ${liveQaMdPath}`);
  console.log(`Retrieval batch rollback manifest written to ${rollbackPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
