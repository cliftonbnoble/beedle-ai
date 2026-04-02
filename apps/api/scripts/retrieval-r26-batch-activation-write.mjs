import fs from "node:fs/promises";
import path from "node:path";
import {
  buildBatchActivationArtifacts,
  buildBatchLiveQaMarkdown,
  runLiveQa
} from "./retrieval-batch-activation-utils.mjs";
import {
  computeCitationTopDocumentShareAverage,
  computeLowSignalStructuralShare,
  resolveCitationTopDocumentShareCeiling
} from "./retrieval-safe-batch-activation-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const manifestName = process.env.RETRIEVAL_R26_INPUT_MANIFEST || "retrieval-next-safe-batch-r25-manifest.json";
const activationReportName = process.env.RETRIEVAL_R26_ACTIVATION_REPORT_NAME || "retrieval-r26-batch-activation-report.json";
const activationMarkdownName = process.env.RETRIEVAL_R26_ACTIVATION_MARKDOWN_NAME || "retrieval-r26-batch-activation-report.md";
const liveQaReportName = process.env.RETRIEVAL_R26_LIVE_QA_REPORT_NAME || "retrieval-r26-batch-live-qa-report.json";
const liveQaMarkdownName = process.env.RETRIEVAL_R26_LIVE_QA_MARKDOWN_NAME || "retrieval-r26-batch-live-qa-report.md";
const rollbackManifestName = process.env.RETRIEVAL_R26_ROLLBACK_MANIFEST_NAME || "retrieval-r26-batch-rollback-manifest.json";

const QUALITY_FLOOR = Number(process.env.RETRIEVAL_R26_MIN_QUALITY || "64.72");
const LOW_SIGNAL_CEILING = Number(process.env.RETRIEVAL_R26_MAX_LOW_SIGNAL_SHARE || "0.0167");
const CITATION_TOP_DOC_SHARE_CEILING = Number(process.env.RETRIEVAL_R26_MAX_CITATION_TOP_DOC_SHARE || "0.1");

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

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
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

function summarizeHardGate({ baselineQueryResults, afterQa, afterQueryResults }) {
  const afterSummary = afterQa?.summary || {};
  const citationShare = computeCitationTopDocumentShareAverage(afterQueryResults || []);
  const lowSignalShare = computeLowSignalStructuralShare(afterQueryResults || []);
  const citationThreshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: baselineQueryResults || [],
    configuredGlobalCeiling: CITATION_TOP_DOC_SHARE_CEILING,
    k: 10
  });
  const checks = {
    qualityAboveFloor: Number(afterSummary.averageQualityScore || 0) >= QUALITY_FLOOR,
    lowSignalStructuralShareAtOrBelowCeiling: Number(lowSignalShare || 0) <= LOW_SIGNAL_CEILING,
    citationTopDocumentConcentrationAtOrBelowCeiling: Number(citationShare || 0) <= Number(citationThreshold.effectiveCeiling || 0),
    outOfCorpusHitQueryCountZero: Number(afterSummary.outOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(afterSummary.zeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(afterSummary.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(afterSummary.citationAnchorCoverageAverage || 0) === 1
  };
  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);
  return {
    passed: failures.length === 0,
    checks,
    failures,
    thresholds: {
      qualityFloor: QUALITY_FLOOR,
      lowSignalShareCeiling: LOW_SIGNAL_CEILING,
      citationTopDocumentShareCeiling: citationThreshold.effectiveCeiling,
      citationTopDocumentShareCeilingConfigured: citationThreshold.configuredGlobalCeiling,
      citationTopDocumentShareCeilingAttainableFloor: citationThreshold.attainableFloorGivenUniqueDocsAtK
    },
    measured: {
      averageQualityScore: Number(afterSummary.averageQualityScore || 0),
      lowSignalStructuralShare: Number(lowSignalShare || 0),
      citationTopDocumentShare: Number(citationShare || 0),
      outOfCorpusHitQueryCount: Number(afterSummary.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(afterSummary.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(afterSummary.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(afterSummary.citationAnchorCoverageAverage || 0)
    }
  };
}

function buildActivationMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R26 Batch Activation Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Hard Gate");
  for (const [k, v] of Object.entries(report.hardGate?.checks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.hardGate?.failures || []).length) lines.push(`- failures: ${(report.hardGate.failures || []).join(", ")}`);
  lines.push("");
  lines.push("## Activated Docs");
  for (const docId of report.docsActivatedExact || []) lines.push(`- ${docId}`);
  lines.push("");
  lines.push("## Rollback");
  lines.push(`- rollbackTriggered: ${report.rollback?.triggered || false}`);
  if (report.rollback?.triggered) {
    lines.push(`- rollbackVerificationPassed: ${report.rollback?.summary?.rollbackVerificationPassed || false}`);
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [manifest, baselineQaReport] = await Promise.all([
    readJson(path.resolve(reportsDir, manifestName)),
    readJson(path.resolve(reportsDir, "retrieval-safe-batch-live-qa-report.json")).catch(() => null)
  ]);

  const nextBatchDocIds = uniqueSorted((manifest?.nextBatchDocIds || []).map(String));
  const baselineTrustedDocIds = uniqueSorted((manifest?.baselineTrustedDocIds || []).map(String));
  if (nextBatchDocIds.length !== 3) {
    throw new Error(`R26 requires exactly 3 approved docs; got ${nextBatchDocIds.length}.`);
  }
  if (!baselineTrustedDocIds.length) {
    throw new Error("No baselineTrustedDocIds in R25 manifest.");
  }

  const previews = await loadPreviews(await resolveDocs());
  const artifacts = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds,
    existingTrustedDocIds: baselineTrustedDocIds,
    activationManifestSource: manifestName
  });
  if (artifacts.docsMissingPreview.length) {
    throw new Error(`Missing retrieval previews for R26 docs: ${artifacts.docsMissingPreview.join(", ")}`);
  }

  const fetchSearchDebug = (payload) =>
    fetchJson(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

  const beforeLiveQa =
    baselineQaReport?.summary?.before && baselineQaReport?.beforeQueryResults
      ? {
          summary: baselineQaReport.summary.before,
          queryResults: baselineQaReport.beforeQueryResults
        }
      : await runLiveQa({
          apiBase,
          trustedDocumentIds: baselineTrustedDocIds,
          fetchSearchDebug,
          limit: 20
        });

  const writeReport = await fetchJson(`${apiBase}/admin/retrieval/activation/write`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(artifacts.payload)
  });

  const afterTrustedDocIds = uniqueSorted([
    ...baselineTrustedDocIds,
    ...(writeReport.documentsActivated || []).map((row) => row.documentId)
  ]);

  const afterLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: afterTrustedDocIds,
    fetchSearchDebug,
    limit: 20
  });

  const hardGate = summarizeHardGate({
    baselineQueryResults: beforeLiveQa.queryResults || [],
    afterQa: afterLiveQa,
    afterQueryResults: afterLiveQa.queryResults || []
  });

  let rollback = {
    triggered: false
  };
  let finalDecision = "keep_batch_active";
  const anomalyFlags = [];
  if (!hardGate.passed) {
    finalDecision = "rollback_batch";
    anomalyFlags.push("hard_gate_failed");
    rollback = {
      triggered: true,
      response: await fetchJson(`${apiBase}/admin/retrieval/activation/rollback`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          rollbackBatchId: artifacts.payload.rollbackManifest.rollbackBatchIds?.[0] || "",
          rollbackManifest: artifacts.payload.rollbackManifest
        })
      })
    };
    rollback.summary = rollback.response?.summary || {};
    if (!rollback.summary?.rollbackVerificationPassed) anomalyFlags.push("rollback_verification_failed");
  }

  const activationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
      requestedBatchSize: nextBatchDocIds.length,
      activatedDocumentCount: Number(writeReport.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport.summary?.activatedChunkCount || 0),
      keepOrRollbackDecision: finalDecision
    },
    docsActivatedExact: nextBatchDocIds,
    beforeLiveSummary: beforeLiveQa.summary,
    afterLiveSummary: afterLiveQa.summary,
    hardGate,
    writeValidation: {
      heldDocsWrittenCount: Number(writeReport.summary?.heldDocsWrittenCount || 0),
      excludedDocsWrittenCount: Number(writeReport.summary?.excludedDocsWrittenCount || 0),
      fixtureDocsWrittenCount: Number(writeReport.summary?.fixtureDocsWrittenCount || 0),
      provenanceFailuresCount: Number(writeReport.summary?.provenanceFailuresCount || 0)
    },
    anomalyFlags,
    rollback,
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
      hardGate,
      decision: finalDecision
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
    fs.writeFile(activationMdPath, buildActivationMarkdown(activationReport)),
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
        keepOrRollbackDecision: activationReport.summary.keepOrRollbackDecision,
        hardGatePassed: hardGate.passed,
        anomalyFlags
      },
      null,
      2
    )
  );
  console.log(`R26 activation report written to ${activationReportPath}`);
  console.log(`R26 live QA report written to ${liveQaPath}`);
  console.log(`R26 rollback manifest written to ${rollbackPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
