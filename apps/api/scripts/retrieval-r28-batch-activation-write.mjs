import fs from "node:fs/promises";
import path from "node:path";
import {
  buildBatchActivationArtifacts,
  buildBatchLiveQaMarkdown,
  compareLiveQa,
  runLiveQa,
  validateBatchActivationOutcome
} from "./retrieval-batch-activation-utils.mjs";
import {
  computeCitationTopDocumentShareAverage,
  computeLowSignalStructuralShare,
  resolveCitationTopDocumentShareCeiling
} from "./retrieval-safe-batch-activation-utils.mjs";
import { loadTrustedActivatedDocumentIds } from "./retrieval-live-search-qa-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const manifestInputName = process.env.RETRIEVAL_R28_INPUT_MANIFEST || "retrieval-r27-next-manifest.json";
const activationReportName = process.env.RETRIEVAL_R28_ACTIVATION_REPORT_NAME || "retrieval-r28-batch-activation-report.json";
const activationMarkdownName = process.env.RETRIEVAL_R28_ACTIVATION_MARKDOWN_NAME || "retrieval-r28-batch-activation-report.md";
const liveQaReportName = process.env.RETRIEVAL_R28_LIVE_QA_REPORT_NAME || "retrieval-r28-batch-live-qa-report.json";
const liveQaMarkdownName = process.env.RETRIEVAL_R28_LIVE_QA_MARKDOWN_NAME || "retrieval-r28-batch-live-qa-report.md";
const rollbackManifestName = process.env.RETRIEVAL_R28_ROLLBACK_MANIFEST_NAME || "retrieval-r28-batch-rollback-manifest.json";

const R28_EXPECTED_DOC_IDS = [
  "doc_1e1f711f-a1f6-40ba-b3d7-d0541171fd69",
  "doc_345fd497-a82c-40ca-a45d-c0aca1b17826"
];
const CITATION_TOP_DOC_SHARE_CEILING = Number(process.env.RETRIEVAL_R28_MAX_CITATION_TOP_DOC_SHARE || "0.1");

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function sameSet(a, b) {
  if (a.length !== b.length) return false;
  const sa = new Set(a);
  for (const x of b) if (!sa.has(x)) return false;
  return true;
}

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

function evaluateHardGate({ baselineSummary, baselineQueryResults, afterSummary, afterQueryResults }) {
  const baselineAvg = Number(baselineSummary?.averageQualityScore || 0);
  const minAllowedQuality = Number((baselineAvg - 0.5).toFixed(2));
  const baselineLowSignalShare = computeLowSignalStructuralShare(baselineQueryResults || []);
  const afterLowSignalShare = computeLowSignalStructuralShare(afterQueryResults || []);
  const afterCitationTopDocumentShare = computeCitationTopDocumentShareAverage(afterQueryResults || []);
  const citationThreshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: baselineQueryResults || [],
    configuredGlobalCeiling: CITATION_TOP_DOC_SHARE_CEILING,
    k: 10
  });

  const checks = {
    qualityAtOrAboveFloor: Number(afterSummary?.averageQualityScore || 0) >= minAllowedQuality,
    citationTopDocumentShareAtOrBelowCeiling: Number(afterCitationTopDocumentShare || 0) <= Number(citationThreshold.effectiveCeiling || 0),
    lowSignalStructuralShareAtOrBelowBaseline: Number(afterLowSignalShare || 0) <= Number(baselineLowSignalShare || 0),
    outOfCorpusHitQueryCountZero: Number(afterSummary?.outOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(afterSummary?.zeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(afterSummary?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(afterSummary?.citationAnchorCoverageAverage || 0) === 1
  };

  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([name]) => name);

  return {
    passed: failures.length === 0,
    checks,
    failures,
    thresholds: {
      minAllowedQuality,
      citationTopDocumentShareCeiling: citationThreshold.effectiveCeiling,
      citationTopDocumentShareCeilingConfigured: citationThreshold.configuredGlobalCeiling,
      citationTopDocumentShareCeilingAttainableFloor: citationThreshold.attainableFloorGivenUniqueDocsAtK,
      lowSignalStructuralShareCeiling: baselineLowSignalShare
    },
    measured: {
      baselineAverageQualityScore: baselineAvg,
      afterAverageQualityScore: Number(afterSummary?.averageQualityScore || 0),
      baselineLowSignalStructuralShare: Number(baselineLowSignalShare || 0),
      afterLowSignalStructuralShare: Number(afterLowSignalShare || 0),
      afterCitationTopDocumentShare: Number(afterCitationTopDocumentShare || 0),
      outOfCorpusHitQueryCount: Number(afterSummary?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(afterSummary?.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(afterSummary?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(afterSummary?.citationAnchorCoverageAverage || 0)
    }
  };
}

function buildActivationMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R28 Batch Activation Report");
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
  lines.push("## Decision");
  lines.push(`- keep_or_rollback: ${report.summary?.keepOrRollbackDecision || "rollback_batch"}`);
  lines.push(`- rollbackTriggered: ${report.rollback?.triggered || false}`);
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const manifest = await readJson(path.resolve(reportsDir, manifestInputName));
  const manifestDocIds = uniqueSorted((manifest?.safestNextDocIds || manifest?.nextBatchDocIds || []).map(String));
  const manifestBaselineTrustedDocIds = uniqueSorted((manifest?.baselineTrustedDocIds || []).map(String));
  const expectedDocIds = uniqueSorted(R28_EXPECTED_DOC_IDS.map(String));
  if (!sameSet(manifestDocIds, expectedDocIds)) {
    throw new Error(`R28 manifest docs mismatch. Expected ${expectedDocIds.join(", ")}, got ${manifestDocIds.join(", ")}`);
  }

  const trusted = await loadTrustedActivatedDocumentIds({
    reportsDir,
    reportName: "retrieval-activation-write-report.json",
    manifestName: "retrieval-trusted-activation-manifest.json"
  });
  const trustedBefore = manifestBaselineTrustedDocIds.length
    ? manifestBaselineTrustedDocIds
    : uniqueSorted((trusted?.trustedDocumentIds || []).map(String));
  if (!trustedBefore.length) {
    throw new Error("No trusted baseline docs found for R28.");
  }

  const previews = await loadPreviews(await resolveDocs());
  const artifacts = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds: manifestDocIds,
    existingTrustedDocIds: trustedBefore,
    activationManifestSource: manifestInputName
  });
  if (artifacts.docsMissingPreview.length) {
    throw new Error(`Missing retrieval previews for R28 docs: ${artifacts.docsMissingPreview.join(", ")}`);
  }

  const fetchSearchDebug = (payload) =>
    fetchJson(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

  const beforeLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: trustedBefore,
    fetchSearchDebug,
    limit: 20
  });

  const writeReport = await fetchJson(`${apiBase}/admin/retrieval/activation/write`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(artifacts.payload)
  });

  const afterTrustedDocIds = uniqueSorted([
    ...trustedBefore,
    ...(writeReport.documentsActivated || []).map((row) => row.documentId)
  ]);
  const afterLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: afterTrustedDocIds,
    fetchSearchDebug,
    limit: 20
  });

  const hardGate = evaluateHardGate({
    baselineSummary: beforeLiveQa.summary,
    baselineQueryResults: beforeLiveQa.queryResults,
    afterSummary: afterLiveQa.summary,
    afterQueryResults: afterLiveQa.queryResults
  });

  const validation = validateBatchActivationOutcome({
    batchDocIds: manifestDocIds,
    activationWriteReport: writeReport,
    corpusAdmissionById: new Map(),
    trustedBeforeIds: trustedBefore,
    trustedAfterIds: afterTrustedDocIds,
    beforeLiveQa,
    afterLiveQa
  });

  let finalDecision = "keep_batch_active";
  const anomalyFlags = [];
  if (!validation.checks.onlyManifestDocsActivated) anomalyFlags.push("non_manifest_doc_activated");
  if (Number(writeReport.summary?.heldDocsWrittenCount || 0) !== 0) anomalyFlags.push("held_docs_written");
  if (Number(writeReport.summary?.excludedDocsWrittenCount || 0) !== 0) anomalyFlags.push("excluded_docs_written");
  if (Number(writeReport.summary?.fixtureDocsWrittenCount || 0) !== 0) anomalyFlags.push("fixture_docs_written");
  if (Number(writeReport.summary?.provenanceFailuresCount || 0) !== 0) anomalyFlags.push("provenance_failures");
  if (!hardGate.passed) anomalyFlags.push("hard_gate_failed");

  let rollback = { triggered: false, summary: null };
  if (anomalyFlags.length) {
    finalDecision = "rollback_batch";
    const rollbackResponse = await fetchJson(`${apiBase}/admin/retrieval/activation/rollback`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        rollbackBatchId: artifacts.payload.rollbackManifest.rollbackBatchIds?.[0] || "",
        rollbackManifest: artifacts.payload.rollbackManifest
      })
    });
    rollback = {
      triggered: true,
      response: rollbackResponse,
      summary: rollbackResponse?.summary || {}
    };
    if (!rollback.summary?.rollbackVerificationPassed) anomalyFlags.push("rollback_verification_failed");
  }

  const activationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
      requestedBatchSize: manifestDocIds.length,
      activatedDocumentCount: Number(writeReport.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport.summary?.activatedChunkCount || 0),
      keepOrRollbackDecision: finalDecision
    },
    docsActivatedExact: manifestDocIds,
    writeValidation: {
      onlyManifestDocsActivated: validation.checks.onlyManifestDocsActivated,
      nonBatchActivatedDocIds: validation.nonBatchActivatedDocIds,
      heldDocsWrittenCount: Number(writeReport.summary?.heldDocsWrittenCount || 0),
      excludedDocsWrittenCount: Number(writeReport.summary?.excludedDocsWrittenCount || 0),
      fixtureDocsWrittenCount: Number(writeReport.summary?.fixtureDocsWrittenCount || 0),
      provenanceFailuresCount: Number(writeReport.summary?.provenanceFailuresCount || 0)
    },
    hardGate,
    beforeLiveSummary: beforeLiveQa.summary,
    afterLiveSummary: afterLiveQa.summary,
    beforeVsAfter: compareLiveQa(beforeLiveQa, afterLiveQa),
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
        docsActivatedExact: activationReport.docsActivatedExact,
        keepOrRollbackDecision: finalDecision,
        anomalyFlags
      },
      null,
      2
    )
  );
  console.log(`R28 activation report written to ${activationReportPath}`);
  console.log(`R28 live QA report written to ${liveQaPath}`);
  console.log(`R28 rollback manifest written to ${rollbackPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
