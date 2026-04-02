import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  buildBatchActivationArtifacts,
  buildBatchLiveQaMarkdown,
  compareLiveQa,
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

const activationReportName = process.env.RETRIEVAL_R44_ACTIVATION_REPORT_NAME || "retrieval-r44-single-activation-report.json";
const activationMarkdownName = process.env.RETRIEVAL_R44_ACTIVATION_MARKDOWN_NAME || "retrieval-r44-single-activation-report.md";
const liveQaReportName = process.env.RETRIEVAL_R44_LIVE_QA_REPORT_NAME || "retrieval-r44-single-live-qa-report.json";
const liveQaMarkdownName = process.env.RETRIEVAL_R44_LIVE_QA_MARKDOWN_NAME || "retrieval-r44-single-live-qa-report.md";
const rollbackManifestName = process.env.RETRIEVAL_R44_ROLLBACK_MANIFEST_NAME || "retrieval-r44-single-rollback-manifest.json";

const TARGET_DOC_ID = "doc_115e68c9-edcc-473c-bbfc-7d3b7538605a";
const CONFIGURED_CITATION_CEILING = Number(process.env.RETRIEVAL_R44_CONFIGURED_CITATION_CEILING || "0.1");
const QUALITY_REGRESSION_TOLERANCE = Number(process.env.RETRIEVAL_R44_MAX_QUALITY_REGRESSION || "0.5");
const LOW_SIGNAL_TOLERANCE = Number(process.env.RETRIEVAL_R44_MAX_LOW_SIGNAL_REGRESSION || "0");

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
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

async function loadPreviews(docRows, onlyDocIds = []) {
  const onlySet = new Set((onlyDocIds || []).map((id) => String(id)));
  const previews = [];
  for (const row of docRows || []) {
    if (onlySet.size && !onlySet.has(String(row.id || ""))) continue;
    const preview = await fetchJson(`${apiBase}/admin/retrieval/documents/${row.id}/chunks?includeText=1`);
    previews.push({ ...preview, isLikelyFixture: row.isLikelyFixture });
  }
  return previews;
}

export function evaluateR44HardGate({ beforeQa, afterQa }) {
  const beforeSummary = beforeQa?.summary || {};
  const afterSummary = afterQa?.summary || {};
  const beforeRows = beforeQa?.queryResults || [];
  const afterRows = afterQa?.queryResults || [];

  const baselineAverageQualityScore = Number(beforeSummary.averageQualityScore || 0);
  const qualityFloor = Number((baselineAverageQualityScore - QUALITY_REGRESSION_TOLERANCE).toFixed(2));
  const beforeLowSignalShare = computeLowSignalStructuralShare(beforeRows);
  const afterLowSignalShare = computeLowSignalStructuralShare(afterRows);
  const citationTopDocumentShare = computeCitationTopDocumentShareAverage(afterRows);
  const citationThreshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: beforeRows,
    configuredGlobalCeiling: CONFIGURED_CITATION_CEILING,
    k: 10
  });

  const checks = {
    qualityNotMateriallyRegressed: Number(afterSummary.averageQualityScore || 0) >= qualityFloor,
    citationTopDocumentShareAtOrBelowEffectiveCeiling:
      Number(citationTopDocumentShare || 0) <= Number(citationThreshold.effectiveCeiling || 0),
    lowSignalStructuralShareNotWorsened:
      Number(afterLowSignalShare || 0) <= Number(beforeLowSignalShare || 0) + Number(LOW_SIGNAL_TOLERANCE || 0),
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
      qualityFloor,
      configuredCitationTopDocumentShareCeiling: citationThreshold.configuredGlobalCeiling,
      attainableFloorGivenUniqueDocsAtK10: citationThreshold.attainableFloorGivenUniqueDocsAtK,
      effectiveCitationCeiling: citationThreshold.effectiveCeiling,
      lowSignalStructuralShareCeiling: Number(beforeLowSignalShare || 0),
      lowSignalTolerance: Number(LOW_SIGNAL_TOLERANCE || 0)
    },
    measured: {
      beforeAverageQualityScore: baselineAverageQualityScore,
      afterAverageQualityScore: Number(afterSummary.averageQualityScore || 0),
      citationTopDocumentShare: Number(citationTopDocumentShare || 0),
      beforeLowSignalStructuralShare: Number(beforeLowSignalShare || 0),
      afterLowSignalStructuralShare: Number(afterLowSignalShare || 0),
      outOfCorpusHitQueryCount: Number(afterSummary.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(afterSummary.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(afterSummary.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(afterSummary.citationAnchorCoverageAverage || 0)
    },
    effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0)
  };
}

function buildActivationMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R44 Single Activation Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Hard Gate");
  for (const [k, v] of Object.entries(report.hardGate?.checks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.hardGate?.failures || []).length) lines.push(`- failures: ${(report.hardGate.failures || []).join(", ")}`);
  lines.push("");
  lines.push("## Write Validation");
  for (const [k, v] of Object.entries(report.writeValidation || {})) lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  lines.push("");
  lines.push("## Decision");
  lines.push(`- keep_or_rollback: ${report.keepOrRollbackDecision || "rollback_batch"}`);
  lines.push(`- rollbackBatchId: ${report.rollbackBatchId || ""}`);
  lines.push(`- rollbackVerificationPassed: ${report.rollbackVerificationPassed}`);
  lines.push("");
  lines.push("- Real activation attempted for one manifest doc; auto-rollback on any hard-gate failure.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r43Report, r42Fix, r36Manifest] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-r43-policy-adoption-simulation-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-r42-reference-lineage-fix-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-r36-next-safe-single-manifest.json"))
  ]);

  if (!r43Report?.safeToProceed) {
    throw new Error("R44 aborted: R43 did not mark safeToProceed=true.");
  }
  const recommended = String(r43Report?.recommendedNextStep || "");
  if (recommended !== `safe_single_doc_activation_candidate:${TARGET_DOC_ID}`) {
    throw new Error(`R44 target mismatch. Expected ${TARGET_DOC_ID}, got ${recommended}`);
  }

  const baselineTrustedDocIds = uniqueSorted((r36Manifest?.baselineTrustedDocIds || []).map(String));
  if (!baselineTrustedDocIds.length) throw new Error("No baseline trusted doc IDs from R36 manifest.");
  if (Number(r42Fix?.correctedReference?.trustedDocumentCount || 0) !== baselineTrustedDocIds.length) {
    throw new Error("R42 corrected trusted count does not match baseline lineage count.");
  }

  const docs = await resolveDocs();
  const previews = await loadPreviews(docs, uniqueSorted([...baselineTrustedDocIds, TARGET_DOC_ID]));
  const artifacts = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds: [TARGET_DOC_ID],
    existingTrustedDocIds: baselineTrustedDocIds,
    activationManifestSource: "retrieval-r43-policy-adoption-simulation-report.json"
  });
  if (artifacts.docsMissingPreview.length) {
    throw new Error(`Missing retrieval preview for target doc: ${artifacts.docsMissingPreview.join(", ")}`);
  }

  const fetchSearchDebug = (payload) =>
    fetchJson(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

  const beforeLiveQa = await runLiveQa({
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

  const activatedDocIds = uniqueSorted((writeReport.documentsActivated || []).map((row) => row.documentId));
  const nonManifestDocIds = activatedDocIds.filter((id) => id !== TARGET_DOC_ID);
  const afterTrustedDocIds = uniqueSorted([...baselineTrustedDocIds, ...activatedDocIds]);

  const afterLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: afterTrustedDocIds,
    fetchSearchDebug,
    limit: 20
  });

  const hardGate = evaluateR44HardGate({ beforeQa: beforeLiveQa, afterQa: afterLiveQa });

  const anomalyFlags = [];
  const onlyManifestDocTouched = nonManifestDocIds.length === 0 && activatedDocIds.length <= 1;
  if (!onlyManifestDocTouched) anomalyFlags.push("non_manifest_doc_activated");
  if (Number(writeReport.summary?.heldDocsWrittenCount || 0) !== 0) anomalyFlags.push("held_docs_written");
  if (Number(writeReport.summary?.excludedDocsWrittenCount || 0) !== 0) anomalyFlags.push("excluded_docs_written");
  if (Number(writeReport.summary?.fixtureDocsWrittenCount || 0) !== 0) anomalyFlags.push("fixture_docs_written");
  if (Number(writeReport.summary?.provenanceFailuresCount || 0) !== 0) anomalyFlags.push("provenance_failures");
  if (!hardGate.passed) anomalyFlags.push("hard_gate_failed");

  let keepOrRollbackDecision = "keep_batch_active";
  let rollback = { triggered: false, summary: null };
  if (anomalyFlags.length) {
    keepOrRollbackDecision = "rollback_batch";
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
    docActivatedExact: TARGET_DOC_ID,
    activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
    keepOrRollbackDecision,
    anomalyFlags,
    beforeLiveMetrics: beforeLiveQa.summary,
    afterLiveMetrics: afterLiveQa.summary,
    hardGate,
    effectiveCitationCeiling: Number(hardGate.effectiveCitationCeiling || 0),
    rollbackBatchId: String(rollback.summary?.rollbackBatchId || artifacts.payload.rollbackManifest.rollbackBatchIds?.[0] || ""),
    rollbackVerificationPassed: Boolean(rollback.summary?.rollbackVerificationPassed || false),
    onlyManifestDocTouched,
    activatedDocIds,
    nonManifestDocIds,
    heldDocsWrittenCount: Number(writeReport.summary?.heldDocsWrittenCount || 0),
    excludedDocsWrittenCount: Number(writeReport.summary?.excludedDocsWrittenCount || 0),
    fixtureDocsWrittenCount: Number(writeReport.summary?.fixtureDocsWrittenCount || 0),
    provenanceFailuresCount: Number(writeReport.summary?.provenanceFailuresCount || 0),
    writeValidation: {
      attemptedTrustedDocumentCount: Number(writeReport.summary?.attemptedTrustedDocumentCount || 0),
      attemptedTrustedChunkCount: Number(writeReport.summary?.attemptedTrustedChunkCount || 0),
      activatedDocumentCount: Number(writeReport.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport.summary?.activatedChunkCount || 0)
    },
    rollback,
    rollbackManifest: artifacts.payload.rollbackManifest,
    rollbackManifestFile: rollbackManifestName,
    beforeVsAfter: compareLiveQa(beforeLiveQa, afterLiveQa),
    summary: {
      activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
      requestedBatchSize: 1,
      activatedDocumentCount: Number(writeReport.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport.summary?.activatedChunkCount || 0),
      keepOrRollbackDecision
    }
  };

  const liveQaReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      before: beforeLiveQa.summary,
      after: afterLiveQa.summary,
      hardGate,
      keepOrRollbackDecision
    },
    beforeQueryResults: beforeLiveQa.queryResults,
    afterQueryResults: afterLiveQa.queryResults
  };

  const activationPath = path.resolve(reportsDir, activationReportName);
  const activationMdPath = path.resolve(reportsDir, activationMarkdownName);
  const liveQaPath = path.resolve(reportsDir, liveQaReportName);
  const liveQaMdPath = path.resolve(reportsDir, liveQaMarkdownName);
  const rollbackPath = path.resolve(reportsDir, rollbackManifestName);

  await Promise.all([
    fs.writeFile(activationPath, JSON.stringify(activationReport, null, 2)),
    fs.writeFile(activationMdPath, buildActivationMarkdown(activationReport)),
    fs.writeFile(liveQaPath, JSON.stringify(liveQaReport, null, 2)),
    fs.writeFile(liveQaMdPath, buildBatchLiveQaMarkdown(afterLiveQa)),
    fs.writeFile(rollbackPath, JSON.stringify(artifacts.payload.rollbackManifest, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        docActivatedExact: TARGET_DOC_ID,
        activationBatchId: activationReport.activationBatchId,
        keepOrRollbackDecision,
        rollbackBatchId: activationReport.rollbackBatchId,
        rollbackVerificationPassed: activationReport.rollbackVerificationPassed,
        onlyManifestDocTouched,
        activatedDocIds,
        nonManifestDocIds
      },
      null,
      2
    )
  );
  console.log(`R44 activation report written to ${activationPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}

