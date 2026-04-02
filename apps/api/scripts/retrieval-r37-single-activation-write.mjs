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

const manifestInputName = process.env.RETRIEVAL_R37_INPUT_MANIFEST || "retrieval-r36-next-safe-single-manifest.json";
const activationReportName = process.env.RETRIEVAL_R37_ACTIVATION_REPORT_NAME || "retrieval-r37-single-activation-report.json";
const activationMarkdownName = process.env.RETRIEVAL_R37_ACTIVATION_MARKDOWN_NAME || "retrieval-r37-single-activation-report.md";
const liveQaReportName = process.env.RETRIEVAL_R37_LIVE_QA_REPORT_NAME || "retrieval-r37-single-live-qa-report.json";
const liveQaMarkdownName = process.env.RETRIEVAL_R37_LIVE_QA_MARKDOWN_NAME || "retrieval-r37-single-live-qa-report.md";
const rollbackManifestName = process.env.RETRIEVAL_R37_ROLLBACK_MANIFEST_NAME || "retrieval-r37-single-rollback-manifest.json";

const R37_TARGET_DOC_ID = "doc_6497e48b-69b3-44e3-b0c9-e7eb370067e2";
const CONFIGURED_CITATION_CEILING = Number(process.env.RETRIEVAL_R37_MAX_CITATION_TOP_DOC_SHARE || "0.1");
const QUALITY_REGRESSION_TOLERANCE = Number(process.env.RETRIEVAL_R37_MAX_QUALITY_REGRESSION || "0.5");

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function sameSet(a, b) {
  if (a.length !== b.length) return false;
  const set = new Set(a);
  for (const value of b) if (!set.has(value)) return false;
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

export function evaluateR37HardGate({ beforeQa, afterQa }) {
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
    lowSignalStructuralShareNotWorsened: Number(afterLowSignalShare || 0) <= Number(beforeLowSignalShare || 0),
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
      lowSignalStructuralShareCeiling: beforeLowSignalShare
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
  lines.push("# Retrieval R37 Single Activation Report");
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
  lines.push(`- keep_or_rollback: ${report.summary?.keepOrRollbackDecision || "rollback_batch"}`);
  lines.push(`- rollbackTriggered: ${report.rollback?.triggered || false}`);
  lines.push(`- rollbackBatchId: ${report.rollback?.summary?.rollbackBatchId || report.rollbackBatchId || ""}`);
  lines.push("\n- Real write executed for single manifest doc only; hard gates unchanged.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const manifest = await readJson(path.resolve(reportsDir, manifestInputName));
  const targetDocId = String(manifest?.nextSafeSingleDocId || "");
  if (!targetDocId) {
    throw new Error(`Missing nextSafeSingleDocId in ${manifestInputName}`);
  }

  if (targetDocId !== R37_TARGET_DOC_ID) {
    throw new Error(`R37 manifest doc mismatch. Expected ${R37_TARGET_DOC_ID}, got ${targetDocId}`);
  }

  const baselineTrustedDocIds = uniqueSorted((manifest?.baselineTrustedDocIds || []).map(String));
  if (!baselineTrustedDocIds.length) {
    throw new Error("No baselineTrustedDocIds in R36 manifest.");
  }

  const docs = await resolveDocs();
  const previews = await loadPreviews(docs, uniqueSorted([...baselineTrustedDocIds, targetDocId]));

  const artifacts = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds: [targetDocId],
    existingTrustedDocIds: baselineTrustedDocIds,
    activationManifestSource: manifestInputName
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
  const nonManifestDocIds = activatedDocIds.filter((id) => id !== targetDocId);
  const afterTrustedDocIds = uniqueSorted([...baselineTrustedDocIds, ...activatedDocIds]);

  const afterLiveQa = await runLiveQa({
    apiBase,
    trustedDocumentIds: afterTrustedDocIds,
    fetchSearchDebug,
    limit: 20
  });

  const hardGate = evaluateR37HardGate({ beforeQa: beforeLiveQa, afterQa: afterLiveQa });

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
    docActivatedExact: targetDocId,
    activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
    effectiveCitationCeiling: Number(hardGate.effectiveCitationCeiling || 0),
    summary: {
      activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
      requestedBatchSize: 1,
      activatedDocumentCount: Number(writeReport.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport.summary?.activatedChunkCount || 0),
      keepOrRollbackDecision
    },
    beforeLiveMetrics: beforeLiveQa.summary,
    afterLiveMetrics: afterLiveQa.summary,
    hardGate,
    anomalyFlags,
    rollbackBatchId: String(
      rollback.summary?.rollbackBatchId || artifacts.payload.rollbackManifest.rollbackBatchIds?.[0] || ""
    ),
    rollbackVerificationPassed: Boolean(rollback.summary?.rollbackVerificationPassed || false),
    onlyManifestDocTouched,
    activatedDocIds,
    nonManifestDocIds,
    heldDocsWrittenCount: Number(writeReport.summary?.heldDocsWrittenCount || 0),
    excludedDocsWrittenCount: Number(writeReport.summary?.excludedDocsWrittenCount || 0),
    fixtureDocsWrittenCount: Number(writeReport.summary?.fixtureDocsWrittenCount || 0),
    provenanceFailuresCount: Number(writeReport.summary?.provenanceFailuresCount || 0),
    rollback,
    rollbackManifest: artifacts.payload.rollbackManifest,
    rollbackManifestFile: rollbackManifestName,
    beforeVsAfter: compareLiveQa(beforeLiveQa, afterLiveQa)
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
        docActivatedExact: targetDocId,
        activationBatchId: activationReport.activationBatchId,
        keepOrRollbackDecision,
        anomalyFlags
      },
      null,
      2
    )
  );
  console.log(`R37 activation report written to ${activationPath}`);
  console.log(`R37 live QA report written to ${liveQaPath}`);
  console.log(`R37 rollback manifest written to ${rollbackPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
