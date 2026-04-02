import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  buildBatchActivationArtifacts,
  buildBatchLiveQaMarkdown,
  compareLiveQa,
  runLiveQa
} from "./retrieval-batch-activation-utils.mjs";
import { evaluateR46HardGate } from "./retrieval-r46-single-activation-write.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const probePlanName = process.env.RETRIEVAL_R51_PROBE_PLAN_NAME || "retrieval-r50-family-probe-plan-report.json";
const baselineManifestName = process.env.RETRIEVAL_R51_BASELINE_MANIFEST_NAME || "retrieval-r46-next-single-manifest.json";

const activationReportName = process.env.RETRIEVAL_R51_ACTIVATION_REPORT_NAME || "retrieval-r51-single-probe-activation-report.json";
const activationMarkdownName = process.env.RETRIEVAL_R51_ACTIVATION_MARKDOWN_NAME || "retrieval-r51-single-probe-activation-report.md";
const liveQaReportName = process.env.RETRIEVAL_R51_LIVE_QA_REPORT_NAME || "retrieval-r51-single-probe-live-qa-report.json";
const liveQaMarkdownName = process.env.RETRIEVAL_R51_LIVE_QA_MARKDOWN_NAME || "retrieval-r51-single-probe-live-qa-report.md";
const rollbackManifestName = process.env.RETRIEVAL_R51_ROLLBACK_MANIFEST_NAME || "retrieval-r51-single-probe-rollback-manifest.json";

const R51_TARGET_DOC_ID = "doc_345fd497-a82c-40ca-a45d-c0aca1b17826";

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
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
  return (payload.documents || [])
    .map((row) => ({ id: row.id, isLikelyFixture: Boolean(row.isLikelyFixture) }))
    .filter((row) => row.id);
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

export function deriveR51FamilyDecision({ keepOrRollbackDecision, hardGateFailures = [], anomalyFlags = [] }) {
  const failed = keepOrRollbackDecision === "rollback_batch" || hardGateFailures.length > 0 || anomalyFlags.length > 0;
  if (failed) {
    return {
      freezeDecision: "freeze_family_pending_model_change",
      freezeReason: uniqueSorted([...hardGateFailures, ...anomalyFlags]).join(","),
      mayProceedToSecondCandidate: false
    };
  }
  return {
    freezeDecision: "no_freeze",
    freezeReason: "",
    mayProceedToSecondCandidate: true
  };
}

function buildActivationMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R51 Single Probe Activation Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Hard Gate");
  for (const [k, v] of Object.entries(report.hardGate?.checks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.hardGate?.failures || []).length) lines.push(`- failures: ${(report.hardGate.failures || []).join(", ")}`);
  lines.push("");
  lines.push("## Decision");
  lines.push(`- keepOrRollbackDecision: ${report.keepOrRollbackDecision}`);
  lines.push(`- freezeDecision: ${report.freezeDecision}`);
  lines.push(`- freezeReason: ${report.freezeReason || ""}`);
  lines.push(`- mayProceedToSecondCandidate: ${report.mayProceedToSecondCandidate}`);
  lines.push("");
  lines.push("- R51 runs candidate 1 only. Candidate 2 is not activated in this phase.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [probePlan, baselineManifest, r45Repair] = await Promise.all([
    readJson(path.resolve(reportsDir, probePlanName)),
    readJson(path.resolve(reportsDir, baselineManifestName)),
    readJson(path.resolve(reportsDir, "retrieval-r45-rollback-repair-report.json"))
  ]);

  if (!r45Repair?.requiredFinalFields?.stateIsSafe || !r45Repair?.requiredFinalFields?.rollbackVerificationPassed) {
    throw new Error("R51 aborted: R45 rollback reconciliation is not clean/safe.");
  }

  const ordered = (probePlan?.candidateOrder || []).map(String).filter(Boolean);
  if (!ordered.length) throw new Error("R51 aborted: no candidateOrder in R50 probe plan.");

  const targetDocId = ordered[0];
  if (targetDocId !== R51_TARGET_DOC_ID) {
    throw new Error(`R51 candidate mismatch. Expected ${R51_TARGET_DOC_ID}, got ${targetDocId}`);
  }

  const baselineTrustedDocIds = uniqueSorted((baselineManifest?.baselineTrustedDocIds || []).map(String));
  if (!baselineTrustedDocIds.length) throw new Error("R51 aborted: no baseline trusted doc IDs available.");

  const docs = await resolveDocs();
  const previews = await loadPreviews(docs, uniqueSorted([...baselineTrustedDocIds, targetDocId]));

  const artifacts = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds: [targetDocId],
    existingTrustedDocIds: baselineTrustedDocIds,
    activationManifestSource: probePlanName
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

  const hardGate = evaluateR46HardGate({ beforeQa: beforeLiveQa, afterQa: afterLiveQa });

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

  const familyDecision = deriveR51FamilyDecision({
    keepOrRollbackDecision,
    hardGateFailures: hardGate.failures || [],
    anomalyFlags
  });

  const activationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    docActivatedExact: targetDocId,
    activationBatchId: String(writeReport.summary?.activationBatchId || artifacts.activationBatchId),
    keepOrRollbackDecision,
    anomalyFlags,
    beforeLiveMetrics: beforeLiveQa.summary,
    afterLiveMetrics: afterLiveQa.summary,
    effectiveCitationCeiling: Number(hardGate.effectiveCitationCeiling || 0),
    rollbackBatchId: String(rollback.summary?.rollbackBatchId || artifacts.payload.rollbackManifest.rollbackBatchIds?.[0] || ""),
    rollbackVerificationPassed: Boolean(rollback.summary?.rollbackVerificationPassed || false),
    onlyManifestDocTouched,
    activatedDocIds,
    nonManifestDocIds,
    freezeDecision: familyDecision.freezeDecision,
    freezeReason: familyDecision.freezeReason,
    mayProceedToSecondCandidate: familyDecision.mayProceedToSecondCandidate,
    hardGate,
    rollback,
    rollbackManifest: artifacts.payload.rollbackManifest,
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
      keepOrRollbackDecision,
      freezeDecision: familyDecision.freezeDecision,
      freezeReason: familyDecision.freezeReason,
      mayProceedToSecondCandidate: familyDecision.mayProceedToSecondCandidate
    },
    beforeQueryResults: beforeLiveQa.queryResults,
    afterQueryResults: afterLiveQa.queryResults,
    beforeVsAfter: compareLiveQa(beforeLiveQa, afterLiveQa)
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
        anomalyFlags,
        freezeDecision: familyDecision.freezeDecision,
        mayProceedToSecondCandidate: familyDecision.mayProceedToSecondCandidate
      },
      null,
      2
    )
  );
  console.log(`R51 probe activation report written to ${activationPath}`);
  console.log(`R51 probe live QA report written to ${liveQaPath}`);
  console.log(`R51 probe rollback manifest written to ${rollbackPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
