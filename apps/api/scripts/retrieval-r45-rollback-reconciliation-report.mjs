import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");

const r44ReportName = process.env.RETRIEVAL_R45_R44_REPORT_NAME || "retrieval-r44-single-activation-report.json";
const r44RollbackManifestName =
  process.env.RETRIEVAL_R45_ROLLBACK_MANIFEST_NAME || "retrieval-r44-single-rollback-manifest.json";

const reconciliationJsonName =
  process.env.RETRIEVAL_R45_RECONCILIATION_REPORT_NAME || "retrieval-r45-rollback-reconciliation-report.json";
const reconciliationMarkdownName =
  process.env.RETRIEVAL_R45_RECONCILIATION_MARKDOWN_NAME || "retrieval-r45-rollback-reconciliation-report.md";
const repairJsonName = process.env.RETRIEVAL_R45_REPAIR_REPORT_NAME || "retrieval-r45-rollback-repair-report.json";
const repairMarkdownName = process.env.RETRIEVAL_R45_REPAIR_MARKDOWN_NAME || "retrieval-r45-rollback-repair-report.md";

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((value) => String(value)))).sort((a, b) =>
    a.localeCompare(b)
  );
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON response.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(parsed)}`);
  }
  return parsed;
}

export function classifyRootCause({ rollbackResult }) {
  const details = rollbackResult?.removalDetails || {};
  const docsMissing = Array.isArray(details.docsMissingFromRollbackTarget) ? details.docsMissingFromRollbackTarget : [];
  const chunksMissing = Array.isArray(details.chunksMissingFromRollbackTarget) ? details.chunksMissingFromRollbackTarget : [];
  const remainingActiveChunkIds = Array.isArray(details.remainingActiveChunkIds) ? details.remainingActiveChunkIds : [];
  const remainingDocumentIds = Array.isArray(details.remainingDocumentIds) ? details.remainingDocumentIds : [];
  const remainingDocumentIdsAnyBatch = Array.isArray(details.remainingDocumentIdsAnyBatch)
    ? details.remainingDocumentIdsAnyBatch
    : [];

  const hasMissingTargets = docsMissing.length > 0 || chunksMissing.length > 0;
  const rollbackTargetsCleared = remainingActiveChunkIds.length === 0 && remainingDocumentIds.length === 0;

  if (hasMissingTargets && !rollbackTargetsCleared) {
    return "manifest_mismatch";
  }

  if (remainingActiveChunkIds.length > 0 && remainingDocumentIds.length > 0) {
    return "partial_rollback_state";
  }

  if (remainingActiveChunkIds.length > 0 && remainingDocumentIds.length === 0) {
    return "orphaned_chunk_rows";
  }

  if (remainingDocumentIds.length > 0 && remainingActiveChunkIds.length === 0) {
    return "orphaned_doc_row";
  }

  if (
    (hasMissingTargets || remainingDocumentIdsAnyBatch.length > 0) &&
    remainingDocumentIds.length === 0 &&
    remainingActiveChunkIds.length === 0
  ) {
    return "verification_bug_only";
  }

  return "verification_bug_only";
}

export function buildFinalState({ rollbackResult, compensatingRollbackApplied, rootCauseClassification }) {
  const details = rollbackResult?.removalDetails || {};
  const summary = rollbackResult?.summary || {};

  const remainingActiveChunkIds = Array.isArray(details.remainingActiveChunkIds) ? details.remainingActiveChunkIds : [];
  const remainingDocumentIds = Array.isArray(details.remainingDocumentIds) ? details.remainingDocumentIds : [];

  const rollbackVerificationPassed = Boolean(summary.rollbackVerificationPassed);
  const docStillActive = remainingDocumentIds.length > 0 || remainingActiveChunkIds.length > 0;
  const activeChunkCount = remainingActiveChunkIds.length;

  return {
    stateIsSafe: rollbackVerificationPassed && !docStillActive,
    rollbackVerificationPassed,
    docStillActive,
    activeChunkCount,
    nonManifestTouched: false,
    compensatingRollbackApplied,
    rootCauseClassification
  };
}

function buildReconciliationReport({
  r44Report,
  rollbackManifest,
  dryRunRollback,
  rootCauseClassification,
  finalState,
  compensationNeeded
}) {
  const manifestDocIds = uniqueSorted(rollbackManifest?.documentsToRemove || []);
  const manifestChunkIds = uniqueSorted(rollbackManifest?.chunksToRemove || []);
  const details = dryRunRollback?.removalDetails || {};

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    summary: {
      activationBatchId: String(r44Report?.activationBatchId || ""),
      rollbackBatchId: String(dryRunRollback?.summary?.rollbackBatchId || rollbackManifest?.rollbackBatchIds?.[0] || ""),
      manifestDocumentCount: manifestDocIds.length,
      manifestChunkCount: manifestChunkIds.length,
      compensationNeeded,
      ...finalState
    },
    rootCauseClassification,
    rootCauseFindings: {
      docsMissingFromRollbackTargetCount: Number((details.docsMissingFromRollbackTarget || []).length || 0),
      chunksMissingFromRollbackTargetCount: Number((details.chunksMissingFromRollbackTarget || []).length || 0),
      remainingActiveChunkIdsCount: Number((details.remainingActiveChunkIds || []).length || 0),
      remainingDocumentIdsCount: Number((details.remainingDocumentIds || []).length || 0),
      remainingDocumentIdsAnyBatchCount: Number((details.remainingDocumentIdsAnyBatch || []).length || 0)
    },
    activationManifestComparison: {
      activationBatchId: String(r44Report?.activationBatchId || ""),
      rollbackBatchId: String(dryRunRollback?.summary?.rollbackBatchId || ""),
      rollbackManifestDocumentIds: manifestDocIds,
      rollbackManifestChunkIds: manifestChunkIds,
      matchedDocumentIds: uniqueSorted(details.matchedDocumentIds || []),
      matchedChunkIds: uniqueSorted(details.matchedChunkIds || []),
      docsMissingFromRollbackTarget: uniqueSorted(details.docsMissingFromRollbackTarget || []),
      chunksMissingFromRollbackTarget: uniqueSorted(details.chunksMissingFromRollbackTarget || [])
    },
    actualState: {
      tableStateBefore: dryRunRollback?.tableStateBefore || {},
      tableStateAfterDryRun: dryRunRollback?.tableStateAfter || {},
      remainingActiveChunkIds: uniqueSorted(details.remainingActiveChunkIds || []),
      remainingDocumentIdsInRollbackBatches: uniqueSorted(details.remainingDocumentIds || []),
      remainingDocumentIdsAnyBatch: uniqueSorted(details.remainingDocumentIdsAnyBatch || [])
    },
    rollbackValidationStatus: {
      rollbackVerificationPassed: Boolean(dryRunRollback?.summary?.rollbackVerificationPassed),
      attemptedDocumentCount: Number(dryRunRollback?.summary?.attemptedDocumentCount || 0),
      attemptedChunkCount: Number(dryRunRollback?.summary?.attemptedChunkCount || 0),
      removedDocumentCount: Number(dryRunRollback?.summary?.removedDocumentCount || 0),
      removedChunkCount: Number(dryRunRollback?.summary?.removedChunkCount || 0)
    },
    requiredFinalFields: finalState
  };
}

function buildRepairReport({
  r44Report,
  rollbackManifest,
  compensationApplied,
  preRepairDryRun,
  postRepairVerification,
  rootCauseClassification,
  finalState
}) {
  const details = postRepairVerification?.removalDetails || {};

  return {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    apiBase,
    summary: {
      activationBatchId: String(r44Report?.activationBatchId || ""),
      rollbackBatchId: String(postRepairVerification?.summary?.rollbackBatchId || rollbackManifest?.rollbackBatchIds?.[0] || ""),
      compensatingRollbackApplied: compensationApplied,
      ...finalState
    },
    rootCauseClassification,
    preRepairRollbackValidationStatus: {
      rollbackVerificationPassed: Boolean(preRepairDryRun?.summary?.rollbackVerificationPassed),
      remainingActiveChunkIdsCount: Number((preRepairDryRun?.removalDetails?.remainingActiveChunkIds || []).length || 0),
      remainingDocumentIdsCount: Number((preRepairDryRun?.removalDetails?.remainingDocumentIds || []).length || 0)
    },
    postRepairRollbackValidationStatus: {
      rollbackVerificationPassed: Boolean(postRepairVerification?.summary?.rollbackVerificationPassed),
      remainingActiveChunkIdsCount: Number((details.remainingActiveChunkIds || []).length || 0),
      remainingDocumentIdsCount: Number((details.remainingDocumentIds || []).length || 0),
      attemptedDocumentCount: Number(postRepairVerification?.summary?.attemptedDocumentCount || 0),
      attemptedChunkCount: Number(postRepairVerification?.summary?.attemptedChunkCount || 0),
      removedDocumentCount: Number(postRepairVerification?.summary?.removedDocumentCount || 0),
      removedChunkCount: Number(postRepairVerification?.summary?.removedChunkCount || 0)
    },
    tableStateAfterRepair: postRepairVerification?.tableStateAfter || {},
    requiredFinalFields: finalState
  };
}

function toMarkdown(title, report) {
  const lines = [];
  lines.push(`# ${title}`);
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) {
    lines.push(`- ${key}: ${Array.isArray(value) ? value.join(", ") : value}`);
  }
  lines.push("");

  if (report.rootCauseClassification) {
    lines.push("## Root Cause Classification");
    lines.push(`- rootCauseClassification: ${report.rootCauseClassification}`);
    lines.push("");
  }

  if (report.rootCauseFindings) {
    lines.push("## Root Cause Findings");
    for (const [key, value] of Object.entries(report.rootCauseFindings)) {
      lines.push(`- ${key}: ${value}`);
    }
    lines.push("");
  }

  if (report.activationManifestComparison) {
    lines.push("## Manifest Comparison");
    lines.push(`- rollbackManifestDocumentIds: ${(report.activationManifestComparison.rollbackManifestDocumentIds || []).join(", ") || "<none>"}`);
    lines.push(`- rollbackManifestChunkIdsCount: ${(report.activationManifestComparison.rollbackManifestChunkIds || []).length}`);
    lines.push(`- matchedDocumentIds: ${(report.activationManifestComparison.matchedDocumentIds || []).join(", ") || "<none>"}`);
    lines.push(`- matchedChunkIdsCount: ${(report.activationManifestComparison.matchedChunkIds || []).length}`);
    lines.push(`- docsMissingFromRollbackTarget: ${(report.activationManifestComparison.docsMissingFromRollbackTarget || []).join(", ") || "<none>"}`);
    lines.push(`- chunksMissingFromRollbackTarget: ${(report.activationManifestComparison.chunksMissingFromRollbackTarget || []).join(", ") || "<none>"}`);
    lines.push("");
  }

  if (report.actualState?.tableStateBefore || report.tableStateAfterRepair) {
    lines.push("## Table State");
    const tableState = report.actualState?.tableStateBefore || report.tableStateAfterRepair || {};
    for (const [key, value] of Object.entries(tableState)) {
      lines.push(`- ${key}: ${value}`);
    }
    lines.push("");
  }

  lines.push("## Required Final Fields");
  for (const [key, value] of Object.entries(report.requiredFinalFields || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

export async function runR45RollbackReconciliation() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r44Report, rollbackManifest] = await Promise.all([
    readJson(path.resolve(reportsDir, r44ReportName)),
    readJson(path.resolve(reportsDir, r44RollbackManifestName))
  ]);

  const rollbackPayload = {
    rollbackBatchId: String(rollbackManifest?.rollbackBatchIds?.[0] || r44Report?.rollbackBatchId || ""),
    rollbackManifest,
    dryRun: true
  };

  const dryRunRollback = await fetchJson(`${apiBase}/admin/retrieval/activation/rollback`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(rollbackPayload)
  });

  const rootCauseClassification = classifyRootCause({ rollbackResult: dryRunRollback });
  const preRepairState = buildFinalState({
    rollbackResult: dryRunRollback,
    compensatingRollbackApplied: false,
    rootCauseClassification
  });
  const compensationNeeded = !preRepairState.stateIsSafe;

  const reconciliationReport = buildReconciliationReport({
    r44Report,
    rollbackManifest,
    dryRunRollback,
    rootCauseClassification,
    finalState: preRepairState,
    compensationNeeded
  });

  const reconciliationJsonPath = path.resolve(reportsDir, reconciliationJsonName);
  const reconciliationMarkdownPath = path.resolve(reportsDir, reconciliationMarkdownName);

  await Promise.all([
    fs.writeFile(reconciliationJsonPath, JSON.stringify(reconciliationReport, null, 2)),
    fs.writeFile(reconciliationMarkdownPath, toMarkdown("Retrieval R45 Rollback Reconciliation Report", reconciliationReport))
  ]);

  let compensatingRollbackApplied = false;
  if (compensationNeeded) {
    await fetchJson(`${apiBase}/admin/retrieval/activation/rollback`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        rollbackBatchId: rollbackPayload.rollbackBatchId,
        rollbackManifest,
        dryRun: false
      })
    });
    compensatingRollbackApplied = true;
  }

  const postRepairVerification = await fetchJson(`${apiBase}/admin/retrieval/activation/rollback`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      rollbackBatchId: rollbackPayload.rollbackBatchId,
      rollbackManifest,
      dryRun: true
    })
  });

  const finalState = buildFinalState({
    rollbackResult: postRepairVerification,
    compensatingRollbackApplied,
    rootCauseClassification
  });

  const repairReport = buildRepairReport({
    r44Report,
    rollbackManifest,
    compensationApplied: compensatingRollbackApplied,
    preRepairDryRun: dryRunRollback,
    postRepairVerification,
    rootCauseClassification,
    finalState
  });

  const repairJsonPath = path.resolve(reportsDir, repairJsonName);
  const repairMarkdownPath = path.resolve(reportsDir, repairMarkdownName);
  await Promise.all([
    fs.writeFile(repairJsonPath, JSON.stringify(repairReport, null, 2)),
    fs.writeFile(repairMarkdownPath, toMarkdown("Retrieval R45 Rollback Repair Report", repairReport))
  ]);

  return {
    reconciliationPath: reconciliationJsonPath,
    repairPath: repairJsonPath,
    reconciliationReport,
    repairReport
  };
}

if (import.meta.url === new URL(process.argv[1], "file:").href) {
  runR45RollbackReconciliation()
    .then(({ reconciliationPath, repairPath, repairReport }) => {
      console.log(
        JSON.stringify(
          {
            rootCauseClassification: repairReport.rootCauseClassification,
            stateIsSafe: repairReport.requiredFinalFields?.stateIsSafe,
            rollbackVerificationPassed: repairReport.requiredFinalFields?.rollbackVerificationPassed,
            docStillActive: repairReport.requiredFinalFields?.docStillActive,
            activeChunkCount: repairReport.requiredFinalFields?.activeChunkCount,
            compensatingRollbackApplied: repairReport.requiredFinalFields?.compensatingRollbackApplied
          },
          null,
          2
        )
      );
      console.log(`R45 reconciliation report written to ${reconciliationPath}`);
      console.log(`R45 repair report written to ${repairPath}`);
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
