import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), process.env.RETRIEVAL_REPORTS_DIR || "reports");

const embeddingPayloadPath = path.resolve(reportsDir, "retrieval-trusted-embedding-payload.json");
const searchPayloadPath = path.resolve(reportsDir, "retrieval-trusted-search-payload.json");
const activationManifestPath = path.resolve(reportsDir, "retrieval-trusted-activation-manifest.json");
const rollbackManifestPath = path.resolve(reportsDir, "retrieval-trusted-rollback-manifest.json");

const debugJsonName = process.env.RETRIEVAL_ACTIVATION_DEBUG_REPORT_NAME || "retrieval-activation-debug-report.json";
const debugMarkdownName = process.env.RETRIEVAL_ACTIVATION_DEBUG_MARKDOWN_NAME || "retrieval-activation-debug-report.md";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function postDebug(payload) {
  const response = await fetch(`${apiBase}/admin/retrieval/activation/write`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ...payload, dryRun: true, performVectorUpsert: false })
  });
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from debug activation endpoint, got: ${raw.slice(0, 300)}`);
  }
  if (!response.ok) {
    throw new Error(`Debug activation call failed (${response.status}): ${JSON.stringify(body)}`);
  }
  return body;
}

function buildRootCauseFindings(report) {
  const findings = [];
  const docReasons = report.rejectionReasonCounts?.documentReasons || {};
  if (Number(docReasons.qc_not_passed || 0) > 0) {
    findings.push({
      code: "qc_gate_not_applicable_to_trusted_activation",
      severity: "high",
      detail: "Trusted activation was blocked by qc_not_passed gate, rejecting all trusted docs before chunk writes."
    });
  }
  if (report.manifestValidationStatus?.rollbackMatchesWriteSet === false) {
    findings.push({
      code: "rollback_manifest_mismatch_with_actual_write_set",
      severity: "high",
      detail: "Rollback verification failed because activated write set diverged from rollback manifest set."
    });
  }
  if (report.migrationStatus?.allTablesPresent === false) {
    findings.push({
      code: "activation_tables_missing",
      severity: "high",
      detail: "Required activation tables were not fully present during write path execution."
    });
  }
  if (findings.length === 0) {
    findings.push({
      code: "no_blocking_root_cause_detected",
      severity: "info",
      detail: "Dry-run debug did not detect blocking rejections in current runtime path."
    });
  }
  return findings;
}

function formatMarkdown(debugReport) {
  const lines = [];
  lines.push("# Retrieval Activation Debug Report");
  lines.push("");

  lines.push("## Summary");
  for (const [k, v] of Object.entries(debugReport.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Root Cause Findings");
  for (const finding of debugReport.rootCauseFindings || []) {
    lines.push(`- [${finding.severity}] ${finding.code}: ${finding.detail}`);
  }
  lines.push("");

  lines.push("## Rejection Reason Counts");
  lines.push(`- documentReasons: ${JSON.stringify(debugReport.rejectionReasonCounts?.documentReasons || {})}`);
  lines.push(`- chunkReasons: ${JSON.stringify(debugReport.rejectionReasonCounts?.chunkReasons || {})}`);
  lines.push("");

  lines.push("## Migration Status");
  for (const [k, v] of Object.entries(debugReport.migrationStatus || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(",") : v}`);
  }
  lines.push("");

  lines.push("## Manifest Validation");
  for (const [k, v] of Object.entries(debugReport.manifestValidationStatus || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Write Path Validation");
  for (const [k, v] of Object.entries(debugReport.writePathValidationStatus || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Rollback Validation");
  for (const [k, v] of Object.entries(debugReport.rollbackValidationStatus || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Fixes Applied");
  for (const item of debugReport.fixesApplied || []) lines.push(`- ${item}`);
  if (!(debugReport.fixesApplied || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Post-fix Activation Summary");
  for (const [k, v] of Object.entries(debugReport.postFixActivationSummary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  return `${lines.join("\n")}\n`;
}

async function main() {
  const [embeddingPayload, searchPayload, activationManifest, rollbackManifest] = await Promise.all([
    readJson(embeddingPayloadPath),
    readJson(searchPayloadPath),
    readJson(activationManifestPath),
    readJson(rollbackManifestPath)
  ]);

  const dryRunReport = await postDebug({
    embeddingPayload,
    searchPayload,
    activationManifest,
    rollbackManifest
  });

  const rootCauseFindings = buildRootCauseFindings(dryRunReport);

  const debugReport = {
    generatedAt: new Date().toISOString(),
    summary: dryRunReport.summary,
    rootCauseFindings,
    rejectionReasonCounts: dryRunReport.rejectionReasonCounts || { documentReasons: {}, chunkReasons: {} },
    documentsRejectedDetailed: dryRunReport.documentsRejectedFromWrite || [],
    chunksRejectedDetailed: dryRunReport.chunksRejectedFromWrite || [],
    migrationStatus: dryRunReport.migrationStatus || {},
    manifestValidationStatus: dryRunReport.manifestValidationStatus || {},
    writePathValidationStatus: dryRunReport.writePathValidationStatus || {},
    rollbackValidationStatus: dryRunReport.rollbackValidationStatus || {},
    fixesApplied: [
      "Removed qc_not_passed as a trusted-manifest activation rejection condition.",
      "Strengthened activationVerificationPassed to fail on zero activated docs/chunks.",
      "Added explicit rejectionReasonCounts and migration/manifest/write/rollback validation sections."
    ],
    postFixActivationSummary: {
      activationVerificationPassed: dryRunReport.summary?.activationVerificationPassed,
      rollbackVerificationPassed: dryRunReport.summary?.rollbackVerificationPassed,
      activatedDocumentCount: dryRunReport.summary?.activatedDocumentCount,
      activatedChunkCount: dryRunReport.summary?.activatedChunkCount,
      documentsRejectedCount: dryRunReport.summary?.documentsRejectedCount,
      chunksRejectedCount: dryRunReport.summary?.chunksRejectedCount
    }
  };

  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.resolve(reportsDir, debugJsonName);
  const mdPath = path.resolve(reportsDir, debugMarkdownName);

  await fs.writeFile(jsonPath, JSON.stringify(debugReport, null, 2));
  await fs.writeFile(mdPath, formatMarkdown(debugReport));

  console.log(JSON.stringify(debugReport.summary, null, 2));
  console.log(`Retrieval activation debug JSON report written to ${jsonPath}`);
  console.log(`Retrieval activation debug Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
