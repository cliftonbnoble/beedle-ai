import { validateActivationWriteReport } from "../retrieval-activation-write-utils.mjs";

export function evaluateDryRunForWrite(dryRunReport, activationManifest = {}) {
  const verificationChecks = validateActivationWriteReport(dryRunReport || {}, {
    trustedDocIds: activationManifest.documentsToActivate || [],
    trustedChunkIds: activationManifest.chunksToActivate || []
  });
  const rollbackMatchesWriteSet =
    verificationChecks.rollbackMatchesWriteSet ||
    (Boolean(dryRunReport?.summary?.rollbackVerificationPassed) &&
      Boolean(
        dryRunReport?.rollbackValidationStatus?.rollbackMatchesWriteSet ??
          dryRunReport?.manifestValidationStatus?.rollbackMatchesWriteSet
      ));

  const reasons = [];
  if (!verificationChecks.onlyTrustedDocsWritten) reasons.push("non_trusted_docs_detected");
  if (!verificationChecks.onlyTrustedChunksWritten) reasons.push("non_trusted_chunks_detected");
  if (!verificationChecks.noHeldOrExcludedOrFixtureWrites) reasons.push("held_or_excluded_or_fixture_write_detected");
  if (!verificationChecks.provenanceComplete) reasons.push("provenance_incomplete");
  if (!rollbackMatchesWriteSet) reasons.push("rollback_manifest_mismatch");
  if (!verificationChecks.nonZeroWritesWhenAttempted) reasons.push("zero_writes_when_attempted");
  if (!verificationChecks.activationVerificationPassed) reasons.push("activation_verification_failed");

  return {
    canWrite: reasons.length === 0,
    reasons,
    verificationChecks: {
      ...verificationChecks,
      rollbackMatchesWriteSet
    }
  };
}

export function formatRetrievalActivationGapMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Activation Gap Report");
  lines.push("");
  lines.push(`- Generated: \`${report.generatedAt}\``);
  lines.push(`- Stage status: \`${report.stageStatus}\``);
  lines.push(`- Read only: \`${report.readOnly}\``);
  lines.push(`- API base: \`${report.apiBase}\``);
  lines.push("");

  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) {
    lines.push(`- ${key}: ${typeof value === "object" ? JSON.stringify(value) : value}`);
  }
  lines.push("");

  lines.push("## Dry-run Evaluation");
  lines.push(`- canWrite: ${report.dryRunEvaluation?.canWrite}`);
  for (const reason of report.dryRunEvaluation?.reasons || []) {
    lines.push(`- reason: ${reason}`);
  }
  for (const [key, value] of Object.entries(report.dryRunEvaluation?.verificationChecks || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Selected Documents");
  for (const doc of (report.selectedDocuments || []).slice(0, 100)) {
    lines.push(`- \`${doc.documentId}\` | \`${doc.citation || ""}\` | \`${doc.title || ""}\``);
  }
  if (!(report.selectedDocuments || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Skipped Documents");
  for (const doc of (report.skippedDocuments || []).slice(0, 100)) {
    lines.push(`- \`${doc.documentId || "<unknown>"}\` | reason=\`${doc.reason || "unknown"}\``);
  }
  if (!(report.skippedDocuments || []).length) lines.push("- none");
  lines.push("");

  return `${lines.join("\n")}\n`;
}
