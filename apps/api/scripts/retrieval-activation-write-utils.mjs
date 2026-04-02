export function deriveActivationHealth(report) {
  const attemptedDocs = Number(report.summary?.attemptedTrustedDocumentCount || report.writeCounts?.attemptedTrustedDocumentCount || 0);
  const attemptedChunks = Number(report.summary?.attemptedTrustedChunkCount || report.writeCounts?.attemptedTrustedChunkCount || 0);
  const activatedDocs = Number(report.summary?.activatedDocumentCount || report.writeCounts?.activatedDocumentCount || 0);
  const activatedChunks = Number(report.summary?.activatedChunkCount || report.writeCounts?.activatedChunkCount || 0);
  const nonZeroWritesWhenAttempted = (attemptedDocs === 0 || activatedDocs > 0) && (attemptedChunks === 0 || activatedChunks > 0);
  return {
    nonZeroWritesWhenAttempted,
    attemptedDocs,
    attemptedChunks,
    activatedDocs,
    activatedChunks
  };
}

export function validateActivationWriteReport(report, context = {}) {
  const trustedDocSet = new Set(context.trustedDocIds || []);
  const trustedChunkSet = new Set(context.trustedChunkIds || []);

  const documentsActivated = report.documentsActivated || [];
  const chunksActivated = report.chunksActivated || [];

  const onlyTrustedDocsWritten =
    trustedDocSet.size === 0 || documentsActivated.every((row) => trustedDocSet.has(String(row.documentId || "")));
  const onlyTrustedChunksWritten =
    trustedChunkSet.size === 0 || chunksActivated.every((row) => trustedChunkSet.has(String(row.chunkId || "")));

  const noHeldOrExcludedOrFixtureWrites =
    Number(report.writeCounts?.heldDocsWrittenCount || 0) === 0 &&
    Number(report.writeCounts?.excludedDocsWrittenCount || 0) === 0 &&
    Number(report.writeCounts?.fixtureDocsWrittenCount || 0) === 0;

  const provenanceComplete = Number(report.writeCounts?.provenanceFailuresCount || 0) === 0;

  const rollbackMatchesWriteSet =
    Boolean(report.rollbackVerificationSummary?.rollbackManifestMatchesActivationSet) &&
    Boolean(report.summary?.rollbackVerificationPassed);
  const health = deriveActivationHealth(report);

  return {
    onlyTrustedDocsWritten,
    onlyTrustedChunksWritten,
    noHeldOrExcludedOrFixtureWrites,
    provenanceComplete,
    rollbackMatchesWriteSet,
    nonZeroWritesWhenAttempted: health.nonZeroWritesWhenAttempted,
    activationVerificationPassed: Boolean(report.summary?.activationVerificationPassed) && health.nonZeroWritesWhenAttempted
  };
}

export function formatRetrievalActivationWriteMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Activation Write Report");
  lines.push("");
  lines.push(`- mode: ${report.readOnly ? "dry_run" : "write"}`);
  lines.push("");

  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Write Counts");
  for (const [k, v] of Object.entries(report.writeCounts || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Verification Summary");
  for (const [k, v] of Object.entries(report.verificationSummary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Activation Batch Summary");
  for (const [k, v] of Object.entries(report.activationBatchSummary || {})) {
    if (typeof v === "object") continue;
    lines.push(`- ${k}: ${v}`);
  }
  for (const [k, v] of Object.entries(report.activationBatchSummary?.trustSourceCounts || {})) {
    lines.push(`- trustSource.${k}: ${v}`);
  }
  lines.push("");

  lines.push("## Rollback Verification Summary");
  for (const [k, v] of Object.entries(report.rollbackVerificationSummary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Documents Activated (Sample)");
  for (const row of (report.documentsActivated || []).slice(0, 50)) {
    lines.push(`- ${row.documentId} | ${row.title} | ${row.trustSource} | writeStatus=${row.writeStatus}`);
  }
  if (!(report.documentsActivated || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Chunks Activated (Sample)");
  for (const row of (report.chunksActivated || []).slice(0, 50)) {
    lines.push(
      `- ${row.chunkId} | doc=${row.documentId} | type=${row.chunkType} | embedding=${row.embeddingWriteStatus} | search=${row.searchWriteStatus} | provenance=${row.provenanceComplete}`
    );
  }
  if (!(report.chunksActivated || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Rejections");
  lines.push(`- documentsRejected: ${(report.documentsRejectedFromWrite || []).length}`);
  lines.push(`- chunksRejected: ${(report.chunksRejectedFromWrite || []).length}`);
  lines.push("");

  return `${lines.join("\n")}\n`;
}
