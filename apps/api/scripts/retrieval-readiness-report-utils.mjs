function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(4));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function toTopKeys(counts, topN = 3) {
  return Object.entries(counts || {})
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .slice(0, topN)
    .map(([key]) => key);
}

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function normalizeStats(stats) {
  return {
    sectionCount: Number(stats?.sectionCount || 0),
    chunkCount: Number(stats?.chunkCount || 0),
    chunkTypeSpread: Number(stats?.chunkTypeSpread || 0),
    usedFallbackChunking: Boolean(stats?.usedFallbackChunking),
    avgChunkLength: Number(stats?.avgChunkLength || 0),
    maxChunkLength: Number(stats?.maxChunkLength || 0),
    minChunkLength: Number(stats?.minChunkLength || 0),
    headingCount: Number(stats?.headingCount || 0),
    paragraphCount: Number(stats?.paragraphCount || 0),
    chunksFlaggedMixedTopic: Number(stats?.chunksFlaggedMixedTopic || 0),
    chunksFlaggedOverlong: Number(stats?.chunksFlaggedOverlong || 0),
    chunksWithWeakHeadingSignal: Number(stats?.chunksWithWeakHeadingSignal || 0),
    chunksWithCanonicalReferenceAlignment: Number(stats?.chunksWithCanonicalReferenceAlignment || 0),
    referenceDensityStats: {
      min: Number(stats?.referenceDensityStats?.min || 0),
      max: Number(stats?.referenceDensityStats?.max || 0),
      avg: Number(stats?.referenceDensityStats?.avg || 0)
    },
    chunkTypeCounts: stats?.chunkTypeCounts || {},
    chunkClassificationReasonCounts: stats?.chunkClassificationReasonCounts || {},
    repairApplied: Boolean(stats?.repairApplied),
    repairStrategyCounts: stats?.repairStrategyCounts || {},
    preRepairChunkCount: Number(stats?.preRepairChunkCount ?? stats?.chunkCount ?? 0),
    postRepairChunkCount: Number(stats?.postRepairChunkCount ?? stats?.chunkCount ?? 0),
    preRepairChunkTypeSpread: Number(stats?.preRepairChunkTypeSpread ?? stats?.chunkTypeSpread ?? 0),
    preRepairChunkTypeCounts: stats?.preRepairChunkTypeCounts || stats?.chunkTypeCounts || {},
    preRepairChunksFlaggedMixedTopic: Number(stats?.preRepairChunksFlaggedMixedTopic || 0),
    preRepairChunksWithCanonicalReferenceAlignment: Number(stats?.preRepairChunksWithCanonicalReferenceAlignment || 0),
    parsingGaps: Array.isArray(stats?.parsingGaps) ? stats.parsingGaps : []
  };
}

function classifyReadiness({ stats, chunkTypeCounts }) {
  const blockingReasons = [];
  const warningReasons = [];
  const chunkCount = Math.max(0, stats.chunkCount);
  const alignmentRatio = chunkCount > 0 ? stats.chunksWithCanonicalReferenceAlignment / chunkCount : 0;
  const mixedRatio = chunkCount > 0 ? stats.chunksFlaggedMixedTopic / chunkCount : 0;
  const weakHeadingRatio = chunkCount > 0 ? stats.chunksWithWeakHeadingSignal / chunkCount : 0;
  const authorityRatio = chunkCount > 0 ? Number(chunkTypeCounts.authority_discussion || 0) / chunkCount : 0;
  const hasReferenceSignals = stats.referenceDensityStats.avg >= 0.35 || stats.referenceDensityStats.max >= 0.8;

  if (stats.paragraphCount === 0) blockingReasons.push("no_paragraph_content");
  if (chunkCount === 0) blockingReasons.push("no_chunks_generated");
  if (stats.paragraphCount >= 10 && chunkCount <= 1) blockingReasons.push("severe_undersegmentation");
  if (stats.maxChunkLength >= 2400 || (chunkCount > 0 && stats.avgChunkLength >= 1700)) {
    blockingReasons.push("extreme_chunk_overlength");
  }
  if (chunkCount >= 3 && mixedRatio >= 0.67) blockingReasons.push("severe_mixed_topic_chunking");
  if (chunkCount >= 3 && hasReferenceSignals && alignmentRatio === 0) {
    blockingReasons.push("severe_reference_alignment_gap");
  }
  if (stats.parsingGaps.includes("no_sections") && stats.usedFallbackChunking && stats.headingCount === 0 && stats.paragraphCount >= 12) {
    blockingReasons.push("malformed_structure_signals");
  }

  if (stats.usedFallbackChunking) warningReasons.push("fallback_chunking_used");
  if (chunkCount <= 2) warningReasons.push("low_chunk_count");
  if (stats.chunkTypeSpread <= 2) warningReasons.push("low_type_diversity");
  if (stats.chunksFlaggedMixedTopic > 0) warningReasons.push("mixed_topic_chunks_present");
  if (stats.chunksFlaggedOverlong > 0) warningReasons.push("overlong_chunks_present");
  if (stats.chunksWithWeakHeadingSignal > 0 || weakHeadingRatio >= 0.5) warningReasons.push("weak_heading_signals");
  if (alignmentRatio < 0.25) warningReasons.push("poor_reference_alignment");
  if ((stats.headingCount === 0 && stats.paragraphCount >= 5) || (stats.sectionCount > 0 && stats.headingCount / stats.sectionCount < 0.3)) {
    warningReasons.push("low_heading_coverage");
  }
  if (chunkCount >= 4 && authorityRatio >= 0.7) warningReasons.push("authority_type_overweight");
  if (stats.referenceDensityStats.max >= 4 || stats.referenceDensityStats.avg >= 2.5) warningReasons.push("reference_density_outlier_high");
  if (stats.paragraphCount >= 8 && stats.referenceDensityStats.avg < 0.03 && stats.referenceDensityStats.max < 0.15) {
    warningReasons.push("reference_density_outlier_low");
  }

  const uniqueBlocking = uniqueSorted(blockingReasons);
  const uniqueWarnings = uniqueSorted(warningReasons);

  let readinessStatus = "retrieval_review_needed";
  if (uniqueBlocking.length > 0) {
    readinessStatus = "retrieval_blocked";
  } else {
    const clearlyReady =
      chunkCount >= 3 &&
      stats.chunkTypeSpread >= 3 &&
      stats.chunksFlaggedMixedTopic === 0 &&
      stats.chunksFlaggedOverlong === 0 &&
      alignmentRatio >= 0.25 &&
      !uniqueWarnings.includes("low_chunk_count") &&
      !uniqueWarnings.includes("low_type_diversity") &&
      !uniqueWarnings.includes("poor_reference_alignment");
    if (clearlyReady) readinessStatus = "retrieval_ready";
  }

  const blockPenalty = uniqueBlocking.reduce((sum) => sum + 35, 0);
  const warningPenaltyByReason = {
    fallback_chunking_used: 6,
    low_chunk_count: 12,
    low_type_diversity: 10,
    mixed_topic_chunks_present: 8,
    overlong_chunks_present: 8,
    weak_heading_signals: 7,
    poor_reference_alignment: 10,
    low_heading_coverage: 7,
    authority_type_overweight: 6,
    reference_density_outlier_high: 5,
    reference_density_outlier_low: 5
  };
  const warningPenalty = uniqueWarnings.reduce((sum, reason) => sum + Number(warningPenaltyByReason[reason] || 4), 0);

  let readinessScore = Math.max(0, 100 - blockPenalty - warningPenalty);
  if (readinessStatus === "retrieval_blocked") readinessScore = Math.min(readinessScore, 39);
  if (readinessStatus === "retrieval_review_needed") readinessScore = Math.max(40, Math.min(readinessScore, 79));
  if (readinessStatus === "retrieval_ready") readinessScore = Math.max(80, readinessScore);

  const readinessSummaryByStatus = {
    retrieval_ready: "Ready for retrieval indexing.",
    retrieval_review_needed: "Inspect chunk structure before trusted indexing.",
    retrieval_blocked: "Do not index until parsing/chunking quality is repaired."
  };

  let suggestedOperatorAction = "Inspect chunk quality details before indexing.";
  if (readinessStatus === "retrieval_ready") suggestedOperatorAction = "Ready for retrieval indexing.";
  if (readinessStatus === "retrieval_blocked") {
    suggestedOperatorAction = "Do not index until chunk structure or parsing quality is repaired.";
  } else if (uniqueWarnings.includes("low_heading_coverage") || uniqueWarnings.includes("weak_heading_signals")) {
    suggestedOperatorAction = "Inspect heading normalization and mixed-topic chunking before indexing.";
  } else if (uniqueWarnings.includes("poor_reference_alignment")) {
    suggestedOperatorAction = "Inspect canonical citation alignment before indexing.";
  }

  return {
    readinessStatus,
    readinessScore,
    blockingReasons: uniqueBlocking,
    warningReasons: uniqueWarnings,
    readinessSummary: readinessSummaryByStatus[readinessStatus],
    suggestedOperatorAction
  };
}

function toDocumentRow(doc, input) {
  const stats = normalizeStats(doc.stats);
  const chunkTypeCounts = stats.chunkTypeCounts || {};
  const reasonCounts = stats.chunkClassificationReasonCounts || {};
  const classified = classifyReadiness({ stats, chunkTypeCounts });
  const preStats = {
    ...stats,
    chunkCount: stats.preRepairChunkCount,
    chunkTypeSpread: stats.preRepairChunkTypeSpread,
    chunksFlaggedMixedTopic: stats.preRepairChunksFlaggedMixedTopic,
    chunksWithCanonicalReferenceAlignment: stats.preRepairChunksWithCanonicalReferenceAlignment
  };
  const preChunkTypeCounts = stats.preRepairChunkTypeCounts || chunkTypeCounts;
  const preClassified = classifyReadiness({ stats: preStats, chunkTypeCounts: preChunkTypeCounts });
  const readinessChangedAfterRepair = stats.repairApplied && preClassified.readinessStatus !== classified.readinessStatus;

  const isLikelyFixture = Boolean(doc.isLikelyFixture);
  const include = !input.realOnly || !isLikelyFixture;

  return {
    include,
    documentId: doc.document?.documentId,
    title: doc.document?.title || "Untitled",
    isLikelyFixture,
    readinessStatus: classified.readinessStatus,
    readinessScore: classified.readinessScore,
    preRepairReadinessStatus: preClassified.readinessStatus,
    postRepairReadinessStatus: classified.readinessStatus,
    readinessChangedAfterRepair,
    blockingReasons: classified.blockingReasons,
    warningReasons: classified.warningReasons,
    keyStats: {
      chunkCount: stats.chunkCount,
      chunkTypeSpread: stats.chunkTypeSpread,
      usedFallbackChunking: stats.usedFallbackChunking,
      chunksFlaggedMixedTopic: stats.chunksFlaggedMixedTopic,
      chunksFlaggedOverlong: stats.chunksFlaggedOverlong,
      chunksWithWeakHeadingSignal: stats.chunksWithWeakHeadingSignal,
      chunksWithCanonicalReferenceAlignment: stats.chunksWithCanonicalReferenceAlignment,
      headingCount: stats.headingCount,
      paragraphCount: stats.paragraphCount,
      avgChunkLength: stats.avgChunkLength
    },
    representativeChunkTypes: toTopKeys(chunkTypeCounts, 5),
    representativeClassificationReasons: toTopKeys(reasonCounts, 5),
    sourceFileRef: doc.document?.sourceFileRef || "",
    sourceLink: doc.document?.sourceLink || "",
    readinessSummary: classified.readinessSummary,
    suggestedOperatorAction: classified.suggestedOperatorAction,
    chunkTypeCounts,
    chunkClassificationReasonCounts: reasonCounts,
    retrievalPriorityCounts: doc.stats?.retrievalPriorityCounts || {},
    repairApplied: stats.repairApplied,
    repairStrategyCounts: stats.repairStrategyCounts,
    preRepairChunkCount: stats.preRepairChunkCount,
    postRepairChunkCount: stats.postRepairChunkCount,
    referenceDensityStats: stats.referenceDensityStats,
    parsingGaps: stats.parsingGaps
  };
}

function sortRows(rows) {
  const statusRank = {
    retrieval_blocked: 0,
    retrieval_review_needed: 1,
    retrieval_ready: 2
  };
  return [...rows].sort((a, b) => {
    const statusDelta = Number(statusRank[a.readinessStatus] ?? 9) - Number(statusRank[b.readinessStatus] ?? 9);
    if (statusDelta !== 0) return statusDelta;
    if (a.readinessScore !== b.readinessScore) return a.readinessScore - b.readinessScore;
    return String(a.documentId || "").localeCompare(String(b.documentId || ""));
  });
}

export function buildRetrievalReadinessReport({ apiBase, input, documents }) {
  const rows = sortRows((documents || []).map((doc) => toDocumentRow(doc, input)).filter((row) => row.include));

  const countsByReadiness = countBy(rows.map((row) => row.readinessStatus));
  const countsByFailureReason = countBy(rows.flatMap((row) => row.blockingReasons || []));
  const countsByWarningReason = countBy(rows.flatMap((row) => row.warningReasons || []));

  const retrievalReadyDocuments = rows.filter((row) => row.readinessStatus === "retrieval_ready");
  const retrievalReviewNeededDocuments = rows.filter((row) => row.readinessStatus === "retrieval_review_needed");
  const retrievalBlockedDocuments = rows.filter((row) => row.readinessStatus === "retrieval_blocked");
  const documentsImprovedByRepair = rows.filter(
    (row) =>
      row.repairApplied &&
      (row.postRepairChunkCount > row.preRepairChunkCount || row.readinessChangedAfterRepair || row.readinessScore >= 80)
  );
  const readinessChangedAfterRepair = rows.filter((row) => row.readinessChangedAfterRepair);
  const docsStillReviewNeededAfterRepair = rows.filter((row) => row.readinessStatus === "retrieval_review_needed");
  const docsStillBlockedAfterRepair = rows.filter((row) => row.readinessStatus === "retrieval_blocked");
  const lowStructureRepairUsageCounts = countBy(
    rows.flatMap((row) =>
      Object.entries(row.repairStrategyCounts || {})
        .filter(([, count]) => Number(count || 0) > 0)
        .map(([key]) => key)
    )
  );

  const documentsNeedingHeadingRepair = rows.filter(
    (row) => row.warningReasons.includes("low_heading_coverage") || row.warningReasons.includes("weak_heading_signals")
  );
  const documentsOverusingFallback = rows.filter((row) => row.keyStats.usedFallbackChunking);
  const documentsWithPoorReferenceAlignment = rows.filter(
    (row) => row.warningReasons.includes("poor_reference_alignment") || row.blockingReasons.includes("severe_reference_alignment_gap")
  );
  const documentsWithMixedTopicChunks = rows.filter(
    (row) => row.keyStats.chunksFlaggedMixedTopic > 0 || row.blockingReasons.includes("severe_mixed_topic_chunking")
  );

  const summary = {
    documentsAnalyzed: rows.length,
    realOnly: Boolean(input.realOnly),
    includeText: Boolean(input.includeText),
    fixtureRowsExcluded: Number((documents || []).length - rows.length),
    avgReadinessScore: avg(rows.map((row) => Number(row.readinessScore || 0))),
    retrievalReadyCount: retrievalReadyDocuments.length,
    retrievalReviewNeededCount: retrievalReviewNeededDocuments.length,
    retrievalBlockedCount: retrievalBlockedDocuments.length,
    documentsNeedingHeadingRepairCount: documentsNeedingHeadingRepair.length,
    documentsOverusingFallbackCount: documentsOverusingFallback.length,
    documentsWithPoorReferenceAlignmentCount: documentsWithPoorReferenceAlignment.length,
    documentsWithMixedTopicChunksCount: documentsWithMixedTopicChunks.length,
    documentsImprovedByRepair: documentsImprovedByRepair.length,
    readinessChangedAfterRepair: readinessChangedAfterRepair.length,
    docsStillReviewNeededAfterRepair: docsStillReviewNeededAfterRepair.length,
    docsStillBlockedAfterRepair: docsStillBlockedAfterRepair.length
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    lowStructureRepairUsageCounts,
    countsByReadiness,
    countsByFailureReason,
    countsByWarningReason,
    documentsImprovedByRepair,
    readinessChangedAfterRepair,
    docsStillReviewNeededAfterRepair,
    docsStillBlockedAfterRepair,
    retrievalReadyDocuments,
    retrievalReviewNeededDocuments,
    retrievalBlockedDocuments,
    documentsNeedingHeadingRepair,
    documentsOverusingFallback,
    documentsWithPoorReferenceAlignment,
    documentsWithMixedTopicChunks,
    documents: rows
  };
}

export function formatRetrievalReadinessMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Readiness Report");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Counts By Readiness");
  for (const [k, v] of Object.entries(report.countsByReadiness || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByReadiness || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Failure Reason");
  for (const [k, v] of Object.entries(report.countsByFailureReason || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByFailureReason || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Warning Reason");
  for (const [k, v] of Object.entries(report.countsByWarningReason || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByWarningReason || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Low-Structure Repair Usage Counts");
  for (const [k, v] of Object.entries(report.lowStructureRepairUsageCounts || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.lowStructureRepairUsageCounts || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Repair Impact");
  lines.push(`- documentsImprovedByRepair: ${report.summary.documentsImprovedByRepair || 0}`);
  lines.push(`- readinessChangedAfterRepair: ${report.summary.readinessChangedAfterRepair || 0}`);
  lines.push(`- docsStillReviewNeededAfterRepair: ${report.summary.docsStillReviewNeededAfterRepair || 0}`);
  lines.push(`- docsStillBlockedAfterRepair: ${report.summary.docsStillBlockedAfterRepair || 0}`);
  lines.push("");

  const sections = [
    ["Documents Improved By Repair", report.documentsImprovedByRepair],
    ["Readiness Changed After Repair", report.readinessChangedAfterRepair],
    ["Docs Still Review Needed After Repair", report.docsStillReviewNeededAfterRepair],
    ["Docs Still Blocked After Repair", report.docsStillBlockedAfterRepair],
    ["Retrieval Ready Documents", report.retrievalReadyDocuments],
    ["Retrieval Review Needed Documents", report.retrievalReviewNeededDocuments],
    ["Retrieval Blocked Documents", report.retrievalBlockedDocuments],
    ["Documents Needing Heading Repair", report.documentsNeedingHeadingRepair],
    ["Documents Overusing Fallback", report.documentsOverusingFallback],
    ["Documents With Poor Reference Alignment", report.documentsWithPoorReferenceAlignment],
    ["Documents With Mixed Topic Chunks", report.documentsWithMixedTopicChunks]
  ];

  for (const [title, rows] of sections) {
    lines.push(`## ${title}`);
    if (!(rows || []).length) {
      lines.push("- none");
      lines.push("");
      continue;
    }
    for (const row of rows) {
      lines.push(
        `- ${row.documentId} | ${row.title} | status=${row.readinessStatus ?? row.postRepairReadinessStatus ?? "<none>"} score=${row.readinessScore ?? "<none>"} | warnings=${row.warningReasons?.join(",") || "<none>"} | blockers=${row.blockingReasons?.join(",") || "<none>"}`
      );
    }
    lines.push("");
  }

  lines.push("## Operator Notes");
  lines.push("- retrieval_ready: Ready for retrieval indexing.");
  lines.push("- retrieval_review_needed: Inspect chunk quality details before trusted indexing.");
  lines.push("- retrieval_blocked: Do not index until parsing/chunking quality is repaired.");
  lines.push("");
  lines.push("- Read-only report only. No ingestion, mutation, approval, metadata, citation, or QC changes are performed.");
  return `${lines.join("\n")}\n`;
}
