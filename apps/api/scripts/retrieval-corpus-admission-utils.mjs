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

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function toTopKeys(counts, topN = 4) {
  return Object.entries(counts || {})
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .slice(0, topN)
    .map(([k]) => k);
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
    retrievalPriorityCounts: stats?.retrievalPriorityCounts || {},
    parsingGaps: Array.isArray(stats?.parsingGaps) ? stats.parsingGaps : [],
    repairApplied: Boolean(stats?.repairApplied),
    repairStrategyCounts: stats?.repairStrategyCounts || {},
    preRepairChunkCount: Number(stats?.preRepairChunkCount ?? stats?.chunkCount ?? 0),
    postRepairChunkCount: Number(stats?.postRepairChunkCount ?? stats?.chunkCount ?? 0),
    preRepairChunkTypeSpread: Number(stats?.preRepairChunkTypeSpread ?? stats?.chunkTypeSpread ?? 0),
    preRepairChunksFlaggedMixedTopic: Number(stats?.preRepairChunksFlaggedMixedTopic || 0),
    preRepairChunksWithCanonicalReferenceAlignment: Number(stats?.preRepairChunksWithCanonicalReferenceAlignment || 0)
  };
}

function classifyReadinessLike(stats) {
  const warnings = [];
  const blockers = [];
  const chunkCount = Math.max(0, stats.chunkCount);
  const alignmentRatio = chunkCount > 0 ? stats.chunksWithCanonicalReferenceAlignment / chunkCount : 0;
  const mixedRatio = chunkCount > 0 ? stats.chunksFlaggedMixedTopic / chunkCount : 0;

  if (stats.paragraphCount === 0) blockers.push("no_paragraph_content");
  if (chunkCount === 0) blockers.push("no_chunks_generated");
  if (stats.paragraphCount >= 10 && chunkCount <= 1) blockers.push("severe_undersegmentation");
  if (stats.maxChunkLength >= 2400 || (chunkCount > 0 && stats.avgChunkLength >= 1700)) blockers.push("extreme_chunk_overlength");
  if (chunkCount >= 3 && mixedRatio >= 0.67) blockers.push("severe_mixed_topic_chunking");
  if (chunkCount >= 3 && alignmentRatio === 0 && (stats.referenceDensityStats.avg >= 0.35 || stats.referenceDensityStats.max >= 0.8)) {
    blockers.push("severe_reference_alignment_gap");
  }

  if (stats.usedFallbackChunking) warnings.push("fallback_chunking_used");
  if (chunkCount <= 2) warnings.push("low_chunk_count");
  if (stats.chunkTypeSpread <= 2) warnings.push("low_type_diversity");
  if (stats.chunksFlaggedMixedTopic > 0) warnings.push("mixed_topic_chunks_present");
  if (stats.chunksFlaggedOverlong > 0) warnings.push("overlong_chunks_present");
  if (stats.chunksWithWeakHeadingSignal > 0) warnings.push("weak_heading_signals");
  if (alignmentRatio < 0.25) warnings.push("poor_reference_alignment");
  if ((stats.headingCount === 0 && stats.paragraphCount >= 5) || (stats.sectionCount > 0 && stats.headingCount / Math.max(1, stats.sectionCount) < 0.3)) {
    warnings.push("low_heading_coverage");
  }

  let status = "retrieval_review_needed";
  if (blockers.length) {
    status = "retrieval_blocked";
  } else {
    const ready =
      chunkCount >= 3 &&
      stats.chunkTypeSpread >= 3 &&
      stats.chunksFlaggedMixedTopic === 0 &&
      stats.chunksFlaggedOverlong === 0 &&
      alignmentRatio >= 0.25 &&
      !warnings.includes("low_chunk_count") &&
      !warnings.includes("low_type_diversity") &&
      !warnings.includes("poor_reference_alignment");
    if (ready) status = "retrieval_ready";
  }

  return {
    readinessStatus: status,
    blockingReasons: uniqueSorted(blockers),
    warningReasons: uniqueSorted(warnings)
  };
}

function classifyCorpusAdmission(row) {
  const stats = row.keyStats;
  const reasons = [];
  const warnings = [];

  const chunkCount = Number(stats.chunkCount || 0);
  const spread = Number(stats.chunkTypeSpread || 0);
  const mixed = Number(stats.chunksFlaggedMixedTopic || 0);
  const overlong = Number(stats.chunksFlaggedOverlong || 0);
  const weakHeading = Number(stats.chunksWithWeakHeadingSignal || 0);
  const aligned = Number(stats.chunksWithCanonicalReferenceAlignment || 0);
  const alignmentRatio = chunkCount > 0 ? aligned / chunkCount : 0;

  const severeRiskSignals =
    row.blockingReasons.includes("no_chunks_generated") ||
    row.blockingReasons.includes("severe_undersegmentation") ||
    row.blockingReasons.includes("extreme_chunk_overlength") ||
    row.blockingReasons.includes("severe_mixed_topic_chunking") ||
    row.blockingReasons.includes("severe_reference_alignment_gap");

  const weakStructureClusterCount = [
    stats.usedFallbackChunking ? 1 : 0,
    chunkCount <= 2 ? 1 : 0,
    spread <= 1 ? 1 : 0,
    mixed > 0 ? 1 : 0,
    weakHeading > 0 ? 1 : 0,
    alignmentRatio < 0.2 ? 1 : 0
  ].reduce((sum, value) => sum + value, 0);

  const mildWarningClusterCount = [
    stats.usedFallbackChunking ? 1 : 0,
    chunkCount <= 3 ? 1 : 0,
    spread <= 2 ? 1 : 0,
    mixed > 0 ? 1 : 0,
    weakHeading > 0 ? 1 : 0,
    alignmentRatio < 0.35 ? 1 : 0
  ].reduce((sum, value) => sum + value, 0);

  let corpusAdmissionStatus = "hold_for_repair_review";

  if (severeRiskSignals || weakStructureClusterCount >= 4) {
    corpusAdmissionStatus = "exclude_from_initial_corpus";
    reasons.push("severe_structure_or_alignment_risk");
    if (severeRiskSignals) reasons.push("readiness_blocking_signals_present");
    if (alignmentRatio < 0.2) reasons.push("insufficient_reference_alignment_for_initial_trust");
    if (mixed > 0) warnings.push("mixed_topic_chunks_detected");
    if (stats.usedFallbackChunking) warnings.push("fallback_chunking_detected");
  } else if (
    row.readinessStatus === "retrieval_ready" &&
    !stats.usedFallbackChunking &&
    chunkCount >= 3 &&
    spread >= 3 &&
    mixed === 0 &&
    overlong === 0 &&
    alignmentRatio >= 0.25
  ) {
    corpusAdmissionStatus = "admit_now";
    reasons.push("stable_retrieval_readiness");
    reasons.push("sufficient_chunk_diversity_and_alignment");
    if (row.readinessChangedAfterRepair) reasons.push("repair_validated_chunk_quality_gain");
    if (mildWarningClusterCount > 0) warnings.push("minor_quality_warnings_present");
  } else {
    corpusAdmissionStatus = "hold_for_repair_review";
    reasons.push("requires_additional_structure_review_before_corpus_admission");
    if (stats.usedFallbackChunking) warnings.push("fallback_chunking_detected");
    if (chunkCount <= 3) warnings.push("low_chunk_count_for_corpus_confidence");
    if (spread <= 2) warnings.push("low_chunk_type_diversity");
    if (mixed > 0) warnings.push("mixed_topic_chunks_detected");
    if (alignmentRatio < 0.35) warnings.push("reference_alignment_below_preferred_corpus_threshold");
  }

  const warningPenalty = uniqueSorted(warnings).length * 6;
  const reasonPenalty = corpusAdmissionStatus === "exclude_from_initial_corpus" ? 55 : corpusAdmissionStatus === "hold_for_repair_review" ? 28 : 0;
  let corpusAdmissionScore = Math.max(0, 100 - reasonPenalty - warningPenalty);
  if (corpusAdmissionStatus === "admit_now") corpusAdmissionScore = Math.max(80, corpusAdmissionScore);
  if (corpusAdmissionStatus === "hold_for_repair_review") corpusAdmissionScore = Math.max(40, Math.min(79, corpusAdmissionScore));
  if (corpusAdmissionStatus === "exclude_from_initial_corpus") corpusAdmissionScore = Math.min(39, corpusAdmissionScore);

  const eligibleForInitialEmbedding = corpusAdmissionStatus === "admit_now";
  const eligibleForSearchExposure = corpusAdmissionStatus === "admit_now";

  let suggestedNextAction = "Inspect chunk segmentation and reference alignment before indexing.";
  let admissionSummary = "Hold for repair/review before first-pass corpus admission.";
  if (corpusAdmissionStatus === "admit_now") {
    suggestedNextAction = "Admit to first-pass retrieval corpus and include in initial embedding queue.";
    admissionSummary = "Safe for first-pass corpus admission.";
  }
  if (corpusAdmissionStatus === "exclude_from_initial_corpus") {
    suggestedNextAction = "Exclude for now; repair segmentation/alignment before considering corpus admission.";
    admissionSummary = "Too risky for initial retrieval corpus inclusion.";
  }

  return {
    corpusAdmissionStatus,
    corpusAdmissionScore,
    corpusAdmissionReasons: uniqueSorted(reasons),
    corpusAdmissionWarnings: uniqueSorted(warnings),
    admissionSummary,
    suggestedNextAction,
    eligibleForInitialEmbedding,
    eligibleForSearchExposure
  };
}

function sortRows(rows) {
  const rank = {
    exclude_from_initial_corpus: 0,
    hold_for_repair_review: 1,
    admit_now: 2
  };
  return [...rows].sort((a, b) => {
    const d = Number(rank[a.corpusAdmissionStatus] ?? 9) - Number(rank[b.corpusAdmissionStatus] ?? 9);
    if (d !== 0) return d;
    if (a.corpusAdmissionScore !== b.corpusAdmissionScore) return a.corpusAdmissionScore - b.corpusAdmissionScore;
    return String(a.documentId || "").localeCompare(String(b.documentId || ""));
  });
}

function toRow(doc, input) {
  const stats = normalizeStats(doc.stats);
  const readiness = classifyReadinessLike(stats);
  const preReadiness = classifyReadinessLike({
    ...stats,
    chunkCount: stats.preRepairChunkCount,
    chunkTypeSpread: stats.preRepairChunkTypeSpread,
    chunksFlaggedMixedTopic: stats.preRepairChunksFlaggedMixedTopic,
    chunksWithCanonicalReferenceAlignment: stats.preRepairChunksWithCanonicalReferenceAlignment
  });

  const base = {
    include: !input.realOnly || !Boolean(doc.isLikelyFixture),
    documentId: doc.document?.documentId,
    title: doc.document?.title || "Untitled",
    isLikelyFixture: Boolean(doc.isLikelyFixture),
    readinessStatus: readiness.readinessStatus,
    readinessWarningReasons: readiness.warningReasons,
    readinessBlockingReasons: readiness.blockingReasons,
    preRepairReadinessStatus: preReadiness.readinessStatus,
    postRepairReadinessStatus: readiness.readinessStatus,
    readinessChangedAfterRepair: Boolean(stats.repairApplied && preReadiness.readinessStatus !== readiness.readinessStatus),
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
    repairApplied: stats.repairApplied,
    repairStrategyCounts: stats.repairStrategyCounts,
    preRepairChunkCount: stats.preRepairChunkCount,
    postRepairChunkCount: stats.postRepairChunkCount,
    representativeChunkTypes: toTopKeys(stats.chunkTypeCounts, 5),
    representativeClassificationReasons: toTopKeys(stats.chunkClassificationReasonCounts, 5),
    sourceFileRef: doc.document?.sourceFileRef || "",
    sourceLink: doc.document?.sourceLink || ""
  };

  return {
    ...base,
    ...classifyCorpusAdmission({
      ...base,
      readinessStatus: readiness.readinessStatus,
      blockingReasons: readiness.blockingReasons,
      warningReasons: readiness.warningReasons
    })
  };
}

export function buildRetrievalCorpusAdmissionReport({ apiBase, input, documents }) {
  const rows = sortRows((documents || []).map((doc) => toRow(doc, input)).filter((row) => row.include));

  const admitNowDocuments = rows.filter((row) => row.corpusAdmissionStatus === "admit_now");
  const holdForRepairReviewDocuments = rows.filter((row) => row.corpusAdmissionStatus === "hold_for_repair_review");
  const excludeFromInitialCorpusDocuments = rows.filter((row) => row.corpusAdmissionStatus === "exclude_from_initial_corpus");

  const documentsEligibleForInitialEmbedding = admitNowDocuments.map((row) => row.documentId);
  const documentsEligibleForSearchExposure = admitNowDocuments.map((row) => row.documentId);

  const documentsHeldDueToFallback = holdForRepairReviewDocuments.filter((row) => row.corpusAdmissionWarnings.includes("fallback_chunking_detected"));
  const documentsHeldDueToMixedTopic = holdForRepairReviewDocuments.filter((row) => row.corpusAdmissionWarnings.includes("mixed_topic_chunks_detected"));
  const documentsExcludedDueToAlignment = excludeFromInitialCorpusDocuments.filter(
    (row) =>
      row.corpusAdmissionReasons.includes("insufficient_reference_alignment_for_initial_trust") ||
      row.corpusAdmissionWarnings.includes("reference_alignment_below_preferred_corpus_threshold")
  );

  const admitNowAfterRepairCount = admitNowDocuments.filter((row) => row.readinessChangedAfterRepair).length;
  const stillHeldAfterRepairCount = holdForRepairReviewDocuments.filter((row) => row.repairApplied).length;

  const countsByCorpusAdmissionStatus = countBy(rows.map((row) => row.corpusAdmissionStatus));
  const countsByAdmissionReason = countBy(rows.flatMap((row) => row.corpusAdmissionReasons || []));
  const countsByAdmissionWarning = countBy(rows.flatMap((row) => row.corpusAdmissionWarnings || []));

  const summary = {
    documentsAnalyzed: rows.length,
    realOnly: Boolean(input.realOnly),
    includeText: Boolean(input.includeText),
    fixtureRowsExcluded: Number((documents || []).length - rows.length),
    avgCorpusAdmissionScore: avg(rows.map((row) => Number(row.corpusAdmissionScore || 0))),
    initialEmbeddingCandidateCount: documentsEligibleForInitialEmbedding.length,
    initialSearchExposureCandidateCount: documentsEligibleForSearchExposure.length,
    heldForRepairReviewCount: holdForRepairReviewDocuments.length,
    excludedFromInitialCorpusCount: excludeFromInitialCorpusDocuments.length,
    admitNowAfterRepairCount,
    stillHeldAfterRepairCount
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    countsByCorpusAdmissionStatus,
    countsByAdmissionReason,
    countsByAdmissionWarning,
    admitNowDocuments,
    holdForRepairReviewDocuments,
    excludeFromInitialCorpusDocuments,
    documentsEligibleForInitialEmbedding,
    documentsEligibleForSearchExposure,
    documentsHeldDueToFallback,
    documentsHeldDueToMixedTopic,
    documentsExcludedDueToAlignment,
    documents: rows,
    admissionBundles: {
      admitNowDocumentIds: documentsEligibleForInitialEmbedding,
      holdDocumentIds: holdForRepairReviewDocuments.map((row) => row.documentId),
      excludeDocumentIds: excludeFromInitialCorpusDocuments.map((row) => row.documentId)
    }
  };
}

export function formatRetrievalCorpusAdmissionMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Corpus Admission Report");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push("");

  lines.push("## Counts By Corpus Admission Status");
  for (const [k, v] of Object.entries(report.countsByCorpusAdmissionStatus || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByCorpusAdmissionStatus || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Admission Reason");
  for (const [k, v] of Object.entries(report.countsByAdmissionReason || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByAdmissionReason || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Admission Warning");
  for (const [k, v] of Object.entries(report.countsByAdmissionWarning || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByAdmissionWarning || {}).length) lines.push("- none");
  lines.push("");

  const sections = [
    ["Admit Now Documents", report.admitNowDocuments],
    ["Hold For Repair Review Documents", report.holdForRepairReviewDocuments],
    ["Exclude From Initial Corpus Documents", report.excludeFromInitialCorpusDocuments],
    ["Documents Held Due To Fallback", report.documentsHeldDueToFallback],
    ["Documents Held Due To Mixed Topic", report.documentsHeldDueToMixedTopic],
    ["Documents Excluded Due To Alignment", report.documentsExcludedDueToAlignment]
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
        `- ${row.documentId} | ${row.title} | status=${row.corpusAdmissionStatus} score=${row.corpusAdmissionScore} | reasons=${row.corpusAdmissionReasons.join(",") || "<none>"} | warnings=${row.corpusAdmissionWarnings.join(",") || "<none>"}`
      );
    }
    lines.push("");
  }

  lines.push("## Admission Bundles (Read-only)");
  lines.push(`- admitNowDocumentIds: ${(report.admissionBundles?.admitNowDocumentIds || []).length}`);
  lines.push(`- holdDocumentIds: ${(report.admissionBundles?.holdDocumentIds || []).length}`);
  lines.push(`- excludeDocumentIds: ${(report.admissionBundles?.excludeDocumentIds || []).length}`);
  lines.push("");
  lines.push("- Read-only report only. No embedding writes, no search index writes, and no ingestion/QC mutations are performed.");

  return `${lines.join("\n")}\n`;
}
