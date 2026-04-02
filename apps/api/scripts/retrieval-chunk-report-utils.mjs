function avg(values) {
  if (!values.length) return 0;
  return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

export function normalizeDocumentStats(doc) {
  return {
    chunkCount: doc.stats?.chunkCount ?? 0,
    chunkTypes: doc.stats?.chunkTypes ?? [],
    chunkTypeSpread: doc.stats?.chunkTypeSpread ?? 0,
    usedFallbackChunking: Boolean(doc.stats?.usedFallbackChunking),
    avgChunkLength: doc.stats?.avgChunkLength ?? 0,
    maxChunkLength: doc.stats?.maxChunkLength ?? 0,
    minChunkLength: doc.stats?.minChunkLength ?? 0,
    headingCount: doc.stats?.headingCount ?? 0,
    paragraphCount: doc.stats?.paragraphCount ?? 0,
    chunkTypeCounts: doc.stats?.chunkTypeCounts ?? {},
    chunkClassificationReasonCounts: doc.stats?.chunkClassificationReasonCounts ?? {},
    retrievalPriorityCounts: doc.stats?.retrievalPriorityCounts ?? {},
    chunksFlaggedOverlong: doc.stats?.chunksFlaggedOverlong ?? 0,
    chunksFlaggedMixedTopic: doc.stats?.chunksFlaggedMixedTopic ?? 0,
    chunksWithWeakHeadingSignal: doc.stats?.chunksWithWeakHeadingSignal ?? 0,
    chunksWithCanonicalReferenceAlignment: doc.stats?.chunksWithCanonicalReferenceAlignment ?? 0,
    repairApplied: Boolean(doc.stats?.repairApplied),
    repairStrategyCounts: doc.stats?.repairStrategyCounts ?? {},
    repairedChunkCount: doc.stats?.repairedChunkCount ?? 0,
    preRepairChunkCount: doc.stats?.preRepairChunkCount ?? doc.stats?.chunkCount ?? 0,
    postRepairChunkCount: doc.stats?.postRepairChunkCount ?? doc.stats?.chunkCount ?? 0,
    preRepairChunkTypeSpread: doc.stats?.preRepairChunkTypeSpread ?? doc.stats?.chunkTypeSpread ?? 0,
    preRepairChunksFlaggedMixedTopic: doc.stats?.preRepairChunksFlaggedMixedTopic ?? 0,
    preRepairChunksWithCanonicalReferenceAlignment: doc.stats?.preRepairChunksWithCanonicalReferenceAlignment ?? 0,
    referenceDensityStats: doc.stats?.referenceDensityStats ?? { min: 0, max: 0, avg: 0 },
    parsingGaps: doc.stats?.parsingGaps ?? []
  };
}

function reviewNeeded(doc) {
  const reasons = [];
  if (doc.stats.usedFallbackChunking) reasons.push("fallback_chunking");
  if (doc.stats.chunkTypeSpread <= 2) reasons.push("low_type_diversity");
  if (doc.stats.chunksFlaggedOverlong > 0) reasons.push("overlong_chunks");
  if (doc.stats.chunksFlaggedMixedTopic > 0) reasons.push("mixed_topic_chunks");
  const total = doc.stats.chunkCount || 1;
  const alignedRatio = (doc.stats.chunksWithCanonicalReferenceAlignment || 0) / total;
  if (alignedRatio < 0.2) reasons.push("poor_reference_alignment");
  const authorityCount = Number(doc.stats.chunkTypeCounts?.authority_discussion || 0);
  if (authorityCount / total >= 0.7) reasons.push("authority_overweighting");
  return reasons;
}

function deriveReadinessStatusFromStats(stats) {
  const chunkCount = Number(stats.chunkCount || 0);
  const spread = Number(stats.chunkTypeSpread || 0);
  const mixed = Number(stats.chunksFlaggedMixedTopic || 0);
  const overlong = Number(stats.chunksFlaggedOverlong || 0);
  const aligned = Number(stats.chunksWithCanonicalReferenceAlignment || 0);
  const alignmentRatio = chunkCount > 0 ? aligned / chunkCount : 0;
  const blocked =
    chunkCount === 0 || (chunkCount <= 1 && Number(stats.paragraphCount || 0) >= 10) || (chunkCount >= 3 && mixed / chunkCount >= 0.67);
  if (blocked) return "retrieval_blocked";
  const ready =
    chunkCount >= 3 &&
    spread >= 3 &&
    mixed === 0 &&
    overlong === 0 &&
    alignmentRatio >= 0.25 &&
    !stats.usedFallbackChunking;
  return ready ? "retrieval_ready" : "retrieval_review_needed";
}

export function buildRetrievalChunkReport({ apiBase, input, documents }) {
  const normalizedDocs = (documents || []).map((doc) => ({
    ...doc,
    stats: normalizeDocumentStats(doc)
  }));

  const allChunks = normalizedDocs.flatMap((doc) => doc.chunks || []);

  const summary = {
    documentsAnalyzed: normalizedDocs.length,
    totalChunks: allChunks.length,
    avgChunksPerDocument: normalizedDocs.length ? Number((allChunks.length / normalizedDocs.length).toFixed(2)) : 0,
    avgChunkLength: avg(allChunks.map((chunk) => Number(chunk.textLength || 0))),
    chunkTypeCounts: countBy(allChunks.map((chunk) => chunk.chunkType)),
    chunkClassificationReasonCounts: countBy(allChunks.map((chunk) => chunk.chunkClassificationReason)),
    retrievalPriorityCounts: countBy(allChunks.map((chunk) => chunk.retrievalPriority)),
    documentsWithOnlyOneChunk: normalizedDocs.filter((doc) => Number(doc.stats.chunkCount || 0) === 1).length,
    documentsWithOnlyTwoChunks: normalizedDocs.filter((doc) => Number(doc.stats.chunkCount || 0) === 2).length,
    documentsUsingFallbackChunking: normalizedDocs.filter((doc) => Boolean(doc.stats.usedFallbackChunking)).length,
    referenceDensityStats: {
      min: allChunks.length ? Math.min(...allChunks.map((chunk) => Number(chunk.referenceDensity || 0))) : 0,
      max: allChunks.length ? Math.max(...allChunks.map((chunk) => Number(chunk.referenceDensity || 0))) : 0,
      avg: allChunks.length
        ? Number((allChunks.reduce((sum, chunk) => sum + Number(chunk.referenceDensity || 0), 0) / allChunks.length).toFixed(4))
        : 0
    },
    parsingGapCounts: normalizedDocs.reduce((acc, doc) => {
      for (const gap of doc.stats?.parsingGaps || []) {
        const key = String(gap);
        acc[key] = (acc[key] || 0) + 1;
      }
      return acc;
    }, {}),
    lowStructureRepairUsageCounts: normalizedDocs.reduce((acc, doc) => {
      const counts = doc.stats.repairStrategyCounts || {};
      for (const [k, v] of Object.entries(counts)) {
        acc[k] = (acc[k] || 0) + Number(v || 0);
      }
      return acc;
    }, {}),
    documentsImprovedByRepair: 0,
    readinessChangedAfterRepair: 0,
    docsStillReviewNeededAfterRepair: 0,
    docsStillBlockedAfterRepair: 0,
    includeText: Boolean(input.includeText),
    realOnly: Boolean(input.realOnly)
  };

  const documentsImprovedByRepair = [];
  const docsStillReviewNeededAfterRepair = [];
  const docsStillBlockedAfterRepair = [];
  const readinessChangedAfterRepair = [];

  for (const doc of normalizedDocs) {
    const preStats = {
      ...doc.stats,
      chunkCount: doc.stats.preRepairChunkCount,
      chunkTypeSpread: doc.stats.preRepairChunkTypeSpread,
      chunksFlaggedMixedTopic: doc.stats.preRepairChunksFlaggedMixedTopic,
      chunksWithCanonicalReferenceAlignment: doc.stats.preRepairChunksWithCanonicalReferenceAlignment
    };
    const preStatus = deriveReadinessStatusFromStats(preStats);
    const postStatus = deriveReadinessStatusFromStats(doc.stats);
    const improved =
      doc.stats.repairApplied &&
      (doc.stats.postRepairChunkCount > doc.stats.preRepairChunkCount ||
        doc.stats.chunkTypeSpread > doc.stats.preRepairChunkTypeSpread ||
        doc.stats.chunksFlaggedMixedTopic < doc.stats.preRepairChunksFlaggedMixedTopic ||
        doc.stats.chunksWithCanonicalReferenceAlignment > doc.stats.preRepairChunksWithCanonicalReferenceAlignment);
    if (improved) {
      documentsImprovedByRepair.push({
        documentId: doc.document.documentId,
        title: doc.document.title,
        preRepairChunkCount: doc.stats.preRepairChunkCount,
        postRepairChunkCount: doc.stats.postRepairChunkCount,
        repairStrategyCounts: doc.stats.repairStrategyCounts,
        preRepairReadinessStatus: preStatus,
        postRepairReadinessStatus: postStatus
      });
    }
    if (doc.stats.repairApplied && preStatus !== postStatus) {
      readinessChangedAfterRepair.push({
        documentId: doc.document.documentId,
        title: doc.document.title,
        preRepairReadinessStatus: preStatus,
        postRepairReadinessStatus: postStatus
      });
    }
    if (postStatus === "retrieval_review_needed") {
      docsStillReviewNeededAfterRepair.push({
        documentId: doc.document.documentId,
        title: doc.document.title
      });
    }
    if (postStatus === "retrieval_blocked") {
      docsStillBlockedAfterRepair.push({
        documentId: doc.document.documentId,
        title: doc.document.title
      });
    }
  }

  summary.documentsImprovedByRepair = documentsImprovedByRepair.length;
  summary.readinessChangedAfterRepair = readinessChangedAfterRepair.length;
  summary.docsStillReviewNeededAfterRepair = docsStillReviewNeededAfterRepair.length;
  summary.docsStillBlockedAfterRepair = docsStillBlockedAfterRepair.length;

  const documentsNeedingChunkReview = normalizedDocs
    .map((doc) => {
      const reasons = reviewNeeded(doc);
      return reasons.length
        ? {
            documentId: doc.document.documentId,
            title: doc.document.title,
            reasons
          }
        : null;
    })
    .filter(Boolean);

  const documentsOverusingFallback = normalizedDocs
    .filter((doc) => doc.stats.usedFallbackChunking)
    .map((doc) => ({ documentId: doc.document.documentId, title: doc.document.title, chunkCount: doc.stats.chunkCount }));

  const documentsWithLowTypeDiversity = normalizedDocs
    .filter((doc) => doc.stats.chunkTypeSpread <= 2)
    .map((doc) => ({ documentId: doc.document.documentId, title: doc.document.title, chunkTypeSpread: doc.stats.chunkTypeSpread }));

  const documentsWithMixedTopicChunks = normalizedDocs
    .filter((doc) => Number(doc.stats.chunksFlaggedMixedTopic || 0) > 0)
    .map((doc) => ({
      documentId: doc.document.documentId,
      title: doc.document.title,
      chunksFlaggedMixedTopic: doc.stats.chunksFlaggedMixedTopic
    }));

  const documentsWithPoorReferenceAlignment = normalizedDocs
    .filter((doc) => {
      const total = doc.stats.chunkCount || 1;
      const alignedRatio = Number(doc.stats.chunksWithCanonicalReferenceAlignment || 0) / total;
      return alignedRatio < 0.2;
    })
    .map((doc) => ({
      documentId: doc.document.documentId,
      title: doc.document.title,
      chunksWithCanonicalReferenceAlignment: doc.stats.chunksWithCanonicalReferenceAlignment,
      chunkCount: doc.stats.chunkCount
    }));

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    documentsImprovedByRepair,
    readinessChangedAfterRepair,
    docsStillReviewNeededAfterRepair,
    docsStillBlockedAfterRepair,
    documentsNeedingChunkReview,
    documentsOverusingFallback,
    documentsWithLowTypeDiversity,
    documentsWithMixedTopicChunks,
    documentsWithPoorReferenceAlignment,
    documents: normalizedDocs
  };
}

export function formatRetrievalChunkMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Chunk Report (R3)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) {
    if (
      [
        "chunkTypeCounts",
        "chunkClassificationReasonCounts",
        "retrievalPriorityCounts",
        "referenceDensityStats",
        "parsingGapCounts",
        "lowStructureRepairUsageCounts"
      ].includes(key)
    ) {
      continue;
    }
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Chunk Type Counts");
  for (const [k, v] of Object.entries(report.summary.chunkTypeCounts || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.summary.chunkTypeCounts || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Chunk Classification Reason Counts");
  for (const [k, v] of Object.entries(report.summary.chunkClassificationReasonCounts || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.summary.chunkClassificationReasonCounts || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Retrieval Priority Counts");
  for (const [k, v] of Object.entries(report.summary.retrievalPriorityCounts || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.summary.retrievalPriorityCounts || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Reference Density Stats");
  lines.push(`- min: ${report.summary.referenceDensityStats?.min ?? 0}`);
  lines.push(`- max: ${report.summary.referenceDensityStats?.max ?? 0}`);
  lines.push(`- avg: ${report.summary.referenceDensityStats?.avg ?? 0}`);
  lines.push("");

  lines.push("## Low-Structure Repair Usage Counts");
  for (const [k, v] of Object.entries(report.summary.lowStructureRepairUsageCounts || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.summary.lowStructureRepairUsageCounts || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Repair Impact");
  lines.push(`- documentsImprovedByRepair: ${report.summary.documentsImprovedByRepair || 0}`);
  lines.push(`- readinessChangedAfterRepair: ${report.summary.readinessChangedAfterRepair || 0}`);
  lines.push(`- docsStillReviewNeededAfterRepair: ${report.summary.docsStillReviewNeededAfterRepair || 0}`);
  lines.push(`- docsStillBlockedAfterRepair: ${report.summary.docsStillBlockedAfterRepair || 0}`);
  lines.push("");

  const sections = [
    ["Documents Improved By Repair", report.documentsImprovedByRepair, (row) => `- ${row.documentId} | ${row.title} | pre=${row.preRepairReadinessStatus} post=${row.postRepairReadinessStatus}`],
    ["Readiness Changed After Repair", report.readinessChangedAfterRepair, (row) => `- ${row.documentId} | ${row.title} | pre=${row.preRepairReadinessStatus} post=${row.postRepairReadinessStatus}`],
    ["Docs Still Review Needed After Repair", report.docsStillReviewNeededAfterRepair, (row) => `- ${row.documentId} | ${row.title}`],
    ["Docs Still Blocked After Repair", report.docsStillBlockedAfterRepair, (row) => `- ${row.documentId} | ${row.title}`],
    ["Documents Needing Chunk Review", report.documentsNeedingChunkReview, (row) => `- ${row.documentId} | ${row.title} | reasons=${row.reasons.join(",")}`],
    ["Documents Overusing Fallback", report.documentsOverusingFallback, (row) => `- ${row.documentId} | ${row.title} | chunkCount=${row.chunkCount}`],
    ["Documents With Low Type Diversity", report.documentsWithLowTypeDiversity, (row) => `- ${row.documentId} | ${row.title} | chunkTypeSpread=${row.chunkTypeSpread}`],
    ["Documents With Mixed Topic Chunks", report.documentsWithMixedTopicChunks, (row) => `- ${row.documentId} | ${row.title} | chunksFlaggedMixedTopic=${row.chunksFlaggedMixedTopic}`],
    ["Documents With Poor Reference Alignment", report.documentsWithPoorReferenceAlignment, (row) => `- ${row.documentId} | ${row.title} | aligned=${row.chunksWithCanonicalReferenceAlignment}/${row.chunkCount}`]
  ];

  for (const [title, rows, formatter] of sections) {
    lines.push(`## ${title}`);
    if (!(rows || []).length) {
      lines.push("- none");
    } else {
      for (const row of rows) lines.push(formatter(row));
    }
    lines.push("");
  }

  lines.push("## Documents");
  for (const doc of report.documents || []) {
    lines.push(`- ${doc.document.documentId} | ${doc.document.title}`);
    lines.push(`  - chunkCount=${doc.stats.chunkCount} headingCount=${doc.stats.headingCount} paragraphCount=${doc.stats.paragraphCount}`);
    lines.push(`  - chunkTypeSpread=${doc.stats.chunkTypeSpread} usedFallbackChunking=${doc.stats.usedFallbackChunking}`);
    lines.push(
      `  - repairApplied=${doc.stats.repairApplied} preRepairChunkCount=${doc.stats.preRepairChunkCount} postRepairChunkCount=${doc.stats.postRepairChunkCount}`
    );
    lines.push(
      `  - retrievalPriorityCounts=${Object.entries(doc.stats.retrievalPriorityCounts || {})
        .map(([k, v]) => `${k}:${v}`)
        .join(",") || "<none>"}`
    );
  }

  lines.push("");
  lines.push("- Read-only report only. No ingestion, mutation, or approval changes are performed.");
  return `${lines.join("\n")}\n`;
}
