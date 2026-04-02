import { buildRetrievalPromotionTriageReport } from "./retrieval-promotion-triage-utils.mjs";
import { buildRetrievalReadinessReport } from "./retrieval-readiness-report-utils.mjs";
import { buildRetrievalCorpusAdmissionReport } from "./retrieval-corpus-admission-utils.mjs";

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

function mapDocById(documents) {
  const map = new Map();
  for (const doc of documents || []) {
    const id = doc?.document?.documentId;
    if (id) map.set(id, doc);
  }
  return map;
}

function buildBaseStatsFromDoc(doc) {
  return {
    sectionCount: Number(doc?.stats?.sectionCount || 0),
    chunkCount: Number(doc?.stats?.chunkCount || 0),
    chunkTypeSpread: Number(doc?.stats?.chunkTypeSpread || 0),
    usedFallbackChunking: Boolean(doc?.stats?.usedFallbackChunking),
    avgChunkLength: Number(doc?.stats?.avgChunkLength || 0),
    maxChunkLength: Number(doc?.stats?.maxChunkLength || 0),
    minChunkLength: Number(doc?.stats?.minChunkLength || 0),
    headingCount: Number(doc?.stats?.headingCount || 0),
    paragraphCount: Number(doc?.stats?.paragraphCount || 0),
    chunksFlaggedMixedTopic: Number(doc?.stats?.chunksFlaggedMixedTopic || 0),
    chunksFlaggedOverlong: Number(doc?.stats?.chunksFlaggedOverlong || 0),
    chunksWithWeakHeadingSignal: Number(doc?.stats?.chunksWithWeakHeadingSignal || 0),
    chunksWithCanonicalReferenceAlignment: Number(doc?.stats?.chunksWithCanonicalReferenceAlignment || 0),
    referenceDensityStats: doc?.stats?.referenceDensityStats || { min: 0, max: 0, avg: 0 },
    chunkTypeCounts: doc?.stats?.chunkTypeCounts || {},
    chunkClassificationReasonCounts: doc?.stats?.chunkClassificationReasonCounts || {},
    retrievalPriorityCounts: doc?.stats?.retrievalPriorityCounts || {},
    parsingGaps: Array.isArray(doc?.stats?.parsingGaps) ? doc.stats.parsingGaps : [],
    repairApplied: Boolean(doc?.stats?.repairApplied),
    repairStrategyCounts: doc?.stats?.repairStrategyCounts || {},
    preRepairChunkCount: Number(doc?.stats?.preRepairChunkCount ?? doc?.stats?.chunkCount ?? 0),
    postRepairChunkCount: Number(doc?.stats?.postRepairChunkCount ?? doc?.stats?.chunkCount ?? 0),
    preRepairChunkTypeSpread: Number(doc?.stats?.preRepairChunkTypeSpread ?? doc?.stats?.chunkTypeSpread ?? 0),
    preRepairChunksFlaggedMixedTopic: Number(doc?.stats?.preRepairChunksFlaggedMixedTopic || 0),
    preRepairChunksWithCanonicalReferenceAlignment: Number(doc?.stats?.preRepairChunksWithCanonicalReferenceAlignment || 0)
  };
}

function buildPreRepairStats(base) {
  return {
    ...base,
    chunkCount: Number(base.preRepairChunkCount || base.chunkCount || 0),
    chunkTypeSpread: Number(base.preRepairChunkTypeSpread || base.chunkTypeSpread || 0),
    chunksFlaggedMixedTopic: Number(base.preRepairChunksFlaggedMixedTopic || base.chunksFlaggedMixedTopic || 0),
    chunksWithCanonicalReferenceAlignment: Number(
      base.preRepairChunksWithCanonicalReferenceAlignment || base.chunksWithCanonicalReferenceAlignment || 0
    ),
    repairApplied: false,
    repairStrategyCounts: {},
    postRepairChunkCount: Number(base.preRepairChunkCount || base.chunkCount || 0)
  };
}

function simulatePostRepairStats(currentStats, triageRow) {
  const simulated = {
    ...currentStats,
    repairApplied: true,
    repairStrategyCounts: { ...(currentStats.repairStrategyCounts || {}) }
  };

  const applied = [];

  if (triageRow.wouldPromoteIfHeadingRepair) {
    simulated.headingCount = Math.max(simulated.headingCount, 2);
    simulated.chunksWithWeakHeadingSignal = Math.max(0, simulated.chunksWithWeakHeadingSignal - Math.max(1, Math.floor(simulated.chunkCount * 0.5)));
    simulated.chunkTypeSpread = Math.min(Math.max(simulated.chunkCount, 1), simulated.chunkTypeSpread + 1);
    simulated.repairStrategyCounts.low_structure_discourse_split = Number(simulated.repairStrategyCounts.low_structure_discourse_split || 0) + 1;
    applied.push("heading_repair");
  }

  if (triageRow.wouldPromoteIfReferenceAlignmentRepair) {
    const target = Math.max(simulated.chunksWithCanonicalReferenceAlignment, Math.ceil(Math.max(simulated.chunkCount, 1) * 0.4));
    simulated.chunksWithCanonicalReferenceAlignment = target;
    simulated.repairStrategyCounts.citation_density_boundary_split = Number(simulated.repairStrategyCounts.citation_density_boundary_split || 0) + 1;
    applied.push("reference_alignment_repair");
  }

  if (triageRow.wouldPromoteIfMixedTopicRepair) {
    simulated.chunksFlaggedMixedTopic = Math.max(0, simulated.chunksFlaggedMixedTopic - 1);
    simulated.chunkTypeSpread = Math.min(Math.max(simulated.chunkCount, 1), simulated.chunkTypeSpread + 1);
    simulated.repairStrategyCounts.low_structure_discourse_split = Number(simulated.repairStrategyCounts.low_structure_discourse_split || 0) + 1;
    applied.push("mixed_topic_repair");
  }

  if (triageRow.wouldPromoteIfFallbackReduction) {
    simulated.usedFallbackChunking = false;
    if (simulated.chunkCount <= 3) {
      simulated.chunkCount += 1;
      simulated.postRepairChunkCount = simulated.chunkCount;
    }
    simulated.chunkTypeSpread = Math.min(Math.max(simulated.chunkCount, 1), simulated.chunkTypeSpread + 1);
    simulated.repairStrategyCounts.disposition_tail_split = Number(simulated.repairStrategyCounts.disposition_tail_split || 0) + 1;
    applied.push("fallback_reduction");
  }

  if (applied.length > 0) {
    simulated.avgChunkLength = Number(Math.max(180, Math.round(simulated.avgChunkLength * 0.88)));
    simulated.maxChunkLength = Number(Math.max(simulated.avgChunkLength, Math.round(simulated.maxChunkLength * 0.9)));
  }

  return {
    simulated,
    repairStrategiesApplied: uniqueSorted(applied)
  };
}

function evaluateStatuses(apiBase, input, docMeta, stats) {
  const rowDoc = {
    document: {
      documentId: docMeta.documentId,
      title: docMeta.title,
      sourceFileRef: docMeta.sourceFileRef,
      sourceLink: docMeta.sourceLink
    },
    isLikelyFixture: false,
    stats,
    chunks: []
  };

  const readiness = buildRetrievalReadinessReport({
    apiBase,
    input: { ...input, realOnly: false },
    documents: [rowDoc]
  });
  const admission = buildRetrievalCorpusAdmissionReport({
    apiBase,
    input: { ...input, realOnly: false },
    documents: [rowDoc]
  });

  return {
    readiness: readiness.documents[0],
    admission: admission.documents[0]
  };
}

function outcomeRank(outcome) {
  if (outcome === "promote_after_repair") return 0;
  if (outcome === "still_hold_after_repair") return 1;
  return 2;
}

export function buildRetrievalPromotionRehearsalReport({ apiBase, input, documents }) {
  const triage = buildRetrievalPromotionTriageReport({ apiBase, input, documents });
  const docsById = mapDocById(documents);

  const lowConfidenceRows = (triage.documents || []).filter(
    (row) => row.promotionClass === "repair_promotable_low_confidence" && !row.isLikelyFixture
  );

  const rehearsed = [];

  for (const row of lowConfidenceRows) {
    const sourceDoc = docsById.get(row.documentId);
    if (!sourceDoc) continue;

    const currentStats = buildBaseStatsFromDoc(sourceDoc);
    const preStats = buildPreRepairStats(currentStats);
    const postSim = simulatePostRepairStats(currentStats, row);

    const preEval = evaluateStatuses(apiBase, input, row, preStats);
    const postEval = evaluateStatuses(apiBase, input, row, postSim.simulated);

    const preAdmissionStatus = preEval.admission?.corpusAdmissionStatus || "hold_for_repair_review";
    const preAdmissionScore = Number(preEval.admission?.corpusAdmissionScore || 0);
    const postAdmissionStatus = postEval.admission?.corpusAdmissionStatus || "hold_for_repair_review";
    const postAdmissionScore = Number(postEval.admission?.corpusAdmissionScore || 0);

    let promotionOutcome = "still_hold_after_repair";
    if (postAdmissionStatus === "admit_now") promotionOutcome = "promote_after_repair";
    else if (postAdmissionStatus === "exclude_from_initial_corpus") promotionOutcome = "still_exclude_after_repair";

    const improvementReasons = [];
    if (postSim.simulated.chunkCount > preStats.chunkCount) improvementReasons.push("chunk_count_increased");
    if (postSim.simulated.chunkTypeSpread > preStats.chunkTypeSpread) improvementReasons.push("chunk_type_spread_improved");
    if (postSim.simulated.chunksFlaggedMixedTopic < preStats.chunksFlaggedMixedTopic) improvementReasons.push("mixed_topic_reduced");
    if (postSim.simulated.chunksWithCanonicalReferenceAlignment > preStats.chunksWithCanonicalReferenceAlignment) {
      improvementReasons.push("canonical_alignment_improved");
    }
    if (preEval.readiness?.readinessStatus !== postEval.readiness?.readinessStatus) improvementReasons.push("readiness_upgraded");
    if (preAdmissionStatus !== postAdmissionStatus) improvementReasons.push("corpus_admission_upgraded");
    if (!improvementReasons.length) improvementReasons.push("no_material_improvement");

    const remainingPromotionBlockers =
      postAdmissionStatus === "admit_now"
        ? []
        : uniqueSorted([...(postEval.admission?.corpusAdmissionReasons || []), ...(postEval.admission?.corpusAdmissionWarnings || [])]);

    rehearsed.push({
      ...row,
      preRepairPromotionClass: row.promotionClass,
      postRepairSimulatedAdmissionStatus: postAdmissionStatus,
      postRepairSimulatedAdmissionScore: postAdmissionScore,
      promotionOutcome,
      promotionDeltaScore: Number((postAdmissionScore - preAdmissionScore).toFixed(4)),
      repairStrategiesApplied: postSim.repairStrategiesApplied,
      repairImprovementReasons: uniqueSorted(improvementReasons),
      remainingPromotionBlockers,
      preRepairChunkCount: preStats.chunkCount,
      postRepairChunkCount: postSim.simulated.chunkCount,
      preRepairChunkTypeSpread: preStats.chunkTypeSpread,
      postRepairChunkTypeSpread: postSim.simulated.chunkTypeSpread,
      preRepairMixedTopicCount: preStats.chunksFlaggedMixedTopic,
      postRepairMixedTopicCount: postSim.simulated.chunksFlaggedMixedTopic,
      preRepairCanonicalAlignmentCount: preStats.chunksWithCanonicalReferenceAlignment,
      postRepairCanonicalAlignmentCount: postSim.simulated.chunksWithCanonicalReferenceAlignment,
      preRepairFallbackUsed: preStats.usedFallbackChunking,
      postRepairFallbackUsed: postSim.simulated.usedFallbackChunking,
      preRepairReadinessStatus: preEval.readiness?.readinessStatus,
      postRepairReadinessStatus: postEval.readiness?.readinessStatus,
      preRepairCorpusAdmissionStatus: preAdmissionStatus,
      postRepairCorpusAdmissionStatus: postAdmissionStatus
    });
  }

  const orderedRows = [...rehearsed].sort((a, b) => {
    const rankDelta = outcomeRank(a.promotionOutcome) - outcomeRank(b.promotionOutcome);
    if (rankDelta !== 0) return rankDelta;
    if (b.promotionDeltaScore !== a.promotionDeltaScore) return b.promotionDeltaScore - a.promotionDeltaScore;
    return String(a.documentId || "").localeCompare(String(b.documentId || ""));
  });

  const promoteAfterRepairDocuments = orderedRows.filter((row) => row.promotionOutcome === "promote_after_repair");
  const stillHoldAfterRepairDocuments = orderedRows.filter((row) => row.promotionOutcome === "still_hold_after_repair");
  const stillExcludeAfterRepairDocuments = orderedRows.filter((row) => row.promotionOutcome === "still_exclude_after_repair");

  const largestPromotionDeltaDocuments = [...orderedRows]
    .sort((a, b) => {
      if (b.promotionDeltaScore !== a.promotionDeltaScore) return b.promotionDeltaScore - a.promotionDeltaScore;
      return String(a.documentId || "").localeCompare(String(b.documentId || ""));
    })
    .slice(0, 25);

  const documentsImprovedInChunkTypeSpread = orderedRows.filter((row) => row.postRepairChunkTypeSpread > row.preRepairChunkTypeSpread);
  const documentsImprovedInReferenceAlignment = orderedRows.filter(
    (row) => row.postRepairCanonicalAlignmentCount > row.preRepairCanonicalAlignmentCount
  );
  const documentsImprovedInMixedTopicReduction = orderedRows.filter((row) => row.postRepairMixedTopicCount < row.preRepairMixedTopicCount);

  const summary = {
    documentsAnalyzed: orderedRows.length,
    realOnly: Boolean(input.realOnly),
    includeText: Boolean(input.includeText),
    promoteAfterRepairCount: promoteAfterRepairDocuments.length,
    stillHoldAfterRepairCount: stillHoldAfterRepairDocuments.length,
    stillExcludeAfterRepairCount: stillExcludeAfterRepairDocuments.length,
    avgPromotionDeltaScore:
      orderedRows.length > 0
        ? Number((orderedRows.reduce((sum, row) => sum + Number(row.promotionDeltaScore || 0), 0) / orderedRows.length).toFixed(4))
        : 0
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    countsByPromotionOutcome: countBy(orderedRows.map((row) => row.promotionOutcome)),
    countsByRepairStrategy: countBy(orderedRows.flatMap((row) => row.repairStrategiesApplied || [])),
    countsByImprovementReason: countBy(orderedRows.flatMap((row) => row.repairImprovementReasons || [])),
    countsByRemainingBlocker: countBy(orderedRows.flatMap((row) => row.remainingPromotionBlockers || [])),
    promoteAfterRepairDocuments,
    stillHoldAfterRepairDocuments,
    stillExcludeAfterRepairDocuments,
    largestPromotionDeltaDocuments,
    documentsImprovedInChunkTypeSpread,
    documentsImprovedInReferenceAlignment,
    documentsImprovedInMixedTopicReduction,
    promotionOutcomeBundles: {
      promoteAfterRepairDocIds: promoteAfterRepairDocuments.map((row) => row.documentId),
      stillHoldDocIds: stillHoldAfterRepairDocuments.map((row) => row.documentId),
      stillExcludeDocIds: stillExcludeAfterRepairDocuments.map((row) => row.documentId)
    },
    documents: orderedRows
  };
}

export function formatRetrievalPromotionRehearsalMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Promotion Rehearsal Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Counts By Promotion Outcome");
  for (const [k, v] of Object.entries(report.countsByPromotionOutcome || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByPromotionOutcome || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Repair Strategy");
  for (const [k, v] of Object.entries(report.countsByRepairStrategy || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByRepairStrategy || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Improvement Reason");
  for (const [k, v] of Object.entries(report.countsByImprovementReason || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByImprovementReason || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Remaining Blocker");
  for (const [k, v] of Object.entries(report.countsByRemainingBlocker || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByRemainingBlocker || {}).length) lines.push("- none");
  lines.push("");

  const sections = [
    ["Promote After Repair", report.promoteAfterRepairDocuments],
    ["Still Hold After Repair", report.stillHoldAfterRepairDocuments],
    ["Still Exclude After Repair", report.stillExcludeAfterRepairDocuments]
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
        `- ${row.documentId} | ${row.title} | outcome=${row.promotionOutcome} | postAdmission=${row.postRepairSimulatedAdmissionStatus} score=${row.postRepairSimulatedAdmissionScore} | delta=${row.promotionDeltaScore}`
      );
    }
    lines.push("");
  }

  lines.push("## Promotion Outcome Bundles");
  lines.push(`- promoteAfterRepairDocIds: ${(report.promotionOutcomeBundles?.promoteAfterRepairDocIds || []).length}`);
  lines.push(`- stillHoldDocIds: ${(report.promotionOutcomeBundles?.stillHoldDocIds || []).length}`);
  lines.push(`- stillExcludeDocIds: ${(report.promotionOutcomeBundles?.stillExcludeDocIds || []).length}`);
  lines.push("");

  lines.push("- Read-only rehearsal only. No admission change, no embedding/index writes, and no ingestion/QC mutations.");
  return `${lines.join("\n")}\n`;
}
