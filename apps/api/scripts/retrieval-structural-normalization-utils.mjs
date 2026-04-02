import { buildRetrievalPromotionRehearsalReport } from "./retrieval-promotion-rehearsal-utils.mjs";
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

function sortedEntries(counts) {
  return Object.entries(counts || {}).sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return String(a[0]).localeCompare(String(b[0]));
  });
}

function mapDocById(documents) {
  const map = new Map();
  for (const doc of documents || []) {
    const id = doc?.document?.documentId;
    if (id) map.set(id, doc);
  }
  return map;
}

function normalizeTitleFamily(title) {
  const t = String(title || "").toLowerCase().trim();
  if (!t) return "untitled";
  if (/^l\d+/.test(t)) return "family_l_decision";
  if (/^t\d+/.test(t)) return "family_t_decision";
  if (/minute order/.test(t)) return "family_minute_order";
  if (/known reference/.test(t)) return "family_known_reference";
  if (/metadata cleanup/.test(t)) return "family_metadata_cleanup";
  return "family_other";
}

function patternLabelFromSignals(parts) {
  const [fallback, headingBand, spreadBand, alignmentBand, mixedBand, titleFamily] = parts;
  return `fallback:${fallback} | headings:${headingBand} | spread:${spreadBand} | align:${alignmentBand} | mixed:${mixedBand} | ${titleFamily}`;
}

function buildStructuralPatternKey(row) {
  const stats = row.keyStats || {};
  const fallback = stats.usedFallbackChunking ? "fallback" : "structured";
  const headingBand = Number(stats.headingCount || 0) === 0 ? "h0" : Number(stats.headingCount || 0) <= 2 ? "h1_2" : "h3p";
  const spreadBand = Number(stats.chunkTypeSpread || 0) <= 1 ? "spread1" : Number(stats.chunkTypeSpread || 0) <= 2 ? "spread2" : "spread3p";
  const alignmentRatio = Number(stats.chunkCount || 0) > 0 ? Number(stats.chunksWithCanonicalReferenceAlignment || 0) / Number(stats.chunkCount || 1) : 0;
  const alignmentBand = alignmentRatio === 0 ? "align0" : alignmentRatio < 0.25 ? "align_low" : "align_ok";
  const mixedBand = Number(stats.chunksFlaggedMixedTopic || 0) > 0 ? "mixed" : "clean";
  const titleFamily = normalizeTitleFamily(row.title);

  const parts = [fallback, headingBand, spreadBand, alignmentBand, mixedBand, titleFamily];
  return {
    structuralPatternKey: parts.join("::"),
    structuralPatternLabel: patternLabelFromSignals(parts),
    structuralPatternConfidence: Number((0.5 + Math.min(0.45, (row.patternFamilySizeHint || 1) * 0.03)).toFixed(4)),
    signatureParts: parts
  };
}

function statsFromDoc(doc) {
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

function preNormalizationStats(base) {
  return {
    ...base,
    repairApplied: false,
    repairStrategyCounts: {},
    chunkCount: Number(base.chunkCount || 0),
    chunkTypeSpread: Number(base.chunkTypeSpread || 0),
    chunksFlaggedMixedTopic: Number(base.chunksFlaggedMixedTopic || 0),
    chunksWithCanonicalReferenceAlignment: Number(base.chunksWithCanonicalReferenceAlignment || 0)
  };
}

function simulateNormalization(stats, patternKey) {
  const simulated = { ...stats, repairApplied: true, repairStrategyCounts: { ...(stats.repairStrategyCounts || {}) } };
  const strategies = [];

  const hasFallback = patternKey.includes("fallback");
  const noHeadings = patternKey.includes("h0");
  const lowSpread = patternKey.includes("spread1") || patternKey.includes("spread2");
  const lowAlign = patternKey.includes("align0") || patternKey.includes("align_low");
  const mixed = patternKey.includes("mixed");

  if (hasFallback && noHeadings && simulated.paragraphCount >= 8) {
    simulated.headingCount = Math.max(simulated.headingCount, 3);
    simulated.chunksWithWeakHeadingSignal = Math.max(0, simulated.chunksWithWeakHeadingSignal - 2);
    simulated.chunkTypeSpread = Math.min(Math.max(simulated.chunkCount, 1), simulated.chunkTypeSpread + 1);
    simulated.repairStrategyCounts.micro_heading_normalization = Number(simulated.repairStrategyCounts.micro_heading_normalization || 0) + 1;
    strategies.push("micro_heading_normalization");
  }

  if (mixed && simulated.chunkCount >= 3) {
    simulated.chunksFlaggedMixedTopic = Math.max(0, simulated.chunksFlaggedMixedTopic - 1);
    simulated.chunkTypeSpread = Math.min(Math.max(simulated.chunkCount, 1), simulated.chunkTypeSpread + 1);
    simulated.repairStrategyCounts.low_structure_discourse_split = Number(simulated.repairStrategyCounts.low_structure_discourse_split || 0) + 1;
    strategies.push("low_structure_discourse_split");
  }

  if (lowAlign && simulated.chunkCount > 0) {
    const targetAlign = Math.max(simulated.chunksWithCanonicalReferenceAlignment, Math.ceil(simulated.chunkCount * 0.35));
    simulated.chunksWithCanonicalReferenceAlignment = targetAlign;
    simulated.repairStrategyCounts.citation_density_boundary_split = Number(simulated.repairStrategyCounts.citation_density_boundary_split || 0) + 1;
    strategies.push("citation_density_boundary_split");
  }

  if (hasFallback && lowSpread && simulated.chunkCount <= 4) {
    simulated.chunkCount += 1;
    simulated.chunkTypeSpread = Math.min(Math.max(simulated.chunkCount, 1), simulated.chunkTypeSpread + 1);
    simulated.usedFallbackChunking = simulated.headingCount > 1 ? false : simulated.usedFallbackChunking;
    simulated.avgChunkLength = Math.max(180, Math.round(simulated.avgChunkLength * 0.9));
    simulated.repairStrategyCounts.disposition_tail_split = Number(simulated.repairStrategyCounts.disposition_tail_split || 0) + 1;
    strategies.push("disposition_tail_split");
  }

  if (!strategies.length) {
    simulated.repairApplied = false;
  }

  return {
    simulated,
    normalizationStrategiesApplied: uniqueSorted(strategies)
  };
}

function evaluate(apiBase, input, rowMeta, stats) {
  const doc = {
    document: {
      documentId: rowMeta.documentId,
      title: rowMeta.title,
      sourceFileRef: rowMeta.sourceFileRef,
      sourceLink: rowMeta.sourceLink
    },
    isLikelyFixture: false,
    stats,
    chunks: []
  };

  const readiness = buildRetrievalReadinessReport({ apiBase, input: { ...input, realOnly: false }, documents: [doc] });
  const admission = buildRetrievalCorpusAdmissionReport({ apiBase, input: { ...input, realOnly: false }, documents: [doc] });

  return {
    readiness: readiness.documents[0],
    admission: admission.documents[0]
  };
}

function outcomeRank(outcome) {
  if (outcome === "promote_after_normalization") return 0;
  if (outcome === "still_hold_after_normalization") return 1;
  return 2;
}

export function buildRetrievalStructuralNormalizationReport({ apiBase, input, documents }) {
  const rehearsal = buildRetrievalPromotionRehearsalReport({ apiBase, input, documents });
  const docsById = mapDocById(documents);

  const targets = (rehearsal.documents || []).filter((row) =>
    ["still_hold_after_repair", "still_exclude_after_repair"].includes(row.promotionOutcome)
  );

  const withPatterns = targets.map((row) => {
    const pattern = buildStructuralPatternKey(row);
    return {
      ...row,
      ...pattern
    };
  });

  const familySizeMap = countBy(withPatterns.map((row) => row.structuralPatternKey));

  const normalizedRows = withPatterns.map((row) => {
    const sourceDoc = docsById.get(row.documentId);
    if (!sourceDoc) return null;

    const base = statsFromDoc(sourceDoc);
    const pre = preNormalizationStats(base);
    const simulated = simulateNormalization(base, row.structuralPatternKey);

    const preEval = evaluate(apiBase, input, row, pre);
    const postEval = evaluate(apiBase, input, row, simulated.simulated);

    const preAdmission = preEval.admission?.corpusAdmissionStatus || "hold_for_repair_review";
    const postAdmission = postEval.admission?.corpusAdmissionStatus || "hold_for_repair_review";

    let postNormalizationPromotionOutcome = "still_hold_after_normalization";
    if (postAdmission === "admit_now") postNormalizationPromotionOutcome = "promote_after_normalization";
    else if (postAdmission === "exclude_from_initial_corpus") postNormalizationPromotionOutcome = "still_exclude_after_normalization";

    const improvementReasons = [];
    if (simulated.simulated.chunkTypeSpread > pre.chunkTypeSpread) improvementReasons.push("chunk_type_spread_improved");
    if (simulated.simulated.chunksWithCanonicalReferenceAlignment > pre.chunksWithCanonicalReferenceAlignment) {
      improvementReasons.push("canonical_alignment_improved");
    }
    if (simulated.simulated.chunksFlaggedMixedTopic < pre.chunksFlaggedMixedTopic) improvementReasons.push("mixed_topic_reduced");
    if (simulated.simulated.usedFallbackChunking !== pre.usedFallbackChunking) improvementReasons.push("fallback_usage_changed");
    if (preEval.readiness?.readinessStatus !== postEval.readiness?.readinessStatus) improvementReasons.push("readiness_upgraded");
    if (preAdmission !== postAdmission) improvementReasons.push("corpus_admission_upgraded");
    if (!improvementReasons.length) improvementReasons.push("no_material_improvement");

    const remainingNormalizationBlockers =
      postAdmission === "admit_now"
        ? []
        : uniqueSorted([...(postEval.admission?.corpusAdmissionReasons || []), ...(postEval.admission?.corpusAdmissionWarnings || [])]);

    return {
      ...row,
      structuralPatternConfidence: Number((0.5 + Math.min(0.45, (familySizeMap[row.structuralPatternKey] || 1) * 0.03)).toFixed(4)),
      patternFamilySize: Number(familySizeMap[row.structuralPatternKey] || 1),
      normalizationRehearsalApplied: simulated.normalizationStrategiesApplied.length > 0,
      normalizationStrategiesApplied: simulated.normalizationStrategiesApplied,
      preNormalizationAdmissionStatus: preAdmission,
      postNormalizationAdmissionStatus: postAdmission,
      preNormalizationReadinessStatus: preEval.readiness?.readinessStatus,
      postNormalizationReadinessStatus: postEval.readiness?.readinessStatus,
      preNormalizationChunkTypeSpread: pre.chunkTypeSpread,
      postNormalizationChunkTypeSpread: simulated.simulated.chunkTypeSpread,
      preNormalizationCanonicalAlignmentCount: pre.chunksWithCanonicalReferenceAlignment,
      postNormalizationCanonicalAlignmentCount: simulated.simulated.chunksWithCanonicalReferenceAlignment,
      preNormalizationFallbackUsed: pre.usedFallbackChunking,
      postNormalizationFallbackUsed: simulated.simulated.usedFallbackChunking,
      normalizationImprovementReasons: uniqueSorted(improvementReasons),
      remainingNormalizationBlockers,
      postNormalizationPromotionOutcome
    };
  });

  const rows = normalizedRows
    .filter(Boolean)
    .sort((a, b) => {
      const rankDelta = outcomeRank(a.postNormalizationPromotionOutcome) - outcomeRank(b.postNormalizationPromotionOutcome);
      if (rankDelta !== 0) return rankDelta;
      if ((b.patternFamilySize || 0) !== (a.patternFamilySize || 0)) return (b.patternFamilySize || 0) - (a.patternFamilySize || 0);
      return String(a.documentId || "").localeCompare(String(b.documentId || ""));
    });

  const clustersRaw = new Map();
  for (const row of rows) {
    const key = row.structuralPatternKey;
    const current = clustersRaw.get(key) || {
      patternKey: key,
      label: row.structuralPatternLabel,
      documentCount: 0,
      heldCount: 0,
      excludedCount: 0,
      commonSignals: [],
      commonBlockers: [],
      commonWarnings: [],
      sampleDocumentIds: [],
      likelyNormalizationOpportunityScore: 0
    };

    current.documentCount += 1;
    if (row.preRepairCorpusAdmissionStatus === "hold_for_repair_review") current.heldCount += 1;
    if (row.preRepairCorpusAdmissionStatus === "exclude_from_initial_corpus") current.excludedCount += 1;
    current.commonSignals.push(...(row.repairOpportunityTypes || []));
    current.commonBlockers.push(...(row.remainingPromotionBlockers || []));
    current.commonWarnings.push(...(row.corpusAdmissionWarnings || []));
    if (current.sampleDocumentIds.length < 10) current.sampleDocumentIds.push(row.documentId);
    if (row.normalizationRehearsalApplied) current.likelyNormalizationOpportunityScore += 1;

    clustersRaw.set(key, current);
  }

  const patternClusters = [...clustersRaw.values()]
    .map((cluster) => ({
      ...cluster,
      commonSignals: sortedEntries(countBy(cluster.commonSignals)).slice(0, 6).map(([k]) => k),
      commonBlockers: sortedEntries(countBy(cluster.commonBlockers)).slice(0, 6).map(([k]) => k),
      commonWarnings: sortedEntries(countBy(cluster.commonWarnings)).slice(0, 6).map(([k]) => k)
    }))
    .sort((a, b) => {
      if (b.documentCount !== a.documentCount) return b.documentCount - a.documentCount;
      return String(a.patternKey).localeCompare(String(b.patternKey));
    });

  const largestPatternClusters = patternClusters.slice(0, 20);
  const patternClustersByHeldCount = [...patternClusters]
    .sort((a, b) => {
      if (b.heldCount !== a.heldCount) return b.heldCount - a.heldCount;
      return String(a.patternKey).localeCompare(String(b.patternKey));
    })
    .slice(0, 20);
  const patternClustersByExcludedCount = [...patternClusters]
    .sort((a, b) => {
      if (b.excludedCount !== a.excludedCount) return b.excludedCount - a.excludedCount;
      return String(a.patternKey).localeCompare(String(b.patternKey));
    })
    .slice(0, 20);
  const patternClustersByLikelyNormalizationOpportunity = [...patternClusters]
    .sort((a, b) => {
      if (b.likelyNormalizationOpportunityScore !== a.likelyNormalizationOpportunityScore) {
        return b.likelyNormalizationOpportunityScore - a.likelyNormalizationOpportunityScore;
      }
      return String(a.patternKey).localeCompare(String(b.patternKey));
    })
    .slice(0, 20);

  const promoteAfterNormalizationDocuments = rows.filter((row) => row.postNormalizationPromotionOutcome === "promote_after_normalization");
  const stillHoldAfterNormalizationDocuments = rows.filter((row) => row.postNormalizationPromotionOutcome === "still_hold_after_normalization");
  const stillExcludeAfterNormalizationDocuments = rows.filter((row) => row.postNormalizationPromotionOutcome === "still_exclude_after_normalization");

  const summary = {
    documentsAnalyzed: rows.length,
    realOnly: Boolean(input.realOnly),
    includeText: Boolean(input.includeText),
    promoteAfterNormalizationCount: promoteAfterNormalizationDocuments.length,
    stillHoldAfterNormalizationCount: stillHoldAfterNormalizationDocuments.length,
    stillExcludeAfterNormalizationCount: stillExcludeAfterNormalizationDocuments.length,
    patternClusterCount: patternClusters.length
  };

  const patternBundles = {};
  for (const cluster of patternClusters) {
    patternBundles[cluster.patternKey] = rows
      .filter((row) => row.structuralPatternKey === cluster.patternKey)
      .map((row) => row.documentId);
  }

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    patternClusters,
    largestPatternClusters,
    patternClustersByHeldCount,
    patternClustersByExcludedCount,
    patternClustersByLikelyNormalizationOpportunity,
    countsByPostNormalizationPromotionOutcome: countBy(rows.map((row) => row.postNormalizationPromotionOutcome)),
    countsByNormalizationStrategy: countBy(rows.flatMap((row) => row.normalizationStrategiesApplied || [])),
    countsByNormalizationImprovementReason: countBy(rows.flatMap((row) => row.normalizationImprovementReasons || [])),
    countsByRemainingNormalizationBlocker: countBy(rows.flatMap((row) => row.remainingNormalizationBlockers || [])),
    promoteAfterNormalizationDocuments,
    stillHoldAfterNormalizationDocuments,
    stillExcludeAfterNormalizationDocuments,
    normalizationOutcomeBundles: {
      promoteAfterNormalizationDocIds: promoteAfterNormalizationDocuments.map((row) => row.documentId),
      stillHoldDocIds: stillHoldAfterNormalizationDocuments.map((row) => row.documentId),
      stillExcludeDocIds: stillExcludeAfterNormalizationDocuments.map((row) => row.documentId)
    },
    patternBundles,
    documents: rows
  };
}

export function formatRetrievalStructuralNormalizationMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Structural Normalization Rehearsal Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Largest Pattern Clusters");
  for (const cluster of report.largestPatternClusters || []) {
    lines.push(
      `- ${cluster.patternKey} | docs=${cluster.documentCount} held=${cluster.heldCount} excluded=${cluster.excludedCount} | signals=${(cluster.commonSignals || []).join(",") || "<none>"}`
    );
  }
  if (!(report.largestPatternClusters || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Outcome Counts");
  for (const [k, v] of Object.entries(report.countsByPostNormalizationPromotionOutcome || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByPostNormalizationPromotionOutcome || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Normalization Bundles");
  lines.push(`- promoteAfterNormalizationDocIds: ${(report.normalizationOutcomeBundles?.promoteAfterNormalizationDocIds || []).length}`);
  lines.push(`- stillHoldDocIds: ${(report.normalizationOutcomeBundles?.stillHoldDocIds || []).length}`);
  lines.push(`- stillExcludeDocIds: ${(report.normalizationOutcomeBundles?.stillExcludeDocIds || []).length}`);
  lines.push("");

  lines.push("- Read-only normalization rehearsal only. No corpus admission changes, no embedding/index writes, and no ingestion/QC mutations.");
  return `${lines.join("\n")}\n`;
}
