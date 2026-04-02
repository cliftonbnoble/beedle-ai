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

function classifyPromotion(row) {
  const stats = row.keyStats || {};
  const reasons = [];
  const blockers = [];
  const opportunities = [];

  const chunkCount = Number(stats.chunkCount || 0);
  const chunkTypeSpread = Number(stats.chunkTypeSpread || 0);
  const fallback = Boolean(stats.usedFallbackChunking);
  const mixed = Number(stats.chunksFlaggedMixedTopic || 0);
  const weakHeading = Number(stats.chunksWithWeakHeadingSignal || 0);
  const aligned = Number(stats.chunksWithCanonicalReferenceAlignment || 0);
  const headingCount = Number(stats.headingCount || 0);
  const paragraphCount = Number(stats.paragraphCount || 0);
  const avgChunkLength = Number(stats.avgChunkLength || 0);
  const alignmentRatio = chunkCount > 0 ? aligned / chunkCount : 0;

  if (chunkCount === 0) blockers.push("no_chunks_generated");
  if (paragraphCount >= 10 && chunkCount <= 1) blockers.push("severe_undersegmented_structure");
  if (avgChunkLength >= 1700) blockers.push("overlong_average_chunks");
  if (chunkTypeSpread <= 1 && chunkCount <= 2) blockers.push("severe_low_type_diversity");
  if (alignmentRatio === 0 && chunkCount >= 3) blockers.push("zero_canonical_alignment");
  if (fallback && headingCount === 0 && paragraphCount >= 12) blockers.push("missing_heading_structure_signals");

  if (fallback && (weakHeading > 0 || headingCount > 0)) {
    reasons.push("heading_repair_candidate");
    opportunities.push("heading_repair");
  }

  if (alignmentRatio < 0.25 && paragraphCount >= 6) {
    reasons.push("reference_alignment_repair_candidate");
    opportunities.push("reference_alignment_repair");
  }

  if (mixed > 0 && chunkCount >= 3) {
    reasons.push("mixed_topic_split_candidate");
    opportunities.push("mixed_topic_repair");
  }

  if (fallback && chunkCount <= 3 && paragraphCount >= 8) {
    reasons.push("fallback_reduction_candidate");
    opportunities.push("fallback_reduction");
  }

  if (chunkTypeSpread <= 2 && chunkCount >= 3) {
    reasons.push("chunk_type_diversity_repair_candidate");
    opportunities.push("chunk_type_diversity_repair");
  }

  const severeBlockers = blockers.filter((value) =>
    ["no_chunks_generated", "severe_undersegmented_structure", "overlong_average_chunks"].includes(value)
  );

  const wouldPromoteIfHeadingRepair =
    opportunities.includes("heading_repair") && severeBlockers.length === 0 && chunkCount >= 2 && alignmentRatio >= 0.15;
  const wouldPromoteIfReferenceAlignmentRepair =
    opportunities.includes("reference_alignment_repair") && severeBlockers.length === 0 && chunkCount >= 2;
  const wouldPromoteIfMixedTopicRepair =
    opportunities.includes("mixed_topic_repair") && severeBlockers.length === 0 && chunkCount >= 3;
  const wouldPromoteIfFallbackReduction =
    opportunities.includes("fallback_reduction") && severeBlockers.length === 0 && paragraphCount >= 8;

  const wouldPromoteIfCombinedNarrowRepairs =
    severeBlockers.length === 0 &&
    opportunities.length >= 2 &&
    !blockers.includes("severe_low_type_diversity") &&
    !(blockers.includes("zero_canonical_alignment") && !wouldPromoteIfReferenceAlignmentRepair);

  let promotionConfidenceScore = 25;
  promotionConfidenceScore += opportunities.length * 14;
  promotionConfidenceScore -= blockers.length * 11;
  if (wouldPromoteIfCombinedNarrowRepairs) promotionConfidenceScore += 12;
  if (row.readinessChangedAfterRepair) promotionConfidenceScore += 8;
  promotionConfidenceScore = Math.max(0, Math.min(100, promotionConfidenceScore));

  let promotionClass = "not_promotable_without_manual_review";
  if (wouldPromoteIfCombinedNarrowRepairs && promotionConfidenceScore >= 70) {
    promotionClass = "repair_promotable_high_confidence";
  } else if ((wouldPromoteIfCombinedNarrowRepairs || opportunities.length >= 2) && promotionConfidenceScore >= 55) {
    promotionClass = "repair_promotable_medium_confidence";
  } else if (opportunities.length >= 1 && promotionConfidenceScore >= 35 && severeBlockers.length === 0) {
    promotionClass = "repair_promotable_low_confidence";
  }

  if (severeBlockers.length > 0 || opportunities.length === 0) {
    promotionClass = "not_promotable_without_manual_review";
  }

  return {
    promotionClass,
    promotionConfidenceScore,
    promotionReasons: uniqueSorted(reasons),
    promotionBlockers: uniqueSorted(blockers),
    repairOpportunityTypes: uniqueSorted(opportunities),
    wouldPromoteIfHeadingRepair,
    wouldPromoteIfReferenceAlignmentRepair,
    wouldPromoteIfMixedTopicRepair,
    wouldPromoteIfFallbackReduction,
    wouldPromoteIfCombinedNarrowRepairs,
    triageSummary:
      promotionClass === "repair_promotable_high_confidence"
        ? "High-confidence narrow repair candidate for next admission pass."
        : promotionClass === "repair_promotable_medium_confidence"
          ? "Likely promotable with targeted narrow repairs."
          : promotionClass === "repair_promotable_low_confidence"
            ? "Potentially promotable but needs careful targeted repair validation."
            : "Not promotable safely without manual structural review."
  };
}

function sortRows(rows) {
  const rank = {
    repair_promotable_high_confidence: 0,
    repair_promotable_medium_confidence: 1,
    repair_promotable_low_confidence: 2,
    not_promotable_without_manual_review: 3
  };

  return [...rows].sort((a, b) => {
    const classDelta = Number(rank[a.promotionClass] ?? 9) - Number(rank[b.promotionClass] ?? 9);
    if (classDelta !== 0) return classDelta;
    if (a.promotionConfidenceScore !== b.promotionConfidenceScore) return b.promotionConfidenceScore - a.promotionConfidenceScore;
    return String(a.documentId || "").localeCompare(String(b.documentId || ""));
  });
}

export function buildRetrievalPromotionTriageReport({ apiBase, input, documents }) {
  const admission = buildRetrievalCorpusAdmissionReport({ apiBase, input, documents });

  const triageTargets = (admission.documents || []).filter(
    (row) =>
      !row.isLikelyFixture &&
      ["hold_for_repair_review", "exclude_from_initial_corpus"].includes(row.corpusAdmissionStatus)
  );

  const triagedRows = sortRows(
    triageTargets.map((row) => {
      const classified = classifyPromotion(row);
      return {
        ...row,
        ...classified
      };
    })
  );

  const highConfidencePromotableDocuments = triagedRows.filter((row) => row.promotionClass === "repair_promotable_high_confidence");
  const mediumConfidencePromotableDocuments = triagedRows.filter((row) => row.promotionClass === "repair_promotable_medium_confidence");
  const lowConfidencePromotableDocuments = triagedRows.filter((row) => row.promotionClass === "repair_promotable_low_confidence");
  const manualReviewRequiredDocuments = triagedRows.filter((row) => row.promotionClass === "not_promotable_without_manual_review");

  const documentsLikelyPromotableByHeadingRepair = triagedRows.filter((row) => row.wouldPromoteIfHeadingRepair);
  const documentsLikelyPromotableByReferenceAlignmentRepair = triagedRows.filter((row) => row.wouldPromoteIfReferenceAlignmentRepair);
  const documentsLikelyPromotableByMixedTopicRepair = triagedRows.filter((row) => row.wouldPromoteIfMixedTopicRepair);
  const documentsLikelyPromotableByCombinedNarrowRepairs = triagedRows.filter((row) => row.wouldPromoteIfCombinedNarrowRepairs);

  const countsByPromotionClass = countBy(triagedRows.map((row) => row.promotionClass));
  const countsByPromotionReason = countBy(triagedRows.flatMap((row) => row.promotionReasons || []));
  const countsByPromotionBlocker = countBy(triagedRows.flatMap((row) => row.promotionBlockers || []));

  const summary = {
    documentsAnalyzed: triagedRows.length,
    realOnly: Boolean(input.realOnly),
    includeText: Boolean(input.includeText),
    highConfidencePromotableCount: highConfidencePromotableDocuments.length,
    mediumConfidencePromotableCount: mediumConfidencePromotableDocuments.length,
    lowConfidencePromotableCount: lowConfidencePromotableDocuments.length,
    manualReviewRequiredCount: manualReviewRequiredDocuments.length,
    likelyPromotableByHeadingRepairCount: documentsLikelyPromotableByHeadingRepair.length,
    likelyPromotableByReferenceAlignmentRepairCount: documentsLikelyPromotableByReferenceAlignmentRepair.length,
    likelyPromotableByMixedTopicRepairCount: documentsLikelyPromotableByMixedTopicRepair.length,
    likelyPromotableByCombinedNarrowRepairsCount: documentsLikelyPromotableByCombinedNarrowRepairs.length
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    countsByPromotionClass,
    countsByPromotionReason,
    countsByPromotionBlocker,
    highConfidencePromotableDocuments,
    mediumConfidencePromotableDocuments,
    lowConfidencePromotableDocuments,
    manualReviewRequiredDocuments,
    documentsLikelyPromotableByHeadingRepair,
    documentsLikelyPromotableByReferenceAlignmentRepair,
    documentsLikelyPromotableByMixedTopicRepair,
    documentsLikelyPromotableByCombinedNarrowRepairs,
    promotionBundles: {
      highConfidenceDocIds: highConfidencePromotableDocuments.map((row) => row.documentId),
      mediumConfidenceDocIds: mediumConfidencePromotableDocuments.map((row) => row.documentId),
      lowConfidenceDocIds: lowConfidencePromotableDocuments.map((row) => row.documentId),
      manualReviewDocIds: manualReviewRequiredDocuments.map((row) => row.documentId)
    },
    documents: triagedRows
  };
}

export function formatRetrievalPromotionTriageMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Promotion Triage Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Counts By Promotion Class");
  for (const [k, v] of Object.entries(report.countsByPromotionClass || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByPromotionClass || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Promotion Reason");
  for (const [k, v] of Object.entries(report.countsByPromotionReason || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByPromotionReason || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts By Promotion Blocker");
  for (const [k, v] of Object.entries(report.countsByPromotionBlocker || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByPromotionBlocker || {}).length) lines.push("- none");
  lines.push("");

  const sections = [
    ["High Confidence Promotable", report.highConfidencePromotableDocuments],
    ["Medium Confidence Promotable", report.mediumConfidencePromotableDocuments],
    ["Low Confidence Promotable", report.lowConfidencePromotableDocuments],
    ["Manual Review Required", report.manualReviewRequiredDocuments]
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
        `- ${row.documentId} | ${row.title} | class=${row.promotionClass} score=${row.promotionConfidenceScore} | reasons=${row.promotionReasons.join(",") || "<none>"} | blockers=${row.promotionBlockers.join(",") || "<none>"}`
      );
    }
    lines.push("");
  }

  lines.push("## Promotion Bundles");
  lines.push(`- highConfidenceDocIds: ${(report.promotionBundles?.highConfidenceDocIds || []).length}`);
  lines.push(`- mediumConfidenceDocIds: ${(report.promotionBundles?.mediumConfidenceDocIds || []).length}`);
  lines.push(`- lowConfidenceDocIds: ${(report.promotionBundles?.lowConfidenceDocIds || []).length}`);
  lines.push(`- manualReviewDocIds: ${(report.promotionBundles?.manualReviewDocIds || []).length}`);
  lines.push("");

  lines.push("- Read-only triage only. No admission state change, no embedding writes, no index writes, and no ingestion/QC mutations.");
  return `${lines.join("\n")}\n`;
}
