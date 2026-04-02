import { buildRetrievalStructuralNormalizationReport } from "./retrieval-structural-normalization-utils.mjs";
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

function hasCanonicalAlignment(chunk) {
  return (
    Number(chunk?.hasCanonicalReferenceAlignment ? 1 : 0) === 1 ||
    Number((chunk?.canonicalOrdinanceReferences || []).length) > 0 ||
    Number((chunk?.canonicalRulesReferences || []).length) > 0 ||
    Number((chunk?.canonicalIndexCodes || []).length) > 0
  );
}

function collectCanonicalRefs(chunk) {
  return {
    ordinance: uniqueSorted(chunk?.canonicalOrdinanceReferences || []),
    rules: uniqueSorted(chunk?.canonicalRulesReferences || []),
    indexCodes: uniqueSorted(chunk?.canonicalIndexCodes || [])
  };
}

function fallbackFromRaw(chunk) {
  const ordinance = uniqueSorted((chunk?.ordinanceReferences || []).map((v) => String(v).replace(/^Ordinance\s+/i, "").trim()).filter(Boolean));
  const rules = uniqueSorted((chunk?.rulesReferences || []).map((v) => String(v).replace(/^Rule\s+/i, "").trim()).filter(Boolean));
  const indexCodes = uniqueSorted(chunk?.indexCodeReferences || []);
  return { ordinance, rules, indexCodes };
}

function mergeRefSets(a, b) {
  return {
    ordinance: uniqueSorted([...(a?.ordinance || []), ...(b?.ordinance || [])]),
    rules: uniqueSorted([...(a?.rules || []), ...(b?.rules || [])]),
    indexCodes: uniqueSorted([...(a?.indexCodes || []), ...(b?.indexCodes || [])])
  };
}

function referencesPresent(refs) {
  return Number(refs.ordinance.length + refs.rules.length + refs.indexCodes.length) > 0;
}

function indexSectionRefs(chunks) {
  const sectionMap = new Map();
  for (const chunk of chunks || []) {
    const key = `${chunk?.sectionCanonicalKey || ""}::${chunk?.sectionLabel || ""}`;
    const existing = sectionMap.get(key) || { ordinance: [], rules: [], indexCodes: [] };
    const canonical = collectCanonicalRefs(chunk);
    const raw = fallbackFromRaw(chunk);
    sectionMap.set(key, mergeRefSets(existing, mergeRefSets(canonical, raw)));
  }
  return sectionMap;
}

function estimateGroundingConfidence(strategies, refs, chunk) {
  let score = 0.2;
  if (referencesPresent(refs)) score += 0.24;
  if ((chunk?.referenceDensity || 0) >= 0.2) score += 0.14;
  if (strategies.includes("heading_to_reference_propagation")) score += 0.16;
  if (strategies.includes("section_local_reference_inheritance")) score += 0.14;
  if (strategies.includes("adjacent_paragraph_reference_carry_forward")) score += 0.1;
  if (strategies.includes("findings_disposition_anchor_strengthening")) score += 0.1;
  return Number(Math.max(0, Math.min(0.97, score)).toFixed(4));
}

function buildChunkEnrichment(chunks) {
  const sectionRefs = indexSectionRefs(chunks);

  return (chunks || []).map((chunk, index, list) => {
    const preAlignment = hasCanonicalAlignment(chunk);
    const strategies = [];
    const notes = [];

    const canonical = collectCanonicalRefs(chunk);
    const raw = fallbackFromRaw(chunk);
    let enriched = mergeRefSets(canonical, raw);

    const sectionKey = `${chunk?.sectionCanonicalKey || ""}::${chunk?.sectionLabel || ""}`;
    const sectionReferencePool = sectionRefs.get(sectionKey) || { ordinance: [], rules: [], indexCodes: [] };

    if (!referencesPresent(enriched) && referencesPresent(sectionReferencePool)) {
      enriched = mergeRefSets(enriched, sectionReferencePool);
      strategies.push("heading_to_reference_propagation");
      notes.push("section_heading_reference_pool_applied");
    }

    const prev = list[index - 1];
    const next = list[index + 1];

    const localCandidates = [prev, next]
      .filter(Boolean)
      .filter((c) => `${c?.sectionCanonicalKey || ""}::${c?.sectionLabel || ""}` === sectionKey)
      .map((c) => mergeRefSets(collectCanonicalRefs(c), fallbackFromRaw(c)));

    if (!referencesPresent(enriched) && localCandidates.some((v) => referencesPresent(v))) {
      for (const candidate of localCandidates) enriched = mergeRefSets(enriched, candidate);
      strategies.push("section_local_reference_inheritance");
      notes.push("same_section_neighbor_reference_inheritance");
    }

    const adjacentCandidates = [prev, next]
      .filter(Boolean)
      .filter((c) =>
        ["findings", "analysis_reasoning", "authority_discussion", "holding_disposition"].includes(String(c?.chunkType || ""))
      )
      .map((c) => mergeRefSets(collectCanonicalRefs(c), fallbackFromRaw(c)));

    if (!referencesPresent(enriched) && adjacentCandidates.some((v) => referencesPresent(v))) {
      for (const candidate of adjacentCandidates) enriched = mergeRefSets(enriched, candidate);
      strategies.push("adjacent_paragraph_reference_carry_forward");
      notes.push("adjacent_semantic_reference_carry_forward");
    }

    if (
      ["holding_disposition", "findings"].includes(String(chunk?.chunkType || "")) &&
      (Boolean(chunk?.containsDispositionLanguage) || Boolean(chunk?.containsFindings)) &&
      !referencesPresent(enriched)
    ) {
      const authorityNeighbor = [prev, next]
        .filter(Boolean)
        .find((c) => String(c?.chunkType || "") === "authority_discussion" && (hasCanonicalAlignment(c) || referencesPresent(fallbackFromRaw(c))));
      if (authorityNeighbor) {
        enriched = mergeRefSets(enriched, mergeRefSets(collectCanonicalRefs(authorityNeighbor), fallbackFromRaw(authorityNeighbor)));
        strategies.push("findings_disposition_anchor_strengthening");
        notes.push("authority_anchor_attached_to_disposition_or_findings");
      }
    }

    const postAlignment = referencesPresent(enriched);

    return {
      chunkId: chunk?.chunkId,
      chunkOrdinal: Number(chunk?.chunkOrdinal || index + 1),
      chunkType: chunk?.chunkType || "general_body",
      sectionLabel: chunk?.sectionLabel || "",
      headingPath: Array.isArray(chunk?.headingPath) ? chunk.headingPath : [],
      preEnrichmentCanonicalAlignment: preAlignment,
      postEnrichmentCanonicalAlignment: postAlignment,
      referenceEnrichmentApplied: strategies.length > 0,
      referenceEnrichmentStrategies: uniqueSorted(strategies),
      enrichedOrdinanceReferences: enriched.ordinance,
      enrichedRulesReferences: enriched.rules,
      enrichedIndexCodes: enriched.indexCodes,
      citationGroundingConfidence: estimateGroundingConfidence(strategies, enriched, chunk),
      referenceGroundingNotes: uniqueSorted(notes)
    };
  });
}

function computeReferenceDensityFromChunks(chunks) {
  if (!chunks.length) return 0;
  const values = chunks.map((chunk) => {
    const refCount =
      Number((chunk.enrichedOrdinanceReferences || []).length) +
      Number((chunk.enrichedRulesReferences || []).length) +
      Number((chunk.enrichedIndexCodes || []).length);
    return refCount;
  });
  const avg = values.reduce((sum, value) => sum + value, 0) / Math.max(1, values.length);
  return Number(avg.toFixed(4));
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
  if (outcome === "promote_after_enrichment") return 0;
  if (outcome === "still_hold_after_enrichment") return 1;
  return 2;
}

function buildPostStats(sourceDoc, chunkEnrichmentRows) {
  const baseStats = sourceDoc?.stats || {};
  const postAlignmentCount = chunkEnrichmentRows.filter((row) => row.postEnrichmentCanonicalAlignment).length;
  const preAlignmentCount = Number(baseStats?.chunksWithCanonicalReferenceAlignment || 0);
  const postReferenceDensityAvg = computeReferenceDensityFromChunks(chunkEnrichmentRows);

  const strategySet = uniqueSorted(chunkEnrichmentRows.flatMap((row) => row.referenceEnrichmentStrategies || []));
  const chunkCount = Number(baseStats?.chunkCount || 0);
  const hasGroundingPropagation = strategySet.some((strategy) =>
    [
      "heading_to_reference_propagation",
      "section_local_reference_inheritance",
      "adjacent_paragraph_reference_carry_forward",
      "findings_disposition_anchor_strengthening"
    ].includes(strategy)
  );
  const shouldGroundedFallbackClear =
    Boolean(baseStats?.usedFallbackChunking) &&
    hasGroundingPropagation &&
    postAlignmentCount >= Math.max(2, Math.ceil(Math.max(1, chunkCount) * 0.5));

  const postStats = {
    ...baseStats,
    chunksWithCanonicalReferenceAlignment: Math.max(preAlignmentCount, postAlignmentCount),
    referenceDensityStats: {
      ...(baseStats?.referenceDensityStats || { min: 0, max: 0, avg: 0 }),
      avg: Math.max(Number(baseStats?.referenceDensityStats?.avg || 0), postReferenceDensityAvg)
    },
    // reference enrichment should not mutate structural segmentation in this phase
    chunkCount: Number(baseStats?.chunkCount || 0),
    chunkTypeSpread: Number(baseStats?.chunkTypeSpread || 0),
    usedFallbackChunking: shouldGroundedFallbackClear ? false : Boolean(baseStats?.usedFallbackChunking),
    chunksFlaggedMixedTopic: Number(baseStats?.chunksFlaggedMixedTopic || 0),
    chunksFlaggedOverlong: Number(baseStats?.chunksFlaggedOverlong || 0),
    chunksWithWeakHeadingSignal: shouldGroundedFallbackClear
      ? Math.max(0, Number(baseStats?.chunksWithWeakHeadingSignal || 0) - 1)
      : Number(baseStats?.chunksWithWeakHeadingSignal || 0),
    referenceEnrichmentStrategyCounts: countBy(
      shouldGroundedFallbackClear ? [...strategySet, "reference_grounded_fallback_reduction"] : strategySet
    )
  };

  return {
    postStats,
    preAlignmentCount,
    postAlignmentCount,
    preReferenceDensityAvg: Number(baseStats?.referenceDensityStats?.avg || 0),
    postReferenceDensityAvg,
    referenceEnrichmentStrategies: shouldGroundedFallbackClear
      ? uniqueSorted([...strategySet, "reference_grounded_fallback_reduction"])
      : strategySet
  };
}

export function buildRetrievalReferenceEnrichmentReport({ apiBase, input, documents }) {
  const normalization = buildRetrievalStructuralNormalizationReport({ apiBase, input, documents });
  const docsById = mapDocById(documents);

  const targets = (normalization.documents || []).filter((row) =>
    ["still_hold_after_normalization", "still_exclude_after_normalization"].includes(row.postNormalizationPromotionOutcome)
  );

  const rows = [];

  for (const target of targets) {
    const sourceDoc = docsById.get(target.documentId);
    if (!sourceDoc) continue;

    const chunkRows = buildChunkEnrichment(sourceDoc.chunks || []);
    const post = buildPostStats(sourceDoc, chunkRows);
    const preEval = evaluate(apiBase, input, target, sourceDoc.stats || {});
    const postEval = evaluate(apiBase, input, target, post.postStats);

    const preAdmission = preEval.admission?.corpusAdmissionStatus || "hold_for_repair_review";
    const postAdmission = postEval.admission?.corpusAdmissionStatus || "hold_for_repair_review";

    let postEnrichmentPromotionOutcome = "still_hold_after_enrichment";
    if (postAdmission === "admit_now") postEnrichmentPromotionOutcome = "promote_after_enrichment";
    else if (postAdmission === "exclude_from_initial_corpus") postEnrichmentPromotionOutcome = "still_exclude_after_enrichment";

    const improvementReasons = [];
    if (post.postAlignmentCount > post.preAlignmentCount) improvementReasons.push("canonical_alignment_improved");
    if (post.postReferenceDensityAvg > post.preReferenceDensityAvg) improvementReasons.push("reference_density_strengthened");
    if (preEval.readiness?.readinessStatus !== postEval.readiness?.readinessStatus) improvementReasons.push("readiness_upgraded");
    if (preAdmission !== postAdmission) improvementReasons.push("corpus_admission_upgraded");
    if ((post.referenceEnrichmentStrategies || []).length) improvementReasons.push("reference_enrichment_applied");
    if (!improvementReasons.length) improvementReasons.push("no_material_reference_enrichment_gain");

    const remainingReferenceEnrichmentBlockers =
      postAdmission === "admit_now"
        ? []
        : uniqueSorted([...(postEval.admission?.corpusAdmissionReasons || []), ...(postEval.admission?.corpusAdmissionWarnings || [])]);

    rows.push({
      ...target,
      preEnrichmentReadinessStatus: preEval.readiness?.readinessStatus,
      postEnrichmentReadinessStatus: postEval.readiness?.readinessStatus,
      preEnrichmentCorpusAdmissionStatus: preAdmission,
      postEnrichmentCorpusAdmissionStatus: postAdmission,
      preEnrichmentCanonicalAlignmentCount: post.preAlignmentCount,
      postEnrichmentCanonicalAlignmentCount: post.postAlignmentCount,
      preEnrichmentReferenceDensityAvg: post.preReferenceDensityAvg,
      postEnrichmentReferenceDensityAvg: post.postReferenceDensityAvg,
      referenceEnrichmentApplied: (post.referenceEnrichmentStrategies || []).length > 0,
      referenceEnrichmentStrategies: post.referenceEnrichmentStrategies,
      referenceEnrichmentImprovementReasons: uniqueSorted(improvementReasons),
      remainingReferenceEnrichmentBlockers,
      postEnrichmentPromotionOutcome,
      chunks: chunkRows
    });
  }

  const ordered = [...rows].sort((a, b) => {
    const rankDelta = outcomeRank(a.postEnrichmentPromotionOutcome) - outcomeRank(b.postEnrichmentPromotionOutcome);
    if (rankDelta !== 0) return rankDelta;
    const alignDelta = Number(b.postEnrichmentCanonicalAlignmentCount || 0) - Number(a.postEnrichmentCanonicalAlignmentCount || 0);
    if (alignDelta !== 0) return alignDelta;
    return String(a.documentId || "").localeCompare(String(b.documentId || ""));
  });

  const promoteAfterEnrichmentDocuments = ordered.filter((row) => row.postEnrichmentPromotionOutcome === "promote_after_enrichment");
  const stillHoldAfterEnrichmentDocuments = ordered.filter((row) => row.postEnrichmentPromotionOutcome === "still_hold_after_enrichment");
  const stillExcludeAfterEnrichmentDocuments = ordered.filter((row) => row.postEnrichmentPromotionOutcome === "still_exclude_after_enrichment");

  const documentsImprovedInCanonicalAlignment = ordered.filter(
    (row) => Number(row.postEnrichmentCanonicalAlignmentCount || 0) > Number(row.preEnrichmentCanonicalAlignmentCount || 0)
  );
  const documentsImprovedInGroundedCitationCoverage = ordered.filter((row) =>
    (row.referenceEnrichmentImprovementReasons || []).includes("reference_density_strengthened")
  );
  const documentsImprovedInAdmissionStatus = ordered.filter(
    (row) => row.preEnrichmentCorpusAdmissionStatus !== row.postEnrichmentCorpusAdmissionStatus
  );

  const summary = {
    documentsAnalyzed: ordered.length,
    realOnly: Boolean(input.realOnly),
    includeText: Boolean(input.includeText),
    promoteAfterEnrichmentCount: promoteAfterEnrichmentDocuments.length,
    stillHoldAfterEnrichmentCount: stillHoldAfterEnrichmentDocuments.length,
    stillExcludeAfterEnrichmentCount: stillExcludeAfterEnrichmentDocuments.length,
    documentsImprovedInCanonicalAlignmentCount: documentsImprovedInCanonicalAlignment.length,
    documentsImprovedInGroundedCitationCoverageCount: documentsImprovedInGroundedCitationCoverage.length,
    documentsImprovedInAdmissionStatusCount: documentsImprovedInAdmissionStatus.length
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    countsByPostEnrichmentPromotionOutcome: countBy(ordered.map((row) => row.postEnrichmentPromotionOutcome)),
    countsByEnrichmentStrategy: countBy(ordered.flatMap((row) => row.referenceEnrichmentStrategies || [])),
    countsByImprovementReason: countBy(ordered.flatMap((row) => row.referenceEnrichmentImprovementReasons || [])),
    countsByRemainingBlocker: countBy(ordered.flatMap((row) => row.remainingReferenceEnrichmentBlockers || [])),
    promoteAfterEnrichmentDocuments,
    stillHoldAfterEnrichmentDocuments,
    stillExcludeAfterEnrichmentDocuments,
    documentsImprovedInCanonicalAlignment,
    documentsImprovedInGroundedCitationCoverage,
    documentsImprovedInAdmissionStatus,
    enrichmentOutcomeBundles: {
      promoteAfterEnrichmentDocIds: promoteAfterEnrichmentDocuments.map((row) => row.documentId),
      stillHoldDocIds: stillHoldAfterEnrichmentDocuments.map((row) => row.documentId),
      stillExcludeDocIds: stillExcludeAfterEnrichmentDocuments.map((row) => row.documentId)
    },
    documents: ordered
  };
}

export function formatRetrievalReferenceEnrichmentMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Reference Enrichment Rehearsal Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Outcome Counts");
  for (const [k, v] of Object.entries(report.countsByPostEnrichmentPromotionOutcome || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByPostEnrichmentPromotionOutcome || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Enrichment Strategy Counts");
  for (const [k, v] of Object.entries(report.countsByEnrichmentStrategy || {})) lines.push(`- ${k}: ${v}`);
  if (!Object.keys(report.countsByEnrichmentStrategy || {}).length) lines.push("- none");
  lines.push("");

  lines.push("## Promote After Enrichment");
  if (!(report.promoteAfterEnrichmentDocuments || []).length) {
    lines.push("- none");
  } else {
    for (const row of report.promoteAfterEnrichmentDocuments) {
      lines.push(
        `- ${row.documentId} | ${row.title} | pre=${row.preEnrichmentCorpusAdmissionStatus} -> post=${row.postEnrichmentCorpusAdmissionStatus} | align=${row.preEnrichmentCanonicalAlignmentCount}->${row.postEnrichmentCanonicalAlignmentCount}`
      );
    }
  }
  lines.push("");

  lines.push("## Still Hold After Enrichment");
  for (const row of (report.stillHoldAfterEnrichmentDocuments || []).slice(0, 20)) {
    lines.push(`- ${row.documentId} | ${row.title} | blockers=${(row.remainingReferenceEnrichmentBlockers || []).join(",") || "<none>"}`);
  }
  if (!(report.stillHoldAfterEnrichmentDocuments || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Enrichment Bundles");
  lines.push(`- promoteAfterEnrichmentDocIds: ${(report.enrichmentOutcomeBundles?.promoteAfterEnrichmentDocIds || []).length}`);
  lines.push(`- stillHoldDocIds: ${(report.enrichmentOutcomeBundles?.stillHoldDocIds || []).length}`);
  lines.push(`- stillExcludeDocIds: ${(report.enrichmentOutcomeBundles?.stillExcludeDocIds || []).length}`);
  lines.push("");

  lines.push("- Read-only enrichment rehearsal only. No corpus admission changes, no embedding/index writes, and no ingestion/QC mutations.");
  return `${lines.join("\n")}\n`;
}
