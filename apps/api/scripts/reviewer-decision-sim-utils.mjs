function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}
const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function hasUnsafeBlockedFamily(packet) {
  return (packet?.blocked37xFamily || []).some((family) => UNSAFE_37X.has(String(family)));
}

function docDispositionFromContext(contextClass) {
  if (contextClass === "likely_ordinance_wording") return "manual_context_review_candidate";
  if (contextClass === "likely_rules_wording") return "escalate";
  if (contextClass === "mixed_ambiguous_wording") return "escalate";
  if (contextClass === "no_useful_context") return "insufficient_context";
  return "keep_blocked";
}

function dominantRootCauses(packet) {
  return (packet.topRootCausesByCount || []).slice(0, 4);
}

function subBucketForDoc(doc) {
  if (doc.simulatedRecommendation === "manual_context_review_candidate") return "ordinance_like_manual_review_candidates";
  if (doc.simulatedRecommendation === "insufficient_context") return "insufficient_context_hold";
  if (doc.simulatedRecommendation === "keep_blocked") return "true_not_found_hold";
  if (doc.simulatedRecommendation === "escalate" && doc.contextSignal === "mixed_ambiguous_wording") return "mixed_cross_context_escalate";
  if (doc.simulatedRecommendation === "escalate") return "ambiguous_context_escalate";
  return "ambiguous_context_escalate";
}

function docRecommendations(packet) {
  const byDoc = new Map();
  const snippets = (packet.patternEvidence || []).flatMap((pattern) => pattern.representativeSnippets || []);
  for (const snippet of snippets) {
    const list = byDoc.get(snippet.documentId) || [];
    list.push(snippet);
    byDoc.set(snippet.documentId, list);
  }
  const sampleDocs = packet.sampleDocs || [];
  return sampleDocs.map((doc) => {
    const snippetsForDoc = byDoc.get(doc.documentId) || [];
    const contexts = snippetsForDoc.map((item) => item.contextClass);
    const contextCounts = countBy(contexts);
    const topContext = contextCounts[0]?.key || "no_useful_context";
    const simulatedRecommendation = docDispositionFromContext(topContext);
    const reason =
      simulatedRecommendation === "manual_context_review_candidate"
        ? "Snippet context is ordinance-like; manual context review may be productive."
        : simulatedRecommendation === "escalate"
          ? "Snippet context is rules-like or mixed; escalate for legal-context review."
          : simulatedRecommendation === "insufficient_context"
            ? "Insufficient local context in sampled snippets."
            : "Keep blocked.";
    return {
      documentId: doc.documentId,
      title: doc.title,
      simulatedRecommendation,
      rationale: reason,
      contextSignal: topContext
    };
  });
}

function splitHeuristic(packet, docSimulations) {
  if (hasUnsafeBlockedFamily(packet)) {
    return {
      recommendSplit: false,
      reasons: ["unsafe_37x_conservative_default"],
      splitReason: "Unsafe 37.x family defaults to conservative keep_blocked handling.",
      splitConfidence: "high",
      outcomeCounts: countBy(docSimulations.map((doc) => doc.simulatedRecommendation))
    };
  }
  const docCount = Number(packet.docCount || 0);
  const ctx = packet.contextSummary || {};
  const outcomeCounts = countBy(docSimulations.map((doc) => doc.simulatedRecommendation));
  const outcomeMap = new Map(outcomeCounts.map((item) => [item.key, item.count]));
  const insufficientCount = outcomeMap.get("insufficient_context") || 0;
  const mixedContext = (ctx.ordinanceLike || 0) > 0 && ((ctx.ambiguous || 0) > 0 || (ctx.noUsefulContext || 0) > 0 || (ctx.rulesLike || 0) > 0);
  const highInsufficient = insufficientCount >= Math.max(3, Math.ceil(docCount * 0.2));
  const crossContextRoot = (packet.topRootCausesByCount || []).some((item) => item.key === "cross_context");
  const hasFamilyCombination = Array.isArray(packet.blocked37xFamily) && packet.blocked37xFamily.length > 1;
  const patternNoise = Array.isArray(packet.topRawCitationStringsByCount) && packet.topRawCitationStringsByCount.length >= 3;
  const largeBatch = docCount >= 15;

  const reasons = [];
  if (largeBatch) reasons.push("large_batch");
  if (mixedContext) reasons.push("mixed_context_distribution");
  if (highInsufficient) reasons.push("high_insufficient_context_docs");
  if (crossContextRoot) reasons.push("cross_context_root_cause_present");
  if (hasFamilyCombination) reasons.push("blocked37x_family_combination");
  if (patternNoise) reasons.push("repeated_citation_pattern_noise");

  const recommendSplit = largeBatch && mixedContext && (highInsufficient || crossContextRoot || hasFamilyCombination || patternNoise);
  return {
    recommendSplit,
    reasons,
    splitReason: recommendSplit
      ? `Large mixed batch with noisy evidence (${reasons.join(", ")}); split before reviewer action.`
      : "No split heuristic triggered.",
    splitConfidence: recommendSplit ? "high" : largeBatch ? "medium" : "low",
    outcomeCounts
  };
}

function proposeSubBuckets(docSimulations) {
  const map = new Map();
  for (const doc of docSimulations) {
    const key = subBucketForDoc(doc);
    const list = map.get(key) || [];
    list.push(doc);
    map.set(key, list);
  }
  const ordered = [
    "ordinance_like_manual_review_candidates",
    "ambiguous_context_escalate",
    "insufficient_context_hold",
    "true_not_found_hold",
    "mixed_cross_context_escalate"
  ];
  const proposedSubBuckets = ordered.filter((key) => map.has(key));
  const subBucketCounts = proposedSubBuckets.map((key) => ({ key, count: map.get(key).length }));
  return { proposedSubBuckets, subBucketCounts };
}

function batchDisposition(packet, splitEval) {
  if (hasUnsafeBlockedFamily(packet)) return "keep_blocked";
  const appearance = String(packet.issueAppearanceLikely || "mixed");
  if (splitEval.recommendSplit) return "split_batch_before_review";
  if (appearance === "ordinance citation ambiguity") return "possible_manual_context_fix_but_no_auto_apply";
  if (appearance === "rules/ordinance cross-context confusion") return "escalate_to_legal_context_review";
  if (appearance === "true not-found") return "keep_blocked";
  return "escalate_to_legal_context_review";
}

function confidenceForBatch(packet, disposition) {
  if (hasUnsafeBlockedFamily(packet) && disposition === "keep_blocked") return "high";
  const docCount = Number(packet.docCount || 0);
  const ctx = packet.contextSummary || {};
  const signal = (ctx.ordinanceLike || 0) + (ctx.rulesLike || 0);
  const noise = (ctx.ambiguous || 0) + (ctx.noUsefulContext || 0);
  if (disposition === "split_batch_before_review") return "high";
  if (signal > noise && docCount <= 12) return "high";
  if (signal >= noise) return "medium";
  return "low";
}

function rationale(packet, disposition) {
  if (hasUnsafeBlockedFamily(packet) && disposition === "keep_blocked") {
    return "Unsafe 37.x family: default conservative keep_blocked recommendation unless explicit allowlisted override exists.";
  }
  const ctx = packet.contextSummary || {};
  if (disposition === "split_batch_before_review") {
    return `Mixed evidence at scale (docs=${packet.docCount}, ambiguous=${ctx.ambiguous || 0}, noContext=${ctx.noUsefulContext || 0}); split into smaller work units first.`;
  }
  if (disposition === "possible_manual_context_fix_but_no_auto_apply") {
    return "Ordinance-like context evidence dominates; allow manual context review candidate path only.";
  }
  if (disposition === "keep_blocked") {
    return "Evidence indicates not-found/unsafe context; keep blocked.";
  }
  return "Cross-context or mixed signals; escalate to legal-context review.";
}

function scoreForTomorrow(batch) {
  const dispositionRank = {
    possible_manual_context_fix_but_no_auto_apply: 0,
    split_batch_before_review: 1,
    escalate_to_legal_context_review: 2,
    keep_blocked: 3
  };
  const confidenceRank = { high: 0, medium: 1, low: 2 };
  return [
    dispositionRank[batch.recommendedSimulatedDisposition] ?? 4,
    confidenceRank[batch.confidenceLevel] ?? 3,
    -Number(batch.docCount || 0),
    String(batch.batchKey)
  ];
}

function sortByScore(a, b) {
  const sa = scoreForTomorrow(a);
  const sb = scoreForTomorrow(b);
  for (let i = 0; i < sa.length; i += 1) {
    if (sa[i] < sb[i]) return -1;
    if (sa[i] > sb[i]) return 1;
  }
  return 0;
}

export function buildReviewerDecisionSimulation(evidenceReport) {
  const packets = (evidenceReport?.packets || []).map((packet) => {
    const docSimulations = docRecommendations(packet);
    const splitEval = splitHeuristic(packet, docSimulations);
    const recommendedSimulatedDisposition = batchDisposition(packet, splitEval);
    const confidenceLevel = confidenceForBatch(packet, recommendedSimulatedDisposition);
    const splitRecommended = recommendedSimulatedDisposition === "split_batch_before_review";
    const { proposedSubBuckets, subBucketCounts } = splitRecommended
      ? proposeSubBuckets(docSimulations)
      : { proposedSubBuckets: [], subBucketCounts: [] };

    return {
      batchKey: packet.batchKey,
      docCount: packet.docCount,
      blocked37xFamily: packet.blocked37xFamily || [],
      contextSummary: packet.contextSummary || {},
      dominantRootCauses: dominantRootCauses(packet),
      recommendedSimulatedDisposition,
      confidenceLevel,
      rationale: rationale(packet, recommendedSimulatedDisposition),
      splitRecommended,
      splitReason: splitEval.splitReason,
      splitConfidence: splitEval.splitConfidence,
      proposedSubBuckets,
      subBucketCounts,
      docSimulationOutcomeCounts: splitEval.outcomeCounts,
      splitHeuristicReasons: splitEval.reasons,
      docSimulations,
      readOnly: true
    };
  });

  const ranked = [...packets].sort(sortByScore);
  const tomorrowMorningReviewPlan = ranked.slice(0, 10).map((batch) => ({
    batchKey: batch.batchKey,
    recommendedSimulatedDisposition: batch.recommendedSimulatedDisposition,
    confidenceLevel: batch.confidenceLevel,
    docCount: batch.docCount,
    rationale: batch.rationale
  }));
  const avoidForNow = ranked
    .filter((batch) => ["keep_blocked", "escalate_to_legal_context_review"].includes(batch.recommendedSimulatedDisposition) && batch.confidenceLevel !== "high")
    .slice(0, 10)
    .map((batch) => ({
      batchKey: batch.batchKey,
      recommendedSimulatedDisposition: batch.recommendedSimulatedDisposition,
      confidenceLevel: batch.confidenceLevel,
      rationale: batch.rationale
    }));
  const mostLikelySafeProgress = ranked
    .filter((batch) => batch.recommendedSimulatedDisposition === "possible_manual_context_fix_but_no_auto_apply")
    .slice(0, 10);
  const leastLikelySafeProgress = ranked
    .filter((batch) => batch.recommendedSimulatedDisposition !== "possible_manual_context_fix_but_no_auto_apply")
    .slice(-10)
    .reverse();
  const large3737 = packets.find((packet) => packet.batchKey.includes("37.3+37.7"));
  const large3737Recommendation = large3737
    ? {
        batchKey: large3737.batchKey,
        shouldSplit: large3737.splitRecommended,
        splitReason: large3737.splitReason,
        splitConfidence: large3737.splitConfidence,
        proposedSubBuckets: large3737.proposedSubBuckets,
        subBucketCounts: large3737.subBucketCounts,
        rationale: large3737.rationale
      }
    : null;
  const splitBatches = ranked.filter((batch) => batch.splitRecommended);
  const allSplitReasons = splitBatches.flatMap((batch) => batch.splitHeuristicReasons || []);
  const allDocOutcomes = ranked.flatMap((batch) => (batch.docSimulationOutcomeCounts || []).map((item) => item.key));

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      batchCount: packets.length,
      dispositionCounts: countBy(packets.map((packet) => packet.recommendedSimulatedDisposition)),
      confidenceCounts: countBy(packets.map((packet) => packet.confidenceLevel)),
      batchesRecommendedToSplit: splitBatches.length,
      largestSplitCandidates: splitBatches
        .slice()
        .sort((a, b) => b.docCount - a.docCount || a.batchKey.localeCompare(b.batchKey))
        .slice(0, 10)
        .map((batch) => ({ batchKey: batch.batchKey, docCount: batch.docCount, splitReason: batch.splitReason })),
      splitHeuristicReasons: countBy(allSplitReasons),
      docSimulationOutcomeCounts: countBy(allDocOutcomes)
    },
    batches: ranked,
    tomorrowMorningReviewPlan,
    avoidForNow,
    topBatchesMostLikelyToYieldSafeReviewerProgress: mostLikelySafeProgress.map((batch) => ({
      batchKey: batch.batchKey,
      docCount: batch.docCount,
      confidenceLevel: batch.confidenceLevel,
      rationale: batch.rationale
    })),
    topBatchesLeastLikelyToYieldSafeProgress: leastLikelySafeProgress.map((batch) => ({
      batchKey: batch.batchKey,
      docCount: batch.docCount,
      confidenceLevel: batch.confidenceLevel,
      rationale: batch.rationale
    })),
    large3737SplitRecommendation: large3737Recommendation
  };
}
