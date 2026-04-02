const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function splitList(value) {
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeRow(row) {
  let unresolved = row.exactUnresolvedReferences || [];
  if (typeof unresolved === "string") {
    try {
      unresolved = JSON.parse(unresolved);
    } catch {
      unresolved = [];
    }
  }
  unresolved = Array.isArray(unresolved) ? unresolved : [];
  return {
    documentId: String(row.documentId || ""),
    title: String(row.title || ""),
    batchKey: String(row.batchKey || ""),
    blocked37xFamily: splitList(row.blocked37xFamily),
    unresolvedTriageBuckets: splitList(row.unresolvedTriageBuckets),
    recurringCitationFamily: splitList(row.recurringCitationFamily),
    blockers: splitList(row.blockers),
    reviewerRiskLevel: String(row.reviewerRiskLevel || "high"),
    estimatedReviewerEffort: String(row.estimatedReviewerEffort || "high"),
    recommendedReviewerAction: String(row.topRecommendedReviewerAction || ""),
    unresolvedReferences: unresolved.map((item) => ({
      referenceType: String(item.referenceType || ""),
      rawValue: String(item.rawValue || ""),
      normalizedValue: String(item.normalizedValue || ""),
      rootCause: String(item.rootCause || ""),
      message: String(item.message || "")
    }))
  };
}

function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

function dominant(items, fallback = "") {
  return countBy(items)[0]?.key || fallback;
}

function classifyIssueProfile(packet) {
  const buckets = new Set(packet.unresolvedTriageBuckets);
  const hasUnsafe37x = packet.blocked37xFamily.some((family) => UNSAFE_37X.has(family));
  const hasCrossContext = buckets.has("cross_context_ambiguous") || packet.topRootCauses.some((item) => item.key === "cross_context");
  const hasNotFound = buckets.has("structurally_blocked_not_found") || packet.topRootCauses.some((item) => item.key === "not_found");
  if (hasUnsafe37x && !hasCrossContext && !hasNotFound) return "ordinance citation ambiguity";
  if (hasCrossContext && !hasNotFound) return "rules/ordinance cross-context confusion";
  if (hasNotFound && !hasCrossContext && !hasUnsafe37x) return "true not-found";
  return "mixed";
}

function reviewerQuestions(issueProfile) {
  const base = [
    "Is this citation intended as ordinance or rules?",
    "Is the source text citing a section family that exists in multiple contexts?",
    "Is there enough nearby context to disambiguate safely?",
    "Should this remain blocked?"
  ];
  if (issueProfile === "true not-found") base.push("Does the cited section exist in the authoritative normalized sources?");
  if (issueProfile === "ordinance citation ambiguity") base.push("Is ordinance-family context explicit enough to permit a manual context fix?");
  return base;
}

function decisionPosture(issueProfile) {
  if (issueProfile === "true not-found") return "keep_blocked";
  if (issueProfile === "rules/ordinance cross-context confusion") return "escalate_to_legal_context_review";
  if (issueProfile === "ordinance citation ambiguity") return "possible_manual_context_fix_but_no_auto_apply";
  return "escalate_to_legal_context_review";
}

function reviewChecklist(issueProfile) {
  return [
    "Confirm citation context directly in source paragraph before any manual decision.",
    "Record whether citation should remain blocked or requires legal-context escalation.",
    issueProfile === "possible_manual_context_fix_but_no_auto_apply"
      ? "If context is clear, note manual context-fix recommendation (no auto-apply)."
      : "Do not auto-resolve or auto-approve from this packet."
  ];
}

export function buildReviewerLegalPackets(rows) {
  const normalized = (rows || []).map(normalizeRow).filter((row) => row.documentId);
  const blockedRows = normalized.filter(
    (row) =>
      row.blocked37xFamily.some((family) => UNSAFE_37X.has(family)) ||
      row.unresolvedTriageBuckets.includes("unsafe_37x_structural_block") ||
      row.unresolvedTriageBuckets.includes("cross_context_ambiguous")
  );

  const groups = new Map();
  for (const row of blockedRows) {
    const key = row.batchKey || `doc:${row.documentId}`;
    const list = groups.get(key) || [];
    list.push(row);
    groups.set(key, list);
  }

  const packets = Array.from(groups.entries())
    .map(([batchKey, docs]) => {
      const unresolved = docs.flatMap((row) => row.unresolvedReferences);
      const topRawCitationStrings = countBy(unresolved.map((item) => item.rawValue).filter(Boolean)).slice(0, 12);
      const topNormalizedValues = countBy(unresolved.map((item) => item.normalizedValue).filter(Boolean)).slice(0, 12);
      const topRootCauses = countBy(unresolved.map((item) => item.rootCause || "unknown")).slice(0, 8);
      const blocked37xFamily = Array.from(new Set(docs.flatMap((row) => row.blocked37xFamily))).sort();
      const unresolvedTriageBuckets = Array.from(new Set(docs.flatMap((row) => row.unresolvedTriageBuckets))).sort();
      const dominantCitationFamily = dominant(docs.flatMap((row) => row.recurringCitationFamily), blocked37xFamily[0] || "");
      const dominantBlockerPattern = dominant(docs.flatMap((row) => row.blockers), "unresolved_references_above_threshold");
      const issueProfile = classifyIssueProfile({
        unresolvedTriageBuckets,
        blocked37xFamily,
        topRootCauses
      });
      const posture = decisionPosture(issueProfile);
      return {
        batchKey,
        docCount: docs.length,
        blocked37xFamily,
        unresolvedTriageBuckets,
        dominantBlockerPattern,
        dominantCitationFamily,
        reviewerRiskLevel: dominant(docs.map((row) => row.reviewerRiskLevel), "high"),
        estimatedReviewerEffort: dominant(docs.map((row) => row.estimatedReviewerEffort), "high"),
        sampleDocs: docs
          .slice()
          .sort((a, b) => a.title.localeCompare(b.title) || a.documentId.localeCompare(b.documentId))
          .slice(0, 8)
          .map((row) => ({ documentId: row.documentId, title: row.title })),
        exactUnresolvedReferencesAggregatedByFrequency: countBy(
          unresolved.map((item) => `${item.referenceType}|${item.rawValue}|${item.normalizedValue}|${item.rootCause}|${item.message}`)
        ).map((row) => ({ value: row.key, count: row.count })),
        topRawCitationStrings,
        topNormalizedValues,
        topRootCauses,
        issueAppearanceLikely: issueProfile,
        recommendedReviewerQuestions: reviewerQuestions(issueProfile),
        recommendedDecisionPosture: posture,
        recommendedReviewerAction: dominant(docs.map((row) => row.recommendedReviewerAction).filter(Boolean), "Manual legal-context review required."),
        reviewChecklist: reviewChecklist(posture)
      };
    })
    .sort((a, b) => b.docCount - a.docCount || a.batchKey.localeCompare(b.batchKey));

  const allBlockedFamilies = packets.flatMap((packet) => packet.blocked37xFamily);
  const allRootCauses = packets.flatMap((packet) => packet.topRootCauses.map((item) => item.key));
  const familyDocCounts = new Map();
  for (const packet of packets) {
    for (const family of packet.blocked37xFamily) {
      familyDocCounts.set(family, (familyDocCounts.get(family) || 0) + packet.docCount);
    }
  }
  return {
    packets,
    summary: {
      blockedBatchCount: packets.length,
      blockedDocCount: packets.reduce((sum, packet) => sum + packet.docCount, 0),
      largestBlockedLegalBatches: packets.slice(0, 10).map((packet) => ({
        batchKey: packet.batchKey,
        docCount: packet.docCount,
        blocked37xFamily: packet.blocked37xFamily
      })),
      citationFamiliesCausingMostBlockedDocs: countBy(allBlockedFamilies).map((row) => ({ family: row.key, docCount: row.count })),
      topRootCausesAcrossBlockedBatches: countBy(allRootCauses).map((row) => ({ rootCause: row.key, count: row.count })),
      docsPerBlocked37xFamily: Array.from(familyDocCounts.entries())
        .map(([family, docCount]) => ({ family, docCount }))
        .sort((a, b) => b.docCount - a.docCount || a.family.localeCompare(b.family)),
      suggestedReviewOrder: packets.map((packet) => ({
        batchKey: packet.batchKey,
        docCount: packet.docCount,
        recommendedDecisionPosture: packet.recommendedDecisionPosture,
        rationale: packet.issueAppearanceLikely
      }))
    }
  };
}
