const EFFORT_RANK = { low: 0, medium: 1, high: 2 };
const RISK_RANK = { low: 0, medium: 1, high: 2 };
const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function splitList(value) {
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return map;
}

function dominant(items, fallback = "") {
  if (!items.length) return fallback;
  const counts = countBy(items);
  return Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(String(b[0])))[0]?.[0] || fallback;
}

export function normalizeReviewerExportRows(rows) {
  return (rows || []).map((row) => ({
    documentId: String(row.documentId || ""),
    title: String(row.title || ""),
    batchKey: String(row.batchKey || ""),
    reviewerRiskLevel: String(row.reviewerRiskLevel || "high"),
    estimatedReviewerEffort: String(row.estimatedReviewerEffort || "high"),
    unresolvedTriageBuckets: splitList(row.unresolvedTriageBuckets),
    blocked37xFamilies: splitList(row.blocked37xFamily),
    recurringCitationFamilies: splitList(row.recurringCitationFamily),
    blockers: splitList(row.blockers),
    recommendedReviewerAction: String(row.topRecommendedReviewerAction || ""),
    safeToBatchReview:
      row.safeToBatchReview === true ||
      String(row.safeToBatchReview || "").toLowerCase() === "true" ||
      (!splitList(row.blocked37xFamily).length &&
        !splitList(row.unresolvedTriageBuckets).some((bucket) =>
          ["cross_context_ambiguous", "unsafe_37x_structural_block", "structurally_blocked_not_found"].includes(bucket)
        )),
    unresolvedCount: Number(row.unresolvedCount || 0)
  }));
}

function classifyPriorityBucket(batch) {
  const hasUnsafe37x = batch.blocked37xFamily.some((family) => UNSAFE_37X.has(family));
  const hasAmbiguous = batch.unresolvedTriageBuckets.includes("cross_context_ambiguous");
  const hasUnsafeStructural = batch.unresolvedTriageBuckets.includes("unsafe_37x_structural_block");
  if (hasUnsafe37x || hasUnsafeStructural) return "blocked_legal_adjudication";
  if (hasAmbiguous) return "review_later";
  if (batch.safeToBatchReview && batch.estimatedReviewerEffort === "low") return "review_now";
  if (batch.safeToBatchReview) return "review_next";
  return "review_later";
}

function compareBatches(a, b) {
  const bucketRank = {
    review_now: 0,
    review_next: 1,
    review_later: 2,
    blocked_legal_adjudication: 3
  };
  const bucketCmp = bucketRank[a.priorityBucket] - bucketRank[b.priorityBucket];
  if (bucketCmp !== 0) return bucketCmp;
  const effortCmp = (EFFORT_RANK[a.estimatedReviewerEffort] ?? 2) - (EFFORT_RANK[b.estimatedReviewerEffort] ?? 2);
  if (effortCmp !== 0) return effortCmp;
  const safeCmp = Number(b.safeToBatchReview) - Number(a.safeToBatchReview);
  if (safeCmp !== 0) return safeCmp;
  const sizeCmp = b.docCount - a.docCount;
  if (sizeCmp !== 0) return sizeCmp;
  const riskCmp = (RISK_RANK[a.reviewerRiskLevel] ?? 2) - (RISK_RANK[b.reviewerRiskLevel] ?? 2);
  if (riskCmp !== 0) return riskCmp;
  return String(a.batchKey).localeCompare(String(b.batchKey));
}

export function buildPrioritizedReviewerBatches(rows) {
  const normalized = normalizeReviewerExportRows(rows).filter((row) => row.documentId);
  const groups = new Map();
  for (const row of normalized) {
    const key = row.batchKey || `doc:${row.documentId}`;
    const list = groups.get(key) || [];
    list.push(row);
    groups.set(key, list);
  }
  const batches = Array.from(groups.entries()).map(([batchKey, rowsInBatch]) => {
    const sortedRows = [...rowsInBatch].sort((a, b) => a.title.localeCompare(b.title) || a.documentId.localeCompare(b.documentId));
    const unresolvedBuckets = Array.from(new Set(rowsInBatch.flatMap((row) => row.unresolvedTriageBuckets))).sort();
    const blocked37xFamily = Array.from(new Set(rowsInBatch.flatMap((row) => row.blocked37xFamilies))).sort();
    const recurringCitationFamilies = Array.from(new Set(rowsInBatch.flatMap((row) => row.recurringCitationFamilies))).sort();
    const reviewerRiskLevel = dominant(rowsInBatch.map((row) => row.reviewerRiskLevel), "high");
    const estimatedReviewerEffort = dominant(rowsInBatch.map((row) => row.estimatedReviewerEffort), "high");
    const recommendedReviewerAction = dominant(
      rowsInBatch.map((row) => row.recommendedReviewerAction).filter(Boolean),
      "Perform manual review with source-backed citation checks."
    );
    const dominantBlockerPattern = dominant(rowsInBatch.flatMap((row) => row.blockers), "unresolved_references_above_threshold");
    const dominantCitationFamily = dominant(recurringCitationFamilies, blocked37xFamily[0] || "");
    const safeToBatchReview = rowsInBatch.every((row) => row.safeToBatchReview);
    const batch = {
      batchKey,
      docCount: rowsInBatch.length,
      sampleTitles: sortedRows.slice(0, 5).map((row) => row.title),
      reviewerRiskLevel,
      estimatedReviewerEffort,
      unresolvedTriageBuckets: unresolvedBuckets,
      blocked37xFamily,
      dominantBlockerPattern,
      dominantCitationFamily,
      recommendedReviewerAction,
      safeToBatchReview,
      rationale: ""
    };
    batch.priorityBucket = classifyPriorityBucket(batch);
    batch.rationale =
      batch.priorityBucket === "review_now"
        ? "Safe-to-batch, low-effort issues with high leverage."
        : batch.priorityBucket === "review_next"
          ? "Safe-to-batch but requires moderate effort."
          : batch.priorityBucket === "review_later"
            ? "Ambiguous or higher-effort issues; review after safer batches."
            : "Blocked legal adjudication required (unsafe 37.x structural family).";
    return batch;
  });

  const prioritized = [...batches].sort(compareBatches);
  return {
    prioritizedBatches: prioritized,
    buckets: {
      review_now: prioritized.filter((batch) => batch.priorityBucket === "review_now"),
      review_next: prioritized.filter((batch) => batch.priorityBucket === "review_next"),
      review_later: prioritized.filter((batch) => batch.priorityBucket === "review_later"),
      blocked_legal_adjudication: prioritized.filter((batch) => batch.priorityBucket === "blocked_legal_adjudication")
    }
  };
}
