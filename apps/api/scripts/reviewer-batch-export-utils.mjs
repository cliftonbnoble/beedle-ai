function asArray(value) {
  return Array.isArray(value) ? value : [];
}

export function applyReviewerExportFilters(rows, filters = {}) {
  const realOnly = filters.realOnly !== false;
  return asArray(rows).filter((row) => {
    if (realOnly && row.isRealDoc === false) return false;
    if (filters.unresolvedTriageBucket && !asArray(row.unresolvedTriageBuckets).includes(filters.unresolvedTriageBucket)) return false;
    if (filters.blocked37xFamily && !asArray(row.blocked37xFamily).includes(filters.blocked37xFamily)) return false;
    if (filters.estimatedReviewerEffort && row.estimatedReviewerEffort !== filters.estimatedReviewerEffort) return false;
    if (filters.reviewerRiskLevel && row.reviewerRiskLevel !== filters.reviewerRiskLevel) return false;
    if (filters.safeToBatchReviewOnly && !row.safeToBatchReview) return false;
    if (filters.batchKey && row.batchKey !== filters.batchKey) return false;
    return true;
  });
}

export function stableBatchGrouping(rows) {
  const sorted = [...asArray(rows)].sort((a, b) => String(a.batchKey || "").localeCompare(String(b.batchKey || "")) || String(a.documentId).localeCompare(String(b.documentId)));
  const groups = new Map();
  for (const row of sorted) {
    const key = String(row.batchKey || "");
    const list = groups.get(key) || [];
    list.push(row.documentId);
    groups.set(key, list);
  }
  return Array.from(groups.entries()).map(([batchKey, documentIds]) => ({ batchKey, documentIds }));
}

export function buildAdjudicationTemplate(rows) {
  return asArray(rows).map((row) => ({
    documentId: row.documentId,
    title: row.title,
    batchKey: row.batchKey || "",
    reviewerDecision: "",
    reviewerNotes: "",
    citationActionType: "",
    citationOriginal: "",
    citationReplacement: "",
    confirmMetadata: "",
    escalate: "",
    doNotApprove: "",
    reviewedBy: "",
    reviewedAt: ""
  }));
}

export function chunkIdsForQuery(ids, chunkSize = 250) {
  const out = [];
  const list = asArray(ids);
  for (let i = 0; i < list.length; i += chunkSize) out.push(list.slice(i, i + chunkSize));
  return out;
}
