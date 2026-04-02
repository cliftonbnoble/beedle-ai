function asArray(value) {
  return Array.isArray(value) ? value : [];
}

const BLOCKED_FAMILIES = new Set(["37.3", "37.7", "37.9"]);

function normalize(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^ordinance/, "")
    .replace(/^rule/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

export function extractBlockedFamily(value) {
  const normalized = normalize(value);
  const match = normalized.match(/^37\.(3|7|9)/);
  return match ? match[0] : null;
}

export function classifyBlocked37xReason(referenceType, message) {
  if (/cross[_\s-]?context/i.test(String(message || ""))) return "cross_context_ambiguous";
  if (referenceType === "rules_section") return "cross_context_ambiguous";
  return "unsafe_37x_structural_block";
}

export function buildBlocked37xDocView(doc, detail) {
  const refs = asArray(detail?.referenceIssues)
    .map((issue) => {
      const family = extractBlockedFamily(issue?.normalizedValue || issue?.rawValue || "");
      if (!family || !BLOCKED_FAMILIES.has(family)) return null;
      return {
        family,
        referenceType: String(issue.referenceType || ""),
        rawValue: String(issue.rawValue || ""),
        normalizedValue: String(issue.normalizedValue || ""),
        message: String(issue.message || ""),
        reason: classifyBlocked37xReason(issue.referenceType, issue.message)
      };
    })
    .filter(Boolean);

  const families = Array.from(new Set(refs.map((ref) => ref.family))).sort();
  const reasons = Array.from(new Set(refs.map((ref) => ref.reason))).sort();
  const reason = reasons.includes("cross_context_ambiguous") ? "cross_context_ambiguous" : refs.length ? "unsafe_37x_structural_block" : "none";
  const safeToBatchReview = refs.length > 0 && refs.every((ref) => ref.referenceType === "ordinance_section") && reason === "unsafe_37x_structural_block";
  const batchKey = refs.length
    ? `${families.join("+")}::${Array.from(new Set(refs.map((ref) => ref.referenceType))).sort().join("+")}::${reason}`
    : null;
  return {
    id: doc.id,
    title: doc.title,
    blocked37xReferences: refs,
    blocked37xReason: reason,
    blocked37xReviewerHint:
      reason === "cross_context_ambiguous"
        ? "Blocked due to ordinance/rules ambiguity; reviewer must decide context manually."
        : refs.length
          ? "Blocked unsafe 37.x citation family; maintain block until source-backed legal review."
          : "No blocked 37.x references.",
    blocked37xSafeToBatchReview: safeToBatchReview,
    blocked37xBatchKey: batchKey
  };
}

export function groupByBatchKey(rows) {
  const byKey = new Map();
  for (const row of rows) {
    if (!row.blocked37xBatchKey) continue;
    const list = byKey.get(row.blocked37xBatchKey) || [];
    list.push(row);
    byKey.set(row.blocked37xBatchKey, list);
  }
  return byKey;
}
