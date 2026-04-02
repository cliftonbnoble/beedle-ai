function asArray(value) {
  return Array.isArray(value) ? value : [];
}

const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function normalizeCitationCore(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

function stripLeadPrefix(input) {
  return normalizeCitationCore(input).replace(/^ordinance/, "").replace(/^rule/, "");
}

export function citationFamilyFromIssue(issue) {
  const normalized = stripLeadPrefix(issue?.normalizedValue || issue?.rawValue || "");
  const match = normalized.match(/^(\d+\.\d+)/);
  return match ? match[1] : null;
}

function issueSignature(issue) {
  return `${String(issue?.referenceType || "")}::${stripLeadPrefix(issue?.normalizedValue || issue?.rawValue || "")}`;
}

export function classifyUnresolvedIssueBucket(issue, context = {}) {
  const message = String(issue?.message || "");
  const refType = String(issue?.referenceType || "");
  const normalizedRaw = normalizeCitationCore(issue?.normalizedValue || issue?.rawValue || "");
  const normalized = stripLeadPrefix(issue?.normalizedValue || issue?.rawValue || "");
  const family = citationFamilyFromIssue(issue);
  const duplicateCount = context.duplicateCounts?.get(issueSignature(issue)) || 0;

  if (refType === "ordinance_section" && UNSAFE_37X.has(family || "")) return "unsafe_37x_structural_block";
  if (
    /cross[_\s-]?context/i.test(message) ||
    (refType === "rules_section" && normalized.startsWith("37.")) ||
    (refType === "ordinance_section" && normalized && !normalized.startsWith("37."))
  ) {
    return "cross_context_ambiguous";
  }
  if (duplicateCount > 1 || /duplicate|redundant/i.test(message)) return "duplicate_or_redundant_reference";
  if (
    /^ordinance37\./.test(normalizedRaw) ||
    /^rule\d+\.\d+/.test(normalizedRaw) ||
    /^ordinance\d+\.\d+/.test(normalizedRaw) ||
    /prefix|parenthetical|format|malformed|unable to parse|unparseable|invalid/i.test(message)
  ) {
    return "likely_parenthetical_or_prefix_fix";
  }
  if ((refType === "rules_section" && normalized.startsWith("37.")) || (refType === "ordinance_section" && /^i{1,4}-?\d/.test(normalized))) {
    return "likely_context_relabel_candidate";
  }
  if (/manual review/i.test(message) && String(issue?.severity || "").toLowerCase() === "warning") {
    return "safe_manual_drop_candidate";
  }
  return "structurally_blocked_not_found";
}

export function classifyDocUnresolvedTriage(doc, detail) {
  const issues = asArray(detail?.referenceIssues);
  const duplicateCounts = new Map();
  for (const issue of issues) {
    const sig = issueSignature(issue);
    duplicateCounts.set(sig, (duplicateCounts.get(sig) || 0) + 1);
  }

  const buckets = new Set();
  const recurringCitationFamilies = new Set();
  const fixes = [];
  const seenFix = new Set();

  for (const issue of issues) {
    const bucket = classifyUnresolvedIssueBucket(issue, { duplicateCounts });
    buckets.add(bucket);
    const family = citationFamilyFromIssue(issue);
    if (family) recurringCitationFamilies.add(family);

    let fix = null;
    if (bucket === "unsafe_37x_structural_block") {
      fix = `Keep "${issue.rawValue}" blocked (unsafe 37.x family).`;
    } else if (bucket === "cross_context_ambiguous") {
      fix = `Manually verify context for "${issue.rawValue}" (rules vs ordinance).`;
    } else if (bucket === "duplicate_or_redundant_reference") {
      fix = `Drop duplicate unresolved reference "${issue.rawValue}" after source check.`;
    } else if (bucket === "likely_parenthetical_or_prefix_fix") {
      const candidate = stripLeadPrefix(issue.rawValue || issue.normalizedValue || "");
      fix = candidate
        ? `Normalize "${issue.rawValue}" to "${candidate}" only if source text supports it.`
        : `Normalize citation formatting for "${issue.rawValue}" with source-backed edit.`;
    } else if (bucket === "safe_manual_drop_candidate") {
      fix = `If "${issue.rawValue}" is non-substantive residue, reviewer may drop it manually.`;
    } else if (bucket === "likely_context_relabel_candidate") {
      fix = `Potential relabel candidate "${issue.rawValue}" requires explicit reviewer confirmation.`;
    }
    if (fix && !seenFix.has(fix)) {
      fixes.push(fix);
      seenFix.add(fix);
    }
  }

  const bucketList = Array.from(buckets);
  const effort =
    bucketList.includes("unsafe_37x_structural_block") ||
    bucketList.includes("cross_context_ambiguous") ||
    bucketList.includes("structurally_blocked_not_found")
      ? "high"
      : issues.length <= 2
        ? "low"
        : "medium";

  const topAction = bucketList.includes("unsafe_37x_structural_block")
    ? "Unsafe 37.x citations require manual legal review; keep staged."
    : bucketList.includes("cross_context_ambiguous")
      ? "Resolve rules/ordinance context ambiguity manually; no auto-relabelling."
      : bucketList.includes("structurally_blocked_not_found")
        ? "Investigate unresolved citations not found in normalized references."
        : bucketList.includes("likely_parenthetical_or_prefix_fix")
          ? "Apply source-backed citation normalization fixes and re-run QC."
          : bucketList.includes("duplicate_or_redundant_reference")
            ? "Remove duplicate unresolved references and re-run QC."
            : "Perform focused manual unresolved-reference cleanup.";

  const status = bucketList.some((bucket) => ["unsafe_37x_structural_block", "cross_context_ambiguous", "structurally_blocked_not_found"].includes(bucket))
    ? "blocked"
    : "reviewer_actionable";

  return {
    status,
    unresolvedBuckets: bucketList,
    topRecommendedReviewerAction: topAction,
    estimatedReviewerEffort: effort,
    candidateManualFixes: fixes.slice(0, 10),
    recurringCitationFamilies: Array.from(recurringCitationFamilies)
  };
}

export function buildBatchGroups(rows) {
  const signatureMap = new Map();
  for (const row of asArray(rows)) {
    const signature = `${asArray(row.unresolvedBuckets).slice().sort().join("|")}::${asArray(row.recurringCitationFamilies).slice().sort().join("|")}`;
    const list = signatureMap.get(signature) || [];
    list.push(row);
    signatureMap.set(signature, list);
  }
  const out = new Map();
  for (const group of signatureMap.values()) {
    if (group.length < 2) continue;
    const ids = group.map((row) => row.id);
    for (const row of group) {
      out.set(row.id, ids.filter((id) => id !== row.id));
    }
  }
  return out;
}
