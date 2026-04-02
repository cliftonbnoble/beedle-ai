function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function norm(value) {
  return String(value || "").toLowerCase().trim();
}

function canonicalize(value) {
  return norm(value).replace(/\s+/g, "").replace(/^rule/, "").replace(/^ordinance/, "");
}

function stripSubsections(value) {
  return canonicalize(value).replace(/\([a-z0-9]+\)/g, "");
}

function isMalformedIssue(issue) {
  return /(malformed|invalid|unable to parse|unparseable|format error|bad citation)/i.test(String(issue?.message || ""));
}

function isCrossContext(issue) {
  const type = norm(issue?.referenceType);
  const value = canonicalize(issue?.normalizedValue || issue?.rawValue || "");
  if (!value) return false;
  if (type === "rules_section" && /^37\./.test(value)) return true;
  if (type === "ordinance_section" && !/^37\./.test(value)) return true;
  return false;
}

function hasParentChildRelation(issue, validSets) {
  const type = norm(issue?.referenceType);
  const normalized = canonicalize(issue?.normalizedValue || issue?.rawValue || "");
  if (!normalized) return false;
  const parent = stripSubsections(normalized);
  const valid = validSets[type] || new Set();
  if (valid.has(parent)) return true;
  for (const value of valid.values()) {
    if (value.startsWith(parent + "(") || parent.startsWith(value + "(")) return true;
  }
  return false;
}

export function classifyUnresolvedIssue(issue, context) {
  const key = `${norm(issue?.referenceType)}::${canonicalize(issue?.normalizedValue || issue?.rawValue || "")}`;
  const duplicateCount = context.issueCounts.get(key) || 0;
  if (duplicateCount > 1) return "duplicate";
  if (isMalformedIssue(issue)) return "malformed";
  if (isCrossContext(issue)) return "cross_context";
  if (hasParentChildRelation(issue, context.validSets)) return "parent_child";
  return "not_found";
}

function indexCodeSource(detail) {
  const warnings = asArray(detail?.extractionWarnings).map((item) => String(item));
  const hasInferred = warnings.some((warning) => /index codes inferred from validated references/i.test(warning));
  const extracted = asArray(detail?.indexCodes);
  if (hasInferred && extracted.length > 0) return "inferred_or_mixed";
  if (extracted.length > 0) return "direct_or_confirmed";
  return "none";
}

export function classifyDocForensics(doc, detail) {
  const blockers = asArray(doc?.approvalReadiness?.blockers);
  const issues = asArray(detail?.referenceIssues);
  const valid = detail?.validReferences || {};
  const validSets = {
    index_code: new Set(asArray(valid.indexCodes).map(canonicalize)),
    rules_section: new Set(asArray(valid.rulesSections).map(canonicalize)),
    ordinance_section: new Set(asArray(valid.ordinanceSections).map(canonicalize))
  };
  const issueCounts = new Map();
  for (const issue of issues) {
    const key = `${norm(issue?.referenceType)}::${canonicalize(issue?.normalizedValue || issue?.rawValue || "")}`;
    issueCounts.set(key, (issueCounts.get(key) || 0) + 1);
  }
  const classifiedIssues = issues.map((issue) => ({
    referenceType: issue.referenceType,
    rawValue: issue.rawValue,
    normalizedValue: issue.normalizedValue,
    message: issue.message,
    severity: issue.severity,
    rootCause: classifyUnresolvedIssue(issue, { validSets, issueCounts })
  }));

  const hasMissingOrd = blockers.includes("missing_ordinance_detection");
  const hasMissingQc = blockers.includes("qc_gate_not_passed");
  const hasMetadata = blockers.includes("metadata_not_confirmed");
  const hasUnresolved = blockers.includes("unresolved_references_above_threshold");

  let blockerCategory = "mixed";
  if (blockers.length === 1 && hasMetadata) blockerCategory = "metadata_only";
  else if (hasMissingOrd || (hasMissingQc && (detail?.missingRulesDetection || detail?.missingOrdinanceDetection))) blockerCategory = "missing_required_detection";
  else if (hasUnresolved) blockerCategory = "unresolved_structural";
  else if (hasMissingQc) blockerCategory = "qc_structural";

  const rootCauseSet = new Set(classifiedIssues.map((issue) => issue.rootCause));
  const lowRiskRootCausesOnly = Array.from(rootCauseSet).every((cause) => ["duplicate", "parent_child", "cross_context"].includes(cause));
  const unresolvedCount = Number(detail?.unresolvedReferenceCount || classifiedIssues.length || 0);
  const metadataConfirmationHelps =
    hasMetadata &&
    !hasMissingOrd &&
    !detail?.missingRulesDetection &&
    !detail?.missingOrdinanceDetection &&
    asArray(detail?.validReferences?.indexCodes).length > 0 &&
    asArray(detail?.validReferences?.rulesSections).length > 0 &&
    asArray(detail?.validReferences?.ordinanceSections).length > 0;

  const reviewerUnlockable =
    (hasMetadata || hasUnresolved || hasMissingQc) &&
    !hasMissingOrd &&
    unresolvedCount <= 4 &&
    lowRiskRootCausesOnly &&
    metadataConfirmationHelps;

  const safeAfterManualConfirmation = reviewerUnlockable && hasMetadata;

  let recommendedNextAction = "Manual detailed QC review required";
  if (safeAfterManualConfirmation) {
    recommendedNextAction = "Reviewer can confirm metadata and re-run approval check; unresolved residue appears low-risk";
  } else if (hasMissingOrd) {
    recommendedNextAction = "Resolve missing ordinance detection in source text/sections before approval";
  } else if (hasUnresolved && !lowRiskRootCausesOnly) {
    recommendedNextAction = "Investigate unresolved references; contains malformed or not-found citations";
  } else if (hasMetadata) {
    recommendedNextAction = "Metadata confirmation may help, but unresolved risk still exceeds conservative criteria";
  }

  return {
    blockerCategory,
    unresolvedDetail: classifiedIssues,
    validatedReferencesPresent: {
      indexCodes: asArray(valid.indexCodes).length,
      rulesSections: asArray(valid.rulesSections).length,
      ordinanceSections: asArray(valid.ordinanceSections).length
    },
    indexCodeSource: indexCodeSource(detail),
    metadataConfirmationHelps,
    reviewerUnlockable,
    safeAfterManualConfirmation,
    recommendedNextAction
  };
}

