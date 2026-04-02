function asArray(value) {
  return Array.isArray(value) ? value : [];
}

const SAFE_SINGLE_MANUAL_FIX_BLOCKER = "unresolved_references_above_threshold";
const UNSAFE_37X_NORMALIZED = new Set(["ordinance37.3", "ordinance37.7", "ordinance37.9"]);

export function classifyReviewerReadinessCandidate(doc) {
  const blockers = asArray(doc?.approvalReadiness?.blockers);
  const reasons = new Set(asArray(doc?.reviewerReadyReasons));
  const unresolvedAfterConfirmation = asArray(doc?.unresolvedBlockersAfterConfirmation);
  const metadataWouldUnlock = Boolean(doc?.metadataConfirmationWouldUnlock);
  const reviewerReady = Boolean(doc?.reviewerReady);

  const confirmationOnly =
    reviewerReady &&
    metadataWouldUnlock &&
    blockers.includes("metadata_not_confirmed") &&
    unresolvedAfterConfirmation.length === 0;

  const confirmationPlusOneManualFix =
    reviewerReady &&
    reasons.has("confirmation_plus_one_manual_citation_fix") &&
    blockers.includes("metadata_not_confirmed") &&
    unresolvedAfterConfirmation.length === 1 &&
    unresolvedAfterConfirmation[0] === SAFE_SINGLE_MANUAL_FIX_BLOCKER;

  if (confirmationOnly) {
    return "confirmation_only";
  }
  if (confirmationPlusOneManualFix) {
    return "confirmation_plus_one_manual_fix";
  }
  return "structurally_blocked";
}

export function hasUnsafe37xIssue(detail) {
  return asArray(detail?.referenceIssues).some(
    (issue) => String(issue?.referenceType || "") === "ordinance_section" && UNSAFE_37X_NORMALIZED.has(String(issue?.normalizedValue || ""))
  );
}

export function hasCrossContextAmbiguityIssue(detail) {
  return asArray(detail?.referenceIssues).some((issue) => /cross[_\s-]?context/i.test(String(issue?.message || "")));
}

export function isSafeForMetadataAutoConfirmation(doc, detail) {
  if (classifyReviewerReadinessCandidate(doc) !== "confirmation_only") return false;
  if (doc?.isLikelyFixture) return false;
  if (hasUnsafe37xIssue(detail)) return false;
  if (hasCrossContextAmbiguityIssue(detail)) return false;

  const validRefs = detail?.validReferences || {};
  const hasValidatedTriad =
    asArray(validRefs.indexCodes).length > 0 &&
    asArray(validRefs.rulesSections).length > 0 &&
    asArray(validRefs.ordinanceSections).length > 0;
  if (!hasValidatedTriad) return false;

  if (asArray(detail?.criticalExceptionReferences).length > 0) return false;
  if (asArray(detail?.unresolvedBlockersAfterConfirmation).length > 0) return false;
  return true;
}

export function splitReviewerReadinessDocs(docs) {
  const output = {
    confirmation_only_candidates: [],
    confirmation_plus_one_manual_fix_candidates: [],
    structurally_blocked_docs: []
  };
  for (const doc of asArray(docs)) {
    const bucket = classifyReviewerReadinessCandidate(doc);
    if (bucket === "confirmation_only") output.confirmation_only_candidates.push(doc);
    else if (bucket === "confirmation_plus_one_manual_fix") output.confirmation_plus_one_manual_fix_candidates.push(doc);
    else output.structurally_blocked_docs.push(doc);
  }
  return output;
}
