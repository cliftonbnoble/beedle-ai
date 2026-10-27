// Pure reviewer readiness, triage and conservative manual-review policy.
import { isLikelyFixtureName } from "./qc-shared";



export function parseJsonArray(input: string | null | undefined): string[] {
  if (!input) return [];
  try {
    const parsed = JSON.parse(input);
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}


export function parseJsonObject(input: string | null | undefined): Record<string, unknown> {
  if (!input) return {};
  try {
    const parsed = JSON.parse(input);
    return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}


export function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}


export function boolish(value: unknown): number {
  return value ? 1 : 0;
}


export function qcFailedRequirements(input: { qcHasIndexCodes: number; qcHasRulesSection: number; qcHasOrdinanceSection: number }) {
  const failed: string[] = [];
  if (!input.qcHasIndexCodes) failed.push("missing_index_codes");
  if (!input.qcHasRulesSection) failed.push("missing_rules_section");
  if (!input.qcHasOrdinanceSection) failed.push("missing_ordinance_section");
  return failed;
}


type ReviewerReadinessInput = {
  isLikelyFixture: boolean;
  blockers: string[];
  qcPassed: number;
  qcRequiredConfirmed: number;
  failedQcRequirements: string[];
  unresolvedReferenceCount: number;
  unresolvedRulesCount: number;
  unresolvedOrdinanceCount: number;
  unresolvedIndexCount: number;
  unresolvedUnsafe37xCount: number;
  criticalExceptionCount: number;
  extractionConfidence: number;
  validIndexRefCount: number;
  validRulesRefCount: number;
  validOrdinanceRefCount: number;
  missingRulesDetection: boolean;
  missingOrdinanceDetection: boolean;
  approvalThresholds: { maxUnresolvedReferences: number };
};


export function computeReviewerReadiness(input: ReviewerReadinessInput) {
  const reasons: string[] = [];
  const actions: string[] = [];
  const unresolvedBlockersAfterConfirmation = input.blockers.filter((blocker) => blocker !== "metadata_not_confirmed");
  const hasValidatedTriad = input.validIndexRefCount > 0 && input.validRulesRefCount > 0 && input.validOrdinanceRefCount > 0;
  const onlyMetadataBlocker = input.blockers.length === 1 && input.blockers[0] === "metadata_not_confirmed";
  const needsOneManualCitationFix =
    input.blockers.includes("metadata_not_confirmed") &&
    unresolvedBlockersAfterConfirmation.length === 1 &&
    unresolvedBlockersAfterConfirmation[0] === "unresolved_references_above_threshold" &&
    input.unresolvedReferenceCount === input.approvalThresholds.maxUnresolvedReferences + 1 &&
    input.unresolvedIndexCount === 0 &&
    input.unresolvedUnsafe37xCount === 0 &&
    (input.unresolvedRulesCount + input.unresolvedOrdinanceCount) === input.unresolvedReferenceCount;

  const metadataConfirmationWouldUnlock =
    onlyMetadataBlocker &&
    hasValidatedTriad &&
    input.qcPassed > 0 &&
    !input.missingRulesDetection &&
    !input.missingOrdinanceDetection &&
    input.criticalExceptionCount === 0;

  let reviewerReady = false;
  let reviewerRiskLevel: "low" | "medium" | "high" = "high";

  if (input.isLikelyFixture) {
    reasons.push("fixture_or_harness_doc");
    actions.push("Exclude fixture/harness docs from real rollout decisions.");
  } else if (metadataConfirmationWouldUnlock) {
    reviewerReady = true;
    reviewerRiskLevel = "low";
    reasons.push("metadata_confirmation_only_blocker");
    actions.push("Confirm extracted metadata using validated references, then re-run approval.");
  } else if (
    needsOneManualCitationFix &&
    hasValidatedTriad &&
    input.qcPassed > 0 &&
    !input.missingRulesDetection &&
    !input.missingOrdinanceDetection &&
    input.criticalExceptionCount === 0
  ) {
    reviewerReady = true;
    reviewerRiskLevel = "medium";
    reasons.push("confirmation_plus_one_manual_citation_fix");
    actions.push("Confirm metadata, then resolve one citation issue and re-run approval.");
  } else {
    reviewerReady = false;
    reviewerRiskLevel = "high";
    if (!hasValidatedTriad) reasons.push("validated_references_incomplete");
    if (input.unresolvedUnsafe37xCount > 0) reasons.push("unsafe_37x_unresolved_present");
    if (input.unresolvedIndexCount > 0) reasons.push("index_code_unresolved_present");
    if (input.missingOrdinanceDetection || input.missingRulesDetection || input.failedQcRequirements.length > 0) {
      reasons.push("structural_qc_blockers_present");
    }
    if (reasons.length === 0) reasons.push("requires_manual_forensics");
    actions.push("Manual reviewer action required; do not auto-confirm metadata.");
  }

  return {
    reviewerReady,
    reviewerReadyReasons: Array.from(new Set(reasons)),
    reviewerRequiredActions: Array.from(new Set(actions)),
    reviewerRiskLevel,
    metadataConfirmationWouldUnlock,
    unresolvedBlockersAfterConfirmation
  };
}


export function isLikelyFixtureDoc(params: { title: string; citation: string; metadata: Record<string, unknown> }) {
  const filename = typeof params.metadata.originalFilename === "string" ? params.metadata.originalFilename : "";
  return isLikelyFixtureName(`${params.title} ${params.citation} ${filename}`);
}


type Blocked37xReference = {
  family: "37.3" | "37.7" | "37.9";
  referenceType: string;
  rawValue: string;
  normalizedValue: string;
  message: string;
};


export function computeBlocked37xReview(issues: UnresolvedIssueLite[]) {
  const blocked: Blocked37xReference[] = [];
  for (const issue of issues || []) {
    const normalized = stripLeadPrefix(issue.normalizedValue || issue.rawValue || "");
    const family = normalized.match(/^37\.(3|7|9)/)?.[0] as "37.3" | "37.7" | "37.9" | undefined;
    if (!family) continue;
    blocked.push({
      family,
      referenceType: issue.referenceType,
      rawValue: issue.rawValue,
      normalizedValue: issue.normalizedValue,
      message: issue.message
    });
  }
  const uniqueFamilies = Array.from(new Set(blocked.map((item) => item.family))).sort();
  const hasCrossContext = blocked.some(
    (item) =>
      /cross[_\s-]?context/i.test(item.message) ||
      (item.referenceType === "rules_section" && item.family.startsWith("37.")) ||
      (item.referenceType === "ordinance_section" && !item.family.startsWith("37."))
  );
  const reason = blocked.length
    ? hasCrossContext
      ? "cross_context_ambiguous_blocked"
      : "unsafe_37x_structural_block"
    : "none";
  const hint = blocked.length
    ? hasCrossContext
      ? "Keep blocked. Reviewer must decide ordinance vs rules context manually."
      : "Keep blocked. Unsafe 37.x family requires source-backed legal review."
    : "No blocked 37.x references.";
  const onlyOrdinanceRefs = blocked.every((item) => item.referenceType === "ordinance_section");
  const safeToBatchReview = blocked.length > 0 && onlyOrdinanceRefs && !hasCrossContext;
  const refTypes = Array.from(new Set(blocked.map((item) => item.referenceType))).sort();
  const batchKey = blocked.length ? `${uniqueFamilies.join("+")}::${refTypes.join("+")}::${reason}` : null;
  return {
    blocked37xReferences: blocked,
    blocked37xReason: reason,
    blocked37xReviewerHint: hint,
    blocked37xSafeToBatchReview: safeToBatchReview,
    blocked37xBatchKey: batchKey
  };
}


export type UnresolvedIssueLite = {
  referenceType: string;
  rawValue: string;
  normalizedValue: string;
  message: string;
  severity: string;
};


type UnresolvedTriage = {
  unresolvedBuckets: string[];
  topRecommendedReviewerAction: string;
  estimatedReviewerEffort: "low" | "medium" | "high";
  candidateManualFixes: string[];
  recurringCitationFamilies: string[];
};


const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

const CONSERVATIVE_MANUAL_ALLOWLIST = new Set(["37.1", "37.2", "37.8"]);

type RuntimeReviewerDisposition = "keep_blocked" | "possible_manual_context_fix_but_no_auto_apply";

type RuntimeManualReasonCode = "none" | "parenthetical_prefix_fix_candidate" | "low_risk_not_found_residue";


function normalizeCitationCore(input: string) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}


function stripLeadPrefix(input: string) {
  return normalizeCitationCore(input).replace(/^ordinance/, "").replace(/^rule/, "");
}


function citationFamily(issue: UnresolvedIssueLite) {
  const normalized = stripLeadPrefix(issue.normalizedValue || issue.rawValue || "");
  const match = normalized.match(/^(\d+\.\d+)/);
  return match ? match[1] : null;
}


export function computeUnresolvedTriage(issues: UnresolvedIssueLite[]): UnresolvedTriage {
  const buckets = new Set<string>();
  const recurringFamilies = new Set<string>();
  const candidateFixes: string[] = [];
  const seenFixes = new Set<string>();
  const duplicateCounter = new Map<string, number>();
  const localIssues = issues || [];

  for (const issue of localIssues) {
    const sig = `${issue.referenceType}::${stripLeadPrefix(issue.normalizedValue || issue.rawValue || "")}`;
    duplicateCounter.set(sig, (duplicateCounter.get(sig) || 0) + 1);
  }

  for (const issue of localIssues) {
    const refType = String(issue.referenceType || "");
    const normalizedRaw = normalizeCitationCore(issue.normalizedValue || issue.rawValue || "");
    const normalized = stripLeadPrefix(issue.normalizedValue || issue.rawValue || "");
    const message = String(issue.message || "");
    const family = citationFamily(issue);
    if (family) recurringFamilies.add(family);

    const isUnsafe37 = refType === "ordinance_section" && UNSAFE_37X.has((family || "").trim());
    const isCrossContext =
      /cross[_\s-]?context/i.test(message) ||
      (refType === "rules_section" && normalized.startsWith("37.")) ||
      (refType === "ordinance_section" && !!normalized && !normalized.startsWith("37."));
    const isDuplicate = (duplicateCounter.get(`${issue.referenceType}::${normalized}`) || 0) > 1 || /duplicate|redundant/i.test(message);
    const hasPrefixResidue =
      /^ordinance37\./.test(normalizedRaw) || /^rule\d+\.\d+/.test(normalizedRaw) || /^ordinance\d+\.\d+/.test(normalizedRaw);
    const likelyParentheticalFix =
      hasPrefixResidue || /prefix|parenthetical|format|malformed|unable to parse|unparseable|invalid/i.test(message);
    const insufficientContext = /insufficient[_\s-]?context|no useful context|context unavailable|unable to determine context/i.test(message);

    if (isUnsafe37) {
      buckets.add("unsafe_37x_structural_block");
      const fix = `Keep ${family} blocked; do not auto-resolve. Source-backed manual legal review required.`;
      if (!seenFixes.has(fix)) {
        candidateFixes.push(fix);
        seenFixes.add(fix);
      }
      continue;
    }

    if (isCrossContext) {
      buckets.add("cross_context_ambiguous");
      const fix = `Review cross-context reference "${issue.rawValue}" and keep blocked until reviewer confirms context.`;
      if (!seenFixes.has(fix)) {
        candidateFixes.push(fix);
        seenFixes.add(fix);
      }
      continue;
    }

    if (isDuplicate) {
      buckets.add("duplicate_or_redundant_reference");
      const fix = `Drop duplicate unresolved reference "${issue.rawValue}" after source check.`;
      if (!seenFixes.has(fix)) {
        candidateFixes.push(fix);
        seenFixes.add(fix);
      }
    }

    if (likelyParentheticalFix) {
      buckets.add("likely_parenthetical_or_prefix_fix");
      const candidate = stripLeadPrefix(issue.rawValue || issue.normalizedValue || "");
      const fix = candidate
        ? `Check citation formatting for "${issue.rawValue}" and normalize to "${candidate}" only if source text matches.`
        : `Check citation formatting for "${issue.rawValue}" and normalize only with source-backed evidence.`;
      if (!seenFixes.has(fix)) {
        candidateFixes.push(fix);
        seenFixes.add(fix);
      }
    }

    if (insufficientContext) {
      buckets.add("insufficient_context_hold");
    }

    if (!isDuplicate && !likelyParentheticalFix) {
      if (/manual review/i.test(message) && String(issue.severity || "").toLowerCase() === "warning") {
        buckets.add("safe_manual_drop_candidate");
        const fix = `If "${issue.rawValue}" is non-substantive residue in source text, reviewer may drop it manually.`;
        if (!seenFixes.has(fix)) {
          candidateFixes.push(fix);
          seenFixes.add(fix);
        }
      } else if ((refType === "rules_section" && normalized.startsWith("37.")) || (refType === "ordinance_section" && /^i{1,4}-?\d/.test(normalized))) {
        buckets.add("likely_context_relabel_candidate");
        const fix = `Potential context relabel for "${issue.rawValue}" requires explicit reviewer confirmation; no auto-conversion.`;
        if (!seenFixes.has(fix)) {
          candidateFixes.push(fix);
          seenFixes.add(fix);
        }
      } else {
        buckets.add("structurally_blocked_not_found");
      }
    }
  }

  if (buckets.size === 0 && localIssues.length > 0) {
    buckets.add("structurally_blocked_not_found");
  }

  const effort: "low" | "medium" | "high" =
    buckets.has("unsafe_37x_structural_block") || buckets.has("cross_context_ambiguous") || buckets.has("structurally_blocked_not_found")
      ? "high"
      : localIssues.length <= 2
        ? "low"
        : "medium";

  const action = buckets.has("unsafe_37x_structural_block")
    ? "Unsafe 37.x citations remain blocked; perform targeted legal-source review."
    : buckets.has("cross_context_ambiguous")
      ? "Cross-context citation ambiguity requires reviewer context decision; keep blocked."
      : buckets.has("structurally_blocked_not_found")
        ? "Resolve true not-found citations or keep staged; do not auto-confirm."
        : buckets.has("likely_parenthetical_or_prefix_fix")
          ? "Apply source-backed citation format fixes, then re-run QC."
          : buckets.has("duplicate_or_redundant_reference")
            ? "Remove duplicate unresolved references and re-run QC."
            : "Perform conservative manual unresolved-reference review.";

  return {
    unresolvedBuckets: Array.from(buckets),
    topRecommendedReviewerAction: action,
    estimatedReviewerEffort: effort,
    candidateManualFixes: candidateFixes.slice(0, 8),
    recurringCitationFamilies: Array.from(recurringFamilies)
  };
}


export function computeRuntimeReviewerPolicy(params: {
  triage: UnresolvedTriage;
  blocked37xReferences: Blocked37xReference[];
  unresolvedReferenceCount: number;
  documentId: string;
  title: string;
  unresolvedBuckets: string[];
  recurringCitationFamilies: string[];
}) {
  const unresolvedBuckets = params.triage.unresolvedBuckets || [];
  const blockedRefs = params.blocked37xReferences || [];
  const recurringFamilies = params.triage.recurringCitationFamilies || [];
  const hasUnsafe37x = blockedRefs.some((ref) => UNSAFE_37X.has(String(ref.family || "").trim()));
  const hasCrossContext = unresolvedBuckets.includes("cross_context_ambiguous");
  const hasNotFound = unresolvedBuckets.includes("structurally_blocked_not_found");
  const hasInsufficientContext = unresolvedBuckets.includes("insufficient_context_hold");
  const hasManualSignal =
    unresolvedBuckets.includes("likely_parenthetical_or_prefix_fix") || unresolvedBuckets.includes("duplicate_or_redundant_reference");
  const lowRiskNotFoundResidue =
    hasNotFound &&
    hasManualSignal &&
    unresolvedBuckets.every((bucket) =>
      ["structurally_blocked_not_found", "likely_parenthetical_or_prefix_fix", "duplicate_or_redundant_reference"].includes(bucket)
    );
  const allowlistedFamily = recurringFamilies.some((family) => CONSERVATIVE_MANUAL_ALLOWLIST.has(String(family || "").trim()));

  const narrowManualCandidate =
    !hasUnsafe37x &&
    !hasCrossContext &&
    (!hasNotFound || lowRiskNotFoundResidue) &&
    !hasInsufficientContext &&
    hasManualSignal &&
    allowlistedFamily &&
    params.unresolvedReferenceCount > 0 &&
    params.unresolvedReferenceCount <= 2;

  const runtimeDisposition: RuntimeReviewerDisposition = narrowManualCandidate
    ? "possible_manual_context_fix_but_no_auto_apply"
    : "keep_blocked";
  const runtimeManualReasonCode: RuntimeManualReasonCode = narrowManualCandidate
    ? lowRiskNotFoundResidue
      ? "low_risk_not_found_residue"
      : "parenthetical_prefix_fix_candidate"
    : "none";
  const runtimeManualReasonSummary =
    runtimeManualReasonCode === "low_risk_not_found_residue"
      ? "Allowlisted citation family with low-risk not-found residue and source-backed formatting signal."
      : runtimeManualReasonCode === "parenthetical_prefix_fix_candidate"
        ? "Allowlisted citation family with deterministic formatting/prefix remediation signal."
        : "No runtime manual-candidate signal; conservatively kept blocked.";
  const runtimeSuggestedOperatorAction =
    runtimeManualReasonCode === "none"
      ? "No runtime manual action needed; keep blocked unless later app workflow provides clearer context."
      : "Review citation formatting during app workflow; leave blocked if generation output is still ambiguous.";

  const runtimePolicyReason = narrowManualCandidate
    ? "Narrow source-backed manual context fix candidate surfaced."
      : hasUnsafe37x
        ? "Unsafe 37.x family defaults to keep_blocked at runtime."
        : hasNotFound
          ? "Not-found citations default to keep_blocked at runtime unless low-risk allowlisted residue."
          : hasInsufficientContext
            ? "Insufficient-context citations default to keep_blocked at runtime."
          : hasCrossContext
            ? "Cross-context ambiguity defaults to keep_blocked at runtime."
            : "Conservative runtime default: keep_blocked.";

  const runtimeOperatorReviewSummary =
    runtimeDisposition === "possible_manual_context_fix_but_no_auto_apply"
      ? `${runtimeManualReasonCode}: ${runtimeManualReasonSummary}`
      : "Not surfaced for runtime manual review.";
  const runtimeReviewDiagnostic =
    runtimeDisposition === "possible_manual_context_fix_but_no_auto_apply"
      ? {
          id: params.documentId,
          title: params.title,
          runtimeManualReasonCode,
          runtimeManualReasonSummary,
          runtimeSuggestedOperatorAction,
          unresolvedBuckets: params.unresolvedBuckets,
          recurringCitationFamilies: params.recurringCitationFamilies,
          runtimeDoNotAutoApply: true
        }
      : null;

  return {
    runtimeDisposition,
    runtimeManualReasonCode,
    runtimeManualReasonSummary,
    runtimeSuggestedOperatorAction,
    runtimeOperatorReviewSummary,
    runtimeReviewDiagnostic,
    runtimePolicyReason,
    runtimeSurfaceForManualReview: narrowManualCandidate,
    runtimeManualReviewRequired: narrowManualCandidate,
    runtimeDoNotAutoApply: true
  };
}
