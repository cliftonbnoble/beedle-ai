import { adminIngestionMetadataUpdateSchema, adminIngestionRejectSchema } from "@beedle/shared";
import type { Env } from "../lib/types";
import { approveDecision, rebuildDocumentTextArtifacts } from "./ingest";
import { parseDocument } from "./parser";
import { inferTaxonomySuggestion } from "./taxonomy-inference";
import { inferIndexCodesFromReferences, refreshDocumentReferenceValidation, validateReferencesAgainstNormalized } from "./legal-references";

export class IngestionListBuildError extends Error {
  operation: string;
  subOperation: string;
  selectedDocCount: number;
  chunkingEnabled: boolean;
  chunkSize: number;
  chunkCount: number;
  currentChunkIndex: number;
  idsInCurrentChunk: number;
  queryKind: string;
  causeMessage: string;

  constructor(params: {
    operation: string;
    subOperation: string;
    selectedDocCount: number;
    chunkingEnabled: boolean;
    chunkSize: number;
    chunkCount: number;
    currentChunkIndex: number;
    idsInCurrentChunk: number;
    queryKind: string;
    cause: unknown;
  }) {
    const causeMessage = params.cause instanceof Error ? params.cause.message : String(params.cause ?? "unknown");
    super(`${params.operation}/${params.subOperation} failed: ${causeMessage}`);
    this.name = "IngestionListBuildError";
    this.operation = params.operation;
    this.subOperation = params.subOperation;
    this.selectedDocCount = params.selectedDocCount;
    this.chunkingEnabled = params.chunkingEnabled;
    this.chunkSize = params.chunkSize;
    this.chunkCount = params.chunkCount;
    this.currentChunkIndex = params.currentChunkIndex;
    this.idsInCurrentChunk = params.idsInCurrentChunk;
    this.queryKind = params.queryKind;
    this.causeMessage = causeMessage;
  }
}

function parseJsonArray(input: string | null | undefined): string[] {
  if (!input) return [];
  try {
    const parsed = JSON.parse(input);
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}

function parseJsonObject(input: string | null | undefined): Record<string, unknown> {
  if (!input) return {};
  try {
    const parsed = JSON.parse(input);
    return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function boolish(value: unknown): number {
  return value ? 1 : 0;
}

function recomputeQcFlags(headings: string[], metadata: { indexCodes: string[]; rulesSections: string[]; ordinanceSections: string[] }) {
  return {
    hasIndexCodes: metadata.indexCodes.length > 0 || headings.some((heading) => /index\s+codes?/i.test(heading)),
    hasRulesSection: metadata.rulesSections.length > 0 || headings.some((heading) => /^rules?$/i.test(heading)),
    hasOrdinanceSection: metadata.ordinanceSections.length > 0 || headings.some((heading) => /^ordinance(s)?$/i.test(heading))
  };
}

function normalizeCitationToken(input: string): string {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/^rule/, "")
    .replace(/^part[0-9a-z.\-]+\-/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

function detectCriticalReferenceExceptions(values: { rules: string[]; ordinance: string[] }) {
  const refs = [...values.rules, ...values.ordinance].map(normalizeCitationToken);
  const hits: string[] = [];
  if (refs.includes("37.2(g)")) hits.push("37.2(g)");
  if (refs.includes("37.15")) hits.push("37.15");
  if (refs.includes("10.10(c)(3)")) hits.push("10.10(c)(3)");
  return Array.from(new Set(hits));
}

export interface ListIngestionDocumentsOptions {
  status?: "all" | "staged" | "searchable" | "approved" | "rejected" | "pending";
  fileType?: "decision_docx" | "law_pdf";
  hasWarnings?: boolean;
  missingRequired?: boolean;
  unresolvedReferencesOnly?: boolean;
  criticalExceptionsOnly?: boolean;
  filteredNoiseOnly?: boolean;
  lowConfidenceTaxonomyOnly?: boolean;
  missingRulesOnly?: boolean;
  missingOrdinanceOnly?: boolean;
  approvalReadyOnly?: boolean;
  reviewerReadyOnly?: boolean;
  unresolvedTriageBucket?: string;
  recurringCitationFamily?: string;
  blocked37xOnly?: boolean;
  blocked37xFamily?: string;
  blocked37xBatchKey?: string;
  safeToBatchReviewOnly?: boolean;
  estimatedReviewerEffort?: "low" | "medium" | "high";
  reviewerRiskLevel?: "low" | "medium" | "high";
  blocker?: string;
  runtimeManualCandidatesOnly?: boolean;
  realOnly?: boolean;
  taxonomyCaseTypeId?: string;
  query?: string;
  limit?: number;
  sort?:
    | "createdAtDesc"
    | "createdAtAsc"
    | "confidenceDesc"
    | "confidenceAsc"
    | "titleAsc"
    | "titleDesc"
    | "warningCountDesc"
    | "unresolvedReferenceDesc"
    | "criticalExceptionDesc"
    | "approvalReadinessDesc"
    | "reviewerReadinessDesc"
    | "reviewerEffortAsc"
    | "batchabilityDesc"
    | "unresolvedLeverageDesc"
    | "blocked37xBatchKeyAsc";
}

const APPROVAL_THRESHOLDS = {
  maxUnresolvedReferences: 1,
  maxWarnings: 8,
  minExtractionConfidence: 0.55
} as const;

const LIST_DOCS_IN_QUERY_CHUNK = 75;

function chunkArray<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

type ApprovalReadinessInput = {
  fileType: "decision_docx" | "law_pdf";
  approvedAt: string | null;
  rejectedAt: string | null;
  qcPassed: number;
  qcRequiredConfirmed: number;
  warningCount: number;
  unresolvedReferenceCount: number;
  criticalExceptionCount: number;
  extractionConfidence: number;
  missingRulesDetection: boolean;
  missingOrdinanceDetection: boolean;
  lowConfidenceTaxonomy: boolean;
};

function effectiveUnresolvedThreshold(input: ApprovalReadinessInput): number {
  const limitedPilotConfirmed =
    input.qcPassed > 0 &&
    input.qcRequiredConfirmed > 0 &&
    input.extractionConfidence >= 0.6 &&
    input.warningCount <= 7 &&
    input.criticalExceptionCount === 0 &&
    !input.missingRulesDetection &&
    !input.missingOrdinanceDetection;
  if (limitedPilotConfirmed) {
    // Limited-pilot conservative tier: still bounded, no critical exceptions, QC-passed + confirmed.
    return 5;
  }
  const highConfidenceClean =
    input.extractionConfidence >= 0.72 &&
    input.warningCount <= 5 &&
    input.criticalExceptionCount === 0 &&
    !input.missingRulesDetection &&
    !input.missingOrdinanceDetection;
  // Conservative tuning: allow 2 unresolved refs only for otherwise clean/high-confidence docs.
  return highConfidenceClean ? 2 : APPROVAL_THRESHOLDS.maxUnresolvedReferences;
}

function computeApprovalReadiness(input: ApprovalReadinessInput) {
  const blockers: string[] = [];
  const cautions: string[] = [];
  const unresolvedThreshold = effectiveUnresolvedThreshold(input);

  if (input.fileType !== "decision_docx") blockers.push("not_decision_docx");
  if (input.rejectedAt) blockers.push("rejected");
  if (input.approvedAt) blockers.push("already_approved");
  if (!input.qcPassed) blockers.push("qc_gate_not_passed");
  if (!input.qcRequiredConfirmed) blockers.push("metadata_not_confirmed");
  if (input.criticalExceptionCount > 0) blockers.push("critical_reference_exception_present");
  if (input.unresolvedReferenceCount > unresolvedThreshold) blockers.push("unresolved_references_above_threshold");
  if (input.warningCount > APPROVAL_THRESHOLDS.maxWarnings) blockers.push("warnings_above_threshold");
  if (input.extractionConfidence < APPROVAL_THRESHOLDS.minExtractionConfidence) blockers.push("extraction_confidence_below_threshold");
  if (input.missingRulesDetection) blockers.push("missing_rules_detection");
  if (input.missingOrdinanceDetection) blockers.push("missing_ordinance_detection");
  if (input.lowConfidenceTaxonomy) cautions.push("low_confidence_taxonomy");

  const score = Math.max(
    0,
    100 -
      blockers.length * 15 -
      Math.min(30, input.unresolvedReferenceCount * 5) -
      Math.min(20, Math.max(0, input.warningCount - 2) * 2) +
      Math.round(Math.min(20, input.extractionConfidence * 20))
  );

  return {
    eligible: blockers.length === 0,
    score,
    blockers,
    cautions,
    thresholds: {
      ...APPROVAL_THRESHOLDS,
      maxUnresolvedReferences: unresolvedThreshold
    },
    nextActions: blockers.map((blocker) => {
      switch (blocker) {
        case "metadata_not_confirmed":
          return "Confirm extracted index/rules/ordinance metadata in QC.";
        case "qc_gate_not_passed":
          return "Fill missing required metadata (Index Codes + Rules + Ordinance).";
        case "unresolved_references_above_threshold":
          return "Review unresolved references and correct malformed/unknown citations.";
        case "critical_reference_exception_present":
          return "Manual legal-reference review required for critical exception citations.";
        case "warnings_above_threshold":
          return "Review extraction warnings and clean metadata before promotion.";
        case "extraction_confidence_below_threshold":
          return "Reprocess document and confirm core sections/anchors.";
        case "missing_rules_detection":
          return "Ensure valid Rules sections are captured.";
        case "missing_ordinance_detection":
          return "Ensure valid Ordinance sections are captured.";
        default:
          return blocker;
      }
    })
  };
}

function qcFailedRequirements(input: { qcHasIndexCodes: number; qcHasRulesSection: number; qcHasOrdinanceSection: number }) {
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

function computeReviewerReadiness(input: ReviewerReadinessInput) {
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

function isLikelyFixtureDoc(params: { title: string; citation: string; metadata: Record<string, unknown> }) {
  const filename = typeof params.metadata.originalFilename === "string" ? params.metadata.originalFilename.toLowerCase() : "";
  const joined = `${params.title} ${params.citation} ${filename}`.toLowerCase();
  return /harness|fixture|seed|decision_pass|decision_fail|decision_invalid|law_sample|bee-harness/.test(joined);
}

function listSortClause(sort: ListIngestionDocumentsOptions["sort"]) {
  switch (sort) {
    case "createdAtAsc":
      return "created_at ASC";
    case "confidenceDesc":
      return "extraction_confidence DESC, created_at DESC";
    case "confidenceAsc":
      return "extraction_confidence ASC, created_at DESC";
    case "titleAsc":
      return "title ASC, created_at DESC";
    case "titleDesc":
      return "title DESC, created_at DESC";
    case "warningCountDesc":
      return "warningCount DESC, unresolvedReferenceCount DESC, created_at DESC";
    case "unresolvedReferenceDesc":
      return "unresolvedReferenceCount DESC, warningCount DESC, created_at DESC";
    case "criticalExceptionDesc":
      return "criticalExceptionCount DESC, warningCount DESC, created_at DESC";
    case "approvalReadinessDesc":
      return "created_at DESC";
    case "reviewerReadinessDesc":
    case "reviewerEffortAsc":
    case "batchabilityDesc":
    case "unresolvedLeverageDesc":
    case "blocked37xBatchKeyAsc":
      return "created_at DESC";
    case "createdAtDesc":
    default:
      return "created_at DESC";
  }
}

type Blocked37xReference = {
  family: "37.3" | "37.7" | "37.9";
  referenceType: string;
  rawValue: string;
  normalizedValue: string;
  message: string;
};

function computeBlocked37xReview(issues: UnresolvedIssueLite[]) {
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

type UnresolvedIssueLite = {
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

function computeUnresolvedTriage(issues: UnresolvedIssueLite[]): UnresolvedTriage {
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

function computeRuntimeReviewerPolicy(params: {
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

export async function listIngestionDocuments(env: Env, options: ListIngestionDocumentsOptions = {}) {
  const where: string[] = [];
  const binds: unknown[] = [];

  if (options.status && options.status !== "all") {
    if (options.status === "staged") where.push("searchable_at IS NULL");
    if (options.status === "searchable") where.push("searchable_at IS NOT NULL");
    if (options.status === "approved") where.push("approved_at IS NOT NULL");
    if (options.status === "rejected") where.push("rejected_at IS NOT NULL");
    if (options.status === "pending") where.push("approved_at IS NULL AND rejected_at IS NULL");
  }

  if (options.fileType) {
    where.push("file_type = ?");
    binds.push(options.fileType);
  }

  if (options.hasWarnings) {
    where.push("extraction_warnings_json <> '[]'");
  }

  if (options.missingRequired) {
    where.push("file_type = 'decision_docx' AND (qc_has_index_codes = 0 OR qc_has_rules_section = 0 OR qc_has_ordinance_section = 0)");
  }
  if (options.missingRulesOnly) {
    where.push("file_type = 'decision_docx' AND qc_has_rules_section = 0");
  }
  if (options.missingOrdinanceOnly) {
    where.push("file_type = 'decision_docx' AND qc_has_ordinance_section = 0");
  }

  if (options.unresolvedReferencesOnly) {
    where.push("EXISTS (SELECT 1 FROM document_reference_issues dri WHERE dri.document_id = d.id)");
  }

  if (options.criticalExceptionsOnly) {
    where.push(
      "EXISTS (SELECT 1 FROM document_reference_links drl WHERE drl.document_id = d.id AND drl.normalized_value IN ('37.2(g)', '37.15', '10.10(c)(3)'))"
    );
  }
  if (options.filteredNoiseOnly) {
    where.push("d.extraction_warnings_json LIKE '%Extraction noise filtered%'");
  }
  if (options.lowConfidenceTaxonomyOnly) {
    where.push("json_extract(d.metadata_json, '$.taxonomy.fallback') = 1 OR COALESCE(json_extract(d.metadata_json, '$.taxonomy.confidence'), 0) < 0.45");
  }

  if (options.taxonomyCaseTypeId) {
    where.push("json_extract(metadata_json, '$.taxonomy.caseTypeId') = ?");
    binds.push(options.taxonomyCaseTypeId);
  }

  if (options.query && options.query.trim().length > 0) {
    where.push("(title LIKE ? OR citation LIKE ? OR case_number LIKE ?)");
    const value = `%${options.query.trim()}%`;
    binds.push(value, value, value);
  }

  const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const orderBy = listSortClause(options.sort);
  const limit = Math.min(Math.max(options.limit ?? 200, 1), 2000);

  let rows;
  try {
    rows = await env.DB.prepare(
      `SELECT
      d.id,
      d.file_type as fileType,
      d.title,
      d.citation,
      d.decision_date as decisionDate,
      d.created_at as createdAt,
      d.approved_at as approvedAt,
      d.rejected_at as rejectedAt,
      d.searchable_at as searchableAt,
      d.qc_passed as qcPassed,
      d.qc_has_index_codes as qcHasIndexCodes,
      d.qc_has_rules_section as qcHasRulesSection,
      d.qc_has_ordinance_section as qcHasOrdinanceSection,
      d.qc_required_confirmed as qcRequiredConfirmed,
      d.extraction_confidence as extractionConfidence,
      d.extraction_warnings_json as extractionWarningsJson,
      d.index_codes_json as indexCodesJson,
      d.metadata_json as metadataJson,
      COALESCE(json_array_length(d.extraction_warnings_json), 0) as warningCount,
      (SELECT COUNT(*) FROM document_reference_issues dri WHERE dri.document_id = d.id) as unresolvedReferenceCount,
      (SELECT COUNT(*) FROM document_reference_issues dri WHERE dri.document_id = d.id AND dri.reference_type = 'rules_section') as unresolvedRulesCount,
      (SELECT COUNT(*) FROM document_reference_issues dri WHERE dri.document_id = d.id AND dri.reference_type = 'ordinance_section') as unresolvedOrdinanceCount,
      (SELECT COUNT(*) FROM document_reference_issues dri WHERE dri.document_id = d.id AND dri.reference_type = 'index_code') as unresolvedIndexCount,
      (
        SELECT COUNT(*)
        FROM document_reference_issues dri
        WHERE dri.document_id = d.id
          AND dri.reference_type = 'ordinance_section'
          AND dri.normalized_value IN ('ordinance37.3', 'ordinance37.7', 'ordinance37.9')
      ) as unresolvedUnsafe37xCount,
      (
        SELECT COUNT(*)
        FROM document_reference_links drl
        WHERE drl.document_id = d.id
          AND drl.reference_type = 'index_code'
          AND drl.is_valid = 1
      ) as validIndexRefCount,
      (
        SELECT COUNT(*)
        FROM document_reference_links drl
        WHERE drl.document_id = d.id
          AND drl.reference_type = 'rules_section'
          AND drl.is_valid = 1
      ) as validRulesRefCount,
      (
        SELECT COUNT(*)
        FROM document_reference_links drl
        WHERE drl.document_id = d.id
          AND drl.reference_type = 'ordinance_section'
          AND drl.is_valid = 1
      ) as validOrdinanceRefCount,
      (
        SELECT COUNT(DISTINCT drl.normalized_value)
        FROM document_reference_links drl
        WHERE drl.document_id = d.id
          AND drl.normalized_value IN ('37.2(g)', '37.15', '10.10(c)(3)')
      ) as criticalExceptionCount
     FROM documents d
     ${whereClause}
     ORDER BY ${orderBy}
     LIMIT ?`
    )
      .bind(...binds, limit)
      .all<{
    id: string;
    fileType: "decision_docx" | "law_pdf";
    title: string;
    citation: string;
    decisionDate: string | null;
    createdAt: string;
    approvedAt: string | null;
    rejectedAt: string | null;
    searchableAt: string | null;
    qcPassed: number;
    qcHasIndexCodes: number;
    qcHasRulesSection: number;
    qcHasOrdinanceSection: number;
    qcRequiredConfirmed: number;
    extractionConfidence: number;
    extractionWarningsJson: string;
    indexCodesJson: string;
    metadataJson: string | null;
    warningCount: number;
    unresolvedReferenceCount: number;
    unresolvedRulesCount: number;
    unresolvedOrdinanceCount: number;
    unresolvedIndexCount: number;
    unresolvedUnsafe37xCount: number;
    validIndexRefCount: number;
    validRulesRefCount: number;
    validOrdinanceRefCount: number;
      criticalExceptionCount: number;
    }>();
  } catch (error) {
    throw new IngestionListBuildError({
      operation: "list_ingestion_documents",
      subOperation: "select_documents",
      selectedDocCount: 0,
      chunkingEnabled: false,
      chunkSize: 0,
      chunkCount: 0,
      currentChunkIndex: 0,
      idsInCurrentChunk: 0,
      queryKind: "documents_select",
      cause: error
    });
  }

  const docIds = (rows.results ?? []).map((row) => row.id);
  const unresolvedByDoc = new Map<string, UnresolvedIssueLite[]>();
  if (docIds.length > 0) {
    const chunks = chunkArray(docIds, LIST_DOCS_IN_QUERY_CHUNK);
    for (let i = 0; i < chunks.length; i += 1) {
      const chunk = chunks[i]!;
      const placeholders = chunk.map(() => "?").join(",");
      let issues;
      try {
        issues = await env.DB.prepare(
          `SELECT document_id as documentId,
                reference_type as referenceType,
                raw_value as rawValue,
                normalized_value as normalizedValue,
                message,
                severity
         FROM document_reference_issues
         WHERE document_id IN (${placeholders})
         ORDER BY created_at DESC`
        )
          .bind(...chunk)
          .all<{
          documentId: string;
          referenceType: string;
          rawValue: string;
          normalizedValue: string;
          message: string;
          severity: string;
          }>();
      } catch (error) {
        throw new IngestionListBuildError({
          operation: "list_ingestion_documents",
          subOperation: "load_unresolved_issues_by_chunk",
          selectedDocCount: docIds.length,
          chunkingEnabled: true,
          chunkSize: LIST_DOCS_IN_QUERY_CHUNK,
          chunkCount: chunks.length,
          currentChunkIndex: i,
          idsInCurrentChunk: chunk.length,
          queryKind: "document_reference_issues_by_doc_ids",
          cause: error
        });
      }
      for (const issue of issues.results ?? []) {
        const list = unresolvedByDoc.get(issue.documentId) ?? [];
        list.push({
          referenceType: issue.referenceType,
          rawValue: issue.rawValue,
          normalizedValue: issue.normalizedValue,
          message: issue.message,
          severity: issue.severity
        });
        unresolvedByDoc.set(issue.documentId, list);
      }
    }
  }

  const documents = (rows.results ?? []).map((row) => {
    const metadata = parseJsonObject(row.metadataJson);
    const taxonomy = asObject(metadata.taxonomy);
    const extractionWarnings = parseJsonArray(row.extractionWarningsJson);
    const filteredNoiseCount = extractionWarnings.filter((warning) => warning.toLowerCase().includes("extraction noise filtered")).length;
    const taxonomyConfidence = typeof taxonomy.confidence === "number" ? taxonomy.confidence : null;
    const taxonomyFallback = Boolean(taxonomy.fallback);
    const doc = {
      ...row,
      extractionWarnings,
      indexCodes: parseJsonArray(row.indexCodesJson),
      taxonomySuggestion: {
        caseTypeId: typeof taxonomy.caseTypeId === "string" ? taxonomy.caseTypeId : null,
        caseTypeLabel: typeof taxonomy.caseTypeLabel === "string" ? taxonomy.caseTypeLabel : null,
        confidence: taxonomyConfidence,
        fallback: taxonomyFallback
      },
      warningCount: row.warningCount || 0,
      filteredNoiseCount,
      unresolvedReferenceCount: row.unresolvedReferenceCount || 0,
      criticalExceptionCount: row.criticalExceptionCount || 0,
      missingRulesDetection: row.qcHasRulesSection === 0,
      missingOrdinanceDetection: row.qcHasOrdinanceSection === 0,
      failedQcRequirements: qcFailedRequirements({
        qcHasIndexCodes: row.qcHasIndexCodes,
        qcHasRulesSection: row.qcHasRulesSection,
        qcHasOrdinanceSection: row.qcHasOrdinanceSection
      }),
      lowConfidenceTaxonomy: taxonomyFallback || (taxonomyConfidence !== null && taxonomyConfidence < 0.45),
      isLikelyFixture: isLikelyFixtureDoc({ title: row.title, citation: row.citation, metadata }),
      status: row.rejectedAt ? "rejected" : row.approvedAt ? "approved" : row.searchableAt ? "searchable" : "staged"
    };
    const approvalReadiness = computeApprovalReadiness({
      fileType: doc.fileType,
      approvedAt: doc.approvedAt,
      rejectedAt: doc.rejectedAt,
      qcPassed: doc.qcPassed,
      qcRequiredConfirmed: doc.qcRequiredConfirmed,
      warningCount: doc.warningCount,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      criticalExceptionCount: doc.criticalExceptionCount,
      extractionConfidence: doc.extractionConfidence || 0,
      missingRulesDetection: doc.missingRulesDetection,
      missingOrdinanceDetection: doc.missingOrdinanceDetection,
      lowConfidenceTaxonomy: doc.lowConfidenceTaxonomy
    });
    const reviewer = computeReviewerReadiness({
      isLikelyFixture: doc.isLikelyFixture,
      blockers: approvalReadiness.blockers,
      qcPassed: doc.qcPassed,
      qcRequiredConfirmed: doc.qcRequiredConfirmed,
      failedQcRequirements: doc.failedQcRequirements,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      unresolvedRulesCount: row.unresolvedRulesCount || 0,
      unresolvedOrdinanceCount: row.unresolvedOrdinanceCount || 0,
      unresolvedIndexCount: row.unresolvedIndexCount || 0,
      unresolvedUnsafe37xCount: row.unresolvedUnsafe37xCount || 0,
      criticalExceptionCount: doc.criticalExceptionCount,
      extractionConfidence: doc.extractionConfidence || 0,
      validIndexRefCount: row.validIndexRefCount || 0,
      validRulesRefCount: row.validRulesRefCount || 0,
      validOrdinanceRefCount: row.validOrdinanceRefCount || 0,
      missingRulesDetection: doc.missingRulesDetection,
      missingOrdinanceDetection: doc.missingOrdinanceDetection,
      approvalThresholds: { maxUnresolvedReferences: approvalReadiness.thresholds.maxUnresolvedReferences }
    });
    const triage = computeUnresolvedTriage(unresolvedByDoc.get(row.id) ?? []);
    const blocked37x = computeBlocked37xReview(unresolvedByDoc.get(row.id) ?? []);
    const runtimePolicy = computeRuntimeReviewerPolicy({
      triage,
      blocked37xReferences: blocked37x.blocked37xReferences,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      documentId: doc.id,
      title: doc.title,
      unresolvedBuckets: triage.unresolvedBuckets,
      recurringCitationFamilies: triage.recurringCitationFamilies
    });
    return {
      ...doc,
      approvalReadiness,
      reviewerReady: reviewer.reviewerReady,
      reviewerReadyReasons: reviewer.reviewerReadyReasons,
      reviewerRequiredActions: reviewer.reviewerRequiredActions,
      reviewerRiskLevel: reviewer.reviewerRiskLevel,
      metadataConfirmationWouldUnlock: reviewer.metadataConfirmationWouldUnlock,
      unresolvedBlockersAfterConfirmation: reviewer.unresolvedBlockersAfterConfirmation,
      unresolvedBuckets: triage.unresolvedBuckets,
      topRecommendedReviewerAction: runtimePolicy.runtimeSurfaceForManualReview
        ? triage.topRecommendedReviewerAction
        : runtimePolicy.runtimePolicyReason,
      estimatedReviewerEffort: triage.estimatedReviewerEffort,
      candidateManualFixes: runtimePolicy.runtimeSurfaceForManualReview ? triage.candidateManualFixes : [],
      recurringCitationFamilies: triage.recurringCitationFamilies,
      runtimeDisposition: runtimePolicy.runtimeDisposition,
      runtimeManualReasonCode: runtimePolicy.runtimeManualReasonCode,
      runtimeManualReasonSummary: runtimePolicy.runtimeManualReasonSummary,
      runtimeSuggestedOperatorAction: runtimePolicy.runtimeSuggestedOperatorAction,
      runtimeOperatorReviewSummary: runtimePolicy.runtimeOperatorReviewSummary,
      runtimeReviewDiagnostic: runtimePolicy.runtimeReviewDiagnostic,
      runtimePolicyReason: runtimePolicy.runtimePolicyReason,
      runtimeSurfaceForManualReview: runtimePolicy.runtimeSurfaceForManualReview,
      runtimeManualReviewRequired: runtimePolicy.runtimeManualReviewRequired,
      runtimeDoNotAutoApply: runtimePolicy.runtimeDoNotAutoApply,
      blocked37xReferences: blocked37x.blocked37xReferences,
      blocked37xReason: blocked37x.blocked37xReason,
      blocked37xReviewerHint: blocked37x.blocked37xReviewerHint,
      blocked37xSafeToBatchReview: blocked37x.blocked37xSafeToBatchReview,
      blocked37xBatchKey: blocked37x.blocked37xBatchKey
    };
  });

  const triageGroups = new Map<string, string[]>();
  for (const item of documents) {
    const signature = `${(item.unresolvedBuckets || []).slice().sort().join("|")}::${(item.recurringCitationFamilies || [])
      .slice()
      .sort()
      .join("|")}`;
    const group = triageGroups.get(signature) ?? [];
    group.push(item.id);
    triageGroups.set(signature, group);
  }
  const withBatchability = documents.map((item) => {
    const signature = `${(item.unresolvedBuckets || []).slice().sort().join("|")}::${(item.recurringCitationFamilies || [])
      .slice()
      .sort()
      .join("|")}`;
    const peers = (triageGroups.get(signature) ?? []).filter((id) => id !== item.id);
    return {
      ...item,
      canBatchReviewWith: peers
    };
  });

  let filtered = withBatchability;
  if (options.realOnly) {
    filtered = filtered.filter((item) => !item.isLikelyFixture);
  }
  if (options.approvalReadyOnly) {
    filtered = filtered.filter((item) => item.approvalReadiness.eligible);
  }
  if (options.reviewerReadyOnly) {
    filtered = filtered.filter((item) => item.reviewerReady);
  }
  if (options.unresolvedTriageBucket) {
    filtered = filtered.filter((item) => (item.unresolvedBuckets || []).includes(options.unresolvedTriageBucket as string));
  }
  if (options.recurringCitationFamily) {
    filtered = filtered.filter((item) => (item.recurringCitationFamilies || []).includes(options.recurringCitationFamily as string));
  }
  if (options.blocked37xOnly) {
    filtered = filtered.filter((item) => (item.blocked37xReferences || []).length > 0);
  }
  if (options.blocked37xFamily) {
    filtered = filtered.filter((item) =>
      (item.blocked37xReferences || []).some((ref: { family: string }) => ref.family === options.blocked37xFamily)
    );
  }
  if (options.blocked37xBatchKey) {
    filtered = filtered.filter((item) => item.blocked37xBatchKey === options.blocked37xBatchKey);
  }
  if (options.safeToBatchReviewOnly) {
    filtered = filtered.filter((item) => item.blocked37xSafeToBatchReview);
  }
  if (options.estimatedReviewerEffort) {
    filtered = filtered.filter((item) => item.estimatedReviewerEffort === options.estimatedReviewerEffort);
  }
  if (options.reviewerRiskLevel) {
    filtered = filtered.filter((item) => item.reviewerRiskLevel === options.reviewerRiskLevel);
  }
  if (options.blocker) {
    filtered = filtered.filter((item) => item.approvalReadiness.blockers.includes(options.blocker as string));
  }
  if (options.runtimeManualCandidatesOnly) {
    filtered = filtered.filter((item) => item.runtimeSurfaceForManualReview);
  }
  if (options.sort === "approvalReadinessDesc") {
    filtered = [...filtered].sort((a, b) => b.approvalReadiness.score - a.approvalReadiness.score || b.createdAt.localeCompare(a.createdAt));
  }
  if (options.sort === "reviewerReadinessDesc") {
    const riskRank = { low: 3, medium: 2, high: 1 } as const;
    filtered = [...filtered].sort(
      (a, b) =>
        Number(b.reviewerReady) - Number(a.reviewerReady) ||
        riskRank[b.reviewerRiskLevel] - riskRank[a.reviewerRiskLevel] ||
        b.approvalReadiness.score - a.approvalReadiness.score ||
        b.createdAt.localeCompare(a.createdAt)
    );
  }
  if (options.sort === "reviewerEffortAsc") {
    const effortRank = { low: 1, medium: 2, high: 3 } as const;
    filtered = [...filtered].sort(
      (a, b) =>
        effortRank[a.estimatedReviewerEffort] - effortRank[b.estimatedReviewerEffort] ||
        (b.canBatchReviewWith?.length || 0) - (a.canBatchReviewWith?.length || 0) ||
        b.approvalReadiness.score - a.approvalReadiness.score
    );
  }
  if (options.sort === "batchabilityDesc") {
    filtered = [...filtered].sort(
      (a, b) =>
        (b.canBatchReviewWith?.length || 0) - (a.canBatchReviewWith?.length || 0) ||
        b.unresolvedReferenceCount - a.unresolvedReferenceCount ||
        b.approvalReadiness.score - a.approvalReadiness.score
    );
  }
  if (options.sort === "unresolvedLeverageDesc") {
    const effortRank = { low: 3, medium: 2, high: 1 } as const;
    filtered = [...filtered].sort(
      (a, b) =>
        effortRank[a.estimatedReviewerEffort] - effortRank[b.estimatedReviewerEffort] ||
        (b.canBatchReviewWith?.length || 0) - (a.canBatchReviewWith?.length || 0) ||
        a.unresolvedReferenceCount - b.unresolvedReferenceCount
    );
  }
  if (options.sort === "blocked37xBatchKeyAsc") {
    filtered = [...filtered].sort(
      (a, b) =>
        String(a.blocked37xBatchKey || "").localeCompare(String(b.blocked37xBatchKey || "")) ||
        Number(b.blocked37xSafeToBatchReview) - Number(a.blocked37xSafeToBatchReview) ||
        b.unresolvedReferenceCount - a.unresolvedReferenceCount
    );
  }

  const blockerBreakdown = new Map<string, number>();
  for (const item of filtered) {
    for (const blocker of item.approvalReadiness.blockers) {
      blockerBreakdown.set(blocker, (blockerBreakdown.get(blocker) || 0) + 1);
    }
  }

  return {
    documents: filtered,
    summary: {
      total: filtered.length,
      approved: filtered.filter((item) => item.approvedAt).length,
      rejected: filtered.filter((item) => item.rejectedAt).length,
      searchable: filtered.filter((item) => item.searchableAt).length,
      staged: filtered.filter((item) => !item.searchableAt).length,
      missingRequired: filtered.filter(
        (item) => item.fileType === "decision_docx" && (item.qcPassed === 0 || item.qcRequiredConfirmed === 0)
      ).length,
      withWarnings: filtered.filter((item) => item.extractionWarnings.length > 0).length,
      withUnresolvedReferences: filtered.filter((item) => item.unresolvedReferenceCount > 0).length,
      withCriticalExceptions: filtered.filter((item) => item.criticalExceptionCount > 0).length,
      withFilteredNoise: filtered.filter((item) => item.filteredNoiseCount > 0).length,
      withLowConfidenceTaxonomy: filtered.filter((item) => item.lowConfidenceTaxonomy).length,
      withMissingRulesDetection: filtered.filter((item) => item.missingRulesDetection).length,
      withMissingOrdinanceDetection: filtered.filter((item) => item.missingOrdinanceDetection).length,
      approvalReady: filtered.filter((item) => item.approvalReadiness.eligible).length,
      reviewerReady: filtered.filter((item) => item.reviewerReady).length,
      likelyFixtures: filtered.filter((item) => item.isLikelyFixture).length,
      realDocs: filtered.filter((item) => !item.isLikelyFixture).length,
      realApprovalReady: filtered.filter((item) => !item.isLikelyFixture && item.approvalReadiness.eligible).length,
      realReviewerReady: filtered.filter((item) => !item.isLikelyFixture && item.reviewerReady).length,
      realApproved: filtered.filter((item) => !item.isLikelyFixture && Boolean(item.approvedAt)).length,
      realSearchable: filtered.filter((item) => !item.isLikelyFixture && Boolean(item.searchableAt)).length,
      surfacedRuntimeManualCandidates: filtered.filter((item) => item.runtimeSurfaceForManualReview).length,
      surfacedRuntimeManualRealCandidates: filtered.filter((item) => item.runtimeSurfaceForManualReview && !item.isLikelyFixture).length,
      surfacedRuntimeManualFixtureCandidates: filtered.filter((item) => item.runtimeSurfaceForManualReview && item.isLikelyFixture).length,
      unsafeRuntimeManualSurfacedViolations: filtered.filter(
        (item) =>
          item.runtimeSurfaceForManualReview &&
          (item.blocked37xReferences || []).some((ref: { family: string }) => ["37.3", "37.7", "37.9"].includes(ref.family))
      ).length,
      unsafeRuntimeManualSuppressedCount: filtered.filter(
        (item) =>
          !item.runtimeSurfaceForManualReview &&
          (item.blocked37xReferences || []).some((ref: { family: string }) => ["37.3", "37.7", "37.9"].includes(ref.family))
      ).length,
      blockerBreakdown: Array.from(blockerBreakdown.entries())
        .map(([blocker, count]) => ({ blocker, count }))
        .sort((a, b) => b.count - a.count)
    }
  };
}

export async function getIngestionDocumentDetail(env: Env, documentId: string) {
  const document = await env.DB.prepare(
    `SELECT
      id,
      file_type as fileType,
      jurisdiction,
      title,
      citation,
      decision_date as decisionDate,
      case_number as caseNumber,
      author_name as authorName,
      outcome_label as outcomeLabel,
      source_r2_key as sourceFileRef,
      source_link as sourceLink,
      qc_has_index_codes as qcHasIndexCodes,
      qc_has_rules_section as qcHasRulesSection,
      qc_has_ordinance_section as qcHasOrdinanceSection,
      qc_passed as qcPassed,
      qc_required_confirmed as qcRequiredConfirmed,
      approved_at as approvedAt,
      searchable_at as searchableAt,
      rejected_at as rejectedAt,
      rejected_reason as rejectedReason,
      extraction_confidence as extractionConfidence,
      extraction_warnings_json as extractionWarningsJson,
      index_codes_json as indexCodesJson,
      rules_sections_json as rulesSectionsJson,
      ordinance_sections_json as ordinanceSectionsJson,
      metadata_json as metadataJson,
      created_at as createdAt,
      updated_at as updatedAt
     FROM documents
     WHERE id = ?`
  )
    .bind(documentId)
    .first<{
      id: string;
      fileType: "decision_docx" | "law_pdf";
      jurisdiction: string;
      title: string;
      citation: string;
      decisionDate: string | null;
      caseNumber: string | null;
      authorName: string | null;
      outcomeLabel: string | null;
      sourceFileRef: string;
      sourceLink: string;
      qcHasIndexCodes: number;
      qcHasRulesSection: number;
      qcHasOrdinanceSection: number;
      qcPassed: number;
      qcRequiredConfirmed: number;
      approvedAt: string | null;
      searchableAt: string | null;
      rejectedAt: string | null;
      rejectedReason: string | null;
      extractionConfidence: number;
      extractionWarningsJson: string;
      indexCodesJson: string;
      rulesSectionsJson: string;
      ordinanceSectionsJson: string;
      metadataJson: string | null;
      createdAt: string;
      updatedAt: string;
    }>();

  if (!document) {
    return null;
  }

  const sections = await env.DB.prepare(
    `SELECT
      s.id,
      s.heading,
      s.canonical_key as canonicalKey,
      s.section_order as sectionOrder,
      p.id as paragraphId,
      p.anchor,
      p.paragraph_order as paragraphOrder,
      p.text
     FROM document_sections s
     LEFT JOIN section_paragraphs p ON p.section_id = s.id
     WHERE s.document_id = ?
     ORDER BY s.section_order ASC, p.paragraph_order ASC`
  )
    .bind(documentId)
    .all<{
      id: string;
      heading: string;
      canonicalKey: string;
      sectionOrder: number;
      paragraphId: string | null;
      anchor: string | null;
      paragraphOrder: number | null;
      text: string | null;
    }>();

  const chunks = await env.DB.prepare(
    `SELECT
      id,
      section_id as sectionId,
      paragraph_anchor as paragraphAnchor,
      paragraph_anchor_end as paragraphAnchorEnd,
      citation_anchor as citationAnchor,
      section_label as sectionLabel,
      chunk_order as chunkOrder,
      chunk_text as chunkText,
      token_estimate as tokenEstimate,
      chunk_warnings_json as chunkWarningsJson
     FROM document_chunks
     WHERE document_id = ?
     ORDER BY chunk_order ASC`
  )
    .bind(documentId)
    .all<{
      id: string;
      sectionId: string;
      paragraphAnchor: string;
      paragraphAnchorEnd: string | null;
      citationAnchor: string;
      sectionLabel: string;
      chunkOrder: number;
      chunkText: string;
      tokenEstimate: number;
      chunkWarningsJson: string;
    }>();

  const referenceIssues = await env.DB.prepare(
    `SELECT reference_type as referenceType, raw_value as rawValue, normalized_value as normalizedValue, message, severity, created_at as createdAt
     FROM document_reference_issues
     WHERE document_id = ?
     ORDER BY created_at DESC`
  )
    .bind(documentId)
    .all<{
      referenceType: string;
      rawValue: string;
      normalizedValue: string;
      message: string;
      severity: string;
      createdAt: string;
    }>();

  const validReferenceLinks = await env.DB.prepare(
    `SELECT reference_type as referenceType, canonical_value as canonicalValue
     FROM document_reference_links
     WHERE document_id = ? AND is_valid = 1 AND canonical_value IS NOT NULL
     ORDER BY reference_type ASC, canonical_value ASC`
  )
    .bind(documentId)
    .all<{ referenceType: "index_code" | "rules_section" | "ordinance_section"; canonicalValue: string }>();

  const criticalExceptionRefs = await env.DB.prepare(
    `SELECT DISTINCT normalized_value as normalizedValue
     FROM document_reference_links
     WHERE document_id = ?
       AND normalized_value IN ('37.2(g)', '37.15', '10.10(c)(3)')
     ORDER BY normalized_value ASC`
  )
    .bind(documentId)
    .all<{ normalizedValue: string }>();

  const sectionMap = new Map<string, {
    id: string;
    heading: string;
    canonicalKey: string;
    sectionOrder: number;
    paragraphs: Array<{ id: string; anchor: string; paragraphOrder: number; text: string }>;
  }>();

  for (const row of sections.results ?? []) {
    const section = sectionMap.get(row.id) ?? {
      id: row.id,
      heading: row.heading,
      canonicalKey: row.canonicalKey,
      sectionOrder: row.sectionOrder,
      paragraphs: []
    };

    if (row.paragraphId && row.anchor && row.paragraphOrder !== null && row.text) {
      section.paragraphs.push({
        id: row.paragraphId,
        anchor: row.anchor,
        paragraphOrder: row.paragraphOrder,
        text: row.text
      });
    }

    sectionMap.set(row.id, section);
  }

  const metadata = parseJsonObject(document.metadataJson);
  const taxonomy = asObject(metadata.taxonomy);

  const detail = {
    ...document,
    extractionWarnings: parseJsonArray(document.extractionWarningsJson),
    indexCodes: parseJsonArray(document.indexCodesJson),
    rulesSections: parseJsonArray(document.rulesSectionsJson),
    ordinanceSections: parseJsonArray(document.ordinanceSectionsJson),
    taxonomySuggestion: {
      caseTypeId: typeof taxonomy.caseTypeId === "string" ? taxonomy.caseTypeId : null,
      caseTypeLabel: typeof taxonomy.caseTypeLabel === "string" ? taxonomy.caseTypeLabel : null,
      confidence: typeof taxonomy.confidence === "number" ? taxonomy.confidence : null,
      signals: Array.isArray(taxonomy.signals) ? taxonomy.signals.map((item) => String(item)) : [],
      fallback: Boolean(taxonomy.fallback)
    },
    referenceIssues: referenceIssues.results ?? [],
    unresolvedReferenceCount: (referenceIssues.results ?? []).length,
    criticalExceptionReferences: (criticalExceptionRefs.results ?? []).map((row) => row.normalizedValue),
    filteredNoiseCount: parseJsonArray(document.extractionWarningsJson).filter((warning) =>
      warning.toLowerCase().includes("extraction noise filtered")
    ).length,
    missingRulesDetection: document.qcHasRulesSection === 0,
    missingOrdinanceDetection: document.qcHasOrdinanceSection === 0,
    lowConfidenceTaxonomy:
      Boolean(taxonomy.fallback) || (typeof taxonomy.confidence === "number" ? taxonomy.confidence < 0.45 : false),
    failedQcRequirements: qcFailedRequirements({
      qcHasIndexCodes: document.qcHasIndexCodes,
      qcHasRulesSection: document.qcHasRulesSection,
      qcHasOrdinanceSection: document.qcHasOrdinanceSection
    }),
    qcGateDiagnostics: {
      hasIndexCodes: Boolean(document.qcHasIndexCodes),
      hasRulesSection: Boolean(document.qcHasRulesSection),
      hasOrdinanceSection: Boolean(document.qcHasOrdinanceSection),
      passed: Boolean(document.qcPassed)
    },
    validReferences: {
      indexCodes: Array.from(
        new Set(
          (validReferenceLinks.results ?? [])
            .filter((row) => row.referenceType === "index_code")
            .map((row) => row.canonicalValue)
        )
      ),
      rulesSections: Array.from(
        new Set(
          (validReferenceLinks.results ?? [])
            .filter((row) => row.referenceType === "rules_section")
            .map((row) => row.canonicalValue)
        )
      ),
      ordinanceSections: Array.from(
        new Set(
          (validReferenceLinks.results ?? [])
            .filter((row) => row.referenceType === "ordinance_section")
            .map((row) => row.canonicalValue)
        )
      )
    },
    isLikelyFixture: isLikelyFixtureDoc({ title: document.title, citation: document.citation, metadata }),
    sections: Array.from(sectionMap.values()),
    chunks: (chunks.results ?? []).map((chunk) => ({
      ...chunk,
      paragraphAnchorEnd: chunk.paragraphAnchorEnd ?? chunk.paragraphAnchor,
      chunkWarnings: parseJsonArray(chunk.chunkWarningsJson)
    }))
  };
  return {
    ...detail,
    ...(function () {
      const approvalReadiness = computeApprovalReadiness({
      fileType: detail.fileType,
      approvedAt: detail.approvedAt,
      rejectedAt: detail.rejectedAt,
      qcPassed: detail.qcPassed,
      qcRequiredConfirmed: detail.qcRequiredConfirmed,
      warningCount: (detail.extractionWarnings || []).length,
      unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
      criticalExceptionCount: (detail.criticalExceptionReferences || []).length,
      extractionConfidence: detail.extractionConfidence || 0,
      missingRulesDetection: detail.missingRulesDetection,
      missingOrdinanceDetection: detail.missingOrdinanceDetection,
      lowConfidenceTaxonomy: detail.lowConfidenceTaxonomy
      });
      const unresolvedRulesCount = (detail.referenceIssues || []).filter((row) => row.referenceType === "rules_section").length;
      const unresolvedOrdinanceCount = (detail.referenceIssues || []).filter((row) => row.referenceType === "ordinance_section").length;
      const unresolvedIndexCount = (detail.referenceIssues || []).filter((row) => row.referenceType === "index_code").length;
      const unresolvedUnsafe37xCount = (detail.referenceIssues || []).filter(
        (row) => row.referenceType === "ordinance_section" && ["ordinance37.3", "ordinance37.7", "ordinance37.9"].includes(row.normalizedValue)
      ).length;
      const reviewer = computeReviewerReadiness({
        isLikelyFixture: detail.isLikelyFixture,
        blockers: approvalReadiness.blockers,
        qcPassed: detail.qcPassed,
        qcRequiredConfirmed: detail.qcRequiredConfirmed,
        failedQcRequirements: detail.failedQcRequirements || [],
        unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
        unresolvedRulesCount,
        unresolvedOrdinanceCount,
        unresolvedIndexCount,
        unresolvedUnsafe37xCount,
        criticalExceptionCount: (detail.criticalExceptionReferences || []).length,
        extractionConfidence: detail.extractionConfidence || 0,
        validIndexRefCount: (detail.validReferences?.indexCodes || []).length,
        validRulesRefCount: (detail.validReferences?.rulesSections || []).length,
        validOrdinanceRefCount: (detail.validReferences?.ordinanceSections || []).length,
        missingRulesDetection: detail.missingRulesDetection,
        missingOrdinanceDetection: detail.missingOrdinanceDetection,
        approvalThresholds: { maxUnresolvedReferences: approvalReadiness.thresholds.maxUnresolvedReferences }
      });
      const triage = computeUnresolvedTriage((detail.referenceIssues || []) as UnresolvedIssueLite[]);
      const blocked37x = computeBlocked37xReview((detail.referenceIssues || []) as UnresolvedIssueLite[]);
      const runtimePolicy = computeRuntimeReviewerPolicy({
        triage,
        blocked37xReferences: blocked37x.blocked37xReferences,
        unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
        documentId: detail.id,
        title: detail.title,
        unresolvedBuckets: triage.unresolvedBuckets,
        recurringCitationFamilies: triage.recurringCitationFamilies
      });
      return {
        approvalReadiness,
        reviewerReady: reviewer.reviewerReady,
        reviewerReadyReasons: reviewer.reviewerReadyReasons,
        reviewerRequiredActions: reviewer.reviewerRequiredActions,
        reviewerRiskLevel: reviewer.reviewerRiskLevel,
        metadataConfirmationWouldUnlock: reviewer.metadataConfirmationWouldUnlock,
        unresolvedBlockersAfterConfirmation: reviewer.unresolvedBlockersAfterConfirmation,
        unresolvedBuckets: triage.unresolvedBuckets,
        topRecommendedReviewerAction: runtimePolicy.runtimeSurfaceForManualReview
          ? triage.topRecommendedReviewerAction
          : runtimePolicy.runtimePolicyReason,
        estimatedReviewerEffort: triage.estimatedReviewerEffort,
        candidateManualFixes: runtimePolicy.runtimeSurfaceForManualReview ? triage.candidateManualFixes : [],
        recurringCitationFamilies: triage.recurringCitationFamilies,
        runtimeDisposition: runtimePolicy.runtimeDisposition,
        runtimeManualReasonCode: runtimePolicy.runtimeManualReasonCode,
        runtimeManualReasonSummary: runtimePolicy.runtimeManualReasonSummary,
        runtimeSuggestedOperatorAction: runtimePolicy.runtimeSuggestedOperatorAction,
        runtimeOperatorReviewSummary: runtimePolicy.runtimeOperatorReviewSummary,
        runtimeReviewDiagnostic: runtimePolicy.runtimeReviewDiagnostic,
        runtimePolicyReason: runtimePolicy.runtimePolicyReason,
        runtimeSurfaceForManualReview: runtimePolicy.runtimeSurfaceForManualReview,
        runtimeManualReviewRequired: runtimePolicy.runtimeManualReviewRequired,
        runtimeDoNotAutoApply: runtimePolicy.runtimeDoNotAutoApply,
        blocked37xReferences: blocked37x.blocked37xReferences,
        blocked37xReason: blocked37x.blocked37xReason,
        blocked37xReviewerHint: blocked37x.blocked37xReviewerHint,
        blocked37xSafeToBatchReview: blocked37x.blocked37xSafeToBatchReview,
        blocked37xBatchKey: blocked37x.blocked37xBatchKey,
        canBatchReviewWith: []
      };
    })()
  };
}

export async function updateIngestionMetadata(env: Env, documentId: string, payload: unknown) {
  const parsed = adminIngestionMetadataUpdateSchema.parse(payload);

  const row = await env.DB.prepare(
    `SELECT id, file_type as fileType, qc_passed as qcPassed FROM documents WHERE id = ?`
  )
    .bind(documentId)
    .first<{ id: string; fileType: "decision_docx" | "law_pdf"; qcPassed: number }>();

  if (!row) {
    return null;
  }

  const updateIndexCodes = parsed.index_codes ?? undefined;
  const updateRules = parsed.rules_sections ?? undefined;
  const updateOrdinance = parsed.ordinance_sections ?? undefined;

  const qcConfirmed = parsed.confirm_required_metadata
    ? boolish((updateIndexCodes?.length ?? 0) > 0 && (updateRules?.length ?? 0) > 0 && (updateOrdinance?.length ?? 0) > 0)
    : undefined;

  const now = new Date().toISOString();

  await env.DB.prepare(
    `UPDATE documents SET
      index_codes_json = COALESCE(?, index_codes_json),
      rules_sections_json = COALESCE(?, rules_sections_json),
      ordinance_sections_json = COALESCE(?, ordinance_sections_json),
      case_number = COALESCE(?, case_number),
      decision_date = COALESCE(?, decision_date),
      author_name = COALESCE(?, author_name),
      outcome_label = COALESCE(?, outcome_label),
      qc_required_confirmed = COALESCE(?, qc_required_confirmed),
      qc_confirmed_at = CASE WHEN ? IS NOT NULL THEN ? ELSE qc_confirmed_at END,
      updated_at = ?
     WHERE id = ?`
  )
    .bind(
      updateIndexCodes ? JSON.stringify(updateIndexCodes) : null,
      updateRules ? JSON.stringify(updateRules) : null,
      updateOrdinance ? JSON.stringify(updateOrdinance) : null,
      parsed.case_number ?? null,
      parsed.decision_date ?? null,
      parsed.author_name ?? null,
      parsed.outcome_label ?? null,
      qcConfirmed ?? null,
      qcConfirmed ?? null,
      qcConfirmed ? now : null,
      now,
      documentId
    )
    .run();

  const persistedRefs = await env.DB.prepare(
    `SELECT index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson, ordinance_sections_json as ordinanceSectionsJson
     FROM documents
     WHERE id = ?`
  )
    .bind(documentId)
    .first<{ indexCodesJson: string; rulesSectionsJson: string; ordinanceSectionsJson: string }>();

  const persistedIndexCodes = parseJsonArray(persistedRefs?.indexCodesJson);
  const persistedRules = parseJsonArray(persistedRefs?.rulesSectionsJson);
  const persistedOrdinance = parseJsonArray(persistedRefs?.ordinanceSectionsJson);
  const hasIndexCodes = persistedIndexCodes.length > 0;
  const hasRules = persistedRules.length > 0;
  const hasOrdinance = persistedOrdinance.length > 0;
  await env.DB.prepare(
    `UPDATE documents
     SET qc_has_index_codes = ?,
         qc_has_rules_section = ?,
         qc_has_ordinance_section = ?,
         qc_passed = ?,
         updated_at = ?
     WHERE id = ?`
  )
    .bind(boolish(hasIndexCodes), boolish(hasRules), boolish(hasOrdinance), boolish(hasIndexCodes && hasRules && hasOrdinance), now, documentId)
    .run();

  await refreshDocumentReferenceValidation(env, documentId, {
    indexCodes: persistedIndexCodes,
    rulesSections: persistedRules,
    ordinanceSections: persistedOrdinance
  });

  return getIngestionDocumentDetail(env, documentId);
}

export async function reprocessIngestionDocument(env: Env, documentId: string) {
  const row = await env.DB.prepare(
    `SELECT
      id,
      file_type as fileType,
      source_r2_key as sourceKey,
      title,
      citation,
      decision_date as decisionDate,
      metadata_json as metadataJson,
      (SELECT COUNT(*) FROM document_sections WHERE document_id = documents.id) as sectionCount,
      (SELECT COUNT(*) FROM section_paragraphs WHERE section_id IN (
        SELECT id FROM document_sections WHERE document_id = documents.id
      )) as paragraphCount,
      (SELECT COUNT(*) FROM document_chunks WHERE document_id = documents.id) as chunkCount
     FROM documents
     WHERE id = ?`
  )
    .bind(documentId)
    .first<{
      id: string;
      fileType: "decision_docx" | "law_pdf";
      sourceKey: string;
      title: string;
      citation: string;
      decisionDate: string | null;
      metadataJson: string | null;
      sectionCount: number;
      paragraphCount: number;
      chunkCount: number;
    }>();

  if (!row) {
    return null;
  }

  const object = await env.SOURCE_BUCKET.get(row.sourceKey);
  if (!object) {
    throw new Error(`Source object missing for ${documentId}: ${row.sourceKey}`);
  }
  const bytes = new Uint8Array(await object.arrayBuffer());
  const parsed = parseDocument(bytes, row.fileType);
  const metadata = {
    ...parsed.extractedMetadata,
    indexCodes: [...parsed.extractedMetadata.indexCodes],
    rulesSections: [...parsed.extractedMetadata.rulesSections],
    ordinanceSections: [...parsed.extractedMetadata.ordinanceSections]
  };
  const warnings = [...parsed.warnings];

  const referenceValidation = await validateReferencesAgainstNormalized(env, {
    indexCodes: metadata.indexCodes,
    rulesSections: metadata.rulesSections,
    ordinanceSections: metadata.ordinanceSections
  });
  const normalizedIndexCount = await env.DB.prepare(`SELECT COUNT(*) as count FROM legal_index_codes WHERE active = 1`).first<{ count: number }>();
  if ((normalizedIndexCount?.count ?? 0) > 0 && referenceValidation.unknownIndexCodes.length > 0) {
    const unknownSet = new Set(referenceValidation.unknownIndexCodes);
    const kept = metadata.indexCodes.filter((value) => !unknownSet.has(value));
    const dropped = metadata.indexCodes.filter((value) => unknownSet.has(value));
    metadata.indexCodes = kept;
    if (dropped.length > 0) {
      warnings.push(`Extraction noise filtered (index codes): ${dropped.join(", ")}`);
    }
  }
  if (metadata.indexCodes.length === 0) {
    const inferred = await inferIndexCodesFromReferences(env, {
      rulesSections: metadata.rulesSections,
      ordinanceSections: metadata.ordinanceSections
    });
    if (inferred.inferredIndexCodes.length > 0) {
      metadata.indexCodes = inferred.inferredIndexCodes;
      warnings.push(
        `Index codes inferred from validated references: ${inferred.inferredIndexCodes.join(", ")} (${inferred.evidence.join("; ")})`
      );
    }
  }

  if (referenceValidation.unknownIndexCodes.length > 0) {
    warnings.push(`Unknown index codes (manual review): ${referenceValidation.unknownIndexCodes.join(", ")}`);
  }
  if (referenceValidation.unknownRules.length > 0) {
    warnings.push(`Unknown rules references (manual review): ${referenceValidation.unknownRules.join(", ")}`);
  }
  if (referenceValidation.unknownOrdinance.length > 0) {
    warnings.push(`Unknown ordinance references (manual review): ${referenceValidation.unknownOrdinance.join(", ")}`);
  }
  const criticalExceptions = detectCriticalReferenceExceptions({
    rules: metadata.rulesSections,
    ordinance: metadata.ordinanceSections
  });
  for (const citation of criticalExceptions) {
    if (citation === "37.15") {
      warnings.push("Critical reference exception: 37.15 may be cross-context ambiguous (ordinance vs rules); manual QC required");
      continue;
    }
    if (citation === "37.2(g)" || citation === "10.10(c)(3)") {
      warnings.push(`Critical reference exception: ${citation} currently classed as parent_or_related_only; manual QC required`);
      continue;
    }
    warnings.push(`Critical reference exception: ${citation}; manual QC required`);
  }

  const headings = parsed.sections.map((section) => section.heading || "");
  const qcFlags = recomputeQcFlags(headings, metadata);
  const taxonomySuggestion = inferTaxonomySuggestion({
    title: row.title,
    citation: row.citation,
    sections: parsed.sections,
    metadata
  });
  if (taxonomySuggestion.fallback || taxonomySuggestion.confidence < 0.45) {
    warnings.push("Taxonomy suggestion is low-confidence; review case type during QC");
  }

  const existingMetadata = parseJsonObject(row.metadataJson);
  const now = new Date().toISOString();
  await env.DB.prepare(
    `UPDATE documents
     SET qc_has_index_codes = ?,
         qc_has_rules_section = ?,
         qc_has_ordinance_section = ?,
         qc_passed = ?,
         index_codes_json = ?,
         rules_sections_json = ?,
         ordinance_sections_json = ?,
         case_number = ?,
         decision_date = COALESCE(decision_date, ?),
         author_name = ?,
         outcome_label = ?,
         extraction_confidence = ?,
         extraction_warnings_json = ?,
         metadata_json = ?,
         updated_at = ?
     WHERE id = ?`
  )
    .bind(
      boolish(qcFlags.hasIndexCodes),
      boolish(qcFlags.hasRulesSection),
      boolish(qcFlags.hasOrdinanceSection),
      boolish(qcFlags.hasIndexCodes && qcFlags.hasRulesSection && qcFlags.hasOrdinanceSection),
      JSON.stringify(metadata.indexCodes),
      JSON.stringify(metadata.rulesSections),
      JSON.stringify(metadata.ordinanceSections),
      metadata.caseNumber,
      row.decisionDate ?? metadata.decisionDate,
      metadata.author,
      metadata.outcomeLabel,
      metadata.extractionConfidence,
      JSON.stringify(Array.from(new Set(warnings))),
      JSON.stringify({
        ...existingMetadata,
        plainTextLength: parsed.plainText.length,
        taxonomy: taxonomySuggestion,
        referenceValidation
      }),
      now,
      documentId
    )
    .run();

  await refreshDocumentReferenceValidation(env, documentId, {
    indexCodes: metadata.indexCodes,
    rulesSections: metadata.rulesSections,
    ordinanceSections: metadata.ordinanceSections
  });

  const shouldRebuildTextArtifacts =
    Number(row.sectionCount || 0) === 0 || Number(row.paragraphCount || 0) === 0 || Number(row.chunkCount || 0) === 0;

  const rebuiltArtifacts = shouldRebuildTextArtifacts
    ? await rebuildDocumentTextArtifacts(env, {
        documentId,
        citation: row.citation,
        sections: parsed.sections,
        performVectorUpsert: false
      })
    : null;

  const detail = await getIngestionDocumentDetail(env, documentId);
  if (!detail) {
    return null;
  }

  return {
    ...detail,
    reprocessArtifacts: {
      rebuilt: Boolean(rebuiltArtifacts),
      sectionCountBefore: Number(row.sectionCount || 0),
      paragraphCountBefore: Number(row.paragraphCount || 0),
      chunkCountBefore: Number(row.chunkCount || 0),
      sectionCountAfter: rebuiltArtifacts?.sectionCount ?? Number(row.sectionCount || 0),
      paragraphCountAfter: rebuiltArtifacts?.paragraphCount ?? Number(row.paragraphCount || 0),
      chunkCountAfter: rebuiltArtifacts?.chunkCount ?? Number(row.chunkCount || 0)
    }
  };
}

export async function rejectIngestionDocument(env: Env, documentId: string, payload: unknown) {
  const parsed = adminIngestionRejectSchema.parse(payload);
  const now = new Date().toISOString();

  await env.DB.prepare(
    `UPDATE documents
     SET rejected_at = ?, rejected_reason = ?, searchable_at = NULL, updated_at = ?
     WHERE id = ?`
  )
    .bind(now, parsed.reason, now, documentId)
    .run();

  return { rejected: true, documentId, reason: parsed.reason };
}

export async function approveIngestionDocument(env: Env, documentId: string) {
  return approveDecision(env, documentId);
}

export type BulkSearchabilityCandidateMode =
  | "qcPassed"
  | "missingIndexOnlyTextReady"
  | "singleContextTextReady"
  | "decisionLikeTextReady";

function buildRealDecisionFilterSql() {
  return `
    d.file_type = 'decision_docx'
    AND d.rejected_at IS NULL
    AND COALESCE(d.title, '') NOT LIKE 'Harness %'
    AND COALESCE(d.title, '') NOT LIKE 'Retrieval %'
    AND COALESCE(d.title, '') NOT LIKE 'R5 %'
    AND COALESCE(d.citation, '') NOT LIKE 'BEE-%'
    AND lower(COALESCE(d.source_r2_key, '')) NOT LIKE '%harness%'
    AND lower(COALESCE(d.source_r2_key, '')) NOT LIKE '%fixture%'
  `;
}

export async function listBulkSearchabilityCandidates(
  env: Env,
  options?: { limit?: number; realOnly?: boolean; mode?: BulkSearchabilityCandidateMode }
) {
  const limit = Math.max(1, Math.min(1000, Number(options?.limit || 200)));
  const mode: BulkSearchabilityCandidateMode =
    options?.mode === "missingIndexOnlyTextReady" ||
    options?.mode === "singleContextTextReady" ||
    options?.mode === "decisionLikeTextReady"
      ? options.mode
      : "qcPassed";
  const where = [
    "d.file_type = 'decision_docx'",
    "d.searchable_at IS NULL",
    "d.rejected_at IS NULL",
    "COALESCE(d.source_r2_key, '') != ''",
    "COALESCE(d.title, '') != ''"
  ];
  if (mode === "missingIndexOnlyTextReady") {
    where.push("d.qc_has_index_codes = 0");
    where.push("d.qc_has_rules_section = 1");
    where.push("d.qc_has_ordinance_section = 1");
  } else if (mode === "singleContextTextReady") {
    where.push("d.qc_passed = 0");
    where.push("COALESCE(d.qc_required_confirmed, 0) = 0");
    where.push(
      "((COALESCE(d.qc_has_rules_section, 0) = 1 AND COALESCE(d.qc_has_ordinance_section, 0) = 0) OR (COALESCE(d.qc_has_rules_section, 0) = 0 AND COALESCE(d.qc_has_ordinance_section, 0) = 1))"
    );
    where.push("EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id)");
    where.push("COALESCE(CAST(json_extract(d.metadata_json, '$.plainTextLength') AS INTEGER), 0) >= 1000");
    where.push("COALESCE(d.extraction_warnings_json, '[]') NOT LIKE '%Unknown index codes (manual review):%'");
    where.push("COALESCE(d.citation, '') NOT LIKE 'UNK-REF-%'");
    where.push("COALESCE(d.title, '') NOT LIKE 'Unknown Reference %'");
  } else if (mode === "decisionLikeTextReady") {
    where.push("d.qc_passed = 0");
    where.push("COALESCE(d.qc_required_confirmed, 0) = 0");
    where.push("COALESCE(d.qc_has_index_codes, 0) = 0");
    where.push("COALESCE(d.qc_has_rules_section, 0) = 0");
    where.push("COALESCE(d.qc_has_ordinance_section, 0) = 0");
    where.push("EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id)");
    where.push("COALESCE(CAST(json_extract(d.metadata_json, '$.plainTextLength') AS INTEGER), 0) >= 1000");
    where.push("COALESCE(d.extraction_warnings_json, '[]') NOT LIKE '%Unknown index codes (manual review):%'");
    where.push("COALESCE(d.citation, '') NOT LIKE 'UNK-REF-%'");
    where.push("COALESCE(d.title, '') NOT LIKE 'Unknown Reference %'");
    where.push(`
      (
        lower(COALESCE(d.title, '')) LIKE '%decision%'
        OR lower(COALESCE(d.title, '')) LIKE '%dismissal%'
        OR lower(COALESCE(d.title, '')) LIKE '%order%'
        OR lower(COALESCE(d.title, '')) LIKE '%remand%'
        OR lower(COALESCE(d.title, '')) LIKE '%appeal%'
        OR lower(COALESCE(d.title, '')) LIKE '%hearing%'
        OR upper(COALESCE(d.citation, '')) LIKE '%DECISION%'
        OR upper(COALESCE(d.citation, '')) LIKE '%DISMISSAL%'
        OR upper(COALESCE(d.citation, '')) LIKE '%ORDER%'
        OR upper(COALESCE(d.citation, '')) LIKE '%REMAND%'
      )
    `);
  } else {
    where.push("d.qc_passed = 1");
  }
  if (options?.realOnly !== false) {
    where.push(buildRealDecisionFilterSql());
  }

  const sql = `
    WITH ranked AS (
      SELECT
        d.id,
        d.title,
        d.citation,
        d.decision_date as decisionDate,
        d.created_at as createdAt,
        d.updated_at as updatedAt,
        d.source_r2_key as sourceR2Key,
        d.qc_has_index_codes as qcHasIndexCodes,
        d.qc_has_rules_section as qcHasRulesSection,
        d.qc_has_ordinance_section as qcHasOrdinanceSection,
        ROW_NUMBER() OVER (
          PARTITION BY d.citation
          ORDER BY d.updated_at DESC, d.created_at DESC, d.id DESC
        ) as citation_rank
      FROM documents d
      WHERE ${where.join("\n        AND ")}
    )
    SELECT *
    FROM ranked
    WHERE citation_rank = 1
    ORDER BY decisionDate DESC, updatedAt DESC
    LIMIT ?
  `;

  const result = await env.DB.prepare(sql).bind(limit).all<{
    id: string;
    title: string;
    citation: string;
    decisionDate: string | null;
    createdAt: string;
    updatedAt: string;
    sourceR2Key: string;
    qcHasIndexCodes: number;
    qcHasRulesSection: number;
    qcHasOrdinanceSection: number;
  }>();

  const candidates = (result.results || []).map((row) => ({
    documentId: row.id,
    title: row.title,
    citation: row.citation,
    decisionDate: row.decisionDate,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    sourceR2Key: row.sourceR2Key,
    qc: {
      hasIndexCodes: Boolean(row.qcHasIndexCodes),
      hasRulesSection: Boolean(row.qcHasRulesSection),
      hasOrdinanceSection: Boolean(row.qcHasOrdinanceSection)
    }
  }));

  const totals = await env.DB.prepare(
    `
      SELECT
        COUNT(*) as qcPassedUnsearchableCount,
        COUNT(DISTINCT citation) as qcPassedUnsearchableDistinctCitationCount
      FROM documents d
      WHERE ${where.join("\n        AND ")}
    `
  ).first<{ qcPassedUnsearchableCount: number; qcPassedUnsearchableDistinctCitationCount: number }>();

  return {
    summary: {
      limit,
      mode,
      realOnly: options?.realOnly !== false,
      qcPassedUnsearchableCount: Number(totals?.qcPassedUnsearchableCount || 0),
      qcPassedUnsearchableDistinctCitationCount: Number(totals?.qcPassedUnsearchableDistinctCitationCount || 0),
      candidateCount: candidates.length
    },
    candidates: candidates.map((candidate) => ({
      ...candidate,
      searchabilityMode: mode
    }))
  };
}

export async function bulkEnableSearchability(
  env: Env,
  options?: { limit?: number; realOnly?: boolean; dryRun?: boolean; mode?: BulkSearchabilityCandidateMode }
) {
  const candidateReport = await listBulkSearchabilityCandidates(env, options);
  const candidates = candidateReport.candidates || [];
  const documentIds = candidates.map((row) => row.documentId);
  const now = new Date().toISOString();
  const maxSearchabilityUpdateBatchSize = 25;

  if (!options?.dryRun && documentIds.length > 0) {
    for (let index = 0; index < documentIds.length; index += maxSearchabilityUpdateBatchSize) {
      const batch = documentIds.slice(index, index + maxSearchabilityUpdateBatchSize);
      const placeholders = batch.map(() => "?").join(",");
      await env.DB.prepare(
        `UPDATE documents
         SET searchable_at = COALESCE(searchable_at, ?),
             updated_at = ?
         WHERE id IN (${placeholders})`
      )
        .bind(now, now, ...batch)
        .run();
    }
  }

  return {
    generatedAt: now,
    dryRun: Boolean(options?.dryRun),
    summary: {
      ...candidateReport.summary,
      enabledCount: documentIds.length
    },
    documentIds,
    candidates
  };
}
