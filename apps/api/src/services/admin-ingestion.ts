import {knownCalendarDate} from "@beedle/shared";
import { referenceEditionBindingSchema } from "@beedle/shared";
export { listIngestionDocuments } from "./reviewer-queue";
import { advanceReviewerQueue } from "./reviewer-queue-index";
import { sha256Bytes } from "../lib/digests";
import { ExtractionReviewRequired } from "./text-extraction";
import { stageDocumentGeneration } from "./document-generations";
import { preserveReviewedMetadata, readReviewerOverrides, reviewerPayloadFields } from "./reviewer-overrides";
import { RequestConflictError, RequestValidationError } from "../lib/http";
import { reviewMutation, reviewerSnapshotSql } from "./reviewer-mutations";
import { computeApprovalReadiness } from "./approval-policy";
import { assertDocumentRevision } from "./document-revision";
import { readyDocumentSql, currentReferenceCatalogSql } from "./publication-policy";
import { adminIngestionMetadataUpdateSchema, adminIngestionRejectSchema } from "@beedle/shared";
import type { Env, ParsedDocument } from "../lib/types";
import { approveDecision } from "./ingest";
import { reviewSourceLink } from "./storage";
import { computeQcFlags, detectCriticalReferenceExceptions, isLikelyFixtureName } from "./qc-shared";
import { parseDocument, parseMarkdownDocument } from "./parser";
import { detectSourceFormat } from "./source-format";
import { inferTaxonomySuggestion, storedTaxonomySuggestion } from "./taxonomy-inference";
import {
  buildDocumentReferenceValidationStatements,
  inferIndexCodesFromReferences,
  validateReferencesAgainstNormalized,prepareReferenceValidation
} from "./legal-references";

export async function advanceReviewerQueueIndex(env:Env){return advanceReviewerQueue(env,buildReviewerRows);}

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
  cursor?: string;
  triageGroupKey?: string;
  excludeDocumentId?: string;
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



const LIST_DOCS_IN_QUERY_CHUNK = 75;
function chunkArray<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
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

// Materialization and detail use the same policy functions; list SQL never approximates them.
export async function buildReviewerRows(env: Env, documentIds: string[]) {
  if (documentIds.length > 25) throw new RequestValidationError("Reviewer materialization is limited to 25 documents per step.");
  const metadataSource="CASE WHEN json_valid(d.metadata_json) THEN d.metadata_json ELSE '{}' END";
  const queueMetadata=`json_object('taxonomy',json_object('caseTypeId',json_extract(${metadataSource},'$.taxonomy.caseTypeId'),
    'caseTypeLabel',json_extract(${metadataSource},'$.taxonomy.caseTypeLabel'),'confidence',CASE WHEN json_type(${metadataSource},'$.taxonomy.confidence') IN ('integer','real') THEN json_extract(${metadataSource},'$.taxonomy.confidence') ELSE NULL END,
    'fallback',json_extract(${metadataSource},'$.taxonomy.fallback'),
    'inferenceVersion',json_extract(${metadataSource},'$.taxonomy.inferenceVersion'),'configVersion',json_extract(${metadataSource},'$.taxonomy.configVersion'),
    'reviewStatus',json_extract(${metadataSource},'$.taxonomy.reviewStatus')),'originalFilename',json_extract(${metadataSource},'$.originalFilename'))`;
  const rowBytes=["d.title","d.citation","d.case_number","d.decision_date","d.extraction_warnings_json","d.index_codes_json","d.rules_sections_json","d.ordinance_sections_json",queueMetadata]
    .map(column=>`COALESCE(length(CAST(${column} AS BLOB)),0)`).join('+');
  let rows;
  try {
    rows = await env.DB.prepare(
      `WITH issue_counts AS (
        SELECT document_id,COUNT(*) AS total,SUM(length(CAST(COALESCE(raw_value,'') AS BLOB))+length(CAST(COALESCE(normalized_value,'') AS BLOB))+length(CAST(COALESCE(message,'') AS BLOB))) AS bytes,
          SUM(reference_type='rules_section') AS rules,SUM(reference_type='ordinance_section') AS ordinance,
          SUM(reference_type='index_code') AS index_count,
          SUM(reference_type='ordinance_section' AND normalized_value IN ('ordinance37.3','ordinance37.7','ordinance37.9')) AS unsafe
        FROM document_reference_issues WHERE document_id IN (SELECT value FROM json_each(?1)) GROUP BY document_id
      ), link_counts AS (
        SELECT document_id,SUM(reference_type='index_code' AND is_valid=1) AS index_count,
          SUM(reference_type='rules_section' AND is_valid=1) AS rules,SUM(reference_type='ordinance_section' AND is_valid=1) AS ordinance,
          COUNT(DISTINCT CASE WHEN normalized_value IN ('37.2(g)','37.15','10.10(c)(3)') THEN normalized_value END) AS critical
        FROM document_reference_links WHERE document_id IN (SELECT value FROM json_each(?1)) GROUP BY document_id
      ) SELECT d.id,d.jurisdiction,d.file_type AS fileType,d.title,d.citation,d.case_number AS caseNumber,d.decision_date AS decisionDate,d.created_at AS createdAt,
        d.approved_at AS approvedAt,d.document_revision AS documentRevision,${readyDocumentSql()} AS ingestionReady,
        ${currentReferenceCatalogSql()} AS referenceCatalogCurrent,d.rejected_at AS rejectedAt,d.searchable_at AS searchableAt,
        d.qc_passed AS qcPassed,d.qc_has_index_codes AS qcHasIndexCodes,d.qc_has_rules_section AS qcHasRulesSection,
        d.qc_has_ordinance_section AS qcHasOrdinanceSection,d.qc_required_confirmed AS qcRequiredConfirmed,
        d.extraction_confidence AS extractionConfidence,d.extraction_warnings_json AS extractionWarningsJson,
        d.index_codes_json AS indexCodesJson,d.rules_sections_json AS rulesSectionsJson,d.ordinance_sections_json AS ordinanceSectionsJson,
        ${queueMetadata} AS metadataJson,COALESCE(json_array_length(d.extraction_warnings_json),0) AS warningCount,
        COALESCE(i.total,0) AS unresolvedReferenceCount,COALESCE(i.bytes,0) AS unresolvedBytes,COALESCE(i.rules,0) AS unresolvedRulesCount,
        COALESCE(i.ordinance,0) AS unresolvedOrdinanceCount,COALESCE(i.index_count,0) AS unresolvedIndexCount,
        COALESCE(i.unsafe,0) AS unresolvedUnsafe37xCount,COALESCE(l.index_count,0) AS validIndexRefCount,
        COALESCE(l.rules,0) AS validRulesRefCount,COALESCE(l.ordinance,0) AS validOrdinanceRefCount,COALESCE(l.critical,0) AS criticalExceptionCount
        FROM documents d LEFT JOIN issue_counts i ON i.document_id=d.id LEFT JOIN link_counts l ON l.document_id=d.id
        WHERE d.id IN (SELECT value FROM json_each(?1)) AND (${rowBytes})<=65536 ORDER BY d.id`
    ).bind(JSON.stringify(documentIds))
      .all<{
    id: string;
    jurisdiction:string;
    fileType: "decision_docx" | "law_pdf";
    title: string;
    citation: string;
    caseNumber: string | null;
    decisionDate: string | null;
    createdAt: string;
    documentRevision: number;
    ingestionReady: number; referenceCatalogCurrent: number;
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
    indexCodesJson: string; rulesSectionsJson: string; ordinanceSectionsJson: string;
    metadataJson: string | null;
    warningCount: number;
    unresolvedReferenceCount: number;
    unresolvedBytes: number;
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

  const candidateRows = rows.results ?? [];
  if(candidateRows.length!==documentIds.length)throw new RequestValidationError("Reviewer rows changed or exceed the 64 KiB policy input limit; inspect metadata before retrying.");
  if(candidateRows.reduce((sum,row)=>sum+row.unresolvedReferenceCount,0)>2000 || candidateRows.reduce((sum,row)=>sum+row.unresolvedBytes,0)>1024*1024) {
    throw new RequestValidationError("Reviewer reference details exceed the 2,000-row/1 MiB step limit; inspect the source issue records before rebuilding.");
  }
  const docIds = candidateRows.map((row) => row.id);
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

  const documents = candidateRows.map((row) => {
    const metadata = parseJsonObject(row.metadataJson);
    const taxonomy = storedTaxonomySuggestion(asObject(metadata.taxonomy),undefined,row.jurisdiction);
    const extractionWarnings = parseJsonArray(row.extractionWarningsJson);
    const filteredNoiseCount = extractionWarnings.filter((warning) => warning.toLowerCase().includes("extraction noise filtered")).length;
    const taxonomyConfidence = typeof taxonomy.confidence === "number" ? taxonomy.confidence : null;
    const taxonomyFallback = taxonomy.fallback;
    const doc = {
      ...row,
      decisionDate:knownCalendarDate(row.decisionDate),
      unverifiedDecisionDate:row.decisionDate&&!knownCalendarDate(row.decisionDate)?row.decisionDate:null,
      extractionWarnings,
      indexCodes: parseJsonArray(row.indexCodesJson),
      taxonomySuggestion: taxonomy,
      warningCount: row.warningCount || 0,
      filteredNoiseCount,
      unresolvedReferenceCount: row.unresolvedReferenceCount || 0,
      criticalExceptionCount: Math.max(row.criticalExceptionCount || 0, detectCriticalReferenceExceptions({ rules: parseJsonArray(row.rulesSectionsJson), ordinance: parseJsonArray(row.ordinanceSectionsJson) }).length),
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
      ingestionReady: doc.ingestionReady,
      referenceCatalogCurrent: doc.referenceCatalogCurrent,
      approvedAt: doc.approvedAt,
      rejectedAt: doc.rejectedAt,
      qcPassed: doc.qcPassed && doc.qcHasIndexCodes && doc.qcHasRulesSection && doc.qcHasOrdinanceSection,
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
      qcPassed: doc.qcPassed && doc.qcHasIndexCodes && doc.qcHasRulesSection && doc.qcHasOrdinanceSection,
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

  return documents;
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
      active_text_generation as activeTextGeneration, reviewer_overrides_json as reviewerOverridesJson,
      document_revision as documentRevision,
      ${readyDocumentSql("documents")} as ingestionReady,
      ${currentReferenceCatalogSql("documents")} as referenceCatalogCurrent,
      searchable_at as searchableAt,
      rejected_at as rejectedAt,
      rejected_reason as rejectedReason,
      extraction_confidence as extractionConfidence,
      extraction_warnings_json as extractionWarningsJson,
      index_codes_json as indexCodesJson,
      rules_sections_json as rulesSectionsJson,
      ordinance_sections_json as ordinanceSectionsJson,
      metadata_json as metadataJson,reference_edition_context_json as referenceEditionContextJson,(SELECT epoch FROM legal_catalog_state WHERE singleton=1) AS referenceRegistryEpoch,
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
      activeTextGeneration: string; reviewerOverridesJson: string;
      documentRevision: number;
      ingestionReady: number; referenceCatalogCurrent: number;
      approvedAt: string | null;
      searchableAt: string | null;
      rejectedAt: string | null;
      rejectedReason: string | null;
      extractionConfidence: number;
      extractionWarningsJson: string;
      indexCodesJson: string;
      rulesSectionsJson: string;
      ordinanceSectionsJson: string;
      metadataJson: string | null;referenceEditionContextJson:string;referenceRegistryEpoch:number;
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
    `SELECT id AS referenceLinkId,reference_type as referenceType, canonical_value as canonicalValue,edition_id AS editionId,edition_provision_id AS editionProvisionId,edition_body_sha256 AS editionBodySha256,applicability_status AS applicabilityStatus
     FROM document_reference_links
     WHERE document_id = ? AND is_valid = 1 AND canonical_value IS NOT NULL
     ORDER BY reference_type ASC, canonical_value ASC`
  )
    .bind(documentId)
    .all<{ referenceLinkId:string;referenceType: "index_code" | "rules_section" | "ordinance_section"; canonicalValue: string;editionId:string|null;editionProvisionId:string|null;editionBodySha256:string|null;applicabilityStatus:string }>();

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

  await assertDocumentRevision(env, documentId, document.documentRevision);
  const metadata = parseJsonObject(document.metadataJson);
  const taxonomy = storedTaxonomySuggestion(asObject(metadata.taxonomy),undefined,document.jurisdiction);

  const capturedEditionParsed=referenceEditionBindingSchema.safeParse(parseJsonObject(document.referenceEditionContextJson));
  const capturedEditionContext=capturedEditionParsed.success?capturedEditionParsed.data:referenceEditionBindingSchema.parse({edition_id:null,catalog_version:null,descriptor_sha256:null,status:"unknown",reason:"legacy_or_unavailable_edition_context",jurisdiction:document.jurisdiction||null,as_of_date:document.decisionDate||null,effective_from:null,effective_to:null});
  const editionContextStale=capturedEditionContext.registry_epoch!==document.referenceRegistryEpoch || (capturedEditionContext.date_basis==="decision_date_proxy" && (capturedEditionContext.as_of_date!==document.decisionDate || capturedEditionContext.jurisdiction!==document.jurisdiction));
  const detail = {
    ...document,
    decisionDate:knownCalendarDate(document.decisionDate),
    unverifiedDecisionDate:document.decisionDate&&!knownCalendarDate(document.decisionDate)?document.decisionDate:null,
    referenceEditionContext:editionContextStale?{...capturedEditionContext,status:"unknown",reason:"captured_edition_context_needs_revalidation"}:capturedEditionContext,
    referenceEditionBindings:validReferenceLinks.results,
    reviewerOverrides: readReviewerOverrides(document.reviewerOverridesJson),
    // The persisted source_link is the example.invalid sentinel; rewrite to the proxy URL so this
    // response never hands a caller a dead link (ARCH-02).
    sourceLink: reviewSourceLink(env, documentId),
    extractionWarnings: parseJsonArray(document.extractionWarningsJson),
    indexCodes: parseJsonArray(document.indexCodesJson),
    rulesSections: parseJsonArray(document.rulesSectionsJson),
    ordinanceSections: parseJsonArray(document.ordinanceSectionsJson),
    taxonomySuggestion: taxonomy,
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
      ingestionReady: detail.ingestionReady,
      referenceCatalogCurrent: detail.referenceCatalogCurrent,
      approvedAt: detail.approvedAt,
      rejectedAt: detail.rejectedAt,
      qcPassed: detail.qcPassed && detail.qcHasIndexCodes && detail.qcHasRulesSection && detail.qcHasOrdinanceSection,
      qcRequiredConfirmed: detail.qcRequiredConfirmed,
      warningCount: (detail.extractionWarnings || []).length,
      unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
      criticalExceptionCount: Math.max((detail.criticalExceptionReferences || []).length, detectCriticalReferenceExceptions({ rules: detail.rulesSections, ordinance: detail.ordinanceSections }).length),
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
        qcPassed: detail.qcPassed && detail.qcHasIndexCodes && detail.qcHasRulesSection && detail.qcHasOrdinanceSection,
        qcRequiredConfirmed: detail.qcRequiredConfirmed,
        failedQcRequirements: detail.failedQcRequirements || [],
        unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
        unresolvedRulesCount,
        unresolvedOrdinanceCount,
        unresolvedIndexCount,
        unresolvedUnsafe37xCount,
        criticalExceptionCount: Math.max((detail.criticalExceptionReferences || []).length, detectCriticalReferenceExceptions({ rules: detail.rulesSections, ordinance: detail.ordinanceSections }).length),
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
        blocked37xBatchKey: blocked37x.blocked37xBatchKey
      };
    })()
  };
}

export async function updateIngestionMetadata(env: Env, documentId: string, payload: unknown) {
  const parsed = adminIngestionMetadataUpdateSchema.parse(payload);
  const operation = await reviewMutation(env, documentId, "metadata", parsed);
  if (operation.existing) {
    const detail = await getIngestionDocumentDetail(env,documentId);
    return detail ? {...detail,mutationReceipt:operation.existing} : null;
  }
  const row = await env.DB.prepare(
    `SELECT id,jurisdiction, document_revision as documentRevision, reviewer_overrides_json as reviewerOverridesJson,
      index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson,
      ordinance_sections_json as ordinanceSectionsJson, case_number as caseNumber, decision_date as decisionDate,
      author_name as authorName, outcome_label as outcomeLabel, ${reviewerSnapshotSql} AS beforeJson
      FROM documents WHERE id = ?`
  ).bind(documentId).first<{
    id: string; jurisdiction:string; documentRevision: number; reviewerOverridesJson: string; beforeJson: string;
    indexCodesJson: string; rulesSectionsJson: string; ordinanceSectionsJson: string;
    caseNumber: string | null; decisionDate: string | null; authorName: string | null; outcomeLabel: string | null;
  }>();
  if (!row) return null;
  if (row.documentRevision !== parsed.expectedRevision) {
    const receipt = await operation.replay();
    if (receipt) {
      const detail = await getIngestionDocumentDetail(env,documentId);
      return detail ? {...detail,mutationReceipt:receipt} : null;
    }
    throw new RequestConflictError("Document changed; reload and review its current revision.");
  }

  const updateIndexCodes = parsed.index_codes;
  const updateRules = parsed.rules_sections;
  const updateOrdinance = parsed.ordinance_sections;
  const persistedIndexCodes = updateIndexCodes ?? parseJsonArray(row.indexCodesJson);
  const persistedRules = updateRules ?? parseJsonArray(row.rulesSectionsJson);
  const persistedOrdinance = updateOrdinance ?? parseJsonArray(row.ordinanceSectionsJson);
  const hasIndexCodes = persistedIndexCodes.some(value=>value.trim().length>0);
  const hasRules = persistedRules.some(value=>value.trim().length>0);
  const hasOrdinance = persistedOrdinance.some(value=>value.trim().length>0);
  if (parsed.confirm_required_metadata === true && !(hasIndexCodes && hasRules && hasOrdinance)) {
    throw new RequestValidationError("Required metadata cannot be confirmed until all three reference groups contain a value.");
  }
  const now = new Date().toISOString();
  const overrides = readReviewerOverrides(row.reviewerOverridesJson);
  let overridesChanged = false;
  for (const [key, field] of Object.entries(reviewerPayloadFields)) {
    const value = parsed[key as keyof typeof reviewerPayloadFields];
    if (value !== undefined && (overrides[field]?.provenance !== "reviewer" || JSON.stringify(overrides[field]?.value) !== JSON.stringify(value))) {
      overrides[field] = {value,provenance:"reviewer",recordedAt:now,basedOnRevision:parsed.expectedRevision};
      overridesChanged = true;
    }
  }
  // Bind the entire resulting record. Undefined retains; explicit null clears.
  const retained = <T>(value:T|undefined,current:T) => value === undefined ? current : value;
  // Omission and a semantic no-op must not become evidence edits solely because
  // an older writer used different JSON whitespace. Preserve those stored bytes.
  const retainedReferences = (value:string[]|undefined,current:string) => {
    if(value===undefined)return current;
    const encoded=JSON.stringify(value);
    try { if(JSON.stringify(JSON.parse(current))===encoded)return current; } catch { /* An explicit replacement repairs invalid stored JSON. */ }
    return encoded;
  };
  const nextIndexJson=retainedReferences(updateIndexCodes,row.indexCodesJson);
  const nextRulesJson=retainedReferences(updateRules,row.rulesSectionsJson);
  const nextOrdinanceJson=retainedReferences(updateOrdinance,row.ordinanceSectionsJson);
  const documentUpdateStatement = env.DB.prepare(`UPDATE documents SET reviewer_overrides_json = ?,
    index_codes_json = ?, rules_sections_json = ?, ordinance_sections_json = ?,
    case_number = ?, decision_date = ?, author_name = ?, outcome_label = ?,
    qc_has_index_codes = ?, qc_has_rules_section = ?, qc_has_ordinance_section = ?, qc_passed = ?, updated_at = ?
    WHERE id = ?`).bind(overridesChanged?JSON.stringify(overrides):row.reviewerOverridesJson,nextIndexJson,nextRulesJson,nextOrdinanceJson,
      retained(parsed.case_number,row.caseNumber),retained(parsed.decision_date,row.decisionDate),retained(parsed.author_name,row.authorName),
      retained(parsed.outcome_label,row.outcomeLabel),boolish(hasIndexCodes),boolish(hasRules),boolish(hasOrdinance),
      boolish(hasIndexCodes && hasRules && hasOrdinance),now,documentId);
  const referencesChanged = nextIndexJson !== row.indexCodesJson || nextRulesJson !== row.rulesSectionsJson || nextOrdinanceJson !== row.ordinanceSectionsJson;
  const referenceValidationStatements = referencesChanged || (parsed.decision_date!==undefined&&parsed.decision_date!==row.decisionDate) || parsed.confirm_required_metadata === true
    ? await buildDocumentReferenceValidationStatements(env, documentId, {
      indexCodes:persistedIndexCodes,rulesSections:persistedRules,ordinanceSections:persistedOrdinance
    },undefined,undefined,{jurisdiction:row.jurisdiction,as_of_date:retained(parsed.decision_date,row.decisionDate)}) : [];
  const mutations = [documentUpdateStatement,...referenceValidationStatements];
  // Run after evidence/reference triggers, which revoke earlier confirmation and approval.
  if (parsed.confirm_required_metadata !== undefined) {
    mutations.push(env.DB.prepare(`UPDATE documents SET qc_required_confirmed = ?, qc_confirmed_at = ? WHERE id = ?`)
      .bind(boolish(parsed.confirm_required_metadata),parsed.confirm_required_metadata ? now : null,documentId));
  }
  const mutationReceipt = await operation.commit(row.beforeJson,mutations);
  const detail = await getIngestionDocumentDetail(env,documentId);
  return detail ? {...detail,mutationReceipt} : null;
}

export async function reprocessIngestionDocument(env: Env, documentId: string) {
  const row = await env.DB.prepare(
    `SELECT
      id, document_revision as documentRevision, reviewer_overrides_json as reviewerOverridesJson,
      case_number as caseNumber, author_name as authorName, outcome_label as outcomeLabel,
      index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson, ordinance_sections_json as ordinanceSectionsJson,
      jurisdiction,file_type as fileType,
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
      id: string; documentRevision: number; reviewerOverridesJson: string;
      caseNumber: string | null; authorName: string | null; outcomeLabel: string | null;
      indexCodesJson: string; rulesSectionsJson: string; ordinanceSectionsJson: string;
      jurisdiction:string;fileType: "decision_docx" | "law_pdf";
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
    throw new RequestConflictError("Captured source is missing; restore it before reprocessing");
  }
  if (object.size > 15 * 1024 * 1024) throw new RequestValidationError("Source exceeds the reprocessing limit; current content was preserved.");
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (bytes.length > 15 * 1024 * 1024) throw new RequestValidationError("Source exceeds the reprocessing limit; current content was preserved.");
  const sourceSha256 = await sha256Bytes(bytes);
  const capturedHash = parseJsonObject(row.metadataJson).sourceSha256;
  if (capturedHash && capturedHash !== sourceSha256) throw new RequestValidationError("Source digest differs from the captured source; the current revision was preserved.");
  const originalFilename = parseJsonObject(row.metadataJson).originalFilename;
  const format = detectSourceFormat(bytes, typeof originalFilename === "string" ? originalFilename : row.sourceKey, row.fileType);
  let parsed: ParsedDocument;
  try { parsed = format === "markdown" ? parseMarkdownDocument(bytes) : await parseDocument(bytes, row.fileType); }
  catch (error) {
    if (error instanceof ExtractionReviewRequired) throw new RequestValidationError("Reprocessing requires manual extraction review; the current revision was preserved.");
    throw error;
  }
  const existingMetadata = parseJsonObject(row.metadataJson);
  const preserved = preserveReviewedMetadata(parsed.extractedMetadata, {
    indexCodes: parseJsonArray(row.indexCodesJson), rulesSections: parseJsonArray(row.rulesSectionsJson),
    ordinanceSections: parseJsonArray(row.ordinanceSectionsJson), caseNumber: row.caseNumber,
    decisionDate: row.decisionDate, author: row.authorName, outcomeLabel: row.outcomeLabel
  }, existingMetadata.extractedMetadata as Record<string, unknown> | undefined, readReviewerOverrides(row.reviewerOverridesJson), row.documentRevision);
  const metadata = preserved.metadata;
  const warnings = [...parsed.warnings];
  if(metadata.decisionDate&&!knownCalendarDate(metadata.decisionDate)){metadata.unverifiedDecisionDate=metadata.decisionDate;metadata.decisionDate=null;warnings.push("Retained invalid calendar date is unknown; review the original source before assigning a date");}

  const validationContext={jurisdiction:row.jurisdiction,as_of_date:metadata.decisionDate||null};
  const referenceValues={indexCodes:metadata.indexCodes,rulesSections:metadata.rulesSections,ordinanceSections:metadata.ordinanceSections};
  const referencePlan=await prepareReferenceValidation(env,referenceValues,validationContext);
  const referenceValidation=await validateReferencesAgainstNormalized(env,referenceValues,validationContext,referencePlan);
  const normalizedIndexCount = await env.DB.prepare(`SELECT COUNT(*) as count FROM legal_index_codes WHERE active = 1`).first<{ count: number }>();
  if (!preserved.overrides.indexCodes && (normalizedIndexCount?.count ?? 0) > 0 && referenceValidation.unknownIndexCodes.length > 0) {
    const unknownSet = new Set(referenceValidation.unknownIndexCodes);
    const kept = metadata.indexCodes.filter((value) => !unknownSet.has(value));
    const dropped = metadata.indexCodes.filter((value) => unknownSet.has(value));
    metadata.indexCodes = kept;
    if (dropped.length > 0) {
      warnings.push(`Extraction noise filtered (index codes): ${dropped.join(", ")}`);
    }
  }
  if (!preserved.overrides.indexCodes && metadata.indexCodes.length === 0) {
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
  const qcFlags = computeQcFlags(headings, metadata);
  const taxonomySuggestion = inferTaxonomySuggestion({
    jurisdiction:row.jurisdiction,
    title: row.title,
    citation: row.citation,
    sections: parsed.sections,
    metadata
  });
  if (taxonomySuggestion.fallback || taxonomySuggestion.confidence < 0.45) {
    warnings.push("Taxonomy suggestion is low-confidence; review case type during QC");
  }

  const published = await stageDocumentGeneration(env, {
    referencePlan,documentId, expectedRevision: row.documentRevision, sourceBytes: bytes, sourceFormat: format, sections: parsed.sections,
    references: {indexCodes:metadata.indexCodes, rulesSections:metadata.rulesSections, ordinanceSections:metadata.ordinanceSections},
    documentChanges: {
      qc_has_index_codes: boolish(qcFlags.hasIndexCodes), qc_has_rules_section: boolish(qcFlags.hasRulesSection),
      qc_has_ordinance_section: boolish(qcFlags.hasOrdinanceSection),
      qc_passed: boolish(qcFlags.hasIndexCodes && qcFlags.hasRulesSection && qcFlags.hasOrdinanceSection),
      index_codes_json: JSON.stringify(metadata.indexCodes), rules_sections_json: JSON.stringify(metadata.rulesSections),
      ordinance_sections_json: JSON.stringify(metadata.ordinanceSections), case_number: metadata.caseNumber,
      decision_date: metadata.decisionDate, author_name: metadata.author, outcome_label: metadata.outcomeLabel,
      extraction_confidence: metadata.extractionConfidence, extraction_warnings_json: JSON.stringify(Array.from(new Set(warnings))),
      reviewer_overrides_json: JSON.stringify(preserved.overrides),
      metadata_json: JSON.stringify({...existingMetadata, sourceSha256, plainTextLength:parsed.plainText.length, extraction:parsed.extraction,
        extractedMetadata:parsed.extractedMetadata, citationEvidence:parsed.citations, taxonomy:taxonomySuggestion, referenceValidation})
    }
  });
  const detail = await getIngestionDocumentDetail(env, documentId);
  if (!detail) return null;
  return {...detail, reprocessArtifacts: {rebuilt:true, generationId:published.generationId, previousGenerationId:published.previousGenerationId,
    sectionCountBefore:Number(row.sectionCount), paragraphCountBefore:Number(row.paragraphCount), chunkCountBefore:Number(row.chunkCount),
    sectionCountAfter:published.manifest.document_sections.count, paragraphCountAfter:published.manifest.section_paragraphs.count,
    chunkCountAfter:published.manifest.document_chunks.count}};
}

export async function rejectIngestionDocument(env: Env, documentId: string, payload: unknown) {
  const parsed = adminIngestionRejectSchema.parse(payload);
  const operation = await reviewMutation(env,documentId,"reject",parsed);
  if (operation.existing) return {rejected:true,documentId,reason:parsed.reason,mutationReceipt:operation.existing};
  const row = await env.DB.prepare(`SELECT document_revision AS revision,${reviewerSnapshotSql} AS beforeJson FROM documents WHERE id=?`)
    .bind(documentId).first<{revision:number;beforeJson:string}>();
  if (!row) return null;
  if (row.revision !== parsed.expectedRevision) {
    const receipt = await operation.replay();
    if (receipt) return {rejected:true,documentId,reason:parsed.reason,mutationReceipt:receipt};
    throw new RequestConflictError("Document changed; reload and review its current revision.");
  }
  const now = new Date().toISOString();
  const mutationReceipt = await operation.commit(row.beforeJson,[env.DB.prepare(`UPDATE documents SET rejected_at = ?, rejected_reason = ?,
    approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, searchable_at = NULL,
    qc_required_confirmed = 0, qc_confirmed_at = NULL, updated_at = ? WHERE id = ?`).bind(now,parsed.reason,now,documentId)]);
  return {rejected:true,documentId,reason:parsed.reason,mutationReceipt};
}

export async function approveIngestionDocument(env: Env, documentId: string, expectedRevision: number) {
  return approveDecision(env, documentId, expectedRevision);
}

export type BulkSearchabilityCandidateMode =
  | "qcPassed"
  | "missingIndexOnlyTextReady"
  | "singleContextTextReady"
  | "decisionLikeTextReady";

function buildRealDecisionFilterSql() {
  return `
    d.file_type = 'decision_docx'
    AND ${readyDocumentSql()}
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
    readyDocumentSql(),
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

  let enabledCount = 0;
  if (!options?.dryRun && documentIds.length > 0) {
    const updateStatements: D1PreparedStatement[] = [];
    for (let index = 0; index < documentIds.length; index += maxSearchabilityUpdateBatchSize) {
      const batch = documentIds.slice(index, index + maxSearchabilityUpdateBatchSize);
      const placeholders = batch.map(() => "?").join(",");
      updateStatements.push(
        env.DB.prepare(
          `UPDATE documents
           SET searchable_at = COALESCE(searchable_at, ?),
               published_revision = document_revision,
               updated_at = ?
           WHERE id IN (${placeholders}) AND ${readyDocumentSql("documents")} RETURNING id`
        ).bind(now, now, ...batch)
      );
    }
    if (updateStatements.length > 0) {
      const updates = await env.DB.batch(updateStatements);
      enabledCount = updates.reduce((count, result) => count + result.results.length, 0);
    }
  }

  return {
    generatedAt: now,
    dryRun: Boolean(options?.dryRun),
    summary: {
      ...candidateReport.summary,
      enabledCount
    },
    documentIds,
    candidates
  };
}
