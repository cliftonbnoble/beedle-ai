export const APPROVAL_POLICY_VERSION = "reviewer-policy-1";

export const APPROVAL_THRESHOLDS = {
  maxUnresolvedReferences: 1,
  maxWarnings: 8,
  minExtractionConfidence: 0.55
} as const;

export type ApprovalReadinessInput = {
  ingestionReady: number;
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

export function computeApprovalReadiness(input: ApprovalReadinessInput) {
  const blockers: string[] = [];
  if (!input.ingestionReady) blockers.push("ingestion_not_ready");
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
    policyVersion: APPROVAL_POLICY_VERSION,
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

