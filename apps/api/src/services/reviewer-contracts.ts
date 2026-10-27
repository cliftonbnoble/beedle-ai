// Reviewer list input contract shared by readers and API compatibility exports.




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
