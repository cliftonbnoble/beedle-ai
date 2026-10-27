import type { ParsedDocument } from "../lib/types";

type Metadata = ParsedDocument["extractedMetadata"];
type Override = { value: unknown; provenance: "reviewer" | "legacy_preserved"; recordedAt: string; basedOnRevision: number };
export type ReviewerOverrides = Record<string, Override>;
export function readReviewerOverrides(value: unknown): ReviewerOverrides {
  try { const parsed = typeof value === "string" ? JSON.parse(value) : value; return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {}; }
  catch { return {}; }
}
const fields = ["indexCodes", "rulesSections", "ordinanceSections", "caseNumber", "decisionDate", "author", "outcomeLabel"] as const;
export const reviewerPayloadFields = {  
index_codes: "indexCodes", rules_sections: "rulesSections", ordinance_sections: "ordinanceSections",
  case_number: "caseNumber", decision_date: "decisionDate", author_name: "author", outcome_label: "outcomeLabel"
} as const;

export function preserveReviewedMetadata(extracted: Metadata, current: Record<string, unknown>, priorExtracted: Record<string, unknown> | undefined,
  existingOverrides: ReviewerOverrides, revision: number) {
  const overrides = { ...existingOverrides };
  const effective = { ...extracted };
  for (const field of fields) {
    const stored = current[field];
    const hasValue = Array.isArray(stored) ? stored.length > 0 : stored !== null && stored !== undefined && stored !== "";
    // Legacy records cannot tell us who edited a field. Preserve populated values
    // when no baseline exists or when a value differs from the extractor baseline.
    if (!overrides[field] && hasValue && (!priorExtracted || JSON.stringify(stored) !== JSON.stringify(priorExtracted[field]))) {
      overrides[field] = { value: stored, provenance: "legacy_preserved", recordedAt: new Date().toISOString(), basedOnRevision: revision };
    }
    if (overrides[field]) (effective as Record<string, unknown>)[field] = overrides[field].value;
  }
  return { metadata: effective, overrides };
}
