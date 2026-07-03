// Shared QC / critical-citation / fixture-detection primitives (DATA-04).
//
// These previously existed as per-file copies in ingest.ts, admin-ingestion.ts, retrieval-activation.ts
// and legal-references.ts. A single-sided edit (adding a critical citation or a fixture term in one copy)
// would silently desync ingest QC from admin QC from activation gating — so the canonical versions live
// here and the services import them.
//
// NOTE: scripts/provisional-topic-candidate-utils.mjs exports a DIFFERENT isLikelyFixtureDoc — an
// operator-script import-scoring heuristic with its own patterns and tests. It is intentionally separate;
// do not "unify" it with these.

// The critical rules/ordinance citations whose presence (or unresolved status) requires reviewer
// attention. Referenced by the exception detector below and by legal-references' resolution gating.
export const CRITICAL_EXCEPTION_CITATIONS = ["37.2(g)", "37.15", "10.10(c)(3)"] as const;

export function normalizeCitationToken(input: string): string {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/^rule/, "")
    .replace(/^part[0-9a-z.\-]+\-/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

export function detectCriticalReferenceExceptions(values: { rules: string[]; ordinance: string[] }): string[] {
  const refs = [...values.rules, ...values.ordinance].map(normalizeCitationToken);
  return CRITICAL_EXCEPTION_CITATIONS.filter((citation) => refs.includes(citation));
}

// The QC gate core: a flag passes if the metadata already carries values OR a section heading indicates
// the material exists. Ingest adapts sections → headings before calling; admin passes headings directly.
export function computeQcFlags(
  headings: string[],
  metadata: { indexCodes: string[]; rulesSections: string[]; ordinanceSections: string[] }
): { hasIndexCodes: boolean; hasRulesSection: boolean; hasOrdinanceSection: boolean } {
  return {
    hasIndexCodes: metadata.indexCodes.length > 0 || headings.some((heading) => /index\s+codes?/i.test(heading)),
    hasRulesSection: metadata.rulesSections.length > 0 || headings.some((heading) => /^rules?$/i.test(heading)),
    hasOrdinanceSection: metadata.ordinanceSections.length > 0 || headings.some((heading) => /^ordinance(s)?$/i.test(heading))
  };
}

// One pattern for "this document is a test fixture/harness artifact, not a real decision". The SQL
// exclusion clause in admin-ingestion (likelyFixtureSqlExclusionClause) encodes the same terms for
// LIKE filtering — keep the two in sync when adding a term.
export const FIXTURE_DOC_NAME_PATTERN = /harness|fixture|seed|decision_pass|decision_fail|decision_invalid|law_sample|bee-harness/;

export function isLikelyFixtureName(joined: string): boolean {
  return FIXTURE_DOC_NAME_PATTERN.test(joined.toLowerCase());
}
