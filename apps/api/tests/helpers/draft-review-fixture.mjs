import { loadTs } from "./load-ts.mjs";
const { draftConclusionsResponseSchema } = loadTs("../../packages/shared/src/index.ts");
const { buildParagraphSupport } = loadTs("src/services/draft-evidence.ts");
const { captureDraftProvenance } = loadTs("src/services/draft-provenance.ts");

export async function draftReviewFixture() {
  const source = { document_id: "reference:rule:test", citation_id: "rule-test-v1", title: "Rule text", citation: "10.10(c)(3)", citation_anchor: "p7", source_link: "http://127.0.0.1:8795/references/rule/test?revision=v1", source_file_ref: "reference:rule:test", snippet: "Under Rule 10.10(c)(3), the tenant did not retain possession of the dwelling.", section_label: "Procedure", support_type: "inference", source_kind: "provision_candidate", source_revision: "v1", why_it_matches: "Exact requested provision; applicability unverified." };
  const draftText = "Under Rule 10.10(c)(3), the tenant retained possession of the dwelling.\n\nZebras orbit Jupiter.";
  return draftConclusionsResponseSchema.parse({
    assessment_version: "retrieval-only-2", generated_at: "2026-10-26T16:00:00.000Z", review_required: true,
    query_summary: "Review candidate law against supplied findings", draft_text: draftText,
    generation_mode: "heuristic_fallback", fallback_reason: "Fixture provider unavailable", model: null,
    draft_sections: [{ id: "conclusions_of_law", heading: "Conclusions of Law", text: draftText, citation_ids: [source.citation_id] }],
    paragraph_support: buildParagraphSupport(draftText, new Map([[source.citation_id, source]])), supporting_authorities: [source], reasoning_notes: ["No disposition has been independently verified."], confidence: "low",
    confidence_signals: { retrieval_strength: 0.4, authority_count: 1, direct_conclusions_count: 0, explicit_support_ratio: 0, findings_coverage: 0.3, law_coverage: 0.5, conflict_index: 0, paragraph_support_ratio: 0, confidence_score: 0 },
    limitations: ["Jurisdiction and effective edition unverified.", "The draft contains an unsupported paragraph and a possible negation conflict."],
    citations: [{ id: source.citation_id, ...source }], provenance: await captureDraftProvenance({}, [source]),
    research_context: { provided_findings: "The tenant retained possession of the dwelling.", supplied_law: "Rule 10.10(c)(3)", requested_rules: ["10.10(c)(3)"], requested_ordinance: [], law_scope: "jurisdiction_and_effective_edition_unverified" }
  });
}
