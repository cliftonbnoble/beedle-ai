import test from "node:test";
import assert from "node:assert/strict";
import { loadTs } from "./helpers/load-ts.mjs";

const { buildParagraphSupport } = loadTs("src/services/draft-evidence.ts");
const { assembleDraft } = loadTs("src/services/draft-conclusions.ts");
const { draftConclusionsResponseSchema, draftConclusionsRequestSchema } = loadTs("../../packages/shared/src/index.ts");
const authority = (id, snippet, document = id) => ({ document_id: document, citation_id: id, title: "Decision", citation: "Case 1", citation_anchor: "p1", source_link: "https://example.invalid/source", source_file_ref: "source", snippet, section_label: "Conclusions of Law", support_type: "explicit", why_it_matches: "Zebras orbit Jupiter. Strongly relevant analysis." });
const assess = (text, rows, findings = "") => buildParagraphSupport(text, new Map(rows.map(row => [row.citation_id, row])), findings)[0];

test("the original unrelated paragraph is unsupported despite section/type/description bonuses", () => {
  const result = assess("Zebras orbit Jupiter.", [authority("rent", "The tenant owes rent."), authority("roof", "The roof required repairs.")]);
  assert.equal(result.support_level, "unsupported");
  assert.deepEqual(result.citation_ids, []);
});

test("exact text remains unverified, spans point into the original excerpt, duplicate decisions count once", () => {
  const sentence = "The tenant retained possession of the dwelling.";
  const rows = [authority("a", sentence, "same"), authority("b", sentence, "same")];
  const result = assess(sentence, rows, sentence);
  assert.equal(result.support_level, "unverified");
  assert.equal(result.claims[0].kind, "provided_fact");
  assert.equal(result.claims[0].evidence.length, 1);
  const span = result.claims[0].evidence[0];
  assert.equal(span.relation, "exact_quote");
  assert.equal(rows[0].snippet.slice(span.start, span.end), span.text);
});

test("negation, different provisions, invented citations and mixed paragraphs cannot become strong support", () => {
  const rows = [authority("a", "Under Rule 10.10(c)(3), the tenant did not retain possession of the dwelling."), authority("b", "Under Rule 6.13, the tenant retained possession of the dwelling.")];
  const opposite = assess("Under Rule 10.10(c)(3), the tenant retained possession of the dwelling.", rows);
  assert.ok(opposite.support_notes.includes("possible_negation_conflict"));
  assert.ok(opposite.support_notes.includes("provision_not_matched"));
  assert.equal(opposite.claims[0].kind, "legal_proposition");
  const fake = assess("The tenant retained possession of the dwelling. [invented]", rows);
  assert.equal(fake.support_level, "unsupported");
  assert.ok(fake.support_notes.includes("unknown_citation_id"));
  const mixed = assess("The tenant retained possession of the dwelling. Zebras orbit Jupiter.", rows);
  assert.equal(mixed.support_level, "unsupported");
  for (const item of [opposite, fake, mixed]) assert.ok(!["strong", "mixed", "weak"].includes(item.support_level));
});

test("draft assembly never turns retrieval into confidence and preserves legacy assessment provenance", async () => {
  const rows = [authority("a", "The tenant retained possession of the dwelling."), authority("b", "The landlord denied the tenant possession of the dwelling.")];
  const research = { query_summary: "Test", similar_cases: rows, relevant_law: [], outcome_guidance: { direction: "unclear", rationale: "Test" }, reasoning_themes: [], vulnerabilities: [], strengthening_suggestions: [], confidence: "high", citations: [], guardrails: [] };
  const parsed = draftConclusionsRequestSchema.parse({ findings_text: "The tenant retained possession of the dwelling." });
  const { draft } = await assembleDraft(parsed, research, {});
  assert.equal(draft.assessment_version, "retrieval-only-2");
  assert.equal(draft.review_required, true);
  assert.equal(draft.confidence, "low");
  assert.equal(draft.confidence_signals.confidence_score, 0);
  assert.ok(draft.paragraph_support.every(item => !["strong", "mixed", "weak"].includes(item.support_level)));
  const old = structuredClone(draft);
  delete old.assessment_version;
  old.paragraph_support[0].support_level = "strong";
  const historical = draftConclusionsResponseSchema.parse(old);
  assert.equal(historical.assessment_version, "legacy-1");
  assert.equal(historical.paragraph_support[0].support_level, "strong");
});

test('same section number in another Part remains a provision mismatch',()=>{
  const result=assess('Under Rule IV-4.5(a), the tenant retained possession of the dwelling.',[authority('wrong-part','Under Rule V-4.5(a), the tenant retained possession of the dwelling.')]);
  assert.ok(result.support_notes.includes('provision_not_matched'));
  assert.equal(result.support_level,'unverified');
});
