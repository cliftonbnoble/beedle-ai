import test from "node:test";
import assert from "node:assert/strict";
import { loadTs } from "./helpers/load-ts.mjs";

const { parseCitation, extractCitations, citationKey } = loadTs("src/services/legal-citations.ts");
const { parseMarkdownDocument } = loadTs("src/services/parser.ts");
const { detectCriticalReferenceExceptions } = loadTs("src/services/qc-shared.ts");
const { verifyCitations, buildDocumentReferenceValidationStatements } = loadTs("src/services/legal-references.ts");

test("subsections survive punctuation, whitespace, Roman/part prefixes, and adjacent contexts", () => {
  const input = "Rule 10.10(c)(3). Apply Ordinance 37.2(g). Rule IV-4.5(a), Rules and Regulations Section 6.13(b); Rule Part 10 - 10.10(c)(3).";
  const found = extractCitations(input);
  assert.deepEqual(found.map(c => [c.context, c.canonical]), [["rule", "10.10(c)(3)"], ["ordinance", "37.2(g)"], ["rule", "4.5(a)"], ["rule", "6.13(b)"], ["rule", "10.10(c)(3)"]]);
  assert.equal(found[0].parent, "10.10(c)");
  assert.equal(found[2].part, "IV");
  assert.equal(found[0].raw, "Rule 10.10(c)(3)");
  assert.equal(parseCitation("Rule 10.10\n(c) (3)").canonical, "10.10(c)(3)");
  assert.equal(parseCitation("Ordinance 37.2(g)", "rule").resolution, "ambiguous");
  assert.equal(citationKey("Ordinance 37.2(g)", "rule"), "");
  assert.equal(parseCitation("Rule 10.10(c)(3)garbage").resolution, "invalid");
  assert.equal(parseCitation("Rule IC-1.11").resolution, "invalid");
  assert.equal(citationKey("IC-12", "rule"), "");
});

test("ambiguous section references remain separate evidence rather than guessed ordinance/rules", () => {
  const citations = extractCitations("Rules and Ordinance govern Section 37.15. Section 37.2(g).");
  assert.ok(citations.every(c => c.resolution === "ambiguous"));
  const result = parseMarkdownDocument(new TextEncoder().encode("# Findings\nRule 10.10(c)(3). Apply Ordinance 37.2(g). These legal citations require review against the original sources."));
  assert.deepEqual(result.extractedMetadata.rulesSections, ["Rule 10.10(c)(3)"]);
  assert.deepEqual(result.extractedMetadata.ordinanceSections, ["Ordinance 37.2(g)"]);
  assert.equal(result.citations[0].resolution, "unresolved");
  assert.deepEqual(detectCriticalReferenceExceptions({ rules: ["Rule 10.10(c)(3)"], ordinance: ["Ordinance 37.2(g)", "Rent Ordinance 37.15"] }), ["37.2(g)", "37.15", "10.10(c)(3)"]);
});

test("catalog resolution distinguishes exact, parent-only, and cross-context ambiguity", async () => {
  const env = { DB: { prepare(sql) { return { all: async () => ({ results: sql.includes("legal_ordinance")
    ? [{ citation: "37.2", normalizedCitation: "37.2" }, { citation: "37.15", normalizedCitation: "37.15" }]
    : [{ citation: "10.10(c)(3)", normalizedCitation: "10.10(c)(3)", normalizedBareCitation: "10.10(c)(3)" }, { citation: "37.15", normalizedCitation: "37.15", normalizedBareCitation: "37.15" }] }) }; } } };
  const { checks } = await verifyCitations(env, { citations: ["Rule 10.10(c)(3)", "Ordinance 37.2(g)", "37.15", "Ordinance 37.15"] });
  assert.equal(checks[0].status, "resolved");
  assert.equal(checks[1].status, "unresolved");
  assert.equal(checks[1].diagnostic, "parent_or_related_only");
  assert.equal(checks[2].status, "ambiguous");
  assert.equal(checks[3].status, "resolved");
  assert.deepEqual(checks[3].rules_matches, []);
});

test("persisted validation keeps raw/exact values and never resolves a subsection to its parent", async () => {
  const env = { DB: { prepare(sql) { return { sql, bind(...args) { this.args = args; return this; } }; } } };
  const lookups = { indexByNormalized: new Map(), rulesByNormalized: new Map([["10.10", "Rule 10.10"]]), rulesByBare: new Map(), ordinanceByNormalized: new Map([["37.2(g)", "37.2(g)"]]) };
  const statements = await buildDocumentReferenceValidationStatements(env, "doc", { indexCodes: [], rulesSections: ["Rule 10.10(c)(3)"], ordinanceSections: ["Ordinance 37.2(g)"] }, lookups);
  const rules = statements.find(s => s.sql.includes("INSERT INTO document_reference_links") && s.sql.includes("'rules_section'"));
  assert.deepEqual(rules.args.slice(2, 6), ["Rule 10.10(c)(3)", "10.10(c)(3)", null, 0]);
  const ord = statements.find(s => s.sql.includes("INSERT INTO document_reference_links") && s.sql.includes("'ordinance_section'"));
  assert.deepEqual(ord.args.slice(2, 6), ["Ordinance 37.2(g)", "37.2(g)", "37.2(g)", 1]);
});

test('Part identifiers survive document metadata and cannot collapse malformed subsection endings', () => {
  const result = parseMarkdownDocument(new TextEncoder().encode('# Findings\nRule IV-4.5(a) applies to the evidence. This source contains enough meaningful text for extraction review.'));
  assert.deepEqual(result.extractedMetadata.rulesSections, ['Rule Part IV - 4.5(a)']);
  for (const suffix of ['(3', '(3]', '(3)junk']) {
    assert.deepEqual(extractCitations(`Apply Rule 10.10(c)${suffix} and review the source.`), [], suffix);
  }
  const { citationIdentity } = loadTs('src/services/legal-citations.ts');
  assert.notEqual(citationIdentity('Rule IV-4.5(a)', 'rule'), citationIdentity('Rule V-4.5(a)', 'rule'));
  assert.equal(citationIdentity('Rule Part IV - 4.5(a)', 'rule'), citationIdentity('Rule IV-4.5(a)', 'rule'));
});
