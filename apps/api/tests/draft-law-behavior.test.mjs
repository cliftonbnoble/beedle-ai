import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { readFileSync } from "node:fs";
import { loadTs } from "./helpers/load-ts.mjs";

const { retrieveDraftResearch, inferDraftDirection } = loadTs("src/services/draft-conclusions.ts");
const { retrieveRequestedLaw } = loadTs("src/services/draft-law.ts");
const { handleGetProvision } = loadTs("src/routes/provision.ts");
const { draftConclusionsRequestSchema } = loadTs("../../packages/shared/src/index.ts");

function environment() {
  const db = new DatabaseSync(":memory:");
  db.exec(readFileSync(new URL("../migrations/0004_reference_normalization.sql", import.meta.url), "utf8"));
  db.exec(readFileSync(new URL("../migrations/0005_rules_dual_key.sql", import.meta.url), "utf8"));
  db.prepare("INSERT INTO legal_rules_sections (id,section_number,citation,normalized_citation,normalized_bare_citation,body_text,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)").run("rule", "10.10(c)(3)", "Rule 10.10(c)(3)", "10.10(c)(3)", "10.10(c)(3)", "The Board must examine the evidence before deciding the request.", "v1", "v1");
  db.prepare("INSERT INTO legal_ordinance_sections (id,section_number,citation,normalized_citation,body_text,created_at,updated_at) VALUES (?,?,?,?,?,?,?)").run("ord", "37.2", "37.2", "37.2", "This is only the parent definition.", "v1", "v1");
  return { db, env: { SOURCE_PROXY_BASE_URL: "https://api.example", DB: { prepare(sql) { const statement = db.prepare(sql); return { bind(...args) { this.args = args; return this; }, async all() { return { results: statement.all(...(this.args || [])) }; }, async first() { return statement.get(...(this.args || [])) || null; } }; } } } };
}

test("rules-only research returns actual exact provision text and never findings-as-law", async () => {
  const { db, env } = environment();
  try {
    const calls = [];
    const parsed = draftConclusionsRequestSchema.parse({ rules_sections: ["Rule 10.10(c)(3)"], ordinance_sections: ["37.2(g)"] });
    const result = await retrieveDraftResearch(env, parsed, async (_env, input) => { calls.push(input); return { results: [] }; });
    assert.equal(result.relevant_law.length, 1);
    assert.equal(result.relevant_law[0].source_kind, "provision_candidate");
    assert.match(result.relevant_law[0].snippet, /Board must examine/);
    assert.equal(result.similar_cases.length, 0);
    assert.ok(result.vulnerabilities.some(item => /37\.2\(g\).*unavailable/.test(item.impact)));
    assert.ok(result.vulnerabilities.some(item => /effective edition/.test(item.impact)));
    assert.ok(calls.every(call => call.filters.fileType === "decision_docx" && call.corpusMode === "trusted_only"));
    assert.ok(calls.some(call => call.query.includes("10.10(c)(3)")));
    const url = result.relevant_law[0].source_link;
    const request = Object.assign(new Request(url), { params: { kind: "rule", id: "rule" } });
    assert.equal((await handleGetProvision(request, env)).status, 200);
    db.exec("UPDATE legal_rules_sections SET updated_at='v2'");
    assert.equal((await handleGetProvision(request, env)).status, 409);
  } finally { db.close(); }
});

test("law input citations are retrieved, missing laws remain absent, and unsupported uploads are rejected", async () => {
  const { db, env } = environment();
  try {
    const found = await retrieveRequestedLaw(env, draftConclusionsRequestSchema.parse({ law_text: "Apply Rule 10.10(c)(3)." }));
    assert.equal(found.authorities.length, 1);
    const missing = await retrieveRequestedLaw(env, draftConclusionsRequestSchema.parse({ rules_sections: ["99.99"] }));
    assert.deepEqual(missing.authorities, []);
    assert.throws(() => draftConclusionsRequestSchema.parse({ findings_text: "Findings", uploaded_doc_ids: ["private"] }), /access controls/);
    assert.throws(() => draftConclusionsRequestSchema.parse({}));
    await assert.rejects(retrieveRequestedLaw(env, draftConclusionsRequestSchema.parse({ rules_sections: ["Ordinance 37.2"] })), /conflicting/);
    await assert.rejects(retrieveRequestedLaw(env, draftConclusionsRequestSchema.parse({ rules_sections: Array.from({ length: 7 }, (_, i) => `10.${i + 1}`) })), /six distinct/);
  } finally { db.close(); }
});

test("disposition is always unclear without reviewed structured findings, regardless of outcome words", () => {
  for (const snippet of ["The request to grant relief was denied.", "Granted granted granted.", "Denied but later granted in part."]) {
    assert.equal(inferDraftDirection([{ snippet, section_label: "Order", why_it_matches: "grant" }]).direction, "unclear");
  }
});

test('law research never substitutes a different Part and keeps explicit law-text qualifiers', async () => {
  const rows=['IV','V'].map(part=>({id:part,citation:`${part}-4.5(a)`,part,sectionNumber:'4.5(a)',body:`Part ${part} requires review of its own procedure.`,revision:'v1',normalized:'4.5(a)'}));
  // Historical normalized columns lost the qualifier. The identity check must
  // still use the retained Part/section fields, rather than the first SQL row.
  const env={DB:{prepare(){return {bind(...args){this.args=args;return this;},async first(){return {version:'catalog',epoch:1};},async all(){assert.ok(this.args.includes('4.5(a)'));return {results:rows};}};}}};
  const exact=await retrieveRequestedLaw(env,draftConclusionsRequestSchema.parse({law_text:'Apply Rule IV-4.5(a).'}));
  assert.equal(exact.authorities.length,1);assert.match(exact.authorities[0].snippet,/Part IV/);
  const missing=await retrieveRequestedLaw(env,draftConclusionsRequestSchema.parse({rules_sections:['Rule VI-4.5(a)']}));
  assert.equal(missing.authorities.length,0);
  const ambiguous=await retrieveRequestedLaw(env,draftConclusionsRequestSchema.parse({rules_sections:['4.5(a)']}));
  assert.equal(ambiguous.authorities.length,0);assert.match(ambiguous.warnings.join(' '),/ambiguous/);
});
