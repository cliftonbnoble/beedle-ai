import { test, before } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const runTag = `dt${Date.now().toString(36)}`;

let contextualResult;

async function readFixtureBase64(name) {
  const bytes = await fs.readFile(path.join(fixturesDir, name));
  return bytes.toString("base64");
}

async function postJson(endpoint, body) {
  const response = await fetch(`${apiBase}${endpoint}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body)
  });
  const raw = await response.text();
  let json;
  try {
    json = JSON.parse(raw);
  } catch {
    json = { raw };
  }
  return { status: response.status, body: json };
}

before(async () => {
  const health = await fetch(`${apiBase}/health`);
  assert.equal(health.status, 200);

  const decision = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Template Decision ${runTag}`,
    citation: `BEE-TEMPLATE-DEC-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "decision_pass.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
    }
  });
  assert.equal(decision.status, 201);
  await postJson(`/admin/ingestion/documents/${decision.body.documentId}/metadata`, {
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    confirm_required_metadata: true
  });
  await postJson(`/decisions/${decision.body.documentId}/approve`, {});

  const law = await postJson("/ingest/law", {
    jurisdiction: "City of Beedle",
    title: `Template Law ${runTag}`,
    citation: `BEE-TEMPLATE-LAW-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "law_sample.pdf",
      mimeType: "application/pdf",
      bytesBase64: await readFixtureBase64("law_sample.pdf")
    }
  });
  assert.equal(law.status, 201);

  const contextual = await postJson("/api/draft/template", {
    case_type: "zoning_variance",
    template_mode: "lightly_contextualized",
    findings_text: "Applicant seeks lot coverage variance with public notice and mitigation commitments.",
    law_text: "Rule 3.1 and Ordinance 77-19 apply.",
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"]
  });
  assert.equal(contextual.status, 200);
  contextualResult = contextual.body;
});

test("template smoke returns canonical structure", async () => {
  const response = await postJson("/api/draft/template", {
    case_type: "zoning_variance",
    template_mode: "guided_scaffold"
  });
  assert.equal(response.status, 200);
  assert.equal(response.body.case_type, "zoning_variance");
  assert.ok(Array.isArray(response.body.template_sections));
  assert.equal(response.body.template_sections.length, 5);
  assert.deepEqual(
    response.body.template_sections.map((s) => s.section_name),
    ["Introduction", "Findings of Fact", "Related Case / Procedural History", "Conclusions of Law", "Order"]
  );
});

test("blank scaffold mode stays non-contextual and citation-free", async () => {
  const findingsPhrase = "EXACT_FACT_PATTERN_SHOULD_NOT_APPEAR";
  const response = await postJson("/api/draft/template", {
    case_type: "zoning_variance",
    template_mode: "blank_scaffold",
    findings_text: findingsPhrase,
    law_text: "Rule 3.1 and Ordinance 77-19 apply."
  });
  assert.equal(response.status, 200);
  const allText = response.body.template_sections.map((s) => s.placeholder_text).join("\n");
  assert.equal(allText.includes(findingsPhrase), false);
  assert.equal(response.body.supporting_authorities.length, 0);
  assert.equal(response.body.citations.length, 0);
  assert.ok(response.body.template_sections.every((s) => s.drafting_prompts.length === 0));
});

test("guided scaffold mode includes prompts and avoids case-specific injected facts", async () => {
  const findingsPhrase = "GUIDED_MODE_SHOULD_NOT_ECHO_THIS_FACT";
  const response = await postJson("/api/draft/template", {
    case_type: "licensing_enforcement",
    template_mode: "guided_scaffold",
    findings_text: findingsPhrase
  });
  assert.equal(response.status, 200);
  assert.ok(response.body.template_sections.some((s) => s.drafting_prompts.length > 0));
  const allText = response.body.template_sections.map((s) => s.placeholder_text).join("\n");
  assert.equal(allText.includes(findingsPhrase), false);
  assert.equal(response.body.citations.length, 0);
  assert.equal(response.body.supporting_authorities.length, 0);
});

test("lightly contextualized mode can include valid support references", () => {
  assert.equal(contextualResult.template_mode, "lightly_contextualized");
  assert.ok(contextualResult.template_sections.length === 5);
  const authorityKeys = new Set(
    contextualResult.supporting_authorities.map((a) => `${a.citation_id}|${a.citation_anchor}|${a.source_link}`)
  );
  for (const citation of contextualResult.citations) {
    const key = `${citation.id}|${citation.citation_anchor}|${citation.source_link}`;
    assert.ok(authorityKeys.has(key), `Unexpected citation key: ${key}`);
  }
});
