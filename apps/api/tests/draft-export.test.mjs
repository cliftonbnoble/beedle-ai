import { test, before } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const runTag = `dx${Date.now().toString(36)}`;

let conclusions;
let template;

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
    title: `Export Decision ${runTag}`,
    citation: `BEE-EXPORT-DEC-${runTag}`,
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
    title: `Export Law ${runTag}`,
    citation: `BEE-EXPORT-LAW-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "law_sample.pdf",
      mimeType: "application/pdf",
      bytesBase64: await readFixtureBase64("law_sample.pdf")
    }
  });
  assert.equal(law.status, 201);

  const con = await postJson("/api/draft/conclusions", {
    findings_text: "Applicant seeks zoning variance relief with notice and mitigation findings.",
    law_text: "Rule 3.1 notice requirements and Ordinance 77-19 apply.",
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    issue_tags: ["variance"]
  });
  assert.equal(con.status, 200);
  conclusions = con.body;

  const tpl = await postJson("/api/draft/template", {
    case_type: "zoning_variance",
    template_mode: "guided_scaffold"
  });
  assert.equal(tpl.status, 200);
  template = tpl.body;
});

test("export endpoint returns stable markdown/text/html payloads", async () => {
  const md = await postJson("/api/draft/export", {
    kind: "conclusions",
    format: "markdown",
    conclusions
  });
  assert.equal(md.status, 200);
  assert.equal(md.body.mime_type.includes("markdown"), true);
  assert.equal(md.body.content.includes("# Conclusions of Law Draft"), true);
  assert.equal(md.body.content.includes("## Paragraph Support"), true);

  const txt = await postJson("/api/draft/export", {
    kind: "conclusions",
    format: "text",
    conclusions
  });
  assert.equal(txt.status, 200);
  assert.equal(txt.body.mime_type.includes("text/plain"), true);
  assert.equal(txt.body.content.includes("Paragraph Support"), true);

  const html = await postJson("/api/draft/export", {
    kind: "conclusions",
    format: "html",
    conclusions
  });
  assert.equal(html.status, 200);
  assert.equal(html.body.mime_type.includes("text/html"), true);
  assert.equal(html.body.content.includes("<!doctype html>"), true);
});

test("section formatting consistency for template export", async () => {
  const md = await postJson("/api/draft/export", {
    kind: "template",
    format: "markdown",
    template
  });
  assert.equal(md.status, 200);
  assert.equal(md.body.content.includes("### Introduction"), true);
  assert.equal(md.body.content.includes("### Findings of Fact"), true);
  assert.equal(md.body.content.includes("### Related Case / Procedural History"), true);
  assert.equal(md.body.content.includes("### Conclusions of Law"), true);
  assert.equal(md.body.content.includes("### Order"), true);
});

test("export metadata preserves citation/support counts", async () => {
  const exported = await postJson("/api/draft/export", {
    kind: "conclusions",
    format: "markdown",
    conclusions
  });
  assert.equal(exported.status, 200);
  assert.equal(exported.body.metadata.citation_count, conclusions.citations.length);
  assert.equal(exported.body.metadata.support_item_count, conclusions.paragraph_support.length);
});

test("no citation/support loss in export preparation", async () => {
  const exported = await postJson("/api/draft/export", {
    kind: "conclusions",
    format: "markdown",
    conclusions
  });
  assert.equal(exported.status, 200);

  for (const citation of conclusions.citations) {
    assert.equal(exported.body.content.includes(citation.id), true, `Expected citation id in export: ${citation.id}`);
  }
  for (const support of conclusions.paragraph_support) {
    assert.equal(exported.body.content.includes(support.paragraph_id), true, `Expected paragraph id in export: ${support.paragraph_id}`);
  }
});
