import { test, before } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const runTag = `ca${Date.now().toString(36)}`;

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

let caseAssistantResult;

before(async () => {
  const health = await fetch(`${apiBase}/health`);
  assert.equal(health.status, 200, `Health check failed for ${apiBase}`);

  const validDecision = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Case Assistant Valid ${runTag}`,
    citation: `BEE-CASE-VALID-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "decision_pass.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
    }
  });
  assert.equal(validDecision.status, 201);
  assert.equal(validDecision.body?.qc?.hasIndexCodes, true);

  const confirm = await postJson(`/admin/ingestion/documents/${validDecision.body.documentId}/metadata`, {
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    confirm_required_metadata: true
  });
  assert.equal(confirm.status, 200);

  const approved = await postJson(`/decisions/${validDecision.body.documentId}/approve`, {});
  assert.equal(approved.status, 200);

  const law = await postJson("/ingest/law", {
    jurisdiction: "City of Beedle",
    title: `Case Assistant Law ${runTag}`,
    citation: `BEE-CASE-LAW-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "law_sample.pdf",
      mimeType: "application/pdf",
      bytesBase64: await readFixtureBase64("law_sample.pdf")
    }
  });
  assert.equal(law.status, 201);

  const result = await postJson("/api/case-assistant", {
    findings_text:
      "Applicant seeks zoning variance relief for lot coverage. Findings include neighborhood notice, traffic impacts, and mitigation commitments.",
    law_text: "Rule 3.1 notice requirements and Ordinance 77-19 lot coverage limits apply.",
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    issue_tags: ["variance", "lot coverage"]
  });

  assert.equal(result.status, 200);
  caseAssistantResult = result.body;
});

test("/api/case-assistant smoke test", () => {
  assert.ok(caseAssistantResult.query_summary);
  assert.ok(Array.isArray(caseAssistantResult.similar_cases));
  assert.ok(Array.isArray(caseAssistantResult.relevant_law));
  assert.ok(caseAssistantResult.outcome_guidance?.direction);
  assert.ok(Array.isArray(caseAssistantResult.reasoning_themes));
  assert.ok(Array.isArray(caseAssistantResult.vulnerabilities));
  assert.ok(Array.isArray(caseAssistantResult.strengthening_suggestions));
  assert.ok(["low", "medium", "high"].includes(caseAssistantResult.confidence));
  assert.ok(Array.isArray(caseAssistantResult.citations));
});

test("groundedness: cited support exists", () => {
  const authorityById = new Map(
    [...caseAssistantResult.similar_cases, ...caseAssistantResult.relevant_law].map((item) => [item.citation_id, item])
  );

  for (const citation of caseAssistantResult.citations) {
    const authority = authorityById.get(citation.id);
    assert.ok(authority, `Citation ${citation.id} must map to an authority object`);
    assert.equal(citation.citation_anchor, authority.citation_anchor);
    assert.equal(citation.source_link, authority.source_link);
    assert.equal(citation.title, authority.title);
    assert.ok(authority.snippet.includes(citation.snippet) || citation.snippet.includes(authority.snippet));
  }
});

test("incomplete retrieval yields lower confidence", async () => {
  const sparse = await postJson("/api/case-assistant", {
    findings_text: "xqzv ptlm nbrw",
    law_text: "zzqq hhty",
    index_codes: [],
    rules_sections: [],
    ordinance_sections: [],
    issue_tags: []
  });

  assert.equal(sparse.status, 200);
  assert.equal(sparse.body.confidence, "low");
});

test("no fabricated citation objects are returned", () => {
  const validAuthorityKeys = new Set(
    [...caseAssistantResult.similar_cases, ...caseAssistantResult.relevant_law].map(
      (item) => `${item.citation_id}|${item.citation_anchor}|${item.source_link}`
    )
  );

  for (const citation of caseAssistantResult.citations) {
    const key = `${citation.id}|${citation.citation_anchor}|${citation.source_link}`;
    assert.ok(validAuthorityKeys.has(key), `Unexpected citation object: ${key}`);
  }
});
