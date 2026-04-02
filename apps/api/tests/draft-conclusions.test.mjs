import { test, before } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const runTag = `dc${Date.now().toString(36)}`;

let draftResult;
let draftDebugResult;

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
  assert.equal(health.status, 200, `Health check failed for ${apiBase}`);

  const decision = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Drafting Decision ${runTag}`,
    citation: `BEE-DRAFT-DECISION-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "decision_pass.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
    }
  });
  assert.equal(decision.status, 201);

  const confirm = await postJson(`/admin/ingestion/documents/${decision.body.documentId}/metadata`, {
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    confirm_required_metadata: true
  });
  assert.equal(confirm.status, 200);

  const approved = await postJson(`/decisions/${decision.body.documentId}/approve`, {});
  assert.equal(approved.status, 200);

  const law = await postJson("/ingest/law", {
    jurisdiction: "City of Beedle",
    title: `Drafting Law ${runTag}`,
    citation: `BEE-DRAFT-LAW-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "law_sample.pdf",
      mimeType: "application/pdf",
      bytesBase64: await readFixtureBase64("law_sample.pdf")
    }
  });
  assert.equal(law.status, 201);

  const result = await postJson("/api/draft/conclusions", {
    findings_text:
      "Applicant seeks zoning variance relief for lot coverage and setback conflicts. Findings include public notice, mitigation commitments, and compliance updates.",
    law_text: "Rule 3.1 notice requirements and Ordinance 77-19 lot coverage standards apply.",
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    issue_tags: ["variance", "lot coverage"]
  });

  assert.equal(result.status, 200);
  draftResult = result.body;

  const debugResult = await postJson("/admin/draft/debug", {
    findings_text:
      "Applicant seeks zoning variance relief for lot coverage and setback conflicts. Findings include public notice, mitigation commitments, and compliance updates.",
    law_text: "Rule 3.1 notice requirements and Ordinance 77-19 lot coverage standards apply.",
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    issue_tags: ["variance", "lot coverage"]
  });
  assert.equal(debugResult.status, 200);
  draftDebugResult = debugResult.body;
});

test("/api/draft/conclusions smoke test", () => {
  assert.ok(draftResult.query_summary);
  assert.ok(draftResult.draft_text);
  assert.ok(Array.isArray(draftResult.draft_sections));
  assert.ok(Array.isArray(draftResult.paragraph_support));
  assert.ok(Array.isArray(draftResult.supporting_authorities));
  assert.ok(Array.isArray(draftResult.reasoning_notes));
  assert.ok(Array.isArray(draftResult.limitations));
  assert.ok(["low", "medium", "high"].includes(draftResult.confidence));
  assert.ok(draftResult.confidence_signals);
  assert.ok(Array.isArray(draftResult.citations));
});

test("paragraph support objects map to known citations", () => {
  const citationIds = new Set(draftResult.citations.map((item) => item.id));
  assert.ok(draftResult.paragraph_support.length >= 3);
  for (const row of draftResult.paragraph_support) {
    assert.ok(["strong", "mixed", "weak", "unsupported"].includes(row.support_level));
    for (const citationId of row.citation_ids) {
      assert.ok(citationIds.has(citationId), `Unknown citation id in paragraph support: ${citationId}`);
    }
  }
});

test("groundedness: citations map to supporting authorities", () => {
  const authorityById = new Map(draftResult.supporting_authorities.map((item) => [item.citation_id, item]));
  for (const citation of draftResult.citations) {
    const authority = authorityById.get(citation.id);
    assert.ok(authority, `Citation ${citation.id} must map to a supporting authority`);
    assert.equal(citation.citation_anchor, authority.citation_anchor);
    assert.equal(citation.source_link, authority.source_link);
    assert.equal(citation.title, authority.title);
  }
});

test("weak retrieval lowers confidence and emits limitations", async () => {
  const sparse = await postJson("/api/draft/conclusions", {
    findings_text: "xqzv ptlm nbrw",
    law_text: "zzqq hhty",
    index_codes: [],
    rules_sections: [],
    ordinance_sections: [],
    issue_tags: []
  });

  assert.equal(sparse.status, 200);
  assert.equal(sparse.body.confidence, "low");
  assert.ok(sparse.body.limitations.length > 0);
});

test("no fabricated citation objects are returned", () => {
  const validAuthorityKeys = new Set(
    draftResult.supporting_authorities.map((item) => `${item.citation_id}|${item.citation_anchor}|${item.source_link}`)
  );

  for (const citation of draftResult.citations) {
    const key = `${citation.id}|${citation.citation_anchor}|${citation.source_link}`;
    assert.ok(validAuthorityKeys.has(key), `Unexpected citation object: ${key}`);
  }
});

test("draft sections reference known citations", () => {
  const citationIds = new Set(draftResult.citations.map((item) => item.id));
  assert.ok(draftResult.draft_sections.length >= 3);

  for (const section of draftResult.draft_sections) {
    assert.ok(section.text.length > 0);
    for (const citationId of section.citation_ids) {
      assert.ok(citationIds.has(citationId), `Unknown citation id in draft section: ${citationId}`);
    }
  }
});

test("debug endpoint exposes inspectable confidence and support mapping", () => {
  assert.ok(draftDebugResult.request);
  assert.ok(draftDebugResult.draft);
  assert.ok(draftDebugResult.debug);
  assert.ok(Array.isArray(draftDebugResult.debug.paragraph_support));
  assert.ok(Array.isArray(draftDebugResult.debug.triggered_limitations));
  assert.ok(Array.isArray(draftDebugResult.debug.chosen_citation_ids));
  assert.ok(Array.isArray(draftDebugResult.debug.unsupported_paragraphs));
  assert.ok(typeof draftDebugResult.debug.confidence_signals.retrieval_strength === "number");
});
