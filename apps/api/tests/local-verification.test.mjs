import { test, before } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const runTag = `run${Date.now().toString(36)}`;

const state = {
  validDecisionId: "",
  invalidDecisionId: "",
  lawId: ""
};

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

async function confirmRequiredMetadata(documentId) {
  return postJson(`/admin/ingestion/documents/${documentId}/metadata`, {
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    confirm_required_metadata: true
  });
}

async function requireHealth() {
  const response = await fetch(`${apiBase}/health`);
  assert.equal(response.status, 200, `Health check failed for ${apiBase}`);
}

before(async () => {
  await requireHealth();

  const validDecision = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Harness Decision Valid ${runTag}`,
    citation: `BEE-HARNESS-VALID-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "decision_pass.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
    }
  });

  assert.equal(validDecision.status, 201, "Valid decision ingest should succeed");
  assert.equal(validDecision.body?.qc?.hasIndexCodes, true);
  assert.equal(validDecision.body?.qc?.hasRulesSection, true);
  assert.equal(validDecision.body?.qc?.hasOrdinanceSection, true);
  state.validDecisionId = validDecision.body.documentId;

  const invalidDecision = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Harness Decision Invalid ${runTag}`,
    citation: `BEE-HARNESS-INVALID-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "decision_invalid_missing_required_metadata.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: await readFixtureBase64("decision_invalid_missing_required_metadata.docx.txt")
    }
  });

  assert.equal(invalidDecision.status, 201, "Invalid decision ingest should still persist");
  assert.equal(invalidDecision.body?.qc?.hasIndexCodes, false);
  assert.equal(invalidDecision.body?.qc?.hasRulesSection, false);
  assert.equal(invalidDecision.body?.qc?.hasOrdinanceSection, false);
  state.invalidDecisionId = invalidDecision.body.documentId;

  const law = await postJson("/ingest/law", {
    jurisdiction: "City of Beedle",
    title: `Harness Law PDF ${runTag}`,
    citation: `BEE-HARNESS-LAW-${runTag}`,
    decisionDate: "2026-03-07",
    sourceFile: {
      filename: "law_sample.pdf",
      mimeType: "application/pdf",
      bytesBase64: await readFixtureBase64("law_sample.pdf")
    }
  });

  assert.equal(law.status, 201, "Law ingest should succeed");
  state.lawId = law.body.documentId;
});

test("QC gate blocks invalid decision approval", async () => {
  const confirmed = await confirmRequiredMetadata(state.validDecisionId);
  assert.equal(confirmed.status, 200);
  assert.equal(confirmed.body?.qcRequiredConfirmed, 1);

  const approved = await postJson(`/decisions/${state.validDecisionId}/approve`, {});
  assert.equal(approved.status, 200);
  assert.equal(approved.body?.approved, true);

  const blocked = await postJson(`/decisions/${state.invalidDecisionId}/approve`, {});
  assert.equal(blocked.status, 422);
  assert.equal(blocked.body?.approved, false);
  assert.match(String(blocked.body?.reason || ""), /QC gate blocked approval/i);
});

test("search smoke returns grounded fields", async () => {
  const result = await postJson("/search", {
    query: "variance",
    limit: 10,
    filters: {
      approvedOnly: true,
      jurisdiction: "City of Beedle"
    }
  });

  assert.equal(result.status, 200);
  assert.ok(Array.isArray(result.body?.results), "Results should be an array");
  assert.ok(result.body.results.length >= 1, "Expected at least one search result");

  const top = result.body.results[0];
  assert.ok(top.snippet);
  assert.ok(top.sectionLabel);
  assert.ok(top.citationAnchor);
  assert.ok(top.sourceFileRef);
  assert.ok(top.sourceLink);
});

test("approved decision is discoverable by content phrase query", async () => {
  const result = await postJson("/search", {
    query: "maximum lot coverage of 40 percent",
    limit: 10,
    filters: {
      approvedOnly: true,
      jurisdiction: "City of Beedle"
    }
  });

  assert.equal(result.status, 200);
  const ids = (result.body.results || []).map((row) => row.documentId);
  assert.ok(ids.includes(state.validDecisionId), "Expected approved decision to be discoverable by content phrase");
});

test("citation anchor formatting is stable", async () => {
  const result = await postJson("/search", {
    query: "Ordinance",
    limit: 20,
    filters: {
      approvedOnly: false,
      jurisdiction: "City of Beedle"
    }
  });

  assert.equal(result.status, 200);
  assert.ok(result.body.results.length > 0, "Expected at least one result for anchor validation");

  for (const row of result.body.results) {
    assert.match(String(row.citationAnchor), /^[A-Za-z0-9-]+#[a-z0-9_]+-p\d+(?:-c\d+)?$/);
    assert.match(String(row.paragraphAnchor), /^[a-z0-9_]+-p\d+$/);
    assert.ok(String(row.citationAnchor).startsWith(`${row.citation}#`));
  }
});
