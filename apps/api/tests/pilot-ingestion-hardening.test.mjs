import { before, test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const tag = `pilot${Date.now().toString(36)}`;

let documentId = "";

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

async function getJson(endpoint) {
  const response = await fetch(`${apiBase}${endpoint}`);
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

  const ingest = await postJson("/ingest/decision", {
    jurisdiction: "City of Pilot",
    title: `Pilot Hardening ${tag}`,
    citation: `PILOT-${tag}`,
    sourceFile: {
      filename: "decision_pass.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
    }
  });

  assert.equal(ingest.status, 201);
  documentId = ingest.body.documentId;
});

test("decision cannot be approved until metadata confirmed in admin QC", async () => {
  const blocked = await postJson(`/admin/ingestion/documents/${documentId}/approve`, {});
  assert.equal(blocked.status, 422);
  assert.match(String(blocked.body.reason || ""), /manually confirmed/i);

  const updated = await postJson(`/admin/ingestion/documents/${documentId}/metadata`, {
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    case_number: `CASE-${tag}`,
    author_name: "Admin Reviewer",
    outcome_label: "grant",
    confirm_required_metadata: true
  });

  assert.equal(updated.status, 200);
  assert.equal(updated.body.qcRequiredConfirmed, 1);

  const approved = await postJson(`/admin/ingestion/documents/${documentId}/approve`, {});
  assert.equal(approved.status, 200);
  assert.equal(approved.body.approved, true);
});

test("admin detail includes section/chunk anchorability", async () => {
  const detail = await getJson(`/admin/ingestion/documents/${documentId}`);
  assert.equal(detail.status, 200);

  assert.ok(Array.isArray(detail.body.sections));
  assert.ok(detail.body.sections.length > 0);
  assert.ok(Array.isArray(detail.body.chunks));
  assert.ok(detail.body.chunks.length > 0);

  for (const chunk of detail.body.chunks.slice(0, 5)) {
    assert.ok(chunk.paragraphAnchor);
    assert.ok(chunk.paragraphAnchorEnd);
    assert.ok(chunk.citationAnchor);
    assert.match(String(chunk.citationAnchor), /^[A-Za-z0-9-]+#[a-z0-9_]+-p\d+(?:-c\d+)?$/);
  }
});
