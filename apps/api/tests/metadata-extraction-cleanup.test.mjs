import { before, test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const runTag = `cleanup${Date.now().toString(36)}`;
let documentId = "";

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
  assert.equal(health.status, 200, `Health check failed for ${apiBase}`);

  const bytes = await fs.readFile(path.join(fixturesDir, "decision_noise_cleanup.docx.txt"));
  const ingested = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Metadata Cleanup ${runTag}`,
    citation: `BEE-CLEANUP-${runTag}`,
    sourceFile: {
      filename: "decision_noise_cleanup.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: bytes.toString("base64")
    }
  });
  assert.equal(ingested.status, 201);
  documentId = ingested.body.documentId;
});

test("index code extraction rejects obvious year/date/amount noise", async () => {
  const detail = await getJson(`/admin/ingestion/documents/${documentId}`);
  assert.equal(detail.status, 200);
  const indexCodes = detail.body.indexCodes || [];
  assert.ok(indexCodes.length <= 5, "index-code extraction should be conservative on noisy inputs");
  assert.ok(indexCodes.every((value) => /^([A-Z]{1,4}-)?\d{2,4}[A-Z]?$/.test(String(value))), "extracted index codes must use strict code shape");
  assert.ok(!indexCodes.some((value) => /^20\d{2}$/.test(String(value))), "year-like values should not be extracted as index codes");
  assert.ok(!indexCodes.some((value) => /\//.test(String(value))), "date-like values should not be extracted as index codes");
});

test("real rules/ordinance citations are detected from decision text", async () => {
  const detail = await getJson(`/admin/ingestion/documents/${documentId}`);
  assert.equal(detail.status, 200);
  const rules = detail.body.rulesSections || [];
  const ordinance = detail.body.ordinanceSections || [];
  assert.ok(rules.some((item) => /6\.13|10\.10\(c\)\(3\)/.test(String(item))), "expected rules references to be detected");
  assert.ok(ordinance.some((item) => /37\.3\(a\)\(1\)|37\.15/.test(String(item))), "expected ordinance references to be detected");
});

test("unresolved-reference signal is bounded to extracted reference volume", async () => {
  const detail = await getJson(`/admin/ingestion/documents/${documentId}`);
  assert.equal(detail.status, 200);
  const extractedRefCount =
    (detail.body.indexCodes || []).length + (detail.body.rulesSections || []).length + (detail.body.ordinanceSections || []).length;
  assert.ok((detail.body.unresolvedReferenceCount || 0) <= extractedRefCount, "unresolved references should not inflate beyond extracted values");
});
