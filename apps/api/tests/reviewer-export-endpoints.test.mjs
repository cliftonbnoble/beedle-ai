import { before, test } from "node:test";
import assert from "node:assert/strict";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
let liveApiAvailable = false;

before(async () => {
  try {
    const health = await fetch(`${apiBase}/health`);
    liveApiAvailable = health.status === 200;
  } catch {
    liveApiAvailable = false;
  }
});

test("reviewer adjudication template csv endpoint handles large limits", async (t) => {
  if (!liveApiAvailable) {
    t.skip(`Live API unavailable at ${apiBase}`);
    return;
  }
  const response = await fetch(`${apiBase}/admin/ingestion/reviewer-adjudication-template?realOnly=1&format=csv&limit=1200`);
  const body = await response.text();
  assert.equal(response.status, 200);
  const firstLine = String(body || "").split(/\r?\n/, 1)[0] || "";
  assert.ok(firstLine.includes("documentId"), "expected canonical adjudication CSV header");
  assert.ok(!firstLine.includes('{"error"'), "expected CSV header, not JSON error payload");
});

test("reviewer adjudication template json endpoint handles large limits", async (t) => {
  if (!liveApiAvailable) {
    t.skip(`Live API unavailable at ${apiBase}`);
    return;
  }
  const response = await fetch(`${apiBase}/admin/ingestion/reviewer-adjudication-template?realOnly=1&format=json&limit=1200`);
  const raw = await response.text();
  assert.equal(response.status, 200);
  const parsed = JSON.parse(raw);
  assert.ok(!parsed.error, "expected template payload, not endpoint error object");
  assert.ok(Array.isArray(parsed.rows), "expected template rows array");
});

test("reviewer export json/csv endpoints handle large limits", async (t) => {
  if (!liveApiAvailable) {
    t.skip(`Live API unavailable at ${apiBase}`);
    return;
  }
  const jsonResponse = await fetch(`${apiBase}/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=1200`);
  const jsonRaw = await jsonResponse.text();
  assert.equal(jsonResponse.status, 200);
  const jsonPayload = JSON.parse(jsonRaw);
  assert.ok(!jsonPayload.error, "expected reviewer export payload, not endpoint error object");
  assert.ok(Array.isArray(jsonPayload.rows), "expected reviewer export rows array");

  const csvResponse = await fetch(`${apiBase}/admin/ingestion/reviewer-export?realOnly=1&format=csv&limit=1200`);
  const csvBody = await csvResponse.text();
  assert.equal(csvResponse.status, 200);
  const csvHeader = String(csvBody || "").split(/\r?\n/, 1)[0] || "";
  assert.ok(csvHeader.includes("documentId"), "expected reviewer export csv header");
  assert.ok(!csvHeader.includes('{"error"'), "expected CSV header, not JSON error payload");
});
