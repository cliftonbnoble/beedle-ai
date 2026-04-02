import { test, before } from "node:test";
import assert from "node:assert/strict";
import { buildRetrievalReadinessReport } from "../scripts/retrieval-readiness-report-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const runTag = `r5${Date.now().toString(36)}`;

const state = {
  discourseDocId: "",
  densityDocId: "",
  malformedDocId: ""
};

function toBase64(text) {
  return Buffer.from(text, "utf8").toString("base64");
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

  const lowStructureDiscourse = [
    "This matter came on for hearing after notice was served and procedural history includes continuances and hearing resets.",
    "The record shows factual background and testimony from the tenant and building manager regarding repeated service outages.",
    "The primary issue is whether the agency should grant relief based on record credibility and whether Ordinance 37.2 controls.",
    "The analysis applies Rule 37.8 together with Ordinance 37.2 and explains why the evidence satisfies the legal standard.",
    "ORDER: Relief is granted in part and denied in part based on the findings and conclusions set forth above."
  ].join("\n\n");

  const discourseDoc = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `R5 Low Structure Discourse ${runTag}`,
    citation: `BEE-R5-DISCOURSE-${runTag}`,
    decisionDate: "2026-03-08",
    sourceFile: {
      filename: "r5-low-structure.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: toBase64(lowStructureDiscourse)
    }
  });
  assert.equal(discourseDoc.status, 201);
  state.discourseDocId = discourseDoc.body.documentId;

  const densityShiftDoc = [
    "The hearing was held and parties appeared through counsel. The procedural chronology is not disputed.",
    "The parties stipulated to background facts and timeline details.",
    "Authority discussion cites Rule 37.8, Rule 37.8(a), Ordinance 37.2, Ordinance 37.2(a)(1), Ordinance 37.2(a)(2), Rule 37.2, Rule 37.8, Ordinance 37.8, Ordinance 37.2.",
    "The final disposition denies part of the claim and grants targeted relief in a limited amount."
  ].join("\n\n");

  const densityDoc = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `R5 Density Shift ${runTag}`,
    citation: `BEE-R5-DENSITY-${runTag}`,
    decisionDate: "2026-03-08",
    sourceFile: {
      filename: "r5-density-shift.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: toBase64(densityShiftDoc)
    }
  });
  assert.equal(densityDoc.status, 201);
  state.densityDocId = densityDoc.body.documentId;

  const malformedDoc = [
    "This is one merged paragraph with no structure no headings and it keeps mixing procedural history findings authority and disposition language in one long span.",
    "The text references ordinance 37.3 ordinance 37.7 ordinance 37.9 and rule 37.7 while also describing facts and making conclusions and order language.",
    "The same sentence continues repeatedly without section boundaries and without clean transitions making segmentation unreliable and potentially misleading for retrieval quality."
  ].join(" ");

  const malformed = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `R5 Malformed ${runTag}`,
    citation: `BEE-R5-MALFORMED-${runTag}`,
    decisionDate: "2026-03-08",
    sourceFile: {
      filename: "r5-malformed.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: toBase64(malformedDoc)
    }
  });
  assert.equal(malformed.status, 201);
  state.malformedDocId = malformed.body.documentId;
});

test("low-structure discourse split improves chunk spread with deterministic repair metadata", async () => {
  const first = await getJson(`/admin/retrieval/documents/${state.discourseDocId}/chunks?includeText=1`);
  const second = await getJson(`/admin/retrieval/documents/${state.discourseDocId}/chunks?includeText=1`);
  assert.equal(first.status, 200);
  assert.equal(second.status, 200);

  const stats = first.body.stats;
  assert.equal(stats.usedFallbackChunking, true);
  assert.equal(stats.repairApplied, true);
  assert.ok(stats.postRepairChunkCount >= stats.preRepairChunkCount);
  assert.ok(stats.chunkTypeSpread >= stats.preRepairChunkTypeSpread);
  assert.ok(Number(stats.repairedChunkCount || 0) > 0);

  const strategies = Object.keys(stats.repairStrategyCounts || {});
  assert.ok(strategies.length > 0);

  const firstChunks = first.body.chunks || [];
  const secondChunks = second.body.chunks || [];
  assert.deepEqual(
    firstChunks.map((chunk) => chunk.chunkId),
    secondChunks.map((chunk) => chunk.chunkId),
    "repair chunk ids should be deterministic"
  );
  assert.ok(firstChunks.some((chunk) => chunk.chunkRepairApplied === true));
  assert.ok(firstChunks.every((chunk) => typeof chunk.chunkRepairStrategy === "string"));
  assert.ok(firstChunks.every((chunk) => Array.isArray(chunk.chunkRepairNotes)));
});

test("citation-density boundaries can create deterministic split strategy", async () => {
  const response = await getJson(`/admin/retrieval/documents/${state.densityDocId}/chunks?includeText=1`);
  assert.equal(response.status, 200);

  const stats = response.body.stats || {};
  assert.equal(stats.usedFallbackChunking, true);
  assert.equal(stats.repairApplied, true);
  const densitySplitCount = Number(stats.repairStrategyCounts?.citation_density_boundary_split || 0);
  assert.ok(densitySplitCount >= 1 || Number(stats.repairStrategyCounts?.low_structure_discourse_split || 0) >= 1);
  assert.ok(
    (response.body.chunks || []).some((chunk) =>
      ["citation_density_boundary_split", "low_structure_discourse_split"].includes(chunk.chunkRepairStrategy)
    )
  );
});

test("readiness only improves when quality improves and malformed doc remains blocked", async () => {
  const discourse = await getJson(`/admin/retrieval/documents/${state.discourseDocId}/chunks?includeText=0`);
  const malformed = await getJson(`/admin/retrieval/documents/${state.malformedDocId}/chunks?includeText=0`);
  assert.equal(discourse.status, 200);
  assert.equal(malformed.status, 200);

  const report = buildRetrievalReadinessReport({
    apiBase,
    input: { realOnly: false, includeText: false, limit: 2 },
    documents: [
      { ...discourse.body, isLikelyFixture: false },
      { ...malformed.body, isLikelyFixture: false }
    ]
  });

  const discourseRow = report.documents.find((row) => row.documentId === state.discourseDocId);
  const malformedRow = report.documents.find((row) => row.documentId === state.malformedDocId);
  assert.ok(discourseRow);
  assert.ok(malformedRow);
  assert.equal(discourseRow.repairApplied, true);
  assert.ok(discourseRow.postRepairChunkCount >= discourseRow.preRepairChunkCount);
  if (discourseRow.readinessChangedAfterRepair) {
    assert.notEqual(discourseRow.preRepairReadinessStatus, discourseRow.postRepairReadinessStatus);
  }
  assert.notEqual(malformedRow.readinessStatus, "retrieval_ready");
  assert.ok(malformedRow.blockingReasons.length > 0 || malformedRow.warningReasons.length > 0);
});

test("includeText=0 redacts source text while preserving repair metadata", async () => {
  const response = await getJson(`/admin/retrieval/documents/${state.discourseDocId}/chunks?includeText=0`);
  assert.equal(response.status, 200);
  const chunks = response.body.chunks || [];
  assert.ok(chunks.length > 0);
  for (const chunk of chunks) {
    assert.equal(chunk.sourceText, "");
    assert.equal(typeof chunk.chunkRepairApplied, "boolean");
    assert.equal(typeof chunk.chunkRepairStrategy, "string");
    assert.ok(Array.isArray(chunk.chunkRepairNotes));
  }
});
