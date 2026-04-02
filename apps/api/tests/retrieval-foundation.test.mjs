import { test, before } from "node:test";
import assert from "node:assert/strict";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const runTag = `retrieval${Date.now().toString(36)}`;

const state = {
  headingDocId: "",
  fallbackDocId: "",
  messyHeadingDocId: ""
};

function toBase64(text) {
  return Buffer.from(text, "utf8").toString("base64");
}

function longParagraph(seed, repeat = 8, extras = "") {
  return Array.from({ length: repeat }, (_, idx) =>
    `${seed} paragraph sentence ${idx + 1}. ${extras} This decision discusses ordinance 37.2 and rule 37.8 and explains detailed reasoning for judicial retrieval quality.`
  ).join(" ");
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

  const headingDocText = [
    "CAPTION",
    `CASE NO. R3-${runTag} Before the Office of Administrative Hearings`,
    "",
    "PROCEDURAL HISTORY",
    longParagraph("Procedural history", 6, "The hearing was continued twice."),
    "",
    "FACTS",
    longParagraph("Facts and background", 6, "The testimony established repeated service reduction allegations."),
    "",
    "ISSUES PRESENTED",
    longParagraph("Issue statement", 5, "The core issue is whether the record supports relief."),
    "",
    "CONCLUSIONS OF LAW",
    longParagraph("Authority discussion", 7, "Rule 37.8 and Ordinance 37.2 were discussed as legal authority."),
    "",
    "DISCUSSION",
    longParagraph("Analysis and reasoning", 7, "Because the findings are consistent, partial relief is appropriate."),
    "",
    "FINDINGS OF FACT",
    longParagraph("Findings section", 5, "The fact finder found multiple violations proved."),
    "",
    "DISPOSITION",
    longParagraph("Order and disposition", 5, "The petition is granted in part and denied in part.")
  ].join("\n\n");

  const headingDoc = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Retrieval Heading Rich ${runTag}`,
    citation: `BEE-R3-HEADING-${runTag}`,
    decisionDate: "2026-03-08",
    sourceFile: {
      filename: "retrieval-heading-rich.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: toBase64(headingDocText)
    }
  });
  assert.equal(headingDoc.status, 201);
  state.headingDocId = headingDoc.body.documentId;

  const fallbackText = [
    longParagraph("No heading block one", 7, "The issue in dispute concerns building code compliance and timing."),
    "",
    longParagraph("No heading block two", 7, "The procedural timeline includes notice and hearing continuances."),
    "",
    longParagraph("No heading block three", 7, "The final order granted partial relief based on analysis and findings."),
    "",
    longParagraph("No heading block four", 7, "The authority mentions are sparse and not dominant.")
  ].join("\n\n");

  const fallbackDoc = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Retrieval Fallback ${runTag}`,
    citation: `BEE-R3-FALLBACK-${runTag}`,
    decisionDate: "2026-03-08",
    sourceFile: {
      filename: "retrieval-fallback.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: toBase64(fallbackText)
    }
  });
  assert.equal(fallbackDoc.status, 201);
  state.fallbackDocId = fallbackDoc.body.documentId;

  const messyHeadingText = [
    "APPEARANCES",
    longParagraph("Caption/parties", 3, "Petitioner and respondent appeared through counsel."),
    "",
    "HISTORY & BACKGROUND",
    longParagraph("Messy history", 4, "Background facts and procedural history are intertwined."),
    "",
    "QUESTIONS PRESENTED",
    longParagraph("Messy issue", 3, "Whether the ordinance should be interpreted strictly."),
    "",
    "ORDER / DECISION",
    longParagraph("Messy disposition", 3, "Relief denied after analysis.")
  ].join("\n\n");

  const messyDoc = await postJson("/ingest/decision", {
    jurisdiction: "City of Beedle",
    title: `Retrieval Messy Headings ${runTag}`,
    citation: `BEE-R3-MESSY-${runTag}`,
    decisionDate: "2026-03-08",
    sourceFile: {
      filename: "retrieval-messy.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64: toBase64(messyHeadingText)
    }
  });
  assert.equal(messyDoc.status, 201);
  state.messyHeadingDocId = messyDoc.body.documentId;
});

test("multi-section real-ish decision yields diverse chunk typing and audit fields", async () => {
  const first = await getJson(`/admin/retrieval/documents/${state.headingDocId}/chunks?includeText=1`);
  const second = await getJson(`/admin/retrieval/documents/${state.headingDocId}/chunks?includeText=1`);

  assert.equal(first.status, 200);
  assert.equal(second.status, 200);

  const firstChunks = first.body.chunks || [];
  const secondChunks = second.body.chunks || [];

  assert.ok(firstChunks.length > 4, "expected multiple chunks");
  assert.equal(first.body.stats.usedFallbackChunking, false);
  assert.ok((first.body.stats.headingCount || 0) >= 4);
  assert.ok((first.body.stats.chunkTypeSpread || 0) >= 4);

  assert.deepEqual(
    firstChunks.map((row) => row.chunkId),
    secondChunks.map((row) => row.chunkId),
    "chunk ids should remain deterministic"
  );

  for (const chunk of firstChunks) {
    assert.ok(chunk.chunkType);
    assert.ok(typeof chunk.chunkTypeConfidence === "number");
    assert.ok(chunk.chunkClassificationReason);
    assert.ok(typeof chunk.retrievalPriority === "string");
    assert.ok(chunk.retrievalPriorityReason);
    assert.ok(typeof chunk.hasCitationAnchorCoverage === "boolean");
    assert.ok(typeof chunk.hasCanonicalReferenceAlignment === "boolean");
    assert.ok(Array.isArray(chunk.segmentQualityFlags));
    assert.ok(chunk.questionFitSignals);
    assert.equal(typeof chunk.questionFitSignals.fitsIssueQuery, "boolean");
    assert.equal(typeof chunk.questionFitSignals.fitsAuthorityQuery, "boolean");
    assert.equal(typeof chunk.questionFitSignals.fitsFindingsQuery, "boolean");
    assert.equal(typeof chunk.questionFitSignals.fitsDispositionQuery, "boolean");
    assert.ok(Array.isArray(chunk.headingPath));
  }
});

test("messy heading variants map to normalized section types", async () => {
  const response = await getJson(`/admin/retrieval/documents/${state.messyHeadingDocId}/chunks?includeText=1`);
  assert.equal(response.status, 200);
  const chunks = response.body.chunks || [];
  const chunkTypes = new Set(chunks.map((chunk) => chunk.chunkType));
  assert.ok(chunkTypes.has("caption_title"));
  assert.ok(chunkTypes.has("issue_statement"));
  assert.ok(chunks.some((chunk) => (chunk.headingPath || []).includes("caption_parties")));
  assert.ok(chunks.some((chunk) => (chunk.headingPath || []).includes("issue_statement")));
  assert.ok(chunks.some((chunk) => chunk.containsDispositionLanguage || chunk.questionFitSignals?.fitsDispositionQuery === true));
});

test("fallback chunking produces non-authority types when language supports it", async () => {
  const response = await getJson(`/admin/retrieval/documents/${state.fallbackDocId}/chunks?includeText=1`);
  assert.equal(response.status, 200);

  assert.equal(response.body.stats.usedFallbackChunking, true);
  const chunks = response.body.chunks || [];
  assert.ok(chunks.length > 2);

  const nonAuthority = chunks.filter((chunk) => chunk.chunkType !== "authority_discussion");
  assert.ok(nonAuthority.length > 0, "fallback should not collapse to authority only");
  assert.ok(chunks.some((chunk) => chunk.chunkClassificationReason === "paragraph_window_fallback"));
});

test("canonical reference alignment and segment quality flags are deterministic", async () => {
  const first = await getJson(`/admin/retrieval/documents/${state.headingDocId}/chunks?includeText=1`);
  const second = await getJson(`/admin/retrieval/documents/${state.headingDocId}/chunks?includeText=1`);

  const one = first.body.chunks || [];
  const two = second.body.chunks || [];
  assert.equal(one.length, two.length);

  for (let i = 0; i < one.length; i += 1) {
    assert.deepEqual(one[i].canonicalOrdinanceReferences, two[i].canonicalOrdinanceReferences);
    assert.deepEqual(one[i].canonicalRulesReferences, two[i].canonicalRulesReferences);
    assert.deepEqual(one[i].canonicalIndexCodes, two[i].canonicalIndexCodes);
    assert.deepEqual(one[i].segmentQualityFlags, two[i].segmentQualityFlags);
  }
});

test("includeText=0 redacts text while preserving retrieval audit metadata", async () => {
  const response = await getJson(`/admin/retrieval/documents/${state.headingDocId}/chunks?includeText=0`);
  assert.equal(response.status, 200);
  const chunks = response.body.chunks || [];
  assert.ok(chunks.length > 0);
  for (const chunk of chunks) {
    assert.equal(chunk.sourceText, "");
    assert.ok(chunk.chunkId);
    assert.ok(chunk.chunkClassificationReason);
    assert.ok(chunk.retrievalPriority);
    assert.ok(Array.isArray(chunk.segmentQualityFlags));
  }
});
