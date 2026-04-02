import fs from "node:fs/promises";
import path from "node:path";
import { LIVE_SEARCH_QA_QUERIES } from "./retrieval-live-search-qa-utils.mjs";
import { buildRetrievalEvalReport } from "./retrieval-eval-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const outJsonName = "retrieval-r29-citation-parity-report.json";
const outMdName = "retrieval-r29-citation-parity-report.md";
const CITATION_SHARE_CEILING = Number(process.env.RETRIEVAL_R29_MAX_CITATION_TOP_DOC_SHARE || "0.1");

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function uniqueSorted(values) {
  return unique(values).sort((a, b) => String(a).localeCompare(String(b)));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCounts(counts, valueKey = "key") {
  return Object.entries(counts || {})
    .sort((a, b) => (b[1] === a[1] ? String(a[0]).localeCompare(String(b[0])) : b[1] - a[1]))
    .map(([key, count]) => ({ [valueKey]: key, count }));
}

function mean(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, n) => sum + Number(n || 0), 0) / values.length).toFixed(4));
}

function repetitionCount(values) {
  const counts = Object.values(countBy(values)).map((n) => Number(n || 0));
  return counts.reduce((sum, n) => sum + Math.max(0, n - 1), 0);
}

function topDocumentShare(rows) {
  if (!rows.length) return 0;
  const max = Math.max(...Object.values(countBy(rows.map((row) => row.documentId))).map((n) => Number(n || 0)));
  return Number((max / rows.length).toFixed(4));
}

function toAnchorKey(row) {
  if (row.citationAnchorStart || row.citationAnchorEnd) {
    return `${row.citationAnchorStart || ""}::${row.citationAnchorEnd || row.citationAnchorStart || ""}`;
  }
  return String(row.citationAnchor || "");
}

function referenceFamiliesForChunk(chunk) {
  const families = uniqueSorted([
    ...(chunk?.canonicalOrdinanceReferences || []),
    ...(chunk?.canonicalRulesReferences || []),
    ...(chunk?.canonicalIndexCodes || []),
    ...(chunk?.citationFamilies || []),
    ...(chunk?.ordinanceReferences || []),
    ...(chunk?.rulesReferences || [])
  ].map((v) => String(v || "").trim()).filter(Boolean));
  return families.length ? families : ["<none>"];
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON.`);
  }
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

async function resolveAllDocs() {
  const url = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${docLimit}`;
  const payload = await fetchJson(url);
  return payload.documents || [];
}

async function fetchPreview(documentId) {
  return fetchJson(`${apiBase}/admin/retrieval/documents/${documentId}/chunks?includeText=1`);
}

function querySpecs() {
  return LIVE_SEARCH_QA_QUERIES.map((q) => ({
    id: q.id,
    query: q.query,
    intent: q.intent
  }));
}

function topRows(rows, n = 10) {
  return (rows || []).slice(0, n);
}

function withChunkMetadata(rows, chunkById) {
  return (rows || []).map((row) => {
    const chunk = chunkById.get(String(row.chunkId || "")) || null;
    const families = referenceFamiliesForChunk(chunk);
    return {
      ...row,
      sectionLabel: row.sectionLabel || chunk?.sectionLabel || "",
      citationAnchorStart: row.citationAnchorStart || chunk?.citationAnchorStart || row.citationAnchor || "",
      citationAnchorEnd: row.citationAnchorEnd || chunk?.citationAnchorEnd || row.citationAnchor || "",
      referenceFamilies: families
    };
  });
}

function perQueryStats(rows, chunkById) {
  const enriched = withChunkMetadata(rows, chunkById);
  const top = topRows(enriched, 10);
  const docIds = top.map((r) => r.documentId);
  const chunkIds = top.map((r) => r.chunkId);
  const anchorKeys = top.map(toAnchorKey).filter(Boolean);
  const familyKeys = top.map((r) => (r.referenceFamilies || ["<none>"]).join("|"));
  return {
    topRows: top,
    topDocumentShare: topDocumentShare(top),
    sameDocumentRepetitionCount: repetitionCount(docIds),
    sameAnchorRepetitionCount: repetitionCount(anchorKeys),
    sameReferenceFamilyRepetitionCount: repetitionCount(familyKeys),
    dominantDocuments: sortCounts(countBy(docIds), "documentId").slice(0, 5),
    dominantChunks: sortCounts(countBy(chunkIds), "chunkId").slice(0, 5),
    dominantCitationAnchors: sortCounts(countBy(anchorKeys), "citationAnchor").slice(0, 5),
    dominantReferenceFamilies: sortCounts(countBy(familyKeys), "referenceFamily").slice(0, 5)
  };
}

function classifyRootCause(queryComparisons) {
  const docHeavy = queryComparisons.filter((q) => q.live.sameDocumentRepetitionCount >= q.live.sameAnchorRepetitionCount && q.live.sameDocumentRepetitionCount >= q.live.sameReferenceFamilyRepetitionCount).length;
  const anchorHeavy = queryComparisons.filter((q) => q.live.sameAnchorRepetitionCount > q.live.sameDocumentRepetitionCount && q.live.sameAnchorRepetitionCount >= q.live.sameReferenceFamilyRepetitionCount).length;
  const familyHeavy = queryComparisons.filter((q) => q.live.sameReferenceFamilyRepetitionCount > q.live.sameDocumentRepetitionCount && q.live.sameReferenceFamilyRepetitionCount > q.live.sameAnchorRepetitionCount).length;
  const shareDrift = queryComparisons.some((q) => Math.abs(Number(q.live.topDocumentShare || 0) - Number(q.sim.topDocumentShare || 0)) >= 0.05);

  const types = [];
  if (docHeavy > 0) types.push("same_document_repetition");
  if (anchorHeavy > 0) types.push("same_anchor_repetition");
  if (familyHeavy > 0) types.push("same_reference_family_repetition");
  if (shareDrift) types.push("live_sim_rerank_shape_drift");

  if (!types.length) types.push("no_material_concentration_root_cause_found");
  return types;
}

function recommendedActions(rootCauseTypes) {
  const actions = [];
  if (rootCauseTypes.includes("same_document_repetition")) {
    actions.push({
      action: "citation_intent_per_document_cap",
      details: "For citation intents only, cap post-diversification top-10 to 1 result per document before fill."
    });
  }
  if (rootCauseTypes.includes("same_anchor_repetition")) {
    actions.push({
      action: "citation_anchor_deduplication",
      details: "For citation intents only, dedupe repeated citationAnchorStart/citationAnchorEnd pairs within top-10."
    });
  }
  if (rootCauseTypes.includes("same_reference_family_repetition")) {
    actions.push({
      action: "citation_family_repeat_penalty",
      details: "Add deterministic penalty when same normalized reference family repeats in citation-intent rerank."
    });
  }
  if (rootCauseTypes.includes("live_sim_rerank_shape_drift")) {
    actions.push({
      action: "parity_harness_alignment",
      details: "Align dry-run simulation rerank penalties/caps with live runtime citation-intent path to reduce parity drift."
    });
  }
  return actions;
}

function markdown(report) {
  const lines = [];
  lines.push("# R29 Citation Parity Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Failing Citation Queries");
  for (const q of report.failingCitationQueries || []) {
    lines.push(`- ${q.queryId} | liveTopDocumentShare=${q.liveTopDocumentShare} | simTopDocumentShare=${q.simTopDocumentShare}`);
  }
  if (!(report.failingCitationQueries || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Root Cause Classification");
  for (const rc of report.rootCauseClassification || []) lines.push(`- ${rc}`);
  lines.push("");
  lines.push("## Recommended Runtime Tuning Actions");
  for (const action of report.recommendedRuntimeTuningActions || []) {
    lines.push(`- ${action.action}: ${action.details}`);
  }
  if (!(report.recommendedRuntimeTuningActions || []).length) lines.push("- none");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r28LiveQa, r27Manifest] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-r28-batch-live-qa-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-r27-next-manifest.json"))
  ]);

  const candidateDocIds = uniqueSorted((r27Manifest?.safestNextDocIds || []).map(String));
  const baselineTrustedDocIds = uniqueSorted((r27Manifest?.baselineTrustedDocIds || []).map(String));
  if (candidateDocIds.length !== 2) {
    throw new Error(`Expected 2 candidate docs in R27 manifest; got ${candidateDocIds.length}`);
  }
  if (!baselineTrustedDocIds.length) {
    throw new Error("Missing baselineTrustedDocIds in R27 manifest.");
  }

  const allDocsRaw = await resolveAllDocs();
  const neededDocIds = new Set([...baselineTrustedDocIds, ...candidateDocIds]);
  const neededDocsMeta = allDocsRaw.filter((d) => neededDocIds.has(String(d.id || "")));
  const previews = await Promise.all(
    neededDocsMeta.map(async (doc) => {
      const preview = await fetchPreview(doc.id);
      return { ...preview, isLikelyFixture: Boolean(doc.isLikelyFixture) };
    })
  );

  const chunkById = new Map();
  for (const preview of previews) {
    for (const chunk of preview.chunks || []) {
      if (!chunk?.chunkId) continue;
      chunkById.set(String(chunk.chunkId), chunk);
    }
  }

  const simEval = buildRetrievalEvalReport({
    apiBase,
    input: { mode: "r29_parity_simulation", trustedDocumentIds: [...baselineTrustedDocIds, ...candidateDocIds] },
    documents: previews,
    queries: querySpecs(),
    includeText: false,
    admittedDocumentIdsOverride: uniqueSorted([...baselineTrustedDocIds, ...candidateDocIds])
  });

  const liveCitationRows = (r28LiveQa?.afterQueryResults || []).filter((row) => /^citation_/.test(String(row?.queryId || "")));
  const simCitationRows = (simEval?.queryResults || []).filter((row) => /^citation_/.test(String(row?.queryId || "")));

  const comparisons = liveCitationRows.map((liveRow) => {
    const simRow = simCitationRows.find((row) => String(row.queryId) === String(liveRow.queryId)) || null;
    const liveStats = perQueryStats(liveRow.topResults || [], chunkById);
    const simStats = perQueryStats(simRow?.topResults || [], chunkById);
    return {
      queryId: liveRow.queryId,
      query: liveRow.query,
      live: {
        topDocumentShare: liveStats.topDocumentShare,
        sameDocumentRepetitionCount: liveStats.sameDocumentRepetitionCount,
        sameAnchorRepetitionCount: liveStats.sameAnchorRepetitionCount,
        sameReferenceFamilyRepetitionCount: liveStats.sameReferenceFamilyRepetitionCount,
        dominantDocuments: liveStats.dominantDocuments,
        dominantChunks: liveStats.dominantChunks,
        dominantCitationAnchors: liveStats.dominantCitationAnchors,
        dominantReferenceFamilies: liveStats.dominantReferenceFamilies
      },
      sim: {
        topDocumentShare: simStats.topDocumentShare,
        sameDocumentRepetitionCount: simStats.sameDocumentRepetitionCount,
        sameAnchorRepetitionCount: simStats.sameAnchorRepetitionCount,
        sameReferenceFamilyRepetitionCount: simStats.sameReferenceFamilyRepetitionCount,
        dominantDocuments: simStats.dominantDocuments,
        dominantChunks: simStats.dominantChunks,
        dominantCitationAnchors: simStats.dominantCitationAnchors,
        dominantReferenceFamilies: simStats.dominantReferenceFamilies
      },
      deltas: {
        topDocumentShare: Number((liveStats.topDocumentShare - simStats.topDocumentShare).toFixed(4)),
        sameDocumentRepetitionCount: liveStats.sameDocumentRepetitionCount - simStats.sameDocumentRepetitionCount,
        sameAnchorRepetitionCount: liveStats.sameAnchorRepetitionCount - simStats.sameAnchorRepetitionCount,
        sameReferenceFamilyRepetitionCount: liveStats.sameReferenceFamilyRepetitionCount - simStats.sameReferenceFamilyRepetitionCount
      }
    };
  });

  const failingCitationQueries = comparisons
    .filter((row) => Number(row.live.topDocumentShare || 0) > CITATION_SHARE_CEILING)
    .map((row) => ({
      queryId: row.queryId,
      query: row.query,
      liveTopDocumentShare: row.live.topDocumentShare,
      simTopDocumentShare: row.sim.topDocumentShare
    }));

  const liveFlattenedTop = liveCitationRows.flatMap((row) => topRows(withChunkMetadata(row.topResults || [], chunkById), 10));
  const rootCauseClassification = classifyRootCause(comparisons);
  const parityHeld = comparisons.every((row) => Math.abs(Number(row.deltas.topDocumentShare || 0)) < 0.05);

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      citationQueryCount: liveCitationRows.length,
      candidateDocCount: candidateDocIds.length,
      baselineTrustedDocCount: baselineTrustedDocIds.length,
      citationTopDocumentShareCeiling: CITATION_SHARE_CEILING,
      failingCitationQueryCount: failingCitationQueries.length,
      liveCitationTopDocumentShareAverage: mean(comparisons.map((r) => r.live.topDocumentShare)),
      simCitationTopDocumentShareAverage: mean(comparisons.map((r) => r.sim.topDocumentShare)),
      liveVsSimParity: parityHeld ? "held" : "drifted"
    },
    failingCitationQueries,
    perQueryCitationComparison: comparisons,
    liveVsSimTopDocumentShare: comparisons.map((row) => ({
      queryId: row.queryId,
      live: row.live.topDocumentShare,
      sim: row.sim.topDocumentShare,
      delta: row.deltas.topDocumentShare
    })),
    liveVsSimAnchorRepetition: comparisons.map((row) => ({
      queryId: row.queryId,
      live: row.live.sameAnchorRepetitionCount,
      sim: row.sim.sameAnchorRepetitionCount,
      delta: row.deltas.sameAnchorRepetitionCount
    })),
    liveVsSimReferenceFamilyRepetition: comparisons.map((row) => ({
      queryId: row.queryId,
      live: row.live.sameReferenceFamilyRepetitionCount,
      sim: row.sim.sameReferenceFamilyRepetitionCount,
      delta: row.deltas.sameReferenceFamilyRepetitionCount
    })),
    dominantDocuments: sortCounts(countBy(liveFlattenedTop.map((r) => r.documentId)), "documentId").slice(0, 10),
    dominantChunks: sortCounts(countBy(liveFlattenedTop.map((r) => r.chunkId)), "chunkId").slice(0, 10),
    dominantCitationAnchors: sortCounts(countBy(liveFlattenedTop.map((r) => toAnchorKey(r))), "citationAnchor").slice(0, 10),
    dominantReferenceFamilies: sortCounts(
      countBy(liveFlattenedTop.map((r) => (r.referenceFamilies || ["<none>"]).join("|"))),
      "referenceFamily"
    ).slice(0, 10),
    rootCauseClassification,
    recommendedRuntimeTuningActions: recommendedActions(rootCauseClassification)
  };

  const jsonPath = path.resolve(reportsDir, outJsonName);
  const mdPath = path.resolve(reportsDir, outMdName);
  await Promise.all([
    fs.writeFile(jsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, markdown(report))
  ]);

  console.log(
    JSON.stringify(
      {
        failingCitationQueryCount: report.summary.failingCitationQueryCount,
        liveVsSimParity: report.summary.liveVsSimParity,
        rootCauseClassification: report.rootCauseClassification
      },
      null,
      2
    )
  );
  console.log(`R29 citation parity report written to ${jsonPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
