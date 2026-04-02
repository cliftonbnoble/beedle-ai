import fs from "node:fs/promises";
import path from "node:path";
import { LIVE_SEARCH_QA_QUERIES } from "./retrieval-live-search-qa-utils.mjs";
import { buildRetrievalEvalReport } from "./retrieval-eval-utils.mjs";
import { summarizeEvalAsQa } from "./retrieval-batch-expansion-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const citationReportName = "retrieval-r27-citation-concentration-report.json";
const citationReportMdName = "retrieval-r27-citation-concentration-report.md";
const decompositionReportName = "retrieval-r27-safe-decomposition-report.json";
const decompositionReportMdName = "retrieval-r27-safe-decomposition-report.md";
const nextManifestName = "retrieval-r27-next-manifest.json";

const QUALITY_FLOOR = Number(process.env.RETRIEVAL_R27_MIN_QUALITY || "64.72");
const LOW_SIGNAL_CEILING = Number(process.env.RETRIEVAL_R27_LOW_SIGNAL_CEILING || "0.0167");
const CITATION_SHARE_CEILING = Number(process.env.RETRIEVAL_R27_CITATION_SHARE_CEILING || "0.1");

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCounts(obj) {
  return Object.entries(obj || {})
    .sort((a, b) => (b[1] === a[1] ? String(a[0]).localeCompare(String(b[0])) : b[1] - a[1]))
    .map(([key, count]) => ({ key, count }));
}

function normalizeChunkType(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function isLowSignalChunk(value) {
  const t = normalizeChunkType(value);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function combinationsTwo(values) {
  const out = [];
  for (let i = 0; i < values.length; i += 1) {
    for (let j = i + 1; j < values.length; j += 1) {
      out.push([values[i], values[j]]);
    }
  }
  return out;
}

function toQuerySpec() {
  return LIVE_SEARCH_QA_QUERIES.map((q) => ({ id: q.id, query: q.query, intent: q.intent }));
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const res = await fetch(url, init);
  const raw = await res.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON.`);
  }
  if (!res.ok) throw new Error(`Request failed (${res.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

async function resolveDocuments() {
  const url = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${docLimit}`;
  const payload = await fetchJson(url);
  return (payload.documents || [])
    .map((doc) => ({ id: doc.id, isLikelyFixture: Boolean(doc.isLikelyFixture) }))
    .filter((doc) => doc.id);
}

async function loadPreviews(documentRows) {
  const previews = [];
  for (const doc of documentRows || []) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=1`;
    const preview = await fetchJson(detailUrl);
    previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture });
  }
  return previews;
}

function computeCitationTopDocumentShareFromEval(evalReport) {
  const queryRows = (evalReport?.queryResults || []).filter((row) => /^citation_/.test(String(row?.queryId || "")));
  const shares = [];
  for (const row of queryRows) {
    const top = (row?.topResults || []).slice(0, 10);
    const counts = countBy(top.map((item) => item.documentId));
    const max = top.length ? Math.max(...Object.values(counts).map((n) => Number(n || 0))) : 0;
    shares.push(top.length ? Number((max / top.length).toFixed(4)) : 0);
  }
  if (!shares.length) return 0;
  return Number((shares.reduce((s, n) => s + n, 0) / shares.length).toFixed(4));
}

function computeLowSignalShareFromEval(evalReport) {
  const nonCitationRows = (evalReport?.queryResults || []).filter((row) => !/^citation_/.test(String(row?.queryId || "")));
  let total = 0;
  let low = 0;
  for (const row of nonCitationRows) {
    const top = (row?.topResults || []).slice(0, 10);
    total += top.length;
    low += top.filter((r) => isLowSignalChunk(r?.chunkType || r?.sectionLabel || "")).length;
  }
  return total ? Number((low / total).toFixed(4)) : 0;
}

function evaluateGate({ summary, citationTopDocumentShare, lowSignalShare }) {
  const checks = {
    qualityAboveFloor: Number(summary?.averageQualityScore || 0) >= QUALITY_FLOOR,
    citationTopDocumentConcentrationAtOrBelowCeiling: Number(citationTopDocumentShare || 0) <= CITATION_SHARE_CEILING,
    lowSignalStructuralShareAtOrBelowCeiling: Number(lowSignalShare || 0) <= LOW_SIGNAL_CEILING,
    outOfCorpusHitQueryCountZero: Number(summary?.outOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(summary?.zeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(summary?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(summary?.citationAnchorCoverageAverage || 0) === 1
  };
  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([name]) => name);
  return { passed: failures.length === 0, checks, failures };
}

function markdownForensics(report) {
  const lines = [];
  lines.push("# R27 Citation Concentration Forensics");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Dominance By Document");
  for (const row of report.dominanceByDocument || []) lines.push(`- ${row.key}: ${row.count}`);
  lines.push("");
  lines.push("## Dominance By Chunk Type");
  for (const row of report.dominanceByChunkType || []) lines.push(`- ${row.key}: ${row.count}`);
  lines.push("");
  lines.push("## Regression Drivers");
  for (const row of report.driverDocs || []) lines.push(`- ${row.documentId} | ${row.title} | top10Hits=${row.top10Hits}`);
  return `${lines.join("\n")}\n`;
}

function markdownDecomposition(report) {
  const lines = [];
  lines.push("# R27 Safe Decomposition (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Single-Doc Simulations");
  for (const row of report.singleDocSimulations || []) {
    lines.push(`- ${row.label}: score=${row.averageQualityScore}, citationTopShare=${row.citationTopDocumentShare}, lowSignalShare=${row.lowSignalStructuralShare}, recommendation=${row.activationRecommendation}`);
  }
  lines.push("");
  lines.push("## Two-Doc Simulations");
  for (const row of report.twoDocSimulations || []) {
    lines.push(`- ${row.label}: score=${row.averageQualityScore}, citationTopShare=${row.citationTopDocumentShare}, lowSignalShare=${row.lowSignalStructuralShare}, recommendation=${row.activationRecommendation}`);
  }
  lines.push("");
  lines.push("## Final Recommendation");
  lines.push(`- ${report.recommendation}`);
  if (report.recommendationReason) lines.push(`- reason: ${report.recommendationReason}`);
  return `${lines.join("\n")}\n`;
}

async function simulateForDocs({ documents, trustedBaselineIds, candidateDocIds }) {
  const admitted = unique([...trustedBaselineIds, ...candidateDocIds]);
  const evalReport = buildRetrievalEvalReport({
    apiBase,
    input: { mode: "r27_decomposition", trustedDocumentIds: admitted },
    documents,
    queries: toQuerySpec(),
    includeText: false,
    admittedDocumentIdsOverride: admitted
  });
  const qa = summarizeEvalAsQa(evalReport);
  const citationTopDocumentShare = computeCitationTopDocumentShareFromEval(evalReport);
  const lowSignalStructuralShare = computeLowSignalShareFromEval(evalReport);
  const gate = evaluateGate({
    summary: qa.summary,
    citationTopDocumentShare,
    lowSignalShare: lowSignalStructuralShare
  });
  return {
    docIds: candidateDocIds,
    averageQualityScore: Number(qa.summary.averageQualityScore || 0),
    citationTopDocumentShare,
    lowSignalStructuralShare,
    outOfCorpusHitQueryCount: Number(qa.summary.outOfCorpusHitQueryCount || 0),
    zeroTrustedResultQueryCount: Number(qa.summary.zeroTrustedResultQueryCount || 0),
    provenanceCompletenessAverage: Number(qa.summary.provenanceCompletenessAverage || 0),
    citationAnchorCoverageAverage: Number(qa.summary.citationAnchorCoverageAverage || 0),
    gate,
    activationRecommendation: gate.passed ? "yes" : "no"
  };
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r26ActivationReport, r26LiveQaReport, r25Manifest] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-r26-batch-activation-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-r26-batch-live-qa-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-next-safe-batch-r25-manifest.json"))
  ]);

  const r26Docs = unique((r26ActivationReport?.docsActivatedExact || []).map(String));
  if (r26Docs.length !== 3) {
    throw new Error(`Expected 3 R26 docs, got ${r26Docs.length}`);
  }
  const baselineTrustedIds = unique((r25Manifest?.baselineTrustedDocIds || []).map(String));
  if (!baselineTrustedIds.length) {
    throw new Error("Missing baseline trusted ids from R25 manifest.");
  }

  const citationQueryRows = (r26LiveQaReport?.afterQueryResults || []).filter((row) => /^citation_/.test(String(row?.queryId || "")));
  const topCitationHits = citationQueryRows.flatMap((row) => (row?.topResults || []).slice(0, 10));
  const inR26TopHits = topCitationHits.filter((row) => r26Docs.includes(String(row?.documentId || "")));

  const dominanceByDocument = sortCounts(countBy(inR26TopHits.map((row) => row.documentId)));
  const dominanceByChunk = sortCounts(countBy(inR26TopHits.map((row) => row.chunkId)));
  const dominanceByChunkType = sortCounts(countBy(inR26TopHits.map((row) => row.chunkType || row.sectionLabel || "")));
  const dominanceBySectionLabel = sortCounts(countBy(inR26TopHits.map((row) => row.sectionLabel || row.chunkType || "")));
  const dominanceByAnchor = sortCounts(
    countBy(
      inR26TopHits.map((row) => `${row.citationAnchorStart || row.citationAnchor || ""}::${row.citationAnchorEnd || row.citationAnchor || ""}`)
    )
  );

  const driverDocs = r26Docs.map((docId) => {
    const hits = inR26TopHits.filter((row) => String(row.documentId) === docId);
    const first = hits[0] || {};
    return {
      documentId: docId,
      title: String(first.title || ""),
      top10Hits: hits.length,
      chunkTypes: unique(hits.map((row) => row.chunkType || row.sectionLabel || "")),
      anchors: unique(hits.map((row) => `${row.citationAnchorStart || row.citationAnchor || ""}::${row.citationAnchorEnd || row.citationAnchor || ""}`))
    };
  });

  const forensicsReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      r26DocCount: r26Docs.length,
      citationQueryCount: citationQueryRows.length,
      citationTopResultCount: topCitationHits.length,
      r26CitationTopResultCount: inR26TopHits.length,
      citationTopDocumentShareMeasured: Number(r26ActivationReport?.hardGate?.measured?.citationTopDocumentShare || 0.2222),
      citationTopDocumentShareCeiling: CITATION_SHARE_CEILING
    },
    r26Docs,
    dominanceByDocument,
    dominanceByChunk,
    dominanceByChunkType,
    dominanceBySectionLabel,
    dominanceByAnchor,
    driverDocs,
    citationQueryRows: citationQueryRows.map((row) => ({
      queryId: row.queryId,
      query: row.query,
      topResults: (row.topResults || []).slice(0, 10)
    }))
  };

  const documents = await loadPreviews(await resolveDocuments());
  const singleDocSimulations = [];
  for (const docId of r26Docs) {
    const sim = await simulateForDocs({ documents, trustedBaselineIds: baselineTrustedIds, candidateDocIds: [docId] });
    singleDocSimulations.push({
      label: docId,
      ...sim
    });
  }

  const twoDocSimulations = [];
  for (const combo of combinationsTwo(r26Docs)) {
    const sim = await simulateForDocs({ documents, trustedBaselineIds: baselineTrustedIds, candidateDocIds: combo });
    twoDocSimulations.push({
      label: combo.join(" + "),
      ...sim
    });
  }

  singleDocSimulations.sort((a, b) => (b.averageQualityScore === a.averageQualityScore ? a.label.localeCompare(b.label) : b.averageQualityScore - a.averageQualityScore));
  twoDocSimulations.sort((a, b) => (b.averageQualityScore === a.averageQualityScore ? a.label.localeCompare(b.label) : b.averageQualityScore - a.averageQualityScore));

  const passingTwo = twoDocSimulations.filter((row) => row.gate.passed);
  const passingSingle = singleDocSimulations.filter((row) => row.gate.passed);

  let recommendation = "no_batch_safe";
  let recommendationReason = "No single-doc or two-doc decomposition passed all hard gates.";
  let safest = [];
  if (passingTwo.length) {
    recommendation = "safe_two_doc_candidate";
    safest = passingTwo[0].docIds;
    recommendationReason = "At least one two-doc decomposition passed all hard gates.";
  } else if (passingSingle.length) {
    recommendation = "safe_single_doc_candidate";
    safest = passingSingle[0].docIds;
    recommendationReason = "At least one single-doc decomposition passed all hard gates.";
  } else if (r26Docs.length) {
    recommendation = "requires_new_selection_logic";
    recommendationReason = "All decompositions failed at least one hard gate; add tighter citation-concentration pre-filtering.";
  }

  const decompositionReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      baselineTrustedDocCount: baselineTrustedIds.length,
      r26CandidateDocCount: r26Docs.length,
      singleDocPassingCount: passingSingle.length,
      twoDocPassingCount: passingTwo.length
    },
    thresholds: {
      qualityFloor: QUALITY_FLOOR,
      citationTopDocumentShareCeiling: CITATION_SHARE_CEILING,
      lowSignalStructuralShareCeiling: LOW_SIGNAL_CEILING
    },
    singleDocSimulations,
    twoDocSimulations,
    recommendation,
    recommendationReason,
    safestCandidateDocIds: safest
  };

  const nextManifest = {
    generatedAt: new Date().toISOString(),
    phase: "R27",
    readOnly: true,
    recommendation,
    recommendationReason,
    baselineTrustedDocIds: baselineTrustedIds,
    candidateDocIds: r26Docs,
    safestNextDocIds: safest
  };

  const paths = {
    citationJson: path.resolve(reportsDir, citationReportName),
    citationMd: path.resolve(reportsDir, citationReportMdName),
    decompJson: path.resolve(reportsDir, decompositionReportName),
    decompMd: path.resolve(reportsDir, decompositionReportMdName),
    manifest: path.resolve(reportsDir, nextManifestName)
  };

  await Promise.all([
    fs.writeFile(paths.citationJson, JSON.stringify(forensicsReport, null, 2)),
    fs.writeFile(paths.citationMd, markdownForensics(forensicsReport)),
    fs.writeFile(paths.decompJson, JSON.stringify(decompositionReport, null, 2)),
    fs.writeFile(paths.decompMd, markdownDecomposition(decompositionReport)),
    fs.writeFile(paths.manifest, JSON.stringify(nextManifest, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        recommendation,
        safestCandidateDocIds: safest,
        singleDocPassingCount: passingSingle.length,
        twoDocPassingCount: passingTwo.length
      },
      null,
      2
    )
  );
  console.log(`R27 citation forensics report written to ${paths.citationJson}`);
  console.log(`R27 decomposition report written to ${paths.decompJson}`);
  console.log(`R27 next manifest written to ${paths.manifest}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
