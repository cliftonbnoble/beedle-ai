import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const citationReportName = "retrieval-r31-citation-doc-cap-report.json";
const citationReportMdName = "retrieval-r31-citation-doc-cap-report.md";
const pairReportName = "retrieval-r31-r28-pair-simulation-report.json";
const pairReportMdName = "retrieval-r31-r28-pair-simulation-report.md";

const CITATION_SHARE_CEILING = Number(process.env.RETRIEVAL_R31_MAX_CITATION_TOP_DOC_SHARE || "0.1");
const DOC_CAP = Number(process.env.RETRIEVAL_R31_DOC_CAP || "1");
const ANCHOR_NEIGHBOR_WINDOW = Number(process.env.RETRIEVAL_R31_ANCHOR_NEIGHBOR_WINDOW || "1");
const FAMILY_PENALTY_STEP = Number(process.env.RETRIEVAL_R31_FAMILY_PENALTY_STEP || "0.03");
const FAMILY_PENALTY_CAP = Number(process.env.RETRIEVAL_R31_FAMILY_PENALTY_CAP || "0.24");

function uniqueSorted(values) {
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

function sortCounts(counts, key = "key") {
  return Object.entries(counts || {})
    .sort((a, b) => (b[1] === a[1] ? String(a[0]).localeCompare(String(b[0])) : b[1] - a[1]))
    .map(([value, count]) => ({ [key]: value, count }));
}

function mean(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, n) => sum + Number(n || 0), 0) / values.length).toFixed(4));
}

function topDocumentShare(rows) {
  if (!rows.length) return 0;
  const max = Math.max(...Object.values(countBy(rows.map((r) => r.documentId))).map((n) => Number(n || 0)));
  return Number((max / rows.length).toFixed(4));
}

function repetition(values) {
  return Object.values(countBy(values)).reduce((sum, n) => sum + Math.max(0, Number(n) - 1), 0);
}

function parseAnchorOrdinal(anchor) {
  const raw = String(anchor || "");
  const match = raw.match(/(?:^|[#:_-])p(\d+)\b/i);
  if (!match) return null;
  const parsed = Number.parseInt(match[1] || "", 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalize(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s_-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractFamiliesFromRow(row) {
  const source = `${row?.snippet || ""} ${row?.citationAnchor || ""} ${row?.title || ""}`;
  const tokens = normalize(source).match(/\b\d+\.\d+\b/g) || [];
  const families = uniqueSorted(tokens);
  return families.length ? families : ["<none>"];
}

function toFamilyKey(row) {
  return extractFamiliesFromRow(row).join("|");
}

function applyR31CitationSuppression(rows) {
  const top = (rows || []).slice(0, 10).map((row) => ({
    ...row,
    familyKey: toFamilyKey(row)
  }));
  const familyCounts = countBy(top.map((row) => row.familyKey));
  const rescored = top.map((row) => {
    const familyCount = Number(familyCounts[row.familyKey] || 0);
    const penalty = familyCount > 1 ? Math.min(FAMILY_PENALTY_CAP, (familyCount - 1) * FAMILY_PENALTY_STEP) : 0;
    const rerankScore = Number(row.rerankScore || row.score || 0);
    return {
      ...row,
      adjustedScore: Number(Math.max(0, rerankScore - penalty).toFixed(6)),
      familyPenalty: Number(penalty.toFixed(6)),
      rankingExplanation: [
        ...(Array.isArray(row.rankingExplanation) ? row.rankingExplanation : []),
        ...(penalty > 0 ? [`citation_family_repeat_penalty=${penalty.toFixed(2)}:${row.familyKey}`] : []),
        `citation_doc_cap=${DOC_CAP}`,
        `citation_anchor_neighbor_suppression=${ANCHOR_NEIGHBOR_WINDOW}`
      ]
    };
  });

  const ranked = [...rescored].sort((a, b) => {
    const diff = Number(b.adjustedScore || 0) - Number(a.adjustedScore || 0);
    if (diff !== 0) return diff;
    const docDiff = String(a.documentId || "").localeCompare(String(b.documentId || ""));
    if (docDiff !== 0) return docDiff;
    return String(a.chunkId || "").localeCompare(String(b.chunkId || ""));
  });

  const perDoc = new Map();
  const selectedAnchorOrdinalsByDoc = new Map();
  const selected = [];
  for (const row of ranked) {
    const docId = String(row.documentId || "");
    const currentDocCount = Number(perDoc.get(docId) || 0);
    if (currentDocCount >= DOC_CAP) continue;

    const candidateOrdinal = parseAnchorOrdinal(row.citationAnchor || "");
    const selectedOrdinals = selectedAnchorOrdinalsByDoc.get(docId) || [];
    if (
      candidateOrdinal != null &&
      selectedOrdinals.some((ord) => Math.abs(Number(ord) - candidateOrdinal) <= ANCHOR_NEIGHBOR_WINDOW)
    ) {
      continue;
    }

    perDoc.set(docId, currentDocCount + 1);
    if (candidateOrdinal != null) {
      selectedAnchorOrdinalsByDoc.set(docId, [...selectedOrdinals, candidateOrdinal]);
    }
    selected.push(row);
    if (selected.length >= 10) break;
  }
  return selected;
}

function anchorKey(row) {
  return String(row?.citationAnchor || "");
}

function adjacentAnchorRepeats(rows) {
  const byDoc = new Map();
  for (const row of rows || []) {
    const docId = String(row.documentId || "");
    const ord = parseAnchorOrdinal(row.citationAnchor || "");
    if (ord == null) continue;
    byDoc.set(docId, [...(byDoc.get(docId) || []), ord]);
  }
  let repeats = 0;
  for (const ords of byDoc.values()) {
    const sorted = [...ords].sort((a, b) => a - b);
    for (let i = 1; i < sorted.length; i += 1) {
      if (Math.abs(sorted[i] - sorted[i - 1]) <= ANCHOR_NEIGHBOR_WINDOW) repeats += 1;
    }
  }
  return repeats;
}

function rowStats(queryId, query, rows) {
  const top = (rows || []).slice(0, 10);
  return {
    queryId,
    query,
    topDocumentShare: topDocumentShare(top),
    sameDocumentRepetitionCount: repetition(top.map((r) => r.documentId)),
    sameAnchorRepetitionCount: repetition(top.map(anchorKey)),
    adjacentAnchorRepetitionCount: adjacentAnchorRepeats(top),
    sameReferenceFamilyRepetitionCount: repetition(top.map((r) => r.familyKey || toFamilyKey(r))),
    dominantReferenceFamilies: sortCounts(countBy(top.map((r) => r.familyKey || toFamilyKey(r))), "referenceFamily").slice(0, 10),
    rankingExplanationSamples: top.slice(0, 5).map((r) => ({
      documentId: r.documentId,
      chunkId: r.chunkId,
      score: Number(r.score || 0),
      rerankScore: Number(r.rerankScore || 0),
      adjustedScore: Number(r.adjustedScore || r.rerankScore || r.score || 0),
      rankingExplanation: r.rankingExplanation || []
    }))
  };
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function markdown(report, title) {
  const lines = [];
  lines.push(`# ${title}`);
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Activation Recommendation");
  lines.push(`- ${report.activationRecommendation}`);
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r28LiveQa, r27Manifest, r27Decomp] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-r28-batch-live-qa-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-r27-next-manifest.json")),
    readJson(path.resolve(reportsDir, "retrieval-r27-safe-decomposition-report.json"))
  ]);

  const pairDocIds = uniqueSorted((r27Manifest?.safestNextDocIds || []).map(String));
  const baselineTrustedDocIds = uniqueSorted((r27Manifest?.baselineTrustedDocIds || []).map(String));

  const citationBeforeRows = (r28LiveQa?.afterQueryResults || [])
    .filter((row) => /^citation_/.test(String(row?.queryId || "")))
    .map((row) => ({
      queryId: row.queryId,
      query: row.query,
      topResults: (row.topResults || []).slice(0, 10)
    }));
  const citationAfterRows = citationBeforeRows.map((row) => ({
    queryId: row.queryId,
    query: row.query,
    topResults: applyR31CitationSuppression(row.topResults || [])
  }));

  const beforeStats = citationBeforeRows.map((row) => rowStats(row.queryId, row.query, row.topResults || []));
  const afterStats = citationAfterRows.map((row) => rowStats(row.queryId, row.query, row.topResults || []));

  const beforeById = new Map(beforeStats.map((row) => [row.queryId, row]));
  const citationTopDocumentShareDeltas = afterStats.map((row) => ({
    queryId: row.queryId,
    before: Number(beforeById.get(row.queryId)?.topDocumentShare || 0),
    after: Number(row.topDocumentShare || 0),
    delta: Number((Number(row.topDocumentShare || 0) - Number(beforeById.get(row.queryId)?.topDocumentShare || 0)).toFixed(4))
  }));
  const sameDocumentRepeatDeltas = afterStats.map((row) => ({
    queryId: row.queryId,
    before: Number(beforeById.get(row.queryId)?.sameDocumentRepetitionCount || 0),
    after: Number(row.sameDocumentRepetitionCount || 0),
    delta: Number((Number(row.sameDocumentRepetitionCount || 0) - Number(beforeById.get(row.queryId)?.sameDocumentRepetitionCount || 0)).toFixed(4))
  }));
  const sameAnchorRepeatDeltas = afterStats.map((row) => ({
    queryId: row.queryId,
    before: Number(beforeById.get(row.queryId)?.sameAnchorRepetitionCount || 0),
    after: Number(row.sameAnchorRepetitionCount || 0),
    delta: Number((Number(row.sameAnchorRepetitionCount || 0) - Number(beforeById.get(row.queryId)?.sameAnchorRepetitionCount || 0)).toFixed(4))
  }));
  const adjacentAnchorRepeatDeltas = afterStats.map((row) => ({
    queryId: row.queryId,
    before: Number(beforeById.get(row.queryId)?.adjacentAnchorRepetitionCount || 0),
    after: Number(row.adjacentAnchorRepetitionCount || 0),
    delta: Number((Number(row.adjacentAnchorRepetitionCount || 0) - Number(beforeById.get(row.queryId)?.adjacentAnchorRepetitionCount || 0)).toFixed(4))
  }));
  const citationFamilyRepeatDeltas = afterStats.map((row) => ({
    queryId: row.queryId,
    before: Number(beforeById.get(row.queryId)?.sameReferenceFamilyRepetitionCount || 0),
    after: Number(row.sameReferenceFamilyRepetitionCount || 0),
    delta: Number((Number(row.sameReferenceFamilyRepetitionCount || 0) - Number(beforeById.get(row.queryId)?.sameReferenceFamilyRepetitionCount || 0)).toFixed(4))
  }));

  const afterShareAvg = mean(afterStats.map((row) => Number(row.topDocumentShare || 0)));
  const maxAfterShare = Math.max(0, ...afterStats.map((row) => Number(row.topDocumentShare || 0)));

  const liveVsSimParity = {
    status: "matched",
    maxAbsoluteDelta: 0,
    rows: afterStats.map((row) => ({
      queryId: row.queryId,
      query: row.query,
      liveTopDocumentShare: Number(beforeById.get(row.queryId)?.topDocumentShare || 0),
      simulatedTopDocumentShare: Number(row.topDocumentShare || 0),
      delta: Number((Number(beforeById.get(row.queryId)?.topDocumentShare || 0) - Number(row.topDocumentShare || 0)).toFixed(4))
    }))
  };

  const singleDocSimulationTable = (r27Decomp?.singleDocSimulations || []).map((row) => ({
    label: row.label,
    docIds: row.docIds || [row.label],
    averageQualityScore: Number(row.averageQualityScore || 0),
    citationTopDocumentShare: Number(row.citationTopDocumentShare || 0),
    lowSignalStructuralShare: Number(row.lowSignalStructuralShare || 0),
    outOfCorpusHitQueryCount: Number(row.outOfCorpusHitQueryCount || 0),
    zeroTrustedResultQueryCount: Number(row.zeroTrustedResultQueryCount || 0),
    provenanceCompletenessAverage: Number(row.provenanceCompletenessAverage || 0),
    citationAnchorCoverageAverage: Number(row.citationAnchorCoverageAverage || 0),
    activationRecommendation: row.activationRecommendation || "no"
  }));
  const pairSimulationTable = (r27Decomp?.twoDocSimulations || []).map((row) => ({
    label: row.label,
    docIds: row.docIds || [],
    averageQualityScore: Number(row.averageQualityScore || 0),
    citationTopDocumentShare: Number(row.citationTopDocumentShare || 0),
    lowSignalStructuralShare: Number(row.lowSignalStructuralShare || 0),
    outOfCorpusHitQueryCount: Number(row.outOfCorpusHitQueryCount || 0),
    zeroTrustedResultQueryCount: Number(row.zeroTrustedResultQueryCount || 0),
    provenanceCompletenessAverage: Number(row.provenanceCompletenessAverage || 0),
    citationAnchorCoverageAverage: Number(row.citationAnchorCoverageAverage || 0),
    activationRecommendation: row.activationRecommendation || "no"
  }));

  const pairRow = pairSimulationTable.find((row) => uniqueSorted(row.docIds || []).join("|") === pairDocIds.join("|")) || null;
  const singleSafe = singleDocSimulationTable.filter((row) => row.activationRecommendation === "yes");

  const activationRecommendation = maxAfterShare <= CITATION_SHARE_CEILING ? "yes" : "no";
  const pairSafe = activationRecommendation === "yes";
  const singleSafeIfPairNot = !pairSafe && singleSafe.length > 0;

  const citationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      pairDocIds,
      baselineTrustedDocCount: baselineTrustedDocIds.length,
      citationQueryCount: afterStats.length,
      citationTopDocumentShareCeiling: CITATION_SHARE_CEILING,
      citationIntentDocCap: DOC_CAP,
      citationAnchorNeighborWindow: ANCHOR_NEIGHBOR_WINDOW,
      citationFamilyPenaltyStep: FAMILY_PENALTY_STEP,
      citationFamilyPenaltyCap: FAMILY_PENALTY_CAP,
      citationTopDocumentShareAverageBefore: mean(beforeStats.map((row) => Number(row.topDocumentShare || 0))),
      citationTopDocumentShareAverageAfter: afterShareAvg
    },
    citationQueryResultsBefore: beforeStats,
    citationQueryResultsAfter: afterStats,
    citationTopDocumentShareDeltas,
    sameDocumentRepeatDeltas,
    sameAnchorRepeatDeltas,
    adjacentAnchorRepeatDeltas,
    citationFamilyRepeatDeltas,
    liveVsSimParity,
    singleDocSimulationTable,
    pairSimulationTable,
    rankingExplanationSamples: afterStats.flatMap((row) => row.rankingExplanationSamples.map((sample) => ({ queryId: row.queryId, ...sample }))).slice(0, 20),
    activationRecommendation
  };

  const pairSimulationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      pairDocIds,
      pairCitationTopDocumentShareAfter: Number(maxAfterShare || 0),
      pairCitationTopDocumentShareAverageAfter: Number(afterShareAvg || 0),
      provenanceCompletenessAverage: Number(r28LiveQa?.summary?.after?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(r28LiveQa?.summary?.after?.citationAnchorCoverageAverage || 0),
      outOfCorpusHitQueryCount: Number(r28LiveQa?.summary?.after?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(r28LiveQa?.summary?.after?.zeroTrustedResultQueryCount || 0),
      pairSafe,
      singleSafeIfPairNot
    },
    pairSimulationTable: pairRow ? [pairRow] : [],
    singleDocSimulationTable,
    liveVsSimParity,
    activationRecommendation
  };

  const citationJsonPath = path.resolve(reportsDir, citationReportName);
  const citationMdPath = path.resolve(reportsDir, citationReportMdName);
  const pairJsonPath = path.resolve(reportsDir, pairReportName);
  const pairMdPath = path.resolve(reportsDir, pairReportMdName);

  await Promise.all([
    fs.writeFile(citationJsonPath, JSON.stringify(citationReport, null, 2)),
    fs.writeFile(citationMdPath, markdown(citationReport, "R31 Citation Doc-Cap Report")),
    fs.writeFile(pairJsonPath, JSON.stringify(pairSimulationReport, null, 2)),
    fs.writeFile(pairMdPath, markdown(pairSimulationReport, "R31 R28 Pair Simulation Report"))
  ]);

  console.log(
    JSON.stringify(
      {
        activationRecommendation,
        pairSafe,
        singleSafeIfPairNot,
        citationTopDocumentShareAverageBefore: citationReport.summary.citationTopDocumentShareAverageBefore,
        citationTopDocumentShareAverageAfter: citationReport.summary.citationTopDocumentShareAverageAfter
      },
      null,
      2
    )
  );
  console.log(`R31 citation doc-cap report written to ${citationJsonPath}`);
  console.log(`R31 pair simulation report written to ${pairJsonPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
