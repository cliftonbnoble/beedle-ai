import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const citationReportName = "retrieval-r30-citation-family-report.json";
const citationReportMdName = "retrieval-r30-citation-family-report.md";
const pairReportName = "retrieval-r30-r28-pair-simulation-report.json";
const pairReportMdName = "retrieval-r30-r28-pair-simulation-report.md";

const CITATION_SHARE_CEILING = Number(process.env.RETRIEVAL_R30_MAX_CITATION_TOP_DOC_SHARE || "0.1");
const CITATION_FAMILY_PENALTY_STEP = Number(process.env.RETRIEVAL_R30_CITATION_FAMILY_PENALTY_STEP || "0.03");
const CITATION_FAMILY_PENALTY_CAP = Number(process.env.RETRIEVAL_R30_CITATION_FAMILY_PENALTY_CAP || "0.24");
const CITATION_PER_DOC_CAP = Number(process.env.RETRIEVAL_R30_CITATION_PER_DOC_CAP || "1");

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

function normalize(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s_-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractFamiliesFromRow(row) {
  const source = `${row?.snippet || ""} ${row?.citationAnchor || ""} ${row?.title || ""}`;
  const normalized = normalize(source);
  const matches = normalized.match(/\b\d+\.\d+\b/g) || [];
  const families = uniqueSorted(matches);
  if (!families.length) return ["<none>"];
  return families;
}

function toAnchor(row) {
  return String(row?.citationAnchor || "");
}

function applyCitationPenaltyAndCap(rows) {
  const input = (rows || []).slice(0, 10).map((row) => ({
    ...row,
    referenceFamilies: extractFamiliesFromRow(row)
  }));
  const familyKeyByChunk = new Map(input.map((row) => [String(row.chunkId || ""), row.referenceFamilies.join("|") || "<none>"]));
  const familyCounts = countBy(input.map((row) => familyKeyByChunk.get(String(row.chunkId || "")) || "<none>"));

  const rescored = input.map((row) => {
    const chunkId = String(row.chunkId || "");
    const familyKey = familyKeyByChunk.get(chunkId) || "<none>";
    const familyCount = Number(familyCounts[familyKey] || 0);
    const familyPenalty = familyCount > 1 ? Math.min(CITATION_FAMILY_PENALTY_CAP, (familyCount - 1) * CITATION_FAMILY_PENALTY_STEP) : 0;
    const rerankScore = Number(row.rerankScore || row.score || 0);
    const adjustedScore = Number(Math.max(0, rerankScore - familyPenalty).toFixed(6));
    return {
      ...row,
      familyKey,
      familyPenalty: Number(familyPenalty.toFixed(6)),
      adjustedScore,
      rankingExplanation: [
        ...(Array.isArray(row.rankingExplanation) ? row.rankingExplanation : []),
        ...(familyPenalty > 0 ? [`citation_family_repeat_penalty=${familyPenalty.toFixed(2)}:${familyKey}`] : [])
      ]
    };
  });

  const ranked = [...rescored].sort((a, b) => {
    const scoreDiff = Number(b.adjustedScore || 0) - Number(a.adjustedScore || 0);
    if (scoreDiff !== 0) return scoreDiff;
    const docDiff = String(a.documentId || "").localeCompare(String(b.documentId || ""));
    if (docDiff !== 0) return docDiff;
    return String(a.chunkId || "").localeCompare(String(b.chunkId || ""));
  });

  const perDoc = new Map();
  const selected = [];
  for (const row of ranked) {
    const docId = String(row.documentId || "");
    const count = Number(perDoc.get(docId) || 0);
    if (count >= CITATION_PER_DOC_CAP) continue;
    perDoc.set(docId, count + 1);
    selected.push(row);
    if (selected.length >= 10) break;
  }

  return selected;
}

function rowStats(rows, queryId, query) {
  const top = (rows || []).slice(0, 10);
  const familyKeys = top.map((row) => (row.referenceFamilies ? row.referenceFamilies.join("|") : extractFamiliesFromRow(row).join("|")));
  return {
    queryId,
    query,
    topDocumentShare: topDocumentShare(top),
    sameDocumentRepetitionCount: repetition(top.map((r) => r.documentId)),
    sameReferenceFamilyRepetitionCount: repetition(familyKeys),
    dominantReferenceFamilies: sortCounts(countBy(familyKeys), "referenceFamily").slice(0, 10),
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
  const [r28LiveQa, r27Manifest] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-r28-batch-live-qa-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-r27-next-manifest.json"))
  ]);

  const pairDocIds = uniqueSorted((r27Manifest?.safestNextDocIds || []).map(String));
  const baselineTrustedDocIds = uniqueSorted((r27Manifest?.baselineTrustedDocIds || []).map(String));

  const citationRowsBefore = (r28LiveQa?.afterQueryResults || [])
    .filter((row) => /^citation_/.test(String(row?.queryId || "")))
    .map((row) => ({
      queryId: row.queryId,
      query: row.query,
      topResults: (row.topResults || []).slice(0, 10)
    }));

  const citationRowsAfter = citationRowsBefore.map((row) => ({
    queryId: row.queryId,
    query: row.query,
    topResults: applyCitationPenaltyAndCap(row.topResults || [])
  }));

  const beforeStats = citationRowsBefore.map((row) => rowStats(row.topResults, row.queryId, row.query));
  const afterStats = citationRowsAfter.map((row) => rowStats(row.topResults, row.queryId, row.query));

  const byBefore = new Map(beforeStats.map((row) => [row.queryId, row]));
  const byAfter = new Map(afterStats.map((row) => [row.queryId, row]));

  const citationFamilyRepeatDeltas = afterStats.map((row) => {
    const before = byBefore.get(row.queryId);
    return {
      queryId: row.queryId,
      before: Number(before?.sameReferenceFamilyRepetitionCount || 0),
      after: Number(row.sameReferenceFamilyRepetitionCount || 0),
      delta: Number((Number(row.sameReferenceFamilyRepetitionCount || 0) - Number(before?.sameReferenceFamilyRepetitionCount || 0)).toFixed(4))
    };
  });
  const citationTopDocumentShareDeltas = afterStats.map((row) => {
    const before = byBefore.get(row.queryId);
    return {
      queryId: row.queryId,
      before: Number(before?.topDocumentShare || 0),
      after: Number(row.topDocumentShare || 0),
      delta: Number((Number(row.topDocumentShare || 0) - Number(before?.topDocumentShare || 0)).toFixed(4))
    };
  });
  const sameDocumentRepeatDeltas = afterStats.map((row) => {
    const before = byBefore.get(row.queryId);
    return {
      queryId: row.queryId,
      before: Number(before?.sameDocumentRepetitionCount || 0),
      after: Number(row.sameDocumentRepetitionCount || 0),
      delta: Number((Number(row.sameDocumentRepetitionCount || 0) - Number(before?.sameDocumentRepetitionCount || 0)).toFixed(4))
    };
  });

  const maxShareAfter = Math.max(0, ...afterStats.map((row) => Number(row.topDocumentShare || 0)));
  const activationRecommendation = maxShareAfter <= CITATION_SHARE_CEILING ? "yes" : "no";

  const liveVsSimParityRows = afterStats.map((row) => {
    const live = byBefore.get(row.queryId);
    const liveShare = Number(live?.topDocumentShare || 0);
    const simShare = Number(row.topDocumentShare || 0);
    return {
      queryId: row.queryId,
      query: row.query,
      liveTopDocumentShare: liveShare,
      simulatedTopDocumentShare: simShare,
      delta: Number((liveShare - simShare).toFixed(4))
    };
  });
  const maxDelta = Math.max(0, ...liveVsSimParityRows.map((row) => Math.abs(Number(row.delta || 0))));
  const parity = maxDelta === 0 ? "matched" : maxDelta <= 0.05 ? "materially_closer" : "drifted";

  const citationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      pairDocIds,
      baselineTrustedDocCount: baselineTrustedDocIds.length,
      citationQueryCount: afterStats.length,
      citationTopDocumentShareCeiling: CITATION_SHARE_CEILING,
      citationFamilyPenaltyStep: CITATION_FAMILY_PENALTY_STEP,
      citationFamilyPenaltyCap: CITATION_FAMILY_PENALTY_CAP,
      citationIntentPerDocumentCap: CITATION_PER_DOC_CAP,
      citationTopDocumentShareAverageBefore: mean(beforeStats.map((row) => Number(row.topDocumentShare || 0))),
      citationTopDocumentShareAverageAfter: mean(afterStats.map((row) => Number(row.topDocumentShare || 0)))
    },
    citationQueryResultsBefore: beforeStats,
    citationQueryResultsAfter: afterStats,
    citationFamilyRepeatDeltas,
    citationTopDocumentShareDeltas,
    sameDocumentRepeatDeltas,
    liveVsSimParity: {
      status: parity,
      maxAbsoluteDelta: maxDelta,
      rows: liveVsSimParityRows
    },
    dominantReferenceFamiliesBefore: sortCounts(
      countBy(beforeStats.flatMap((row) => (row.dominantReferenceFamilies || []).map((item) => item.referenceFamily))),
      "referenceFamily"
    ),
    dominantReferenceFamiliesAfter: sortCounts(
      countBy(afterStats.flatMap((row) => (row.dominantReferenceFamilies || []).map((item) => item.referenceFamily))),
      "referenceFamily"
    ),
    rankingExplanationSamples: afterStats.flatMap((row) => row.rankingExplanationSamples.map((sample) => ({ queryId: row.queryId, ...sample }))).slice(0, 20),
    activationRecommendation
  };

  const pairSimulationReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      pairDocIds,
      citationTopDocumentShareAverageBefore: citationReport.summary.citationTopDocumentShareAverageBefore,
      citationTopDocumentShareAverageAfter: citationReport.summary.citationTopDocumentShareAverageAfter,
      provenanceCompletenessAverage: Number(r28LiveQa?.summary?.after?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(r28LiveQa?.summary?.after?.citationAnchorCoverageAverage || 0),
      outOfCorpusHitQueryCount: Number(r28LiveQa?.summary?.after?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(r28LiveQa?.summary?.after?.zeroTrustedResultQueryCount || 0)
    },
    liveVsSimParity: citationReport.liveVsSimParity,
    activationRecommendation
  };

  const citationJsonPath = path.resolve(reportsDir, citationReportName);
  const citationMdPath = path.resolve(reportsDir, citationReportMdName);
  const simJsonPath = path.resolve(reportsDir, pairReportName);
  const simMdPath = path.resolve(reportsDir, pairReportMdName);

  await Promise.all([
    fs.writeFile(citationJsonPath, JSON.stringify(citationReport, null, 2)),
    fs.writeFile(citationMdPath, markdown(citationReport, "R30 Citation Family Report")),
    fs.writeFile(simJsonPath, JSON.stringify(pairSimulationReport, null, 2)),
    fs.writeFile(simMdPath, markdown(pairSimulationReport, "R30 R28 Pair Simulation Report"))
  ]);

  console.log(
    JSON.stringify(
      {
        activationRecommendation,
        citationTopDocumentShareAverageBefore: citationReport.summary.citationTopDocumentShareAverageBefore,
        citationTopDocumentShareAverageAfter: citationReport.summary.citationTopDocumentShareAverageAfter,
        liveVsSimParity: citationReport.liveVsSimParity.status
      },
      null,
      2
    )
  );
  console.log(`R30 citation report written to ${citationJsonPath}`);
  console.log(`R30 pair simulation report written to ${simJsonPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
