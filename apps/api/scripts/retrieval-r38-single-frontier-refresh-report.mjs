import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildRetrievalEvalReport } from "./retrieval-eval-utils.mjs";
import {
  LIVE_SEARCH_QA_QUERIES,
  buildRetrievalLiveSearchQaReport,
  loadTrustedActivatedDocumentIds
} from "./retrieval-live-search-qa-utils.mjs";
import { buildImprovementSignals, classifyBlockerFamilies, evaluateSingleDocGate } from "./retrieval-r36-single-safe-frontier-report.mjs";
import { computeLowSignalStructuralShare, resolveCitationTopDocumentShareCeiling } from "./retrieval-safe-batch-activation-utils.mjs";
import { summarizeEvalAsQa } from "./retrieval-batch-expansion-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const outputJsonName =
  process.env.RETRIEVAL_R38_SINGLE_FRONTIER_REFRESH_REPORT_NAME || "retrieval-r38-single-frontier-refresh-report.json";
const outputMdName =
  process.env.RETRIEVAL_R38_SINGLE_FRONTIER_REFRESH_MARKDOWN_NAME || "retrieval-r38-single-frontier-refresh-report.md";
const outputManifestName =
  process.env.RETRIEVAL_R38_NEXT_SINGLE_MANIFEST_NAME || "retrieval-r38-next-single-manifest.json";

const qualityRegressionTolerance = Number(process.env.RETRIEVAL_R38_MAX_QUALITY_REGRESSION || "0.5");
const configuredCitationCeiling = Number(process.env.RETRIEVAL_R38_CONFIGURED_CITATION_CEILING || "0.1");
const queryDeltaThreshold = Number(process.env.RETRIEVAL_R38_QUERY_DELTA_THRESHOLD || "0.01");

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function normalizeChunkType(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function isLowSignalStructural(value) {
  const t = normalizeChunkType(value);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(obj) {
  return Object.entries(obj || {})
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

function sortSafeR38Candidates(rows = []) {
  return [...rows].sort((a, b) => {
    const aGain = Number(a.projectedQualityDelta || 0);
    const bGain = Number(b.projectedQualityDelta || 0);
    if (bGain !== aGain) return bGain - aGain;
    const aRisk = Number(a.projectedCitationTopDocumentShare || 0);
    const bRisk = Number(b.projectedCitationTopDocumentShare || 0);
    if (aRisk !== bRisk) return aRisk - bRisk;
    return String(a.documentId).localeCompare(String(b.documentId));
  });
}

export function buildR38QualityRiskRanking(candidateRows = []) {
  return [...candidateRows]
    .sort((a, b) => {
      const aRisk = Number(a?.projectedQualityDelta ?? 0);
      const bRisk = Number(b?.projectedQualityDelta ?? 0);
      if (bRisk !== aRisk) return bRisk - aRisk;
      const aCitation = Number(a?.projectedCitationTopDocumentShare ?? 0);
      const bCitation = Number(b?.projectedCitationTopDocumentShare ?? 0);
      if (aCitation !== bCitation) return aCitation - bCitation;
      return String(a?.documentId || "").localeCompare(String(b?.documentId || ""));
    })
    .map((row, index) => ({ rank: index + 1, ...row }));
}

export function buildQueryDeltaClassification({
  baselineByQuery = [],
  expandedByQuery = [],
  threshold = 0.01
} = {}) {
  const baselineMap = new Map((baselineByQuery || []).map((row) => [String(row?.queryId || ""), row]));
  const expandedMap = new Map((expandedByQuery || []).map((row) => [String(row?.queryId || ""), row]));
  const queryIds = unique([...baselineMap.keys(), ...expandedMap.keys()]);

  const deltas = queryIds.map((queryId) => {
    const base = baselineMap.get(queryId) || {};
    const next = expandedMap.get(queryId) || {};
    const baselineQuality = Number(base?.metrics?.qualityScore || 0);
    const projectedQuality = Number(next?.metrics?.qualityScore || 0);
    const qualityDelta = Number((projectedQuality - baselineQuality).toFixed(2));
    const baselineTopDocShare = Number(base?.metrics?.topDocumentShare || 0);
    const projectedTopDocShare = Number(next?.metrics?.topDocumentShare || 0);
    const baselineResultCount = Number(base?.metrics?.resultCount || 0);
    const projectedResultCount = Number(next?.metrics?.resultCount || 0);
    return {
      queryId,
      baselineQualityScore: baselineQuality,
      projectedQualityScore: projectedQuality,
      qualityDelta,
      baselineTopDocumentShare: baselineTopDocShare,
      projectedTopDocumentShare: projectedTopDocShare,
      baselineResultCount,
      projectedResultCount
    };
  });

  const improvements = deltas.filter((row) => Number(row.qualityDelta || 0) > Number(threshold || 0));
  const regressions = deltas.filter((row) => Number(row.qualityDelta || 0) < -Number(threshold || 0));

  return {
    queryDeltaRows: deltas,
    queryLevelImprovements: improvements.map((row) => row.queryId),
    queryLevelRegressions: regressions.map((row) => row.queryId),
    queryLevelNetDelta: Number(deltas.reduce((sum, row) => sum + Number(row.qualityDelta || 0), 0).toFixed(2))
  };
}

async function readJson(fileName) {
  const raw = await fs.readFile(path.resolve(reportsDir, fileName), "utf8");
  return JSON.parse(raw);
}

async function readJsonIfExists(fileName) {
  try {
    return await readJson(fileName);
  } catch {
    return null;
  }
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON response.`);
  }
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

export async function checkApiReachability({ apiBaseUrl = apiBase } = {}) {
  const listUrl = `${apiBaseUrl}/admin/ingestion/documents?status=all&fileType=decision_docx&sort=createdAtDesc&limit=1`;
  try {
    await fetchJson(listUrl);
    return { reachable: true, error: null };
  } catch (error) {
    return { reachable: false, error: error instanceof Error ? error.message : String(error) };
  }
}

async function resolveDocuments() {
  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  return (payload.documents || [])
    .map((doc) => ({ id: doc.id, isLikelyFixture: Boolean(doc.isLikelyFixture), title: String(doc.title || "") }))
    .filter((doc) => doc.id);
}

async function loadPreviews(documentRows) {
  const previews = [];
  const failures = [];
  for (const doc of documentRows || []) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=0`;
    try {
      const preview = await fetchJson(detailUrl);
      previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture, titleFromList: doc.title });
    } catch (error) {
      failures.push({ documentId: doc.id, title: doc.title, error: error instanceof Error ? error.message : String(error) });
    }
  }
  return { previews, failures };
}

function computeCandidateFeatureProfile(preview) {
  const chunks = Array.isArray(preview?.chunks) ? preview.chunks : [];
  const chunkTypeCounts = sortCountEntries(countBy(chunks.map((chunk) => chunk.chunkType || "")));
  const sectionLabelCounts = sortCountEntries(countBy(chunks.map((chunk) => chunk.sectionLabel || "")));
  const citationFamilyCounts = sortCountEntries(countBy(chunks.flatMap((chunk) => chunk.citationFamilies || [])));
  const lowSignalCount = chunks.filter((chunk) => isLowSignalStructural(chunk?.chunkType || chunk?.sectionLabel || "")).length;
  const lowSignalShare = chunks.length ? Number((lowSignalCount / chunks.length).toFixed(4)) : 0;
  return {
    chunkCount: chunks.length,
    chunkTypeCounts,
    sectionLabelCounts,
    citationFamilyCounts,
    lowSignalChunkCount: lowSignalCount,
    lowSignalChunkShare: lowSignalShare
  };
}

function computeCitationContributionByDoc(expandedEval, documentId) {
  const rows = (expandedEval?.queryResults || []).filter((row) => /citation_/.test(String(row?.queryId || "")));
  const topRows = rows.flatMap((row) => (row?.topResults || []).slice(0, 10));
  const hits = topRows.filter((row) => String(row?.documentId || "") === String(documentId)).length;
  const ratio = topRows.length ? Number((hits / topRows.length).toFixed(4)) : 0;
  return {
    citationTopResultHits: hits,
    citationTopResultTotal: topRows.length,
    citationTopResultShare: ratio
  };
}

function mapLegacyR36RowToR38(row) {
  const metrics = row?.metrics || {};
  return {
    documentId: String(row?.documentId || ""),
    title: String(row?.title || ""),
    projectedAverageQualityScore: Number(metrics.averageQualityScoreAfter || 0),
    projectedQualityDelta: Number(metrics.qualityDelta || 0),
    projectedCitationTopDocumentShare: Number(metrics.citationTopDocumentShareAfter || 0),
    projectedLowSignalStructuralShare: Number(metrics.lowSignalStructuralShareAfter || 0),
    projectedOutOfCorpusHitQueryCount: Number(metrics.outOfCorpusHitQueryCountAfter || 0),
    projectedZeroTrustedResultQueryCount: Number(metrics.zeroTrustedResultQueryCountAfter || 0),
    projectedProvenanceCompletenessAverage: Number(metrics.provenanceCompletenessAverageAfter || 0),
    projectedCitationAnchorCoverageAverage: Number(metrics.citationAnchorCoverageAverageAfter || 0),
    keepOrDoNotActivate: String(row?.keep_or_do_not_activate || "do_not_activate") === "keep" ? "keep" : "do_not_activate",
    failingGates: (row?.failingGates || []).map(String),
    blockerFamilies: (row?.blockerFamilies || []).map(String),
    improvementSignals: (row?.improvementSignals || []).map(String),
    regressionSignals: (row?.failingGates || []).map((gate) => `gate_failed:${gate}`),
    queryLevelImprovements: [],
    queryLevelRegressions: [],
    queryDeltaRows: [],
    qualityRiskScore: Number((0 - Number(metrics.qualityDelta || 0)).toFixed(2)),
    dominantFeatureDiagnosis: row?.dominantFeatureDiagnosis || {
      chunkTypeMix: [],
      lowSignalChunkShare: null,
      sectionLabelProfile: [],
      citationFamilyProfile: [],
      citationFamilyConcentrationContribution: {
        citationTopResultHits: 0,
        citationTopResultTotal: 0,
        citationTopResultShare: 0
      }
    }
  };
}

export function buildR38ReportFromRows({
  candidateRows = [],
  trustedDocIds = [],
  baselineLiveMetrics = {},
  configuredCitationCeilingValue = configuredCitationCeiling,
  effectiveCitationCeiling = configuredCitationCeiling,
  dataMode = "live",
  offlineFallbackInputs = [],
  offlineLimitations = []
} = {}) {
  const sortedRows = [...candidateRows].sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));
  const safeSingleCandidates = sortSafeR38Candidates(sortedRows.filter((row) => row.keepOrDoNotActivate === "keep"));
  const blockedSingleCandidates = [...sortedRows]
    .filter((row) => row.keepOrDoNotActivate !== "keep")
    .sort((a, b) => {
      const aF = (a.failingGates || []).length;
      const bF = (b.failingGates || []).length;
      if (bF !== aF) return bF - aF;
      const aDelta = Number(a.projectedQualityDelta || 0);
      const bDelta = Number(b.projectedQualityDelta || 0);
      if (aDelta !== bDelta) return aDelta - bDelta;
      return String(a.documentId).localeCompare(String(b.documentId));
    });

  const candidatesFailingOnlyOnQuality = blockedSingleCandidates.filter(
    (row) => (row.failingGates || []).length === 1 && (row.failingGates || []).includes("qualityNotMateriallyRegressed")
  );

  const blockerFamilyCounts = sortCountEntries(countBy(blockedSingleCandidates.flatMap((row) => row.blockerFamilies || [])));
  const blockerFamilies = Object.fromEntries(
    blockerFamilyCounts.map((row) => [
      row.key,
      blockedSingleCandidates
        .filter((doc) => (doc.blockerFamilies || []).includes(row.key))
        .map((doc) => doc.documentId)
        .sort((a, b) => String(a).localeCompare(String(b)))
    ])
  );

  const dominantBlockedChunkTypes = sortCountEntries(
    countBy(
      blockedSingleCandidates.flatMap((row) =>
        (row.dominantFeatureDiagnosis?.chunkTypeMix || []).slice(0, 3).map((entry) => entry.key)
      )
    )
  );
  const dominantBlockedSectionLabels = sortCountEntries(
    countBy(
      blockedSingleCandidates.flatMap((row) =>
        (row.dominantFeatureDiagnosis?.sectionLabelProfile || []).slice(0, 3).map((entry) => entry.key)
      )
    )
  );

  const qualityRiskRanking = buildR38QualityRiskRanking(
    sortedRows.map((row) => ({
      documentId: row.documentId,
      projectedAverageQualityScore: row.projectedAverageQualityScore,
      projectedQualityDelta: row.projectedQualityDelta,
      projectedCitationTopDocumentShare: row.projectedCitationTopDocumentShare,
      keepOrDoNotActivate: row.keepOrDoNotActivate,
      blockerFamilies: row.blockerFamilies
    }))
  );

  const topSafeCandidate = safeSingleCandidates[0] || null;
  const activationRecommendation = topSafeCandidate ? "yes" : "no";

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R38",
    dataMode,
    offlineFallbackUsed: dataMode === "offline_fallback",
    offlineFallbackInputs: unique(offlineFallbackInputs),
    offlineLimitations: unique(offlineLimitations),
    baselineLiveMetrics,
    summary: {
      candidatesScanned: sortedRows.length,
      safeCandidateCount: safeSingleCandidates.length,
      blockedCandidateCount: blockedSingleCandidates.length,
      candidatesFailingOnlyOnQuality: candidatesFailingOnlyOnQuality.length,
      configuredCitationCeiling: configuredCitationCeilingValue,
      effectiveCitationCeiling: Number(effectiveCitationCeiling || configuredCitationCeilingValue || 0),
      nextSafeSingleDocId: topSafeCandidate?.documentId || "",
      activationRecommendation
    },
    candidatesScanned: sortedRows.length,
    safeCandidateCount: safeSingleCandidates.length,
    blockedCandidateCount: blockedSingleCandidates.length,
    candidatesFailingOnlyOnQuality,
    blockerFamilyCounts,
    blockerFamilies,
    qualityRiskRanking,
    safeSingleCandidates,
    blockedSingleCandidates,
    dominantBlockedChunkTypes,
    dominantBlockedSectionLabels,
    topSafeCandidate,
    topBlockedCandidates: blockedSingleCandidates.slice(0, 10),
    nextSafeSingleDocId: topSafeCandidate?.documentId || "",
    activationRecommendation,
    candidateRows: sortedRows
  };

  const manifest = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    dataMode,
    baselineTrustedDocIds: unique(trustedDocIds),
    nextSafeSingleDocId: topSafeCandidate?.documentId || "",
    expandedTrustedDocIds: topSafeCandidate ? unique([...trustedDocIds, topSafeCandidate.documentId]) : unique(trustedDocIds),
    activationRecommendation,
    keepOrDoNotActivate: topSafeCandidate ? "keep" : "do_not_activate",
    effectiveCitationCeiling: Number(effectiveCitationCeiling || configuredCitationCeilingValue || 0),
    candidatesScanned: sortedRows.length
  };

  return { report, manifest };
}

export async function buildOfflineFallbackArtifacts({
  reportsDirPath = reportsDir,
  configuredCitationCeilingValue = configuredCitationCeiling,
  trustedDocIds = []
} = {}) {
  const inputNames = [];
  const limitations = [];
  const read = async (name) => {
    const file = path.resolve(reportsDirPath, name);
    try {
      const raw = await fs.readFile(file, "utf8");
      inputNames.push(name);
      return JSON.parse(raw);
    } catch {
      return null;
    }
  };

  const [corpusAdmission, r35Stability, r36Frontier, r37Activation, r37Live, r34Live] = await Promise.all([
    read("retrieval-corpus-admission-report.json"),
    read("retrieval-r35-stability-report.json"),
    read("retrieval-r36-single-safe-frontier-report.json"),
    read("retrieval-r37-single-activation-report.json"),
    read("retrieval-r37-single-live-qa-report.json"),
    read("retrieval-r34-gate-revision-live-qa-report.json")
  ]);

  if (!corpusAdmission) {
    throw new Error("Offline fallback unavailable: retrieval-corpus-admission-report.json missing.");
  }

  const baselineFromR37 = r37Live?.summary?.before || null;
  const baselineFromR35 = r35Stability?.summary || null;
  const baselineFromR34 = r34Live?.summary?.after || null;
  const baseline = baselineFromR37 || baselineFromR35 || baselineFromR34;
  if (!baseline) {
    throw new Error("Offline fallback unavailable: no baseline metrics artifact found.");
  }

  if (!r36Frontier) limitations.push("candidate_simulation_reused_from_r36_frontier_snapshot");
  limitations.push("offline_mode_uses_cached_artifacts_no_live_api_refresh");

  const trustedSet = new Set((trustedDocIds || []).map(String));
  const eligibleCandidateIds = unique(
    (corpusAdmission?.documents || [])
      .filter((row) => String(row?.corpusAdmissionStatus || "") === "hold_for_repair_review")
      .filter((row) => !Boolean(row?.isLikelyFixture))
      .map((row) => String(row?.documentId || ""))
      .filter((id) => id && !trustedSet.has(id))
  );

  const legacyRows = [
    ...(r36Frontier?.safeSingleCandidates || []),
    ...(r36Frontier?.blockedSingleCandidates || []),
    ...(r36Frontier?.candidateRows || [])
  ];
  const legacyById = new Map(
    legacyRows
      .filter((row) => row?.documentId)
      .map((row) => [String(row.documentId), row])
  );

  const candidateRows = eligibleCandidateIds.map((documentId) => {
    const legacy = legacyById.get(documentId);
    if (legacy) {
      return mapLegacyR36RowToR38(legacy);
    }
    return {
      documentId,
      title: String((corpusAdmission?.documents || []).find((row) => String(row?.documentId || "") === documentId)?.title || ""),
      projectedAverageQualityScore: Number(baseline.averageQualityScore || 0),
      projectedQualityDelta: 0,
      projectedCitationTopDocumentShare: Number(baseline.citationTopDocumentShare || 0),
      projectedLowSignalStructuralShare: Number(baseline.lowSignalStructuralShare || 0),
      projectedOutOfCorpusHitQueryCount: Number(baseline.outOfCorpusHitQueryCount || 0),
      projectedZeroTrustedResultQueryCount: Number(baseline.zeroTrustedResultQueryCount || 0),
      projectedProvenanceCompletenessAverage: Number(baseline.provenanceCompletenessAverage || 0),
      projectedCitationAnchorCoverageAverage: Number(baseline.citationAnchorCoverageAverage || 0),
      keepOrDoNotActivate: "do_not_activate",
      failingGates: ["offline_missing_candidate_simulation"],
      blockerFamilies: ["provenance_or_anchor_regression"],
      improvementSignals: [],
      regressionSignals: ["offline_missing_candidate_simulation"],
      queryLevelImprovements: [],
      queryLevelRegressions: [],
      queryDeltaRows: [],
      qualityRiskScore: 0,
      dominantFeatureDiagnosis: {
        chunkTypeMix: [],
        lowSignalChunkShare: null,
        sectionLabelProfile: [],
        citationFamilyProfile: [],
        citationFamilyConcentrationContribution: {
          citationTopResultHits: 0,
          citationTopResultTotal: 0,
          citationTopResultShare: 0
        }
      }
    };
  });

  const effectiveCitationCeiling = Number(
    r37Activation?.hardGate?.thresholds?.effectiveCitationCeiling ||
      r36Frontier?.summary?.effectiveCitationCeiling ||
      Math.max(configuredCitationCeilingValue, Number(baseline.citationTopDocumentShare || 0) || 0)
  );

  const fallbackCitationTopDocumentShare = Number(
    baselineFromR35?.citationTopDocumentShare ??
      r37Activation?.hardGate?.measured?.citationTopDocumentShare ??
      baselineFromR34?.citationTopDocumentShare ??
      0
  );
  const fallbackLowSignalStructuralShare = Number(
    baselineFromR35?.lowSignalStructuralShare ??
      r37Activation?.hardGate?.measured?.beforeLowSignalStructuralShare ??
      baselineFromR34?.lowSignalStructuralShare ??
      0
  );

  const baselineLiveMetrics = {
    trustedDocumentCount: Number(baseline.trustedDocumentCount || (trustedDocIds || []).length),
    averageQualityScore: Number(baseline.averageQualityScore || 0),
    citationTopDocumentShare: Number(
      baseline.citationTopDocumentShare ?? fallbackCitationTopDocumentShare ?? 0
    ),
    effectiveCitationCeiling,
    lowSignalStructuralShare: Number(
      baseline.lowSignalStructuralShare ?? fallbackLowSignalStructuralShare ?? 0
    ),
    outOfCorpusHitQueryCount: Number(baseline.outOfCorpusHitQueryCount || 0),
    zeroTrustedResultQueryCount: Number(baseline.zeroTrustedResultQueryCount || 0),
    provenanceCompletenessAverage: Number(baseline.provenanceCompletenessAverage || 0),
    citationAnchorCoverageAverage: Number(baseline.citationAnchorCoverageAverage || 0)
  };

  return {
    candidateRows,
    baselineLiveMetrics,
    effectiveCitationCeiling,
    offlineFallbackInputs: inputNames,
    offlineLimitations: limitations
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R38 Single Frontier Refresh (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push(`- dataMode: ${report.dataMode || "live"}`);
  lines.push(`- offlineFallbackUsed: ${Boolean(report.offlineFallbackUsed)}`);
  lines.push("");

  if (report.offlineFallbackUsed) {
    lines.push("## Offline Fallback");
    lines.push(`- inputs: ${(report.offlineFallbackInputs || []).join(", ") || "<none>"}`);
    for (const row of report.offlineLimitations || []) lines.push(`- limitation: ${row}`);
    if (!(report.offlineLimitations || []).length) lines.push("- limitation: <none>");
    lines.push("");
  }

  lines.push("## Baseline Live Metrics");
  for (const [k, v] of Object.entries(report.baselineLiveMetrics || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Top Safe Candidates");
  for (const row of (report.safeSingleCandidates || []).slice(0, 10)) {
    lines.push(
      `- ${row.documentId} | qualityDelta=${row.projectedQualityDelta} | citationShare=${row.projectedCitationTopDocumentShare} | lowSignal=${row.projectedLowSignalStructuralShare}`
    );
  }
  if (!(report.safeSingleCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Candidates Failing Only On Quality");
  for (const row of (report.candidatesFailingOnlyOnQuality || []).slice(0, 10)) {
    lines.push(`- ${row.documentId} | projectedQualityDelta=${row.projectedQualityDelta}`);
  }
  if (!(report.candidatesFailingOnlyOnQuality || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Blocker Families");
  for (const row of report.blockerFamilyCounts || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.blockerFamilyCounts || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Recommendation");
  lines.push(`- nextSafeSingleDocId: ${report.nextSafeSingleDocId || "<none>"}`);
  lines.push(`- activationRecommendation: ${report.activationRecommendation}`);
  lines.push("- Dry-run only. No activation/rollback writes executed.");
  return `${lines.join("\n")}\n`;
}

async function buildLiveArtifacts({ corpusAdmission, trustedDocIds, docRows }) {
  const trustedSet = new Set(trustedDocIds);
  const docById = new Map((docRows || []).map((row) => [String(row.id), row]));

  const candidateDocIds = unique(
    (corpusAdmission?.documents || [])
      .filter((row) => String(row?.corpusAdmissionStatus || "") === "hold_for_repair_review")
      .filter((row) => !Boolean(row?.isLikelyFixture))
      .map((row) => String(row?.documentId || ""))
      .filter((id) => id && !trustedSet.has(id) && !Boolean(docById.get(id)?.isLikelyFixture))
  );

  const { previews, failures: previewFailures } = await loadPreviews(docRows);
  const previewById = new Map((previews || []).map((p) => [String(p?.document?.documentId || ""), p]));

  const candidateWithPreview = candidateDocIds.filter((id) => previewById.has(id));
  const candidateMissingPreview = candidateDocIds.filter((id) => !previewById.has(id));

  const baselineEval = buildRetrievalEvalReport({
    apiBase,
    input: { mode: "r38_baseline", trustedDocumentIds: trustedDocIds },
    documents: previews,
    queries: LIVE_SEARCH_QA_QUERIES,
    includeText: false,
    admittedDocumentIdsOverride: trustedDocIds
  });
  const baselineSim = summarizeEvalAsQa(baselineEval);
  const baselineLowSignalStructuralShare = computeLowSignalStructuralShare(baselineEval.queryResults || []);
  const baselineLiveQa = await buildRetrievalLiveSearchQaReport({
    apiBase,
    trustedDocumentIds: trustedDocIds,
    queries: LIVE_SEARCH_QA_QUERIES,
    fetchSearchDebug: (payload) =>
      fetchJson(`${apiBase}/admin/retrieval/debug`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      }),
    limit: 20,
    realOnly: true
  });

  const citationThreshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: (baselineLiveQa.queryResults || []).filter((row) => /citation_/.test(String(row?.queryId || ""))),
    configuredGlobalCeiling: configuredCitationCeiling,
    k: 10
  });

  const simulationRows = [];
  for (const documentId of candidateWithPreview) {
    const expandedIds = unique([...trustedDocIds, documentId]);
    const expandedEval = buildRetrievalEvalReport({
      apiBase,
      input: { mode: "r38_single", trustedDocumentIds: expandedIds },
      documents: previews,
      queries: LIVE_SEARCH_QA_QUERIES,
      includeText: false,
      admittedDocumentIdsOverride: expandedIds
    });
    const expandedSim = summarizeEvalAsQa(expandedEval);
    const expandedLowSignalStructuralShare = computeLowSignalStructuralShare(expandedEval.queryResults || []);

    const baselineMetrics = {
      averageQualityScore: Number(baselineSim.summary.averageQualityScore || 0),
      citationTopDocumentShare: Number(baselineSim.summary.topDocumentShareAverage || 0),
      lowSignalStructuralShare: Number(baselineLowSignalStructuralShare || 0),
      outOfCorpusHitQueryCount: Number(baselineSim.summary.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(baselineSim.summary.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(baselineSim.summary.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(baselineSim.summary.citationAnchorCoverageAverage || 0),
      uniqueDocumentsPerQueryAvg: Number(baselineSim.summary.uniqueDocumentsPerQueryAvg || 0),
      uniqueChunkTypesPerQueryAvg: Number(baselineSim.summary.uniqueChunkTypesPerQueryAvg || 0)
    };

    const expandedMetrics = {
      averageQualityScore: Number(expandedSim.summary.averageQualityScore || 0),
      citationTopDocumentShare: Number(expandedSim.summary.topDocumentShareAverage || 0),
      lowSignalStructuralShare: Number(expandedLowSignalStructuralShare || 0),
      outOfCorpusHitQueryCount: Number(expandedSim.summary.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(expandedSim.summary.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(expandedSim.summary.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(expandedSim.summary.citationAnchorCoverageAverage || 0),
      uniqueDocumentsPerQueryAvg: Number(expandedSim.summary.uniqueDocumentsPerQueryAvg || 0),
      uniqueChunkTypesPerQueryAvg: Number(expandedSim.summary.uniqueChunkTypesPerQueryAvg || 0)
    };

    const gate = evaluateSingleDocGate({
      baseline: baselineMetrics,
      expanded: expandedMetrics,
      effectiveCitationCeiling: citationThreshold.effectiveCeiling,
      qualityRegressionToleranceValue: qualityRegressionTolerance
    });

    const queryDelta = buildQueryDeltaClassification({
      baselineByQuery: baselineSim.byQuery || [],
      expandedByQuery: expandedSim.byQuery || [],
      threshold: queryDeltaThreshold
    });

    const preview = previewById.get(documentId);
    const featureProfile = computeCandidateFeatureProfile(preview);
    const citationContribution = computeCitationContributionByDoc(expandedEval, documentId);
    const blockerFamilies = classifyBlockerFamilies(gate.failingGates);
    const regressionSignals = [
      ...queryDelta.queryLevelRegressions.map((queryId) => `query_regressed:${queryId}`),
      ...gate.failingGates.map((gateName) => `gate_failed:${gateName}`)
    ];

    simulationRows.push({
      documentId,
      title: String(preview?.document?.title || docById.get(documentId)?.title || ""),
      projectedAverageQualityScore: expandedMetrics.averageQualityScore,
      projectedQualityDelta: Number((expandedMetrics.averageQualityScore - baselineMetrics.averageQualityScore).toFixed(2)),
      projectedCitationTopDocumentShare: expandedMetrics.citationTopDocumentShare,
      projectedLowSignalStructuralShare: expandedMetrics.lowSignalStructuralShare,
      projectedOutOfCorpusHitQueryCount: expandedMetrics.outOfCorpusHitQueryCount,
      projectedZeroTrustedResultQueryCount: expandedMetrics.zeroTrustedResultQueryCount,
      projectedProvenanceCompletenessAverage: expandedMetrics.provenanceCompletenessAverage,
      projectedCitationAnchorCoverageAverage: expandedMetrics.citationAnchorCoverageAverage,
      keepOrDoNotActivate: gate.keepOrDoNotActivate,
      failingGates: gate.failingGates,
      blockerFamilies,
      improvementSignals: [
        ...buildImprovementSignals({ baseline: baselineMetrics, expanded: expandedMetrics }),
        ...queryDelta.queryLevelImprovements.map((queryId) => `query_improved:${queryId}`)
      ],
      regressionSignals,
      queryLevelImprovements: queryDelta.queryLevelImprovements,
      queryLevelRegressions: queryDelta.queryLevelRegressions,
      queryDeltaRows: queryDelta.queryDeltaRows,
      qualityRiskScore: Number((0 - Number(expandedMetrics.averageQualityScore - baselineMetrics.averageQualityScore || 0)).toFixed(2)),
      dominantFeatureDiagnosis: {
        chunkTypeMix: featureProfile.chunkTypeCounts,
        lowSignalChunkShare: featureProfile.lowSignalChunkShare,
        sectionLabelProfile: featureProfile.sectionLabelCounts,
        citationFamilyProfile: featureProfile.citationFamilyCounts,
        citationFamilyConcentrationContribution: citationContribution
      }
    });
  }

  for (const missingId of candidateMissingPreview) {
    const miss = previewFailures.find((row) => row.documentId === missingId);
    simulationRows.push({
      documentId: missingId,
      title: String(docById.get(missingId)?.title || ""),
      projectedAverageQualityScore: Number(baselineSim.summary.averageQualityScore || 0),
      projectedQualityDelta: 0,
      projectedCitationTopDocumentShare: Number(baselineSim.summary.topDocumentShareAverage || 0),
      projectedLowSignalStructuralShare: Number(baselineLowSignalStructuralShare || 0),
      projectedOutOfCorpusHitQueryCount: Number(baselineSim.summary.outOfCorpusHitQueryCount || 0),
      projectedZeroTrustedResultQueryCount: Number(baselineSim.summary.zeroTrustedResultQueryCount || 0),
      projectedProvenanceCompletenessAverage: Number(baselineSim.summary.provenanceCompletenessAverage || 0),
      projectedCitationAnchorCoverageAverage: Number(baselineSim.summary.citationAnchorCoverageAverage || 0),
      keepOrDoNotActivate: "do_not_activate",
      failingGates: ["candidate_preview_unreadable"],
      blockerFamilies: ["provenance_or_anchor_regression"],
      improvementSignals: [],
      regressionSignals: ["candidate_preview_unreadable"],
      queryLevelImprovements: [],
      queryLevelRegressions: [],
      queryDeltaRows: [],
      qualityRiskScore: 0,
      dominantFeatureDiagnosis: {
        chunkTypeMix: [],
        lowSignalChunkShare: null,
        sectionLabelProfile: [],
        citationFamilyProfile: [],
        citationFamilyConcentrationContribution: {
          citationTopResultHits: 0,
          citationTopResultTotal: 0,
          citationTopResultShare: 0
        },
        previewReadError: miss?.error || "preview_unavailable"
      }
    });
  }

  return {
    candidateRows: simulationRows,
    baselineLiveMetrics: {
      trustedDocumentCount: trustedDocIds.length,
      averageQualityScore: Number(baselineLiveQa.summary?.averageQualityScore || 0),
      citationTopDocumentShare: Number(
        (baselineLiveQa.queryResults || [])
          .filter((row) => /citation_/.test(String(row?.queryId || "")))
          .reduce((sum, row, _idx, arr) => sum + Number(row?.metrics?.topDocumentShare || 0) / Math.max(1, arr.length), 0)
          .toFixed(4)
      ),
      effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0),
      lowSignalStructuralShare: Number(computeLowSignalStructuralShare(baselineLiveQa.queryResults || []) || 0),
      outOfCorpusHitQueryCount: Number(baselineLiveQa.summary?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(baselineLiveQa.summary?.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(baselineLiveQa.summary?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(baselineLiveQa.summary?.citationAnchorCoverageAverage || 0)
    },
    effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0)
  };
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const trustedFromReports = await loadTrustedActivatedDocumentIds({ reportsDir });
  const trustedDocIds = unique((trustedFromReports?.trustedDocumentIds || []).map(String));
  if (!trustedDocIds.length) {
    throw new Error("No trusted baseline docs found from activation artifacts.");
  }

  let dataMode = "live";
  let offlineFallbackInputs = [];
  let offlineLimitations = [];
  let candidateRows = [];
  let baselineLiveMetrics = {};
  let effectiveCitationCeiling = configuredCitationCeiling;

  const corpusAdmission = await readJson("retrieval-corpus-admission-report.json");
  const health = await checkApiReachability({ apiBaseUrl: apiBase });

  if (health.reachable) {
    try {
      const docRows = await resolveDocuments();
      const live = await buildLiveArtifacts({ corpusAdmission, trustedDocIds, docRows });
      candidateRows = live.candidateRows;
      baselineLiveMetrics = live.baselineLiveMetrics;
      effectiveCitationCeiling = live.effectiveCitationCeiling;
    } catch (error) {
      dataMode = "offline_fallback";
      offlineLimitations.push(`live_mode_failed:${error instanceof Error ? error.message : String(error)}`);
    }
  } else {
    dataMode = "offline_fallback";
    offlineLimitations.push(`api_unreachable:${health.error || "fetch_failed"}`);
  }

  if (dataMode === "offline_fallback") {
    const offline = await buildOfflineFallbackArtifacts({
      reportsDirPath: reportsDir,
      configuredCitationCeilingValue: configuredCitationCeiling,
      trustedDocIds
    });
    candidateRows = offline.candidateRows;
    baselineLiveMetrics = offline.baselineLiveMetrics;
    effectiveCitationCeiling = offline.effectiveCitationCeiling;
    offlineFallbackInputs = offline.offlineFallbackInputs;
    offlineLimitations = unique([...(offlineLimitations || []), ...(offline.offlineLimitations || [])]);
  }

  const { report, manifest } = buildR38ReportFromRows({
    candidateRows,
    trustedDocIds,
    baselineLiveMetrics,
    configuredCitationCeilingValue: configuredCitationCeiling,
    effectiveCitationCeiling,
    dataMode,
    offlineFallbackInputs,
    offlineLimitations
  });

  const reportPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  const manifestPath = path.resolve(reportsDir, outputManifestName);

  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, buildMarkdown(report)),
    fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        dataMode: report.dataMode,
        candidatesScanned: report.summary.candidatesScanned,
        safeCandidateCount: report.summary.safeCandidateCount,
        blockedCandidateCount: report.summary.blockedCandidateCount,
        candidatesFailingOnlyOnQuality: report.summary.candidatesFailingOnlyOnQuality,
        nextSafeSingleDocId: report.summary.nextSafeSingleDocId,
        activationRecommendation: report.summary.activationRecommendation
      },
      null,
      2
    )
  );
  console.log(`R38 single frontier refresh report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
