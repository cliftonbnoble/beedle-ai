import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildRetrievalEvalReport } from "./retrieval-eval-utils.mjs";
import { summarizeEvalAsQa } from "./retrieval-batch-expansion-utils.mjs";
import { LIVE_SEARCH_QA_QUERIES, buildRetrievalLiveSearchQaReport } from "./retrieval-live-search-qa-utils.mjs";
import {
  computeLowSignalStructuralShare,
  resolveCitationTopDocumentShareCeiling
} from "./retrieval-safe-batch-activation-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const outputJsonName =
  process.env.RETRIEVAL_R36_SINGLE_SAFE_FRONTIER_REPORT_NAME || "retrieval-r36-single-safe-frontier-report.json";
const outputMdName =
  process.env.RETRIEVAL_R36_SINGLE_SAFE_FRONTIER_MARKDOWN_NAME || "retrieval-r36-single-safe-frontier-report.md";
const outputManifestName =
  process.env.RETRIEVAL_R36_NEXT_SAFE_SINGLE_MANIFEST_NAME || "retrieval-r36-next-safe-single-manifest.json";

const qualityRegressionTolerance = Number(process.env.RETRIEVAL_R36_MAX_QUALITY_REGRESSION || "0.5");
const configuredCitationCeiling = Number(process.env.RETRIEVAL_R36_CONFIGURED_CITATION_CEILING || "0.1");

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

export function classifyBlockerFamilies(failingGates = []) {
  const f = new Set(failingGates || []);
  const families = [];
  if (f.has("lowSignalStructuralShareNotWorsened")) families.push("low_signal_structural_share_increase");
  if (f.has("citationTopDocumentShareAtOrBelowEffectiveCeiling")) families.push("citation_concentration_above_effective_ceiling");
  if (f.has("qualityNotMateriallyRegressed")) families.push("quality_regression");
  if (
    f.has("outOfCorpusHitQueryCountZero") ||
    f.has("zeroTrustedResultQueryCountZero") ||
    f.has("provenanceCompletenessOne") ||
    f.has("citationAnchorCoverageOne")
  ) {
    families.push("provenance_or_anchor_regression");
  }
  return families.length ? families : ["none"];
}

export function buildImprovementSignals({ baseline, expanded }) {
  const signals = [];
  if (Number(expanded.averageQualityScore || 0) > Number(baseline.averageQualityScore || 0)) signals.push("quality_improved");
  if (Number(expanded.citationTopDocumentShare || 0) < Number(baseline.citationTopDocumentShare || 0)) {
    signals.push("citation_concentration_improved");
  }
  if (Number(expanded.lowSignalStructuralShare || 0) < Number(baseline.lowSignalStructuralShare || 0)) {
    signals.push("low_signal_structural_share_reduced");
  }
  if (Number(expanded.uniqueDocumentsPerQueryAvg || 0) > Number(baseline.uniqueDocumentsPerQueryAvg || 0)) {
    signals.push("document_diversity_improved");
  }
  if (Number(expanded.uniqueChunkTypesPerQueryAvg || 0) > Number(baseline.uniqueChunkTypesPerQueryAvg || 0)) {
    signals.push("chunk_type_diversity_improved");
  }
  return signals;
}

export function evaluateSingleDocGate({
  baseline,
  expanded,
  effectiveCitationCeiling,
  qualityRegressionToleranceValue = 0.5
}) {
  const qualityFloor = Number((Number(baseline.averageQualityScore || 0) - Number(qualityRegressionToleranceValue || 0.5)).toFixed(2));
  const checks = {
    citationTopDocumentShareAtOrBelowEffectiveCeiling:
      Number(expanded.citationTopDocumentShare || 0) <= Number(effectiveCitationCeiling || 0),
    lowSignalStructuralShareNotWorsened:
      Number(expanded.lowSignalStructuralShare || 0) <= Number(baseline.lowSignalStructuralShare || 0),
    outOfCorpusHitQueryCountZero: Number(expanded.outOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(expanded.zeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(expanded.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(expanded.citationAnchorCoverageAverage || 0) === 1,
    qualityNotMateriallyRegressed: Number(expanded.averageQualityScore || 0) >= qualityFloor
  };

  const failingGates = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([name]) => name);

  return {
    checks,
    failingGates,
    keepOrDoNotActivate: failingGates.length === 0 ? "keep" : "do_not_activate",
    thresholds: {
      qualityFloor,
      effectiveCitationCeiling,
      baselineLowSignalStructuralShareCeiling: Number(baseline.lowSignalStructuralShare || 0)
    }
  };
}

export function sortSafeSingleCandidates(rows = []) {
  return [...rows].sort((a, b) => {
    const aGain = Number(a.metrics?.qualityDelta || 0);
    const bGain = Number(b.metrics?.qualityDelta || 0);
    if (bGain !== aGain) return bGain - aGain;
    const aRisk = Number(a.metrics?.citationTopDocumentShareAfter || 0);
    const bRisk = Number(b.metrics?.citationTopDocumentShareAfter || 0);
    if (aRisk !== bRisk) return aRisk - bRisk;
    return String(a.documentId).localeCompare(String(b.documentId));
  });
}

async function readJson(fileName) {
  const raw = await fs.readFile(path.resolve(reportsDir, fileName), "utf8");
  return JSON.parse(raw);
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

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R36 Single-Doc Safe Frontier (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Current Baseline");
  for (const [k, v] of Object.entries(report.currentBaseline || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Top Safe Candidates");
  for (const row of (report.safeSingleCandidates || []).slice(0, 10)) {
    lines.push(`- ${row.documentId} | qualityDelta=${row.metrics.qualityDelta} | citationAfter=${row.metrics.citationTopDocumentShareAfter}`);
  }
  if (!(report.safeSingleCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Top Blocked Candidates");
  for (const row of (report.topBlockedCandidates || []).slice(0, 10)) {
    lines.push(`- ${row.documentId} | blockers=${(row.blockerFamilies || []).join(", ")} | failingGates=${(row.failingGates || []).join(", ")}`);
  }
  if (!(report.topBlockedCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Blocker Families");
  for (const [family, docs] of Object.entries(report.blockerFamilies || {})) lines.push(`- ${family}: ${(docs || []).length}`);
  lines.push("");

  lines.push("## Recommendation");
  lines.push(`- ${report.manifest?.activationRecommendation === "yes" ? "keep" : "do_not_activate"}`);
  lines.push("- Dry-run only. No activation, rollback, or gate mutation.");

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r27Manifest, r34Activation, corpusAdmission, docRows] = await Promise.all([
    readJson("retrieval-r27-next-manifest.json"),
    readJson("retrieval-r34-gate-revision-activation-report.json"),
    readJson("retrieval-corpus-admission-report.json"),
    resolveDocuments()
  ]);

  const baselineTrustedDocIds = unique((r27Manifest?.baselineTrustedDocIds || []).map(String));
  const r34Kept = String(r34Activation?.summary?.keepOrRollbackDecision || "") === "keep_batch_active";
  const r34Doc = String(r34Activation?.docActivatedExact || "").trim();
  const trustedDocIds = unique([...baselineTrustedDocIds, ...(r34Kept && r34Doc ? [r34Doc] : [])]);
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

  const candidateSet = new Set(candidateDocIds);
  const candidateWithPreview = candidateDocIds.filter((id) => previewById.has(id));
  const candidateMissingPreview = candidateDocIds.filter((id) => !previewById.has(id));

  const baselineEval = buildRetrievalEvalReport({
    apiBase,
    input: { mode: "r36_baseline", trustedDocumentIds: trustedDocIds },
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
      input: { mode: "r36_single", trustedDocumentIds: expandedIds },
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

    const preview = previewById.get(documentId);
    const featureProfile = computeCandidateFeatureProfile(preview);
    const citationContribution = computeCitationContributionByDoc(expandedEval, documentId);
    const blockerFamilies = classifyBlockerFamilies(gate.failingGates);

    simulationRows.push({
      documentId,
      title: String(preview?.document?.title || docById.get(documentId)?.title || ""),
      keep_or_do_not_activate: gate.keepOrDoNotActivate,
      failingGates: gate.failingGates,
      blockerFamilies,
      improvementSignals: buildImprovementSignals({ baseline: baselineMetrics, expanded: expandedMetrics }),
      metrics: {
        averageQualityScoreBefore: baselineMetrics.averageQualityScore,
        averageQualityScoreAfter: expandedMetrics.averageQualityScore,
        qualityDelta: Number((expandedMetrics.averageQualityScore - baselineMetrics.averageQualityScore).toFixed(2)),
        citationTopDocumentShareBefore: baselineMetrics.citationTopDocumentShare,
        citationTopDocumentShareAfter: expandedMetrics.citationTopDocumentShare,
        effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0),
        lowSignalStructuralShareBefore: baselineMetrics.lowSignalStructuralShare,
        lowSignalStructuralShareAfter: expandedMetrics.lowSignalStructuralShare,
        outOfCorpusHitQueryCountBefore: baselineMetrics.outOfCorpusHitQueryCount,
        outOfCorpusHitQueryCountAfter: expandedMetrics.outOfCorpusHitQueryCount,
        zeroTrustedResultQueryCountBefore: baselineMetrics.zeroTrustedResultQueryCount,
        zeroTrustedResultQueryCountAfter: expandedMetrics.zeroTrustedResultQueryCount,
        provenanceCompletenessAverageBefore: baselineMetrics.provenanceCompletenessAverage,
        provenanceCompletenessAverageAfter: expandedMetrics.provenanceCompletenessAverage,
        citationAnchorCoverageAverageBefore: baselineMetrics.citationAnchorCoverageAverage,
        citationAnchorCoverageAverageAfter: expandedMetrics.citationAnchorCoverageAverage
      },
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
      keep_or_do_not_activate: "do_not_activate",
      failingGates: ["candidate_preview_unreadable"],
      blockerFamilies: ["provenance_or_anchor_regression"],
      improvementSignals: [],
      metrics: {
        averageQualityScoreBefore: Number(baselineSim.summary.averageQualityScore || 0),
        averageQualityScoreAfter: Number(baselineSim.summary.averageQualityScore || 0),
        qualityDelta: 0,
        citationTopDocumentShareBefore: Number(baselineSim.summary.topDocumentShareAverage || 0),
        citationTopDocumentShareAfter: Number(baselineSim.summary.topDocumentShareAverage || 0),
        effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0),
        lowSignalStructuralShareBefore: Number(baselineLowSignalStructuralShare || 0),
        lowSignalStructuralShareAfter: Number(baselineLowSignalStructuralShare || 0),
        outOfCorpusHitQueryCountBefore: Number(baselineSim.summary.outOfCorpusHitQueryCount || 0),
        outOfCorpusHitQueryCountAfter: Number(baselineSim.summary.outOfCorpusHitQueryCount || 0),
        zeroTrustedResultQueryCountBefore: Number(baselineSim.summary.zeroTrustedResultQueryCount || 0),
        zeroTrustedResultQueryCountAfter: Number(baselineSim.summary.zeroTrustedResultQueryCount || 0),
        provenanceCompletenessAverageBefore: Number(baselineSim.summary.provenanceCompletenessAverage || 0),
        provenanceCompletenessAverageAfter: Number(baselineSim.summary.provenanceCompletenessAverage || 0),
        citationAnchorCoverageAverageBefore: Number(baselineSim.summary.citationAnchorCoverageAverage || 0),
        citationAnchorCoverageAverageAfter: Number(baselineSim.summary.citationAnchorCoverageAverage || 0)
      },
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

  const safeSingleCandidates = sortSafeSingleCandidates(
    simulationRows.filter((row) => row.keep_or_do_not_activate === "keep")
  );

  const blockedSingleCandidates = [...simulationRows]
    .filter((row) => row.keep_or_do_not_activate !== "keep")
    .sort((a, b) => {
      const aF = (a.failingGates || []).length;
      const bF = (b.failingGates || []).length;
      if (bF !== aF) return bF - aF;
      const aDelta = Number(a.metrics?.qualityDelta || 0);
      const bDelta = Number(b.metrics?.qualityDelta || 0);
      if (aDelta !== bDelta) return aDelta - bDelta;
      return String(a.documentId).localeCompare(String(b.documentId));
    });

  const blockerCounts = sortCountEntries(countBy(blockedSingleCandidates.flatMap((row) => row.failingGates || [])));
  const blockerFamilies = Object.fromEntries(
    sortCountEntries(countBy(blockedSingleCandidates.flatMap((row) => row.blockerFamilies || []))).map((row) => [
      row.key,
      blockedSingleCandidates.filter((doc) => (doc.blockerFamilies || []).includes(row.key)).map((doc) => doc.documentId)
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

  const topSafeCandidate = safeSingleCandidates[0] || null;
  const topBlockedCandidates = blockedSingleCandidates.slice(0, 10);

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R36",
    summary: {
      candidatesScanned: simulationRows.length,
      safeCandidateCount: safeSingleCandidates.length,
      blockedCandidateCount: blockedSingleCandidates.length,
      configuredCitationCeiling,
      effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0),
      activationRecommendation: topSafeCandidate ? "yes" : "no"
    },
    currentBaseline: {
      trustedDocumentCount: trustedDocIds.length,
      averageQualityScore: Number(baselineSim.summary.averageQualityScore || 0),
      citationTopDocumentShare: Number(baselineSim.summary.topDocumentShareAverage || 0),
      effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0),
      lowSignalStructuralShare: Number(baselineLowSignalStructuralShare || 0),
      outOfCorpusHitQueryCount: Number(baselineSim.summary.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(baselineSim.summary.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(baselineSim.summary.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(baselineSim.summary.citationAnchorCoverageAverage || 0)
    },
    safeSingleCandidates,
    blockedSingleCandidates,
    blockerCounts,
    blockerFamilies,
    dominantBlockedChunkTypes,
    dominantBlockedSectionLabels,
    topSafeCandidate,
    topBlockedCandidates,
    manifest: {
      generatedAt: new Date().toISOString(),
      readOnly: true,
      baselineTrustedDocIds: trustedDocIds,
      nextSafeSingleDocId: topSafeCandidate?.documentId || "",
      expandedTrustedDocIds: topSafeCandidate ? unique([...trustedDocIds, topSafeCandidate.documentId]) : trustedDocIds,
      activationRecommendation: topSafeCandidate ? "yes" : "no",
      keepOrDoNotActivate: topSafeCandidate ? "keep" : "do_not_activate",
      effectiveCitationCeiling: Number(citationThreshold.effectiveCeiling || 0),
      failingCandidatesCount: blockedSingleCandidates.length
    }
  };

  const manifest = report.manifest;

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
        candidatesScanned: report.summary.candidatesScanned,
        safeCandidateCount: report.summary.safeCandidateCount,
        blockedCandidateCount: report.summary.blockedCandidateCount,
        nextSafeSingleDocId: manifest.nextSafeSingleDocId,
        activationRecommendation: manifest.activationRecommendation
      },
      null,
      2
    )
  );
  console.log(`R36 single safe frontier report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
