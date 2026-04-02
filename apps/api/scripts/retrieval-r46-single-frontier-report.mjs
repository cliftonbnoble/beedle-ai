import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildRetrievalEvalReport } from "./retrieval-eval-utils.mjs";
import { summarizeEvalAsQa } from "./retrieval-batch-expansion-utils.mjs";
import { LIVE_SEARCH_QA_QUERIES, buildRetrievalLiveSearchQaReport } from "./retrieval-live-search-qa-utils.mjs";
import { classifyBlockerFamilies, buildImprovementSignals, evaluateSingleDocGate } from "./retrieval-r36-single-safe-frontier-report.mjs";
import {
  computeCitationTopDocumentShareAverage,
  computeLowSignalStructuralShare,
  resolveCitationTopDocumentShareCeiling
} from "./retrieval-safe-batch-activation-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const outputJsonName = process.env.RETRIEVAL_R46_SINGLE_FRONTIER_REPORT_NAME || "retrieval-r46-single-frontier-report.json";
const outputMdName = process.env.RETRIEVAL_R46_SINGLE_FRONTIER_MARKDOWN_NAME || "retrieval-r46-single-frontier-report.md";
const outputManifestName = process.env.RETRIEVAL_R46_NEXT_SINGLE_MANIFEST_NAME || "retrieval-r46-next-single-manifest.json";

const configuredCitationCeiling = Number(process.env.RETRIEVAL_R46_CONFIGURED_CITATION_CEILING || "0.1");
const qualityRegressionTolerance = Number(process.env.RETRIEVAL_R46_MAX_QUALITY_REGRESSION || "0.5");
const lowSignalTolerance = Number(process.env.RETRIEVAL_R46_MAX_LOW_SIGNAL_REGRESSION || "0");

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(obj = {}) {
  return Object.entries(obj)
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

function sortSafe(rows) {
  return [...rows].sort((a, b) => {
    const aq = Number(a.projectedQualityDelta || 0);
    const bq = Number(b.projectedQualityDelta || 0);
    if (bq !== aq) return bq - aq;
    const ac = Number(a.projectedCitationTopDocumentShare || 0);
    const bc = Number(b.projectedCitationTopDocumentShare || 0);
    if (ac !== bc) return ac - bc;
    return String(a.documentId).localeCompare(String(b.documentId));
  });
}

function safeNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function buildR46FrontierReportFromRows({
  baseline = {},
  effectiveCitationCeiling = 0,
  configuredCitationCeilingValue = 0.1,
  candidateRows = [],
  trustedDocIds = []
}) {
  const rows = [...candidateRows].sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));
  const safeRows = sortSafe(rows.filter((row) => row.keepOrDoNotActivate === "keep"));
  const blockedRows = rows.filter((row) => row.keepOrDoNotActivate !== "keep");
  const nextSafeSingleDocId = safeRows[0]?.documentId || "";
  const activationRecommendation = nextSafeSingleDocId ? "yes" : "no";

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R46",
    trustedDocumentCount: trustedDocIds.length,
    averageQualityScore: safeNum(baseline.averageQualityScore),
    effectiveCitationCeiling: safeNum(effectiveCitationCeiling),
    configuredCitationCeiling: safeNum(configuredCitationCeilingValue),
    candidatesScanned: rows.length,
    safeCandidateCount: safeRows.length,
    blockedCandidateCount: blockedRows.length,
    nextSafeSingleDocId,
    activationRecommendation,
    baselineLiveMetrics: baseline,
    blockerFamilyCounts: sortCountEntries(countBy(blockedRows.flatMap((row) => row.blockerFamilies || []))),
    safeSingleCandidates: safeRows,
    blockedSingleCandidates: blockedRows,
    candidateRows: rows
  };

  const manifest = {
    generatedAt: report.generatedAt,
    readOnly: true,
    phase: "R46",
    baselineTrustedDocIds: unique(trustedDocIds),
    trustedDocumentCount: trustedDocIds.length,
    nextSafeSingleDocId,
    activationRecommendation,
    activationPaths: {
      activationReportJson: "reports/retrieval-r46-single-activation-report.json",
      activationReportMarkdown: "reports/retrieval-r46-single-activation-report.md",
      liveQaReportJson: "reports/retrieval-r46-single-live-qa-report.json",
      liveQaReportMarkdown: "reports/retrieval-r46-single-live-qa-report.md",
      rollbackManifest: "reports/retrieval-r46-single-rollback-manifest.json"
    }
  };

  return { report, manifest };
}

async function readJson(name) {
  const raw = await fs.readFile(path.resolve(reportsDir, name), "utf8");
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
  const url = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(url);
  return (payload.documents || [])
    .map((doc) => ({ id: String(doc.id || ""), title: String(doc.title || ""), isLikelyFixture: Boolean(doc.isLikelyFixture) }))
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
      failures.push({ documentId: doc.id, error: error instanceof Error ? error.message : String(error) });
    }
  }
  return { previews, failures };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R46 Single Frontier Report (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- trustedDocumentCount: ${report.trustedDocumentCount}`);
  lines.push(`- averageQualityScore: ${report.averageQualityScore}`);
  lines.push(`- effectiveCitationCeiling: ${report.effectiveCitationCeiling}`);
  lines.push(`- candidatesScanned: ${report.candidatesScanned}`);
  lines.push(`- safeCandidateCount: ${report.safeCandidateCount}`);
  lines.push(`- blockedCandidateCount: ${report.blockedCandidateCount}`);
  lines.push(`- nextSafeSingleDocId: ${report.nextSafeSingleDocId || ""}`);
  lines.push(`- activationRecommendation: ${report.activationRecommendation}`);
  lines.push("");

  lines.push("## Top Safe Single Candidates");
  for (const row of (report.safeSingleCandidates || []).slice(0, 10)) {
    lines.push(
      `- ${row.documentId} | qualityDelta=${row.projectedQualityDelta} | citationShare=${row.projectedCitationTopDocumentShare} | lowSignal=${row.projectedLowSignalStructuralShare}`
    );
  }
  if (!(report.safeSingleCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Top Blocked Single Candidates");
  for (const row of (report.blockedSingleCandidates || []).slice(0, 10)) {
    lines.push(`- ${row.documentId} | blockers=${(row.blockerFamilies || []).join(", ")} | failingGates=${(row.failingGates || []).join(", ")}`);
  }
  if (!(report.blockedSingleCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Blocker Families");
  for (const row of report.blockerFamilyCounts || []) lines.push(`- ${row.key}: ${row.count}`);
  lines.push("");
  lines.push("- Dry-run only. No activation, rollback, or policy mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r45Repair, r36Manifest, corpusAdmission, documents] = await Promise.all([
    readJson("retrieval-r45-rollback-repair-report.json"),
    readJson("retrieval-r36-next-safe-single-manifest.json"),
    readJson("retrieval-corpus-admission-report.json"),
    resolveDocuments()
  ]);

  if (!r45Repair?.requiredFinalFields?.stateIsSafe || !r45Repair?.requiredFinalFields?.rollbackVerificationPassed) {
    throw new Error("R46 aborted: R45 state is not safe/clean.");
  }

  const trustedDocIds = unique((r36Manifest?.baselineTrustedDocIds || []).map(String));
  if (!trustedDocIds.length) throw new Error("R46 aborted: no trusted doc IDs resolved from live lineage.");

  const documentById = new Map((documents || []).map((doc) => [doc.id, doc]));
  const trustedSet = new Set(trustedDocIds);

  const candidateDocIds = unique(
    (corpusAdmission?.documents || [])
      .filter((row) => String(row?.corpusAdmissionStatus || "") === "hold_for_repair_review")
      .filter((row) => !Boolean(row?.isLikelyFixture))
      .map((row) => String(row?.documentId || ""))
      .filter((id) => id && !trustedSet.has(id) && !Boolean(documentById.get(id)?.isLikelyFixture))
  );

  const { previews, failures: previewFailures } = await loadPreviews(documents);
  const previewByDocId = new Map((previews || []).map((preview) => [String(preview?.document?.documentId || ""), preview]));

  const candidateWithPreview = candidateDocIds.filter((id) => previewByDocId.has(id));

  const fetchSearchDebug = (payload) =>
    fetchJson(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

  const baselineLiveQa = await buildRetrievalLiveSearchQaReport({
    apiBase,
    trustedDocumentIds: trustedDocIds,
    queries: LIVE_SEARCH_QA_QUERIES,
    fetchSearchDebug,
    limit: 20,
    realOnly: true
  });

  const baselineEval = buildRetrievalEvalReport({
    apiBase,
    input: { mode: "r46_baseline", trustedDocumentIds: trustedDocIds },
    documents: previews,
    queries: LIVE_SEARCH_QA_QUERIES,
    includeText: false,
    admittedDocumentIdsOverride: trustedDocIds
  });
  const baselineSim = summarizeEvalAsQa(baselineEval);

  const baselineCitationQueryRows = (baselineLiveQa.queryResults || []).filter((row) => /citation_/.test(String(row?.queryId || "")));
  const citationThreshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: baselineCitationQueryRows,
    configuredGlobalCeiling: configuredCitationCeiling,
    k: 10
  });

  const baseline = {
    trustedDocumentCount: trustedDocIds.length,
    averageQualityScore: safeNum(baselineLiveQa?.summary?.averageQualityScore),
    citationTopDocumentShare: safeNum(computeCitationTopDocumentShareAverage(baselineLiveQa?.queryResults || [])),
    lowSignalStructuralShare: safeNum(computeLowSignalStructuralShare(baselineEval?.queryResults || [])),
    outOfCorpusHitQueryCount: safeNum(baselineLiveQa?.summary?.outOfCorpusHitQueryCount),
    zeroTrustedResultQueryCount: safeNum(baselineLiveQa?.summary?.zeroTrustedResultQueryCount),
    provenanceCompletenessAverage: safeNum(baselineLiveQa?.summary?.provenanceCompletenessAverage),
    citationAnchorCoverageAverage: safeNum(baselineLiveQa?.summary?.citationAnchorCoverageAverage)
  };

  const candidateRows = [];
  for (const documentId of candidateWithPreview) {
    const expandedIds = unique([...trustedDocIds, documentId]);
    const expandedEval = buildRetrievalEvalReport({
      apiBase,
      input: { mode: "r46_single", trustedDocumentIds: expandedIds },
      documents: previews,
      queries: LIVE_SEARCH_QA_QUERIES,
      includeText: false,
      admittedDocumentIdsOverride: expandedIds
    });
    const expanded = summarizeEvalAsQa(expandedEval);

    const expandedMetrics = {
      averageQualityScore: safeNum(expanded?.summary?.averageQualityScore),
      citationTopDocumentShare: safeNum(expanded?.summary?.citationTopDocumentShare),
      lowSignalStructuralShare: safeNum(computeLowSignalStructuralShare(expandedEval?.queryResults || [])),
      outOfCorpusHitQueryCount: safeNum(expanded?.summary?.outOfCorpusHitQueryCount),
      zeroTrustedResultQueryCount: safeNum(expanded?.summary?.zeroTrustedResultQueryCount),
      provenanceCompletenessAverage: safeNum(expanded?.summary?.provenanceCompletenessAverage),
      citationAnchorCoverageAverage: safeNum(expanded?.summary?.citationAnchorCoverageAverage)
    };

    const gate = evaluateSingleDocGate({
      baseline,
      expanded: expandedMetrics,
      effectiveCitationCeiling: citationThreshold.effectiveCeiling,
      qualityRegressionToleranceValue: qualityRegressionTolerance
    });

    if (
      expandedMetrics.lowSignalStructuralShare > Number(baseline.lowSignalStructuralShare || 0) + Number(lowSignalTolerance || 0) &&
      !gate.failingGates.includes("lowSignalStructuralShareNotWorsened")
    ) {
      gate.failingGates.push("lowSignalStructuralShareNotWorsened");
      gate.keepOrDoNotActivate = "do_not_activate";
    }

    const improvementSignals = buildImprovementSignals({ baseline, expanded: expandedMetrics });
    const regressionSignals = [];
    if (expandedMetrics.averageQualityScore < baseline.averageQualityScore) {
      regressionSignals.push("average_quality_score_decreased");
    }
    if (expandedMetrics.citationTopDocumentShare > baseline.citationTopDocumentShare) {
      regressionSignals.push("citation_concentration_increased");
    }
    if (expandedMetrics.lowSignalStructuralShare > baseline.lowSignalStructuralShare) {
      regressionSignals.push("low_signal_structural_share_increased");
    }

    const blockerFamilies = classifyBlockerFamilies(gate.failingGates || []);

    candidateRows.push({
      documentId,
      title: String(previewByDocId.get(documentId)?.document?.title || documentById.get(documentId)?.title || ""),
      projectedAverageQualityScore: expandedMetrics.averageQualityScore,
      projectedQualityDelta: Number((expandedMetrics.averageQualityScore - baseline.averageQualityScore).toFixed(2)),
      projectedCitationTopDocumentShare: expandedMetrics.citationTopDocumentShare,
      projectedLowSignalStructuralShare: expandedMetrics.lowSignalStructuralShare,
      projectedOutOfCorpusHitQueryCount: expandedMetrics.outOfCorpusHitQueryCount,
      projectedZeroTrustedResultQueryCount: expandedMetrics.zeroTrustedResultQueryCount,
      projectedProvenanceCompletenessAverage: expandedMetrics.provenanceCompletenessAverage,
      projectedCitationAnchorCoverageAverage: expandedMetrics.citationAnchorCoverageAverage,
      keepOrDoNotActivate: gate.keepOrDoNotActivate,
      failingGates: unique(gate.failingGates || []),
      blockerFamilies,
      improvementSignals,
      regressionSignals
    });
  }

  const { report, manifest } = buildR46FrontierReportFromRows({
    baseline,
    effectiveCitationCeiling: citationThreshold.effectiveCeiling,
    configuredCitationCeilingValue: configuredCitationCeiling,
    candidateRows,
    trustedDocIds
  });

  report.previewFailures = previewFailures.filter((row) => candidateDocIds.includes(String(row.documentId || "")));

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  const manifestPath = path.resolve(reportsDir, outputManifestName);

  await Promise.all([
    fs.writeFile(jsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, buildMarkdown(report)),
    fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        trustedDocumentCount: report.trustedDocumentCount,
        averageQualityScore: report.averageQualityScore,
        effectiveCitationCeiling: report.effectiveCitationCeiling,
        candidatesScanned: report.candidatesScanned,
        safeCandidateCount: report.safeCandidateCount,
        blockedCandidateCount: report.blockedCandidateCount,
        nextSafeSingleDocId: report.nextSafeSingleDocId,
        activationRecommendation: report.activationRecommendation
      },
      null,
      2
    )
  );
  console.log(`R46 frontier report written to ${jsonPath}`);
  console.log(`R46 frontier manifest written to ${manifestPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
