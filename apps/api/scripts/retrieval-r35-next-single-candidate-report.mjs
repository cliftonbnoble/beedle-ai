import fs from "node:fs/promises";
import path from "node:path";
import {
  buildRetrievalBatchExpansionReport,
  selectNextBatchCandidates
} from "./retrieval-batch-expansion-utils.mjs";
import { resolveCitationTopDocumentShareCeiling } from "./retrieval-safe-batch-activation-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const stabilityReportName = process.env.RETRIEVAL_R35_STABILITY_REPORT_NAME || "retrieval-r35-stability-report.json";
const stabilityMarkdownName = process.env.RETRIEVAL_R35_STABILITY_MARKDOWN_NAME || "retrieval-r35-stability-report.md";
const candidateReportName =
  process.env.RETRIEVAL_R35_NEXT_SINGLE_CANDIDATE_REPORT_NAME || "retrieval-r35-next-single-candidate-report.json";
const candidateMarkdownName =
  process.env.RETRIEVAL_R35_NEXT_SINGLE_CANDIDATE_MARKDOWN_NAME || "retrieval-r35-next-single-candidate-report.md";
const manifestName = process.env.RETRIEVAL_R35_NEXT_SINGLE_MANIFEST_NAME || "retrieval-r35-next-single-manifest.json";

const QUALITY_REGRESSION_TOLERANCE = Number(process.env.RETRIEVAL_R35_MAX_QUALITY_REGRESSION || "0.5");
const LOW_SIGNAL_DOMINATED_THRESHOLD = Number(process.env.RETRIEVAL_R35_LOW_SIGNAL_DOMINATED_THRESHOLD || "0.4");

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
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function resolveDocuments() {
  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  return (payload.documents || [])
    .map((doc) => ({ id: doc.id, isLikelyFixture: Boolean(doc.isLikelyFixture) }))
    .filter((doc) => doc.id);
}

async function loadPreviews(documentRows) {
  const previews = [];
  for (const doc of documentRows || []) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=0`;
    const preview = await fetchJson(detailUrl);
    previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture });
  }
  return previews;
}

function buildChunkProfileMap(previews) {
  const map = new Map();
  for (const preview of previews || []) {
    const id = String(preview?.document?.documentId || "");
    if (!id) continue;
    const chunks = Array.isArray(preview?.chunks) ? preview.chunks : [];
    const lowSignalCount = chunks.filter((chunk) => isLowSignalStructural(chunk?.chunkType || chunk?.sectionLabel || "")).length;
    const lowSignalShare = chunks.length ? Number((lowSignalCount / chunks.length).toFixed(4)) : 0;
    map.set(id, {
      documentId: id,
      title: String(preview?.document?.title || ""),
      chunkCount: chunks.length,
      lowSignalChunkCount: lowSignalCount,
      lowSignalChunkShare: lowSignalShare
    });
  }
  return map;
}

function filterCorpusAdmissionRows({ corpusAdmission, trustedIds, regressionDocIds, chunkProfileMap }) {
  const trusted = new Set((trustedIds || []).map(String));
  const regressionSet = new Set((regressionDocIds || []).map(String));
  const exclusionReasons = [];
  const rows = (corpusAdmission?.documents || []).map((row) => ({ ...row }));

  const filteredRows = rows.map((row) => {
    const id = String(row?.documentId || "");
    const reasons = [];
    if (!id) reasons.push("missing_document_id");
    if (row?.isLikelyFixture) reasons.push("fixture_excluded");
    if (trusted.has(id)) reasons.push("already_trusted_excluded");
    if (String(row?.corpusAdmissionStatus || "") !== "hold_for_repair_review") reasons.push("not_hold_status_excluded");
    if (regressionSet.has(id)) reasons.push("prior_regression_family_doc_excluded");
    if (/retrieval messy headings/i.test(String(row?.title || ""))) reasons.push("previous_regression_family_title_excluded");

    const profile = chunkProfileMap.get(id);
    if (profile && Number(profile.lowSignalChunkShare || 0) > LOW_SIGNAL_DOMINATED_THRESHOLD) {
      reasons.push("low_signal_structural_dominated");
    }

    const include = reasons.length === 0;
    if (!include) exclusionReasons.push(...reasons);
    return {
      ...row,
      include,
      r35ExclusionReasons: reasons,
      r35LowSignalChunkShare: Number(profile?.lowSignalChunkShare || 0)
    };
  });

  return {
    filtered: { ...(corpusAdmission || {}), documents: filteredRows },
    exclusionReasonCounts: sortCountEntries(countBy(exclusionReasons))
  };
}

function evaluateR35Gate({
  baselineSummary,
  baselineCitationRows,
  expandedSummary,
  concentrationDeltas,
  configuredCitationCeiling
}) {
  const citationThreshold = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: baselineCitationRows,
    configuredGlobalCeiling: configuredCitationCeiling,
    k: 10
  });

  const minAllowedQuality = Number((Number(baselineSummary.averageQualityScore || 0) - QUALITY_REGRESSION_TOLERANCE).toFixed(2));
  const checks = {
    qualityNotMateriallyRegressed: Number(expandedSummary.averageQualityScore || 0) >= minAllowedQuality,
    citationTopDocumentShareAtOrBelowEffectiveCeiling:
      Number(concentrationDeltas.expandedCitationTopDocumentShareAvg || 0) <= Number(citationThreshold.effectiveCeiling || 0),
    lowSignalStructuralShareNotWorsened:
      Number(concentrationDeltas.expandedLowSignalStructuralShare || 0) <=
      Number(concentrationDeltas.baselineLowSignalStructuralShare || 0),
    outOfCorpusHitQueryCountZero: Number(expandedSummary.outOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(expandedSummary.zeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(expandedSummary.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(expandedSummary.citationAnchorCoverageAverage || 0) === 1
  };
  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([name]) => name);

  return {
    passed: failures.length === 0,
    checks,
    failures,
    thresholds: {
      qualityFloor: minAllowedQuality,
      configuredCitationTopDocumentShareCeiling: citationThreshold.configuredGlobalCeiling,
      attainableFloorGivenUniqueDocsAtK10: citationThreshold.attainableFloorGivenUniqueDocsAtK,
      effectiveCitationTopDocumentShareCeiling: citationThreshold.effectiveCeiling,
      baselineLowSignalStructuralShareCeiling: Number(concentrationDeltas.baselineLowSignalStructuralShare || 0)
    }
  };
}

function buildStabilityMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R35 Stability Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Trusted Set");
  lines.push(`- trustedDocumentCount: ${report.trustedDocumentCount}`);
  lines.push(`- activeR34DocIncluded: ${report.activeR34DocIncluded}`);
  lines.push("");
  lines.push("## Gate Baseline");
  for (const [k, v] of Object.entries(report.citationGateBaseline || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Exclusion Signals");
  for (const row of report.filtering.exclusionReasonCounts || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.filtering.exclusionReasonCounts || []).length) lines.push("- none");
  return `${lines.join("\n")}\n`;
}

function buildCandidateMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R35 Next Single Candidate (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Proposed Candidate");
  if (report.proposedNextSingle) {
    lines.push(`- documentId: ${report.proposedNextSingle.documentId}`);
    lines.push(`- title: ${report.proposedNextSingle.title}`);
    lines.push(`- candidateScore: ${report.proposedNextSingle.candidateScore}`);
    lines.push(`- candidateReasons: ${(report.proposedNextSingle.candidateReasons || []).join(", ") || "<none>"}`);
    lines.push(`- candidateBlockers: ${(report.proposedNextSingle.candidateBlockers || []).join(", ") || "<none>"}`);
  } else {
    lines.push("- none");
  }
  lines.push("");
  lines.push("## Simulated Before vs After");
  for (const [k, v] of Object.entries(report.beforeVsAfterMetrics || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Hard Gate");
  for (const [k, v] of Object.entries(report.hardGate?.checks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.hardGate?.failures || []).length) lines.push(`- failures: ${(report.hardGate.failures || []).join(", ")}`);
  lines.push("");
  lines.push(`- recommendation: ${report.summary.keepOrDoNotActivate}`);
  lines.push("- Dry-run only. No activation, rollback, or mutation executed.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [corpusAdmission, referenceEnrichment, promotionRehearsal, r27Manifest, r34Activation] = await Promise.all([
    readJson("retrieval-corpus-admission-report.json"),
    readJson("retrieval-reference-enrichment-report.json"),
    readJson("retrieval-promotion-rehearsal-report.json"),
    readJson("retrieval-r27-next-manifest.json"),
    readJson("retrieval-r34-gate-revision-activation-report.json")
  ]);

  const baselineTrustedDocIds = unique((r27Manifest?.baselineTrustedDocIds || []).map(String));
  const r34Kept = String(r34Activation?.summary?.keepOrRollbackDecision || "") === "keep_batch_active";
  const r34DocId = String(r34Activation?.docActivatedExact || "").trim();
  const trustedDocIds = unique([...baselineTrustedDocIds, ...(r34Kept && r34DocId ? [r34DocId] : [])]);

  if (!trustedDocIds.length) {
    throw new Error("No trusted baseline doc IDs available for R35.");
  }

  const regressionDocIds = unique([
    ...((await readJson("retrieval-r26-batch-activation-report.json").catch(() => ({ docsActivatedExact: [] })))?.docsActivatedExact || []),
    ...((await readJson("retrieval-r28-batch-activation-report.json").catch(() => ({ docsActivatedExact: [] })))?.docsActivatedExact || [])
  ]);

  const documents = await loadPreviews(await resolveDocuments());
  const chunkProfileMap = buildChunkProfileMap(documents);

  const filteredCorpus = filterCorpusAdmissionRows({
    corpusAdmission,
    trustedIds: trustedDocIds,
    regressionDocIds,
    chunkProfileMap
  });

  const simulation = await buildRetrievalBatchExpansionReport({
    apiBase,
    documents,
    corpusAdmissionReport: filteredCorpus.filtered,
    referenceEnrichmentReport: referenceEnrichment,
    promotionRehearsalReport: promotionRehearsal,
    trustedDocumentIds: trustedDocIds,
    batchSize: 1,
    strictSafeMode: true,
    baselineTargetScore: Number(r34Activation?.beforeLiveSummary?.averageQualityScore || 65.22),
    maxBaselineRegression: QUALITY_REGRESSION_TOLERANCE,
    fetchSearchDebug: (payload) =>
      fetchJson(`${apiBase}/admin/retrieval/debug`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      })
  });

  const baselineCitationRows = (simulation?.liveQaBaseline?.queryResults || []).filter((row) => /citation_/.test(String(row?.queryId || "")));
  const hardGate = evaluateR35Gate({
    baselineSummary: simulation?.beforeVsAfterQa?.baseline?.liveSummary || {},
    baselineCitationRows,
    expandedSummary: simulation?.beforeVsAfterQa?.expanded?.simulatedSummary || {},
    concentrationDeltas: simulation?.concentrationDeltas || {},
    configuredCitationCeiling: 0.1
  });

  const proposed = simulation?.proposedNextBatch?.[0] || null;
  const keepOrDoNotActivate = proposed && hardGate.passed ? "keep" : "do_not_activate";

  const stabilityReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R35",
    summary: {
      trustedDocumentCount: trustedDocIds.length,
      averageQualityScore: Number(simulation?.beforeVsAfterQa?.baseline?.liveSummary?.averageQualityScore || 0),
      outOfCorpusHitQueryCount: Number(simulation?.beforeVsAfterQa?.baseline?.liveSummary?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(simulation?.beforeVsAfterQa?.baseline?.liveSummary?.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(simulation?.beforeVsAfterQa?.baseline?.liveSummary?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(simulation?.beforeVsAfterQa?.baseline?.liveSummary?.citationAnchorCoverageAverage || 0),
      lowSignalStructuralShare: Number(simulation?.concentrationDeltas?.baselineLowSignalStructuralShare || 0),
      citationTopDocumentShare: Number(simulation?.concentrationDeltas?.baselineCitationTopDocumentShareAvg || 0)
    },
    trustedDocumentCount: trustedDocIds.length,
    trustedDocumentIds: trustedDocIds,
    activeR34DocIncluded: r34Kept && trustedDocIds.includes(r34DocId),
    activeR34DocId: r34DocId,
    citationGateBaseline: {
      configuredGlobalCeiling: hardGate.thresholds.configuredCitationTopDocumentShareCeiling,
      attainableFloorGivenUniqueDocsAtK10: hardGate.thresholds.attainableFloorGivenUniqueDocsAtK10,
      effectiveCitationTopDocumentShareCeiling: hardGate.thresholds.effectiveCitationTopDocumentShareCeiling,
      baselineCitationTopDocumentShare: Number(simulation?.concentrationDeltas?.baselineCitationTopDocumentShareAvg || 0)
    },
    filtering: {
      lowSignalDominatedThreshold: LOW_SIGNAL_DOMINATED_THRESHOLD,
      priorRegressionDocIdsExcluded: regressionDocIds,
      exclusionReasonCounts: filteredCorpus.exclusionReasonCounts
    }
  };

  const candidateReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R35",
    summary: {
      proposedBatchSize: Number(simulation?.summary?.proposedBatchSize || 0),
      candidatePoolCount: Number(simulation?.summary?.candidatePoolCount || 0),
      activationWorthy: Boolean(hardGate.passed && proposed),
      keepOrDoNotActivate
    },
    proposedNextSingle: proposed,
    beforeVsAfterMetrics: {
      baselineAverageQualityScore: Number(simulation?.beforeVsAfterQa?.baseline?.liveSummary?.averageQualityScore || 0),
      simulatedAverageQualityScore: Number(simulation?.beforeVsAfterQa?.expanded?.simulatedSummary?.averageQualityScore || 0),
      baselineCitationTopDocumentShare: Number(simulation?.concentrationDeltas?.baselineCitationTopDocumentShareAvg || 0),
      simulatedCitationTopDocumentShare: Number(simulation?.concentrationDeltas?.expandedCitationTopDocumentShareAvg || 0),
      baselineLowSignalStructuralShare: Number(simulation?.concentrationDeltas?.baselineLowSignalStructuralShare || 0),
      simulatedLowSignalStructuralShare: Number(simulation?.concentrationDeltas?.expandedLowSignalStructuralShare || 0),
      outOfCorpusHitQueryCount: Number(simulation?.beforeVsAfterQa?.expanded?.simulatedSummary?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(simulation?.beforeVsAfterQa?.expanded?.simulatedSummary?.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(simulation?.beforeVsAfterQa?.expanded?.simulatedSummary?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(simulation?.beforeVsAfterQa?.expanded?.simulatedSummary?.citationAnchorCoverageAverage || 0)
    },
    hardGate,
    topIncludedReasons: simulation?.topIncludedReasons || [],
    topExcludedReasons: simulation?.topExcludedReasons || [],
    candidateExclusions: simulation?.candidateExclusions || []
  };

  const manifest = {
    generatedAt: candidateReport.generatedAt,
    readOnly: true,
    phase: "R35",
    baselineTrustedDocIds: trustedDocIds,
    nextSingleDocId: proposed?.documentId || "",
    expandedTrustedDocIds: unique([...trustedDocIds, ...(proposed?.documentId ? [proposed.documentId] : [])]),
    activationRecommendation: keepOrDoNotActivate === "keep" ? "yes" : "no",
    keepOrDoNotActivate,
    hardGate: {
      passed: hardGate.passed,
      checks: hardGate.checks,
      failures: hardGate.failures,
      thresholds: hardGate.thresholds
    },
    includeReasons: proposed?.candidateReasons || [],
    excludeReasons: proposed?.candidateBlockers || []
  };

  const stabilityPath = path.resolve(reportsDir, stabilityReportName);
  const stabilityMdPath = path.resolve(reportsDir, stabilityMarkdownName);
  const candidatePath = path.resolve(reportsDir, candidateReportName);
  const candidateMdPath = path.resolve(reportsDir, candidateMarkdownName);
  const manifestPath = path.resolve(reportsDir, manifestName);

  await Promise.all([
    fs.writeFile(stabilityPath, JSON.stringify(stabilityReport, null, 2)),
    fs.writeFile(stabilityMdPath, buildStabilityMarkdown(stabilityReport)),
    fs.writeFile(candidatePath, JSON.stringify(candidateReport, null, 2)),
    fs.writeFile(candidateMdPath, buildCandidateMarkdown(candidateReport)),
    fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2))
  ]);

  console.log(JSON.stringify({
    trustedDocumentCount: stabilityReport.summary.trustedDocumentCount,
    averageQualityScore: stabilityReport.summary.averageQualityScore,
    nextSingleDocId: manifest.nextSingleDocId,
    keepOrDoNotActivate
  }, null, 2));
  console.log(`R35 stability report written to ${stabilityPath}`);
  console.log(`R35 next single candidate report written to ${candidatePath}`);
  console.log(`R35 next single manifest written to ${manifestPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
