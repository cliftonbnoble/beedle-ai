import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { LIVE_SEARCH_QA_QUERIES, buildRetrievalLiveSearchQaReport, loadTrustedActivatedDocumentIds } from "./retrieval-live-search-qa-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const reportsDir = path.resolve(process.cwd(), "reports");
const outputJsonName =
  process.env.RETRIEVAL_R41_REFERENCE_INTEGRITY_REPORT_NAME || "retrieval-r41-reference-integrity-report.json";
const outputMdName =
  process.env.RETRIEVAL_R41_REFERENCE_INTEGRITY_MARKDOWN_NAME || "retrieval-r41-reference-integrity-report.md";

const METRIC_KEYS = [
  "averageQualityScore",
  "citationTopDocumentShare",
  "lowSignalStructuralShare",
  "outOfCorpusHitQueryCount",
  "zeroTrustedResultQueryCount",
  "provenanceCompletenessAverage",
  "citationAnchorCoverageAverage"
];

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function setDiff(a = [], b = []) {
  const bSet = new Set((b || []).map(String));
  return unique((a || []).map(String).filter((id) => !bSet.has(id)));
}

function numberOrZero(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function metricDelta(a = {}, b = {}) {
  const out = {};
  for (const key of METRIC_KEYS) {
    out[key] = {
      a: numberOrZero(a[key]),
      b: numberOrZero(b[key]),
      delta: Number((numberOrZero(b[key]) - numberOrZero(a[key])).toFixed(4))
    };
  }
  return out;
}

async function readJson(name) {
  const raw = await fs.readFile(path.resolve(reportsDir, name), "utf8");
  return JSON.parse(raw);
}

async function readJsonIfExists(name) {
  try {
    return await readJson(name);
  } catch {
    return null;
  }
}

async function fetchJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON response from ${url}`);
  }
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

function baselineMetricFromR34(r34Live = {}) {
  return {
    averageQualityScore: numberOrZero(r34Live?.summary?.after?.averageQualityScore),
    citationTopDocumentShare: numberOrZero(r34Live?.summary?.hardGate?.measured?.citationTopDocumentShare),
    lowSignalStructuralShare: numberOrZero(r34Live?.summary?.hardGate?.measured?.afterLowSignalStructuralShare),
    outOfCorpusHitQueryCount: numberOrZero(r34Live?.summary?.after?.outOfCorpusHitQueryCount),
    zeroTrustedResultQueryCount: numberOrZero(r34Live?.summary?.after?.zeroTrustedResultQueryCount),
    provenanceCompletenessAverage: numberOrZero(r34Live?.summary?.after?.provenanceCompletenessAverage),
    citationAnchorCoverageAverage: numberOrZero(r34Live?.summary?.after?.citationAnchorCoverageAverage)
  };
}

export function analyzeReferenceIntegrity({
  baselineRefMetrics = {},
  currentRefMetrics = {},
  sourceSets = {},
  liveQaBySource = {}
} = {}) {
  const loaderIds = unique(sourceSets.loaderTrustedDocIds || []);
  const truthIds = unique(sourceSets.sourceOfTruthTrustedDocIds || []);
  const baselineIds = unique(sourceSets.r34ReconstructedTrustedDocIds || []);
  const r36Ids = unique(sourceSets.r36BaselineTrustedDocIds || []);

  const trustedSetMismatchDetected = JSON.stringify(loaderIds) !== JSON.stringify(truthIds);
  const staleArtifactDetected = trustedSetMismatchDetected && loaderIds.length < truthIds.length;
  const mixedRollbackStateDetected =
    Boolean(sourceSets.rollbackVerified) && Boolean(sourceSets.loaderUsedActivationWriteOnly) && trustedSetMismatchDetected;

  const loaderLive = liveQaBySource?.loader || null;
  const truthLive = liveQaBySource?.truth || null;
  const qaInputMismatchDetected =
    Boolean(loaderLive && truthLive) &&
    (numberOrZero(loaderLive.averageQualityScore) !== numberOrZero(truthLive.averageQualityScore) ||
      numberOrZero(loaderLive.outOfCorpusHitQueryCount) !== numberOrZero(truthLive.outOfCorpusHitQueryCount) ||
      numberOrZero(loaderLive.provenanceCompletenessAverage) !== numberOrZero(truthLive.provenanceCompletenessAverage));

  const realRuntimeRegressionDetected =
    Boolean(truthLive) &&
    (numberOrZero(truthLive.outOfCorpusHitQueryCount) > 0 ||
      numberOrZero(truthLive.zeroTrustedResultQueryCount) > 0 ||
      numberOrZero(truthLive.provenanceCompletenessAverage) < 1 ||
      numberOrZero(truthLive.citationAnchorCoverageAverage) < 1 ||
      numberOrZero(truthLive.averageQualityScore) < 64.72);

  const referenceMismatchFindings = [];
  if (staleArtifactDetected) referenceMismatchFindings.push("stale_artifact_selection");
  if (mixedRollbackStateDetected) referenceMismatchFindings.push("mixed_post_rollback_state");
  if (trustedSetMismatchDetected) referenceMismatchFindings.push("trusted_set_mismatch");
  if (qaInputMismatchDetected) referenceMismatchFindings.push("qa_input_mismatch");
  if (realRuntimeRegressionDetected) referenceMismatchFindings.push("real_runtime_regression");

  const sourceOfTruthDecision = truthIds.length
    ? "use_r36_or_r34_kept_lineage_trusted_set"
    : "insufficient_trusted_lineage_data";

  let recommendedFix = "rerun_r40_after_reference_integrity_fix";
  let recommendedFixReason = "align_current_live_reference_to_latest_kept_trusted_lineage_before_policy_sensitivity";
  if (!trustedSetMismatchDetected && !qaInputMismatchDetected) {
    recommendedFix = "no_reference_fix_required";
    recommendedFixReason = "current_live_reference_matches_trusted_lineage";
  } else if (realRuntimeRegressionDetected) {
    recommendedFix = "investigate_runtime_regression_before_r40_rerun";
    recommendedFixReason = "source_of_truth_live_qa_still_fails_non_citation_gates";
  }

  return {
    staleArtifactDetected,
    mixedRollbackStateDetected,
    trustedSetMismatchDetected,
    qaInputMismatchDetected,
    realRuntimeRegressionDetected,
    referenceMismatchFindings,
    sourceOfTruthDecision,
    recommendedFix,
    recommendedFixReason,
    canRerunR40Safely: !realRuntimeRegressionDetected && (trustedSetMismatchDetected || qaInputMismatchDetected),
    metricMismatchBreakdown: {
      baselineVsCurrentReference: metricDelta(baselineRefMetrics, currentRefMetrics),
      truthLiveVsLoaderLive: truthLive && loaderLive ? metricDelta(loaderLive, truthLive) : null
    },
    trustedCounts: {
      loaderTrustedDocCount: loaderIds.length,
      r34ReconstructedTrustedDocCount: baselineIds.length,
      r36BaselineTrustedDocCount: r36Ids.length,
      sourceOfTruthTrustedDocCount: truthIds.length
    },
    trustedIdDiffs: {
      missingInLoaderVsTruth: setDiff(truthIds, loaderIds),
      extraInLoaderVsTruth: setDiff(loaderIds, truthIds)
    }
  };
}

function formatMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R41 Current-Live Reference Integrity Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Mismatch Findings");
  for (const finding of report.referenceMismatchFindings || []) lines.push(`- ${finding}`);
  if (!(report.referenceMismatchFindings || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Trusted Set Diffs");
  lines.push(
    `- missingInLoaderVsTruth: ${(report.actualCurrentTrustedState?.trustedIdDiffs?.missingInLoaderVsTruth || []).join(", ") || "<none>"}`
  );
  lines.push(
    `- extraInLoaderVsTruth: ${(report.actualCurrentTrustedState?.trustedIdDiffs?.extraInLoaderVsTruth || []).join(", ") || "<none>"}`
  );
  lines.push("");
  lines.push("## Recommended Fix");
  lines.push(`- recommendedFix: ${report.recommendedFix}`);
  lines.push(`- recommendedFixReason: ${report.recommendedFixReason}`);
  lines.push(`- canRerunR40Safely: ${report.canRerunR40Safely}`);
  lines.push("");
  lines.push("- Dry-run only. No activation, rollback, or gate mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r40, r38, r34Live, r34Activation, r36Manifest, r27Manifest, r37Activation, r37Live, activationWrite, batchRollback] =
    await Promise.all([
      readJson("retrieval-r40-citation-gate-sensitivity-report.json"),
      readJson("retrieval-r38-single-frontier-refresh-report.json"),
      readJsonIfExists("retrieval-r34-gate-revision-live-qa-report.json"),
      readJsonIfExists("retrieval-r34-gate-revision-activation-report.json"),
      readJsonIfExists("retrieval-r36-next-safe-single-manifest.json"),
      readJsonIfExists("retrieval-r27-next-manifest.json"),
      readJsonIfExists("retrieval-r37-single-activation-report.json"),
      readJsonIfExists("retrieval-r37-single-live-qa-report.json"),
      readJsonIfExists("retrieval-activation-write-report.json"),
      readJsonIfExists("retrieval-batch-rollback-report.json")
    ]);

  const baselineReferenceInputs = {
    artifactFiles: ["retrieval-r34-gate-revision-live-qa-report.json", "retrieval-r40-citation-gate-sensitivity-report.json"],
    metricsUsed: r40?.baselineReference || baselineMetricFromR34(r34Live || {})
  };

  const currentLiveReferenceInputs = {
    artifactFiles: ["retrieval-r38-single-frontier-refresh-report.json", "retrieval-r40-citation-gate-sensitivity-report.json"],
    metricsUsed: r40?.currentLiveReference || r38?.baselineLiveMetrics || {}
  };

  const loaderOut = await loadTrustedActivatedDocumentIds({ reportsDir });
  const loaderTrustedDocIds = unique(loaderOut.trustedDocumentIds || []);

  const r27BaselineTrustedDocIds = unique((r27Manifest?.baselineTrustedDocIds || []).map(String));
  const r34ActivatedExact = unique((r34Activation?.docsActivatedExact || []).map(String));
  const r34Keep = String(r34Activation?.summary?.keepOrRollbackDecision || "") === "keep_batch_active";
  const r34ReconstructedTrustedDocIds = unique([...r27BaselineTrustedDocIds, ...(r34Keep ? r34ActivatedExact : [])]);
  const r36BaselineTrustedDocIds = unique((r36Manifest?.baselineTrustedDocIds || []).map(String));
  const sourceOfTruthTrustedDocIds = r36BaselineTrustedDocIds.length ? r36BaselineTrustedDocIds : r34ReconstructedTrustedDocIds;

  const artifactLineage = {
    retrievalActivationWriteReport: {
      file: "retrieval-activation-write-report.json",
      trustedDocumentCount: Number((activationWrite?.documentsActivated || []).length),
      trustedDocumentIds: unique((activationWrite?.documentsActivated || []).map((row) => row?.documentId))
    },
    retrievalR27Manifest: {
      file: "retrieval-r27-next-manifest.json",
      trustedDocumentCount: r27BaselineTrustedDocIds.length,
      trustedDocumentIds: r27BaselineTrustedDocIds
    },
    retrievalR34Activation: {
      file: "retrieval-r34-gate-revision-activation-report.json",
      keepOrRollbackDecision: String(r34Activation?.summary?.keepOrRollbackDecision || ""),
      docsActivatedExact: r34ActivatedExact
    },
    retrievalR36Manifest: {
      file: "retrieval-r36-next-safe-single-manifest.json",
      trustedDocumentCount: r36BaselineTrustedDocIds.length,
      trustedDocumentIds: r36BaselineTrustedDocIds
    },
    retrievalR37Activation: {
      file: "retrieval-r37-single-activation-report.json",
      keepOrRollbackDecision: String(r37Activation?.summary?.keepOrRollbackDecision || ""),
      rollbackVerificationPassed: Boolean(r37Activation?.rollbackVerificationPassed || false)
    },
    retrievalBatchRollback: {
      file: "retrieval-batch-rollback-report.json",
      rollbackVerificationPassed: Boolean(batchRollback?.summary?.rollbackVerificationPassed || false),
      removedDocumentCount: Number(batchRollback?.summary?.removedDocumentCount || 0)
    },
    trustedLoader: {
      sources: loaderOut.sources || [],
      trustedDocumentCount: loaderTrustedDocIds.length,
      trustedDocumentIds: loaderTrustedDocIds
    }
  };

  let truthLive = null;
  let loaderLive = null;
  const qaErrors = [];
  try {
    const fetchSearchDebug = (payload) => fetchJson(`${apiBase}/admin/retrieval/debug`, payload);
    loaderLive = (
      await buildRetrievalLiveSearchQaReport({
        apiBase,
        trustedDocumentIds: loaderTrustedDocIds,
        queries: LIVE_SEARCH_QA_QUERIES,
        limit: 20,
        realOnly,
        fetchSearchDebug
      })
    )?.summary;
    truthLive = (
      await buildRetrievalLiveSearchQaReport({
        apiBase,
        trustedDocumentIds: sourceOfTruthTrustedDocIds,
        queries: LIVE_SEARCH_QA_QUERIES,
        limit: 20,
        realOnly,
        fetchSearchDebug
      })
    )?.summary;
  } catch (error) {
    qaErrors.push(error instanceof Error ? error.message : String(error));
  }

  const analysis = analyzeReferenceIntegrity({
    baselineRefMetrics: baselineReferenceInputs.metricsUsed,
    currentRefMetrics: currentLiveReferenceInputs.metricsUsed,
    sourceSets: {
      loaderTrustedDocIds,
      r34ReconstructedTrustedDocIds,
      r36BaselineTrustedDocIds,
      sourceOfTruthTrustedDocIds,
      rollbackVerified: Boolean(batchRollback?.summary?.rollbackVerificationPassed || r37Activation?.rollbackVerificationPassed),
      loaderUsedActivationWriteOnly: JSON.stringify(unique(loaderOut.sources || [])) === JSON.stringify(["activation_write_report"])
    },
    liveQaBySource: {
      loader: loaderLive,
      truth: truthLive
    }
  });

  const actualCurrentTrustedState = {
    sourceOfTruthDecision: analysis.sourceOfTruthDecision,
    sourceOfTruthTrustedDocIds,
    sourceOfTruthTrustedDocCount: sourceOfTruthTrustedDocIds.length,
    loaderTrustedDocIds,
    loaderTrustedDocCount: loaderTrustedDocIds.length,
    trustedIdDiffs: analysis.trustedIdDiffs,
    liveQaBySource: {
      loader: loaderLive,
      sourceOfTruth: truthLive
    },
    qaErrors
  };

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R41",
    summary: {
      staleArtifactDetected: analysis.staleArtifactDetected,
      mixedRollbackStateDetected: analysis.mixedRollbackStateDetected,
      trustedSetMismatchDetected: analysis.trustedSetMismatchDetected,
      qaInputMismatchDetected: analysis.qaInputMismatchDetected,
      realRuntimeRegressionDetected: analysis.realRuntimeRegressionDetected
    },
    baselineReferenceInputs,
    currentLiveReferenceInputs,
    actualCurrentTrustedState,
    referenceMismatchFindings: analysis.referenceMismatchFindings,
    metricMismatchBreakdown: analysis.metricMismatchBreakdown,
    artifactLineage,
    recommendedFix: analysis.recommendedFix,
    recommendedFixReason: analysis.recommendedFixReason,
    canRerunR40Safely: analysis.canRerunR40Safely,
    staleArtifactDetected: analysis.staleArtifactDetected,
    mixedRollbackStateDetected: analysis.mixedRollbackStateDetected,
    trustedSetMismatchDetected: analysis.trustedSetMismatchDetected,
    qaInputMismatchDetected: analysis.qaInputMismatchDetected,
    realRuntimeRegressionDetected: analysis.realRuntimeRegressionDetected
  };

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, formatMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        recommendedFix: report.recommendedFix,
        recommendedFixReason: report.recommendedFixReason,
        canRerunR40Safely: report.canRerunR40Safely,
        trustedSetMismatchDetected: report.trustedSetMismatchDetected
      },
      null,
      2
    )
  );
  console.log(`R41 reference integrity report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}

