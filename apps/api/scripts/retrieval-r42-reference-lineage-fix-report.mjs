import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { LIVE_SEARCH_QA_QUERIES, buildRetrievalLiveSearchQaReport } from "./retrieval-live-search-qa-utils.mjs";
import { analyzeR40Sensitivity } from "./retrieval-r40-citation-gate-sensitivity-report.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const reportsDir = path.resolve(process.cwd(), "reports");

const fixReportName =
  process.env.RETRIEVAL_R42_REFERENCE_LINEAGE_FIX_REPORT_NAME || "retrieval-r42-reference-lineage-fix-report.json";
const fixMarkdownName =
  process.env.RETRIEVAL_R42_REFERENCE_LINEAGE_FIX_MARKDOWN_NAME || "retrieval-r42-reference-lineage-fix-report.md";
const rerunReportName =
  process.env.RETRIEVAL_R42_RERUN_CITATION_GATE_SENSITIVITY_REPORT_NAME ||
  "retrieval-r42-rerun-citation-gate-sensitivity-report.json";
const rerunMarkdownName =
  process.env.RETRIEVAL_R42_RERUN_CITATION_GATE_SENSITIVITY_MARKDOWN_NAME ||
  "retrieval-r42-rerun-citation-gate-sensitivity-report.md";

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function setDiff(a = [], b = []) {
  const bSet = new Set((b || []).map(String));
  return unique((a || []).map(String).filter((id) => !bSet.has(id)));
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
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

export function resolveCorrectedTrustedDocIds({ r36Manifest, r27Manifest, r34Activation }) {
  const r36Ids = unique((r36Manifest?.baselineTrustedDocIds || []).map(String));
  if (r36Ids.length) {
    return {
      correctedTrustedDocIds: r36Ids,
      lineageSourcesUsed: ["retrieval-r36-next-safe-single-manifest.json"]
    };
  }

  const r27Ids = unique((r27Manifest?.baselineTrustedDocIds || []).map(String));
  const r34Keep = String(r34Activation?.summary?.keepOrRollbackDecision || "") === "keep_batch_active";
  const r34Docs = r34Keep ? unique((r34Activation?.docsActivatedExact || []).map(String)) : [];

  return {
    correctedTrustedDocIds: unique([...r27Ids, ...r34Docs]),
    lineageSourcesUsed: [
      "retrieval-r27-next-manifest.json",
      ...(r34Keep ? ["retrieval-r34-gate-revision-activation-report.json"] : [])
    ]
  };
}

function metricDiff(before = {}, after = {}) {
  const keys = [
    "averageQualityScore",
    "citationTopDocumentShare",
    "lowSignalStructuralShare",
    "outOfCorpusHitQueryCount",
    "zeroTrustedResultQueryCount",
    "provenanceCompletenessAverage",
    "citationAnchorCoverageAverage"
  ];
  const out = {};
  for (const k of keys) {
    out[k] = { before: num(before[k]), after: num(after[k]), delta: Number((num(after[k]) - num(before[k])).toFixed(4)) };
  }
  return out;
}

function metricsFromR34After(r34Live = {}) {
  return {
    source: "r34_kept_state_after",
    averageQualityScore: num(r34Live?.summary?.after?.averageQualityScore),
    citationTopDocumentShare: num(r34Live?.summary?.hardGate?.measured?.citationTopDocumentShare),
    lowSignalStructuralShare: num(r34Live?.summary?.hardGate?.measured?.afterLowSignalStructuralShare),
    outOfCorpusHitQueryCount: num(r34Live?.summary?.after?.outOfCorpusHitQueryCount),
    zeroTrustedResultQueryCount: num(r34Live?.summary?.after?.zeroTrustedResultQueryCount),
    provenanceCompletenessAverage: num(r34Live?.summary?.after?.provenanceCompletenessAverage),
    citationAnchorCoverageAverage: num(r34Live?.summary?.after?.citationAnchorCoverageAverage)
  };
}

function formatFixMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R42 Reference Lineage Fix (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Trusted Set Diff");
  lines.push(
    `- missingInPreviousBrokenReference: ${(report.trustedSetDiff?.missingInPreviousBrokenReference || []).join(", ") || "<none>"}`
  );
  lines.push(`- extraInPreviousBrokenReference: ${(report.trustedSetDiff?.extraInPreviousBrokenReference || []).join(", ") || "<none>"}`);
  lines.push("");
  lines.push("## Recommended Next Step");
  lines.push(`- ${report.recommendedFix}`);
  lines.push(`- reason: ${report.recommendedFixReason}`);
  lines.push(`- canProceedToPolicyDecision: ${report.canProceedToPolicyDecision}`);
  lines.push("");
  lines.push("- Dry-run only. No activation, rollback, runtime gate, or ranking mutation.");
  return `${lines.join("\n")}\n`;
}

function formatRerunMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R42 Rerun Citation Gate Sensitivity (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Recommended Policy");
  lines.push(`- ${report.recommendedPolicy}`);
  lines.push(`- reason: ${report.recommendedPolicyReason}`);
  lines.push("");
  lines.push("## Policy Rows");
  for (const row of report.policyRows || []) {
    lines.push(
      `- ${row.policyId} (${row.citationCeilingResolved}) | baselinePasses=${row.baselinePasses} | currentLivePasses=${row.currentLivePasses} | unlocked=${row.currentlyBlockedCandidatesUnlockedCount}`
    );
  }
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r40, r38, r39, r41, r34Live, r34Report, r36Manifest, r27Manifest, r34Activation] = await Promise.all([
    readJson("retrieval-r40-citation-gate-sensitivity-report.json"),
    readJson("retrieval-r38-single-frontier-refresh-report.json"),
    readJson("retrieval-r39-frontier-blocker-breakdown-report.json"),
    readJson("retrieval-r41-reference-integrity-report.json"),
    readJsonIfExists("retrieval-r34-gate-revision-live-qa-report.json"),
    readJsonIfExists("retrieval-r34-gate-revision-report.json"),
    readJsonIfExists("retrieval-r36-next-safe-single-manifest.json"),
    readJsonIfExists("retrieval-r27-next-manifest.json"),
    readJsonIfExists("retrieval-r34-gate-revision-activation-report.json")
  ]);

  const previousBrokenReference = r40?.currentLiveReference || r38?.baselineLiveMetrics || {};
  const previousBrokenIds = unique(
    r41?.artifactLineage?.trustedLoader?.trustedDocumentIds || r41?.actualCurrentTrustedState?.loaderTrustedDocIds || []
  );

  const lineage = resolveCorrectedTrustedDocIds({ r36Manifest, r27Manifest, r34Activation });
  const correctedTrustedDocIds = lineage.correctedTrustedDocIds;
  const correctedCount = correctedTrustedDocIds.length;

  const correctedFallbackMetrics = metricsFromR34After(r34Live || {});
  let correctedLiveMetrics = { ...correctedFallbackMetrics };
  let liveRecomputeSource = "fallback_from_r34_kept_state";
  let liveRecomputeError = "";
  try {
    const live = await buildRetrievalLiveSearchQaReport({
      apiBase,
      trustedDocumentIds: correctedTrustedDocIds,
      queries: LIVE_SEARCH_QA_QUERIES,
      limit: 20,
      realOnly,
      fetchSearchDebug: (payload) => fetchJson(`${apiBase}/admin/retrieval/debug`, payload)
    });
    correctedLiveMetrics = {
      source: "live_recompute_corrected_lineage",
      averageQualityScore: num(live?.summary?.averageQualityScore),
      citationTopDocumentShare: num(
        (live?.queryResults || [])
          .filter((row) => /citation_/.test(String(row?.queryId || "")))
          .reduce((sum, row, _idx, arr) => sum + num(row?.metrics?.topDocumentShare) / Math.max(1, arr.length), 0)
          .toFixed(4)
      ),
      lowSignalStructuralShare: num(
        (live?.queryResults || [])
          .filter((row) => !/citation_/.test(String(row?.queryId || "")))
          .flatMap((row) => (row?.topResults || []).slice(0, 10))
          .filter((row) => /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(String(row?.chunkType || "").toLowerCase()))
          .length /
          Math.max(
            1,
            (live?.queryResults || [])
              .filter((row) => !/citation_/.test(String(row?.queryId || "")))
              .flatMap((row) => (row?.topResults || []).slice(0, 10)).length
          )
      ),
      outOfCorpusHitQueryCount: num(live?.summary?.outOfCorpusHitQueryCount),
      zeroTrustedResultQueryCount: num(live?.summary?.zeroTrustedResultQueryCount),
      provenanceCompletenessAverage: num(live?.summary?.provenanceCompletenessAverage),
      citationAnchorCoverageAverage: num(live?.summary?.citationAnchorCoverageAverage)
    };
    liveRecomputeSource = "live_recompute";
  } catch (error) {
    liveRecomputeError = error instanceof Error ? error.message : String(error);
  }

  const correctedReference = {
    ...correctedLiveMetrics,
    trustedDocumentCount: correctedCount
  };

  const trustedSetDiff = {
    previousBrokenTrustedDocCount: previousBrokenIds.length,
    correctedTrustedDocCount: correctedCount,
    missingInPreviousBrokenReference: setDiff(correctedTrustedDocIds, previousBrokenIds),
    extraInPreviousBrokenReference: setDiff(previousBrokenIds, correctedTrustedDocIds)
  };

  const r38Corrected = {
    ...r38,
    dataMode: "corrected_lineage_dry_run",
    baselineLiveMetrics: {
      ...r38?.baselineLiveMetrics,
      ...correctedReference
    }
  };

  const rerunRaw = analyzeR40Sensitivity({ r38: r38Corrected, r39, r34Live, r34Report });
  const rerun = {
    ...rerunRaw,
    correctedCurrentLiveReference: rerunRaw.currentLiveReference
  };
  delete rerun.currentLiveReference;

  const canProceedToPolicyDecision = Boolean(
    !trustedSetDiff.missingInPreviousBrokenReference.length && rerun.summary?.recommendedPolicy !== "none"
  );
  const recommendedFix = canProceedToPolicyDecision
    ? "proceed_to_policy_decision_with_corrected_lineage_reference"
    : "rerun_r40_with_corrected_lineage_then_decide_policy";
  const recommendedFixReason = trustedSetDiff.missingInPreviousBrokenReference.length
    ? "trusted lineage corrected but previous broken set still mismatched; rerun needed"
    : "corrected lineage applied and sensitivity result is now decision-ready";

  const fixReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R42",
    summary: {
      previousBrokenTrustedDocCount: trustedSetDiff.previousBrokenTrustedDocCount,
      correctedTrustedDocCount: trustedSetDiff.correctedTrustedDocCount,
      correctedMissingCount: trustedSetDiff.missingInPreviousBrokenReference.length,
      liveRecomputeSource,
      liveRecomputeError,
      canProceedToPolicyDecision
    },
    previousBrokenReference,
    correctedReference,
    trustedSetDiff,
    lineageSourcesUsed: lineage.lineageSourcesUsed,
    metricDiffAfterFix: metricDiff(previousBrokenReference, correctedReference),
    recommendedNextStep: recommendedFix,
    recommendedFix,
    recommendedFixReason,
    canProceedToPolicyDecision
  };

  const rerunReport = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R42",
    summary: rerun.summary,
    policiesEvaluated: rerun.policiesEvaluated,
    baselineReference: rerun.baselineReference,
    correctedCurrentLiveReference: rerun.correctedCurrentLiveReference,
    policyRows: rerun.policyRows,
    recommendedPolicy: rerun.recommendedPolicy,
    recommendedPolicyReason: rerun.recommendedPolicyReason,
    candidateUnlockSummary: rerun.candidateUnlockSummary,
    safestUnlockCandidates: rerun.safestUnlockCandidates,
    unsafePolicyWarnings: rerun.unsafePolicyWarnings
  };

  const fixJsonPath = path.resolve(reportsDir, fixReportName);
  const fixMdPath = path.resolve(reportsDir, fixMarkdownName);
  const rerunJsonPath = path.resolve(reportsDir, rerunReportName);
  const rerunMdPath = path.resolve(reportsDir, rerunMarkdownName);

  await Promise.all([
    fs.writeFile(fixJsonPath, JSON.stringify(fixReport, null, 2)),
    fs.writeFile(fixMdPath, formatFixMarkdown(fixReport)),
    fs.writeFile(rerunJsonPath, JSON.stringify(rerunReport, null, 2)),
    fs.writeFile(rerunMdPath, formatRerunMarkdown(rerunReport))
  ]);

  console.log(
    JSON.stringify(
      {
        correctedTrustedDocCount: trustedSetDiff.correctedTrustedDocCount,
        missingRecoveredCount: trustedSetDiff.missingInPreviousBrokenReference.length,
        recommendedPolicy: rerunReport.recommendedPolicy,
        canProceedToPolicyDecision
      },
      null,
      2
    )
  );
  console.log(`R42 reference-lineage fix report written to ${fixJsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}

