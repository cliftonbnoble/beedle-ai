import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r55Name = process.env.RETRIEVAL_R56_R55_REPORT_NAME || "retrieval-r55-remediation-experiment-report.json";
const r39Name = process.env.RETRIEVAL_R56_R39_REPORT_NAME || "retrieval-r39-frontier-blocker-breakdown-report.json";
const r53Name = process.env.RETRIEVAL_R56_R53_REPORT_NAME || "retrieval-r53-frontier-policy-lock-report.json";
const outputJsonName = process.env.RETRIEVAL_R56_REPORT_NAME || "retrieval-r56-remediation-validation-report.json";
const outputMdName = process.env.RETRIEVAL_R56_MARKDOWN_NAME || "retrieval-r56-remediation-validation-report.md";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((value) => String(value)))).sort((a, b) => a.localeCompare(b));
}

function countBy(values = []) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCandidates(rows, scoreKey) {
  return rows
    .slice()
    .sort((a, b) => {
      const as = toNumber(a?.[scoreKey], 0);
      const bs = toNumber(b?.[scoreKey], 0);
      if (bs !== as) return bs - as;
      const ac = toNumber(a?.projectedCitationTopDocumentShare, 0);
      const bc = toNumber(b?.projectedCitationTopDocumentShare, 0);
      if (ac !== bc) return ac - bc;
      return String(a.documentId || "").localeCompare(String(b.documentId || ""));
    })
    .map((row, index) => ({ ...row, rank: index + 1 }));
}

function buildFamilyBudgetMap(r55Report) {
  const budgets = {};
  for (const row of r55Report?.familyRiskPatternChanges || []) {
    budgets[String(row?.familyLabel || "unknown")] = toNumber(row?.appliedErrorBudget, 0);
  }
  return budgets;
}

function buildRemediatedRows({ candidateRows, familyBudgets, frozenFamilies, baselineLiveMetrics, qualityFloor }) {
  const effectiveCitationCeiling = toNumber(baselineLiveMetrics?.effectiveCitationCeiling, 0.2);
  const baselineLowSignal = toNumber(baselineLiveMetrics?.lowSignalStructuralShare, 0);

  return (candidateRows || []).map((row) => {
    const familyLabel = String(row?.documentFamilyLabel || "unknown");
    const budget = toNumber(familyBudgets[familyLabel], 0);
    const remediatedProjectedQualityDelta = Number((toNumber(row?.projectedQualityDelta, 0) - budget).toFixed(4));
    const remediatedProjectedAverageQualityScore = Number(
      (toNumber(baselineLiveMetrics?.averageQualityScore, 0) + remediatedProjectedQualityDelta).toFixed(4)
    );
    const frozenFamilyLocked = frozenFamilies.includes(familyLabel);

    const hardChecks = {
      qualityPass: remediatedProjectedAverageQualityScore >= qualityFloor,
      citationPass: toNumber(row?.projectedCitationTopDocumentShare, 0) <= effectiveCitationCeiling,
      lowSignalPass: toNumber(row?.projectedLowSignalStructuralShare, 0) <= baselineLowSignal,
      outOfCorpusPass: toNumber(row?.projectedOutOfCorpusHitQueryCount, 0) === 0,
      zeroTrustedResultPass: toNumber(row?.projectedZeroTrustedResultQueryCount, 0) === 0,
      provenancePass: toNumber(row?.projectedProvenanceCompletenessAverage, 0) === 1,
      anchorPass: toNumber(row?.projectedCitationAnchorCoverageAverage, 0) === 1
    };

    const simulatedSafeWithoutFamilyLock = Object.values(hardChecks).every(Boolean);
    const remediatedKeepOrDoNotActivate =
      simulatedSafeWithoutFamilyLock && !frozenFamilyLocked ? "keep" : "do_not_activate";

    const failingGates = Object.entries(hardChecks)
      .filter(([, ok]) => !ok)
      .map(([gate]) => gate)
      .sort((a, b) => a.localeCompare(b));
    if (frozenFamilyLocked) failingGates.push("frozen_family_policy_lock");

    return {
      documentId: String(row?.documentId || ""),
      title: String(row?.title || ""),
      documentFamilyLabel: familyLabel,
      baselineProjectedQualityDelta: toNumber(row?.projectedQualityDelta, 0),
      remediatedProjectedQualityDelta,
      remediatedProjectedAverageQualityScore,
      projectedCitationTopDocumentShare: toNumber(row?.projectedCitationTopDocumentShare, 0),
      projectedLowSignalStructuralShare: toNumber(row?.projectedLowSignalStructuralShare, 0),
      projectedOutOfCorpusHitQueryCount: toNumber(row?.projectedOutOfCorpusHitQueryCount, 0),
      projectedZeroTrustedResultQueryCount: toNumber(row?.projectedZeroTrustedResultQueryCount, 0),
      projectedProvenanceCompletenessAverage: toNumber(row?.projectedProvenanceCompletenessAverage, 0),
      projectedCitationAnchorCoverageAverage: toNumber(row?.projectedCitationAnchorCoverageAverage, 0),
      baselineKeepOrDoNotActivate: String(row?.keepOrDoNotActivate || "do_not_activate"),
      remediatedKeepOrDoNotActivate,
      frozenFamilyLocked,
      appliedErrorBudget: budget,
      failingGates
    };
  });
}

function buildCandidateOrderingChanges({ baselineRows, remediatedRows }) {
  const baselineRanked = sortCandidates(baselineRows, "projectedQualityDelta");
  const remediatedRanked = sortCandidates(remediatedRows, "remediatedProjectedQualityDelta");

  const baselineRanks = new Map(baselineRanked.map((row) => [String(row.documentId), row.rank]));
  const remediatedRanks = new Map(remediatedRanked.map((row) => [String(row.documentId), row.rank]));

  const deltas = remediatedRanked
    .map((row) => {
      const docId = String(row.documentId);
      const baselineRank = toNumber(baselineRanks.get(docId), 0);
      const remediatedRank = toNumber(remediatedRanks.get(docId), 0);
      return {
        documentId: docId,
        baselineRank,
        remediatedRank,
        rankDelta: baselineRank - remediatedRank
      };
    })
    .filter((row) => row.rankDelta !== 0)
    .sort((a, b) => {
      if (Math.abs(b.rankDelta) !== Math.abs(a.rankDelta)) return Math.abs(b.rankDelta) - Math.abs(a.rankDelta);
      return String(a.documentId).localeCompare(String(b.documentId));
    });

  return deltas;
}

function buildMetricAggregates(rows = []) {
  const total = rows.length || 1;
  const sum = (key) => rows.reduce((acc, row) => acc + toNumber(row?.[key], 0), 0);
  const max = (key) => rows.reduce((acc, row) => Math.max(acc, toNumber(row?.[key], 0)), 0);
  const min = (key) => rows.reduce((acc, row) => Math.min(acc, toNumber(row?.[key], 0)), 1);
  return {
    citationAvg: Number((sum("projectedCitationTopDocumentShare") / total).toFixed(4)),
    citationMax: Number(max("projectedCitationTopDocumentShare").toFixed(4)),
    lowSignalAvg: Number((sum("projectedLowSignalStructuralShare") / total).toFixed(4)),
    lowSignalMax: Number(max("projectedLowSignalStructuralShare").toFixed(4)),
    outOfCorpusAvg: Number((sum("projectedOutOfCorpusHitQueryCount") / total).toFixed(4)),
    zeroTrustedAvg: Number((sum("projectedZeroTrustedResultQueryCount") / total).toFixed(4)),
    provenanceMin: Number(min("projectedProvenanceCompletenessAverage").toFixed(4)),
    anchorMin: Number(min("projectedCitationAnchorCoverageAverage").toFixed(4))
  };
}

export function buildR56RemediationValidation({ r55Report, r39Report, r53Report }) {
  const experimentValidated = r55Report?.experimentImplemented || null;
  const frozenFamilies = unique(r53Report?.frozenFamilies || r55Report?.frozenFamiliesEvaluated || []);
  const familyBudgets = buildFamilyBudgetMap(r55Report);
  const baselineLiveMetrics = {
    averageQualityScore: toNumber(r39Report?.baselineLiveMetrics?.averageQualityScore, 0),
    effectiveCitationCeiling: toNumber(
      r39Report?.baselineLiveMetrics?.effectiveCitationCeiling,
      r39Report?.baselineLiveMetrics?.citationTopDocumentShare || 0.2
    ),
    lowSignalStructuralShare: toNumber(r39Report?.baselineLiveMetrics?.lowSignalStructuralShare, 0)
  };

  const qualityFloor = Number((baselineLiveMetrics.averageQualityScore - 0.5).toFixed(4));
  const baselineRows = Array.isArray(r39Report?.candidateRows) ? r39Report.candidateRows : [];
  const remediatedRows = buildRemediatedRows({
    candidateRows: baselineRows,
    familyBudgets,
    frozenFamilies,
    baselineLiveMetrics,
    qualityFloor
  });

  const baselineFrontierSummary = {
    candidatesScanned: baselineRows.length,
    safeCount: (r39Report?.safeCandidateCount && Number(r39Report.safeCandidateCount)) || 0,
    blockedCount: (r39Report?.blockedCandidateCount && Number(r39Report.blockedCandidateCount)) || baselineRows.length
  };

  const newlySafeSimulatedCandidates = remediatedRows
    .filter((row) => row.remediatedKeepOrDoNotActivate === "keep")
    .map((row) => ({
      documentId: row.documentId,
      familyLabel: row.documentFamilyLabel,
      remediatedProjectedQualityDelta: row.remediatedProjectedQualityDelta,
      projectedCitationTopDocumentShare: row.projectedCitationTopDocumentShare,
      projectedLowSignalStructuralShare: row.projectedLowSignalStructuralShare
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const stillBlockedCandidates = remediatedRows
    .filter((row) => row.remediatedKeepOrDoNotActivate !== "keep")
    .map((row) => ({
      documentId: row.documentId,
      familyLabel: row.documentFamilyLabel,
      failingGates: row.failingGates,
      remediatedProjectedQualityDelta: row.remediatedProjectedQualityDelta
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const remediatedFrontierSummary = {
    safeCount: newlySafeSimulatedCandidates.length,
    blockedCount: stillBlockedCandidates.length,
    blockerCounts: countBy(stillBlockedCandidates.flatMap((row) => row.failingGates))
  };

  const baselineAgg = buildMetricAggregates(baselineRows);
  const remediatedAgg = buildMetricAggregates(remediatedRows);
  const querySafetyChecks = {
    citationConcentrationNoRegression:
      remediatedAgg.citationAvg <= baselineAgg.citationAvg && remediatedAgg.citationMax <= baselineAgg.citationMax,
    lowSignalShareNoRegression:
      remediatedAgg.lowSignalAvg <= baselineAgg.lowSignalAvg && remediatedAgg.lowSignalMax <= baselineAgg.lowSignalMax,
    provenanceCompletenessMaintained: remediatedAgg.provenanceMin >= baselineAgg.provenanceMin && remediatedAgg.provenanceMin === 1,
    citationAnchorCoverageMaintained: remediatedAgg.anchorMin >= baselineAgg.anchorMin && remediatedAgg.anchorMin === 1,
    zeroTrustedResultQueriesMaintained: remediatedAgg.zeroTrustedAvg <= baselineAgg.zeroTrustedAvg,
    outOfCorpusLeakageMaintained: remediatedAgg.outOfCorpusAvg <= baselineAgg.outOfCorpusAvg,
    baselineAggregates: baselineAgg,
    remediatedAggregates: remediatedAgg
  };

  const baselinePredictionError = toNumber(r55Report?.baselinePredictionError, 0);
  const remediatedPredictionError = toNumber(r55Report?.remediatedPredictionError, 0);

  const candidateOrderingChanges = buildCandidateOrderingChanges({
    baselineRows,
    remediatedRows
  });

  const safetyChecksPass = Object.entries(querySafetyChecks)
    .filter(([, value]) => typeof value === "boolean")
    .every(([, value]) => value);
  const recommendedNextStep =
    newlySafeSimulatedCandidates.length > 0 && safetyChecksPass
      ? "proceed_to_controlled_dry_run_activation_rehearsal_for_top_remediated_candidate"
      : "no_safe_reopen_under_experiment_1_try_next_r54_experiment";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R56",
    summary: {
      candidatesScanned: baselineRows.length,
      newlySafeSimulatedCandidateCount: newlySafeSimulatedCandidates.length,
      stillBlockedCandidateCount: stillBlockedCandidates.length,
      safetyChecksPass
    },
    experimentValidated,
    baselineFrontierSummary,
    remediatedFrontierSummary,
    baselinePredictionError,
    remediatedPredictionError,
    candidateOrderingChanges,
    newlySafeSimulatedCandidates,
    stillBlockedCandidates,
    querySafetyChecks,
    recommendedNextStep
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R56 Remediation Validation (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push(`- experimentValidated: ${report.experimentValidated?.experimentId || ""}`);
  lines.push(`- baselinePredictionError: ${report.baselinePredictionError}`);
  lines.push(`- remediatedPredictionError: ${report.remediatedPredictionError}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Baseline Frontier Summary");
  for (const [key, value] of Object.entries(report.baselineFrontierSummary || {})) lines.push(`- ${key}: ${value}`);
  lines.push("");

  lines.push("## Remediated Frontier Summary");
  for (const [key, value] of Object.entries(report.remediatedFrontierSummary || {})) {
    if (typeof value === "object" && value !== null) lines.push(`- ${key}: ${JSON.stringify(value)}`);
    else lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Query Safety Checks");
  for (const [key, value] of Object.entries(report.querySafetyChecks || {})) lines.push(`- ${key}: ${value}`);
  lines.push("");

  lines.push("## Newly Safe Simulated Candidates");
  for (const row of report.newlySafeSimulatedCandidates || []) {
    lines.push(
      `- ${row.documentId} | family=${row.familyLabel} | remediatedDelta=${row.remediatedProjectedQualityDelta} | citationShare=${row.projectedCitationTopDocumentShare} | lowSignal=${row.projectedLowSignalStructuralShare}`
    );
  }
  if (!(report.newlySafeSimulatedCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Candidate Ordering Changes");
  for (const row of report.candidateOrderingChanges || []) {
    lines.push(`- ${row.documentId}: baselineRank=${row.baselineRank}, remediatedRank=${row.remediatedRank}, rankDelta=${row.rankDelta}`);
  }
  if (!(report.candidateOrderingChanges || []).length) lines.push("- none");
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, trust, admission, provenance, or runtime ranking mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r55, r39, r53] = await Promise.all([
    readJson(path.resolve(reportsDir, r55Name)),
    readJson(path.resolve(reportsDir, r39Name)),
    readJson(path.resolve(reportsDir, r53Name))
  ]);

  const report = buildR56RemediationValidation({ r55Report: r55, r39Report: r39, r53Report: r53 });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        experimentValidated: report.experimentValidated?.experimentId || "",
        newlySafeSimulatedCandidateCount: report.summary.newlySafeSimulatedCandidateCount,
        stillBlockedCandidateCount: report.summary.stillBlockedCandidateCount,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R56 remediation validation report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
