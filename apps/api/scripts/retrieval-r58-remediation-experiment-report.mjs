import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r54Name = process.env.RETRIEVAL_R58_R54_REPORT_NAME || "retrieval-r54-remediation-plan-report.json";
const r39Name = process.env.RETRIEVAL_R58_R39_REPORT_NAME || "retrieval-r39-frontier-blocker-breakdown-report.json";
const r48Name = process.env.RETRIEVAL_R58_R48_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const r52Name = process.env.RETRIEVAL_R58_R52_REPORT_NAME || "retrieval-r52-frozen-family-postmortem-report.json";
const r53Name = process.env.RETRIEVAL_R58_R53_REPORT_NAME || "retrieval-r53-frontier-policy-lock-report.json";
const outputJsonName = process.env.RETRIEVAL_R58_REPORT_NAME || "retrieval-r58-remediation-experiment-report.json";
const outputMdName = process.env.RETRIEVAL_R58_MARKDOWN_NAME || "retrieval-r58-remediation-experiment-report.md";

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

function avg(values = []) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + Number(value || 0), 0) / values.length).toFixed(4));
}

function pickThirdExperiment(r54Report) {
  return (r54Report?.recommendedExperiments || [])[2] || null;
}

function sortRows(rows, scoreKey) {
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

function buildCandidateOrderingChanges({ baselineRows, remediatedRows }) {
  const baselineRanked = sortRows(baselineRows, "projectedQualityDelta");
  const remediatedRanked = sortRows(remediatedRows, "remediatedProjectedQualityDelta");
  const baselineRanks = new Map(baselineRanked.map((row) => [String(row.documentId || ""), row.rank]));
  const remediatedRanks = new Map(remediatedRanked.map((row) => [String(row.documentId || ""), row.rank]));
  return remediatedRanked
    .map((row) => {
      const docId = String(row.documentId || "");
      const baselineRank = toNumber(baselineRanks.get(docId), 0);
      const remediatedRank = toNumber(remediatedRanks.get(docId), 0);
      return { documentId: docId, baselineRank, remediatedRank, rankDelta: baselineRank - remediatedRank };
    })
    .filter((row) => row.rankDelta !== 0)
    .sort((a, b) => {
      if (Math.abs(b.rankDelta) !== Math.abs(a.rankDelta)) return Math.abs(b.rankDelta) - Math.abs(a.rankDelta);
      return String(a.documentId).localeCompare(String(b.documentId));
    });
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

function buildKnownRows({ r48Report, r52Report, byDocId }) {
  const out = new Map();
  for (const row of r48Report?.simulationVsRealityRows || []) {
    if (row?.actualKnownQualityDelta === null || row?.actualKnownQualityDelta === undefined) continue;
    const docId = String(row?.documentId || "");
    out.set(docId, {
      documentId: docId,
      familyLabel: String(row?.documentFamilyLabel || "unknown"),
      simulatedQualityDelta: toNumber(row?.simulatedQualityDelta, 0),
      actualQualityDeltaIfKnown: toNumber(row?.actualKnownQualityDelta, 0)
    });
  }
  const r52Family = String(r52Report?.frozenFamilyLabel || "");
  for (const row of r52Report?.candidateRows || []) {
    if (row?.actualQualityDeltaIfKnown === null || row?.actualQualityDeltaIfKnown === undefined) continue;
    const docId = String(row?.documentId || "");
    const existing = out.get(docId) || {};
    out.set(docId, {
      documentId: docId,
      familyLabel: r52Family || existing.familyLabel || "unknown",
      simulatedQualityDelta: toNumber(row?.simulatedQualityDelta, toNumber(existing.simulatedQualityDelta, 0)),
      actualQualityDeltaIfKnown: toNumber(row?.actualQualityDeltaIfKnown, 0)
    });
  }

  return Array.from(out.values())
    .map((row) => ({ ...row, frontier: byDocId.get(String(row.documentId || "")) || null }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));
}

function deriveInsertionModel({ knownRows, frozenFamilies }) {
  const knownErrors = knownRows.map((row) =>
    Math.max(0, toNumber(row.simulatedQualityDelta, 0) - toNumber(row.actualQualityDeltaIfKnown, 0))
  );
  const avgOverPrediction = avg(knownErrors);
  const basePenalty = Number((avgOverPrediction / 10).toFixed(4));

  return {
    basePenalty,
    citationWeight: 0.8,
    lowSignalWeight: 1.2,
    frozenFamilyWeight: 0.3,
    frozenFamilies: unique(frozenFamilies || [])
  };
}

function insertionPenalty({ row, model }) {
  const citation = toNumber(row?.projectedCitationTopDocumentShare, 0);
  const lowSignal = toNumber(row?.projectedLowSignalStructuralShare, 0);
  const familyLabel = String(row?.documentFamilyLabel || "unknown");
  const frozen = model.frozenFamilies.includes(familyLabel) ? model.frozenFamilyWeight : 0;
  return Number((model.basePenalty + model.citationWeight * citation + model.lowSignalWeight * lowSignal + frozen).toFixed(4));
}

export function buildR58RemediationExperiment({ r54Report, r39Report, r48Report, r52Report, r53Report }) {
  const experimentImplemented = pickThirdExperiment(r54Report);
  if (!experimentImplemented || experimentImplemented.experimentId !== "r54_exp_03_insertion_effect_probe") {
    throw new Error("R58 requires third R54 experiment r54_exp_03_insertion_effect_probe");
  }

  const baselineRows = Array.isArray(r39Report?.candidateRows) ? r39Report.candidateRows : [];
  const byDocId = new Map(baselineRows.map((row) => [String(row?.documentId || ""), row]));
  const frozenFamilies = unique(r53Report?.frozenFamilies || r54Report?.frozenFamilies || []);
  const knownRows = buildKnownRows({ r48Report, r52Report, byDocId });
  const insertionModel = deriveInsertionModel({ knownRows, frozenFamilies });

  const baselineLiveMetrics = {
    averageQualityScore: toNumber(r39Report?.baselineLiveMetrics?.averageQualityScore, 0),
    effectiveCitationCeiling: toNumber(r39Report?.baselineLiveMetrics?.effectiveCitationCeiling, 0.2),
    lowSignalStructuralShare: toNumber(r39Report?.baselineLiveMetrics?.lowSignalStructuralShare, 0)
  };
  const qualityFloor = Number((baselineLiveMetrics.averageQualityScore - 0.5).toFixed(4));

  const remediatedRows = baselineRows.map((row) => {
    const penalty = insertionPenalty({ row, model: insertionModel });
    const remediatedProjectedQualityDelta = Number((toNumber(row?.projectedQualityDelta, 0) - penalty).toFixed(4));
    const remediatedProjectedAverageQualityScore = Number(
      (baselineLiveMetrics.averageQualityScore + remediatedProjectedQualityDelta).toFixed(4)
    );

    const hardChecks = {
      qualityPass: remediatedProjectedAverageQualityScore >= qualityFloor,
      citationPass: toNumber(row?.projectedCitationTopDocumentShare, 0) <= baselineLiveMetrics.effectiveCitationCeiling,
      lowSignalPass: toNumber(row?.projectedLowSignalStructuralShare, 0) <= baselineLiveMetrics.lowSignalStructuralShare,
      outOfCorpusPass: toNumber(row?.projectedOutOfCorpusHitQueryCount, 0) === 0,
      zeroTrustedResultPass: toNumber(row?.projectedZeroTrustedResultQueryCount, 0) === 0,
      provenancePass: toNumber(row?.projectedProvenanceCompletenessAverage, 0) === 1,
      anchorPass: toNumber(row?.projectedCitationAnchorCoverageAverage, 0) === 1
    };
    const remediatedKeepOrDoNotActivate = Object.values(hardChecks).every(Boolean) ? "keep" : "do_not_activate";
    const failingGates = Object.entries(hardChecks)
      .filter(([, value]) => !value)
      .map(([key]) => key)
      .sort((a, b) => a.localeCompare(b));

    return {
      ...row,
      appliedInsertionPenalty: penalty,
      remediatedProjectedQualityDelta,
      remediatedProjectedAverageQualityScore,
      remediatedKeepOrDoNotActivate,
      failingGates
    };
  });

  const knownWithErrors = knownRows.filter((row) => row.actualQualityDeltaIfKnown !== null);
  const baselinePredictionError = Number(
    avg(
      knownWithErrors.map((row) => Math.abs(toNumber(row.simulatedQualityDelta, 0) - toNumber(row.actualQualityDeltaIfKnown, 0)))
    ).toFixed(4)
  );
  const remediatedPredictionError = Number(
    avg(
      knownWithErrors.map((row) => {
        const frontierRow = row.frontier || {};
        const adjustedPred = toNumber(row.simulatedQualityDelta, 0) - insertionPenalty({ row: frontierRow, model: insertionModel });
        return Math.abs(adjustedPred - toNumber(row.actualQualityDeltaIfKnown, 0));
      })
    ).toFixed(4)
  );

  const baselineFrontierSummary = {
    candidatesScanned: baselineRows.length,
    safeCount: toNumber(r39Report?.safeCandidateCount, 0),
    blockedCount: toNumber(r39Report?.blockedCandidateCount, baselineRows.length)
  };
  const newlySafeSimulatedCandidates = remediatedRows
    .filter((row) => String(row?.keepOrDoNotActivate || "do_not_activate") !== "keep")
    .filter((row) => row.remediatedKeepOrDoNotActivate === "keep")
    .map((row) => ({
      documentId: String(row.documentId || ""),
      title: String(row.title || ""),
      familyLabel: String(row.documentFamilyLabel || ""),
      remediatedProjectedQualityDelta: row.remediatedProjectedQualityDelta,
      appliedInsertionPenalty: row.appliedInsertionPenalty
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const stillBlockedCandidates = remediatedRows
    .filter((row) => row.remediatedKeepOrDoNotActivate !== "keep")
    .map((row) => ({
      documentId: String(row.documentId || ""),
      title: String(row.title || ""),
      familyLabel: String(row.documentFamilyLabel || ""),
      failingGates: unique(row.failingGates || []),
      remediatedProjectedQualityDelta: row.remediatedProjectedQualityDelta
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const remediatedFrontierSummary = {
    safeCount: newlySafeSimulatedCandidates.length,
    blockedCount: stillBlockedCandidates.length,
    blockerCounts: stillBlockedCandidates.reduce((acc, row) => {
      for (const gate of row.failingGates || []) acc[gate] = (acc[gate] || 0) + 1;
      return acc;
    }, {})
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
      : "stop_activation_work_and_move_to_model_ranking_redesign";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R58",
    summary: {
      candidatesScanned: baselineRows.length,
      newlySafeSimulatedCandidateCount: newlySafeSimulatedCandidates.length,
      stillBlockedCandidateCount: stillBlockedCandidates.length,
      insertionModel
    },
    experimentImplemented,
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
  lines.push("# Retrieval R58 Remediation Experiment (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) {
    if (typeof value === "object" && value !== null) lines.push(`- ${key}: ${JSON.stringify(value)}`);
    else lines.push(`- ${key}: ${value}`);
  }
  lines.push(`- experimentImplemented: ${report.experimentImplemented?.experimentId || ""}`);
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
  for (const [key, value] of Object.entries(report.querySafetyChecks || {})) {
    if (typeof value === "object" && value !== null) lines.push(`- ${key}: ${JSON.stringify(value)}`);
    else lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Newly Safe Simulated Candidates");
  for (const row of report.newlySafeSimulatedCandidates || []) {
    lines.push(
      `- ${row.documentId} | family=${row.familyLabel} | remediatedDelta=${row.remediatedProjectedQualityDelta} | insertionPenalty=${row.appliedInsertionPenalty}`
    );
  }
  if (!(report.newlySafeSimulatedCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, trust, admission, provenance, or runtime ranking mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r54, r39, r48, r52, r53] = await Promise.all([
    readJson(path.resolve(reportsDir, r54Name)),
    readJson(path.resolve(reportsDir, r39Name)),
    readJson(path.resolve(reportsDir, r48Name)),
    readJson(path.resolve(reportsDir, r52Name)),
    readJson(path.resolve(reportsDir, r53Name))
  ]);

  const report = buildR58RemediationExperiment({
    r54Report: r54,
    r39Report: r39,
    r48Report: r48,
    r52Report: r52,
    r53Report: r53
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        experimentImplemented: report.experimentImplemented?.experimentId || "",
        newlySafeSimulatedCandidateCount: report.summary.newlySafeSimulatedCandidateCount,
        stillBlockedCandidateCount: report.summary.stillBlockedCandidateCount,
        baselinePredictionError: report.baselinePredictionError,
        remediatedPredictionError: report.remediatedPredictionError,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R58 remediation experiment report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
