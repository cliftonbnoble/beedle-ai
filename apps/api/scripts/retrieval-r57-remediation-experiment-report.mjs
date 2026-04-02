import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r54Name = process.env.RETRIEVAL_R57_R54_REPORT_NAME || "retrieval-r54-remediation-plan-report.json";
const r39Name = process.env.RETRIEVAL_R57_R39_REPORT_NAME || "retrieval-r39-frontier-blocker-breakdown-report.json";
const r48Name = process.env.RETRIEVAL_R57_R48_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const r52Name = process.env.RETRIEVAL_R57_R52_REPORT_NAME || "retrieval-r52-frozen-family-postmortem-report.json";
const r53Name = process.env.RETRIEVAL_R57_R53_REPORT_NAME || "retrieval-r53-frontier-policy-lock-report.json";
const r46Name = process.env.RETRIEVAL_R57_R46_REPORT_NAME || "retrieval-r46-single-frontier-report.json";
const outputJsonName = process.env.RETRIEVAL_R57_REPORT_NAME || "retrieval-r57-remediation-experiment-report.json";
const outputMdName = process.env.RETRIEVAL_R57_MARKDOWN_NAME || "retrieval-r57-remediation-experiment-report.md";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function readJsonIfExists(filePath) {
  try {
    return await readJson(filePath);
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") return null;
    throw error;
  }
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

function pickSecondExperiment(r54Report) {
  return (r54Report?.recommendedExperiments || [])[1] || null;
}

function isCitationSensitive(row = {}) {
  const regressionSignals = (row?.regressionSignals || []).map(String);
  const improvementSignals = (row?.improvementSignals || []).map(String);
  return (
    regressionSignals.some((value) => value.includes("citation")) ||
    improvementSignals.some((value) => value.includes("citation")) ||
    (row?.blockerFamilies || []).map(String).includes("citation_concentration_above_effective_ceiling")
  );
}

function citationRiskPenalty({ row, effectiveCitationCeiling, frozenFamilies }) {
  const projectedShare = toNumber(row?.projectedCitationTopDocumentShare, 0);
  const ratio = effectiveCitationCeiling > 0 ? projectedShare / effectiveCitationCeiling : 0;
  const ratioPenalty = Math.max(0, ratio - 1) * 1.5;
  const blockerPenalty = (row?.blockerFamilies || []).map(String).includes("citation_concentration_above_effective_ceiling") ? 0.6 : 0;
  const signalPenalty = isCitationSensitive(row) ? 0.4 : 0;
  const frozenPenalty = frozenFamilies.includes(String(row?.documentFamilyLabel || "")) ? 0.3 : 0;
  return Number((ratioPenalty + blockerPenalty + signalPenalty + frozenPenalty).toFixed(4));
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
  const baselineRank = new Map(baselineRanked.map((row) => [String(row.documentId), row.rank]));
  const remediatedRank = new Map(remediatedRanked.map((row) => [String(row.documentId), row.rank]));
  return remediatedRanked
    .map((row) => {
      const docId = String(row.documentId);
      const b = toNumber(baselineRank.get(docId), 0);
      const r = toNumber(remediatedRank.get(docId), 0);
      return { documentId: docId, baselineRank: b, remediatedRank: r, rankDelta: b - r };
    })
    .filter((row) => row.rankDelta !== 0)
    .sort((a, b) => {
      if (Math.abs(b.rankDelta) !== Math.abs(a.rankDelta)) return Math.abs(b.rankDelta) - Math.abs(a.rankDelta);
      return String(a.documentId).localeCompare(String(b.documentId));
    });
}

function buildKnownRows({ r48Report, r52Report, frozenFamilies, byDocId }) {
  const merged = new Map();
  for (const row of r48Report?.simulationVsRealityRows || []) {
    const docId = String(row?.documentId || "");
    const familyLabel = String(row?.documentFamilyLabel || "unknown");
    if (!frozenFamilies.includes(familyLabel)) continue;
    merged.set(docId, {
      documentId: docId,
      familyLabel,
      simulatedQualityDelta: toNumber(row?.simulatedQualityDelta, 0),
      actualQualityDeltaIfKnown:
        row?.actualKnownQualityDelta === null || row?.actualKnownQualityDelta === undefined
          ? null
          : toNumber(row?.actualKnownQualityDelta, 0)
    });
  }
  const r52Family = String(r52Report?.frozenFamilyLabel || "");
  if (frozenFamilies.includes(r52Family)) {
    for (const row of r52Report?.candidateRows || []) {
      const docId = String(row?.documentId || "");
      const existing = merged.get(docId) || {};
      merged.set(docId, {
        documentId: docId,
        familyLabel: r52Family || existing.familyLabel || "unknown",
        simulatedQualityDelta: toNumber(row?.simulatedQualityDelta, toNumber(existing.simulatedQualityDelta, 0)),
        actualQualityDeltaIfKnown:
          row?.actualQualityDeltaIfKnown === null || row?.actualQualityDeltaIfKnown === undefined
            ? existing.actualQualityDeltaIfKnown ?? null
            : toNumber(row?.actualQualityDeltaIfKnown, 0)
      });
    }
  }

  return Array.from(merged.values())
    .map((row) => ({
      ...row,
      frontier: byDocId.get(String(row.documentId)) || null
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));
}

export function buildR57RemediationExperiment({
  r54Report,
  r39Report,
  r48Report,
  r52Report,
  r53Report,
  r46Report
}) {
  const experimentImplemented = pickSecondExperiment(r54Report);
  if (!experimentImplemented || experimentImplemented.experimentId !== "r54_exp_02_citation_intent_risk_feature") {
    throw new Error("R57 requires second R54 experiment r54_exp_02_citation_intent_risk_feature");
  }

  const frozenFamilies = unique(r53Report?.frozenFamilies || r54Report?.frozenFamilies || []);
  const baselineRows = Array.isArray(r39Report?.candidateRows) ? r39Report.candidateRows : [];
  const byDocId = new Map(baselineRows.map((row) => [String(row?.documentId || ""), row]));

  const baselineLiveMetrics = {
    averageQualityScore: toNumber(r39Report?.baselineLiveMetrics?.averageQualityScore, 0),
    effectiveCitationCeiling: toNumber(
      r39Report?.baselineLiveMetrics?.effectiveCitationCeiling,
      r39Report?.baselineLiveMetrics?.citationTopDocumentShare || 0.2
    ),
    lowSignalStructuralShare: toNumber(r39Report?.baselineLiveMetrics?.lowSignalStructuralShare, 0)
  };
  const qualityFloor = Number((baselineLiveMetrics.averageQualityScore - 0.5).toFixed(4));

  const remediatedRows = baselineRows.map((row) => {
    const penalty = citationRiskPenalty({
      row,
      effectiveCitationCeiling: baselineLiveMetrics.effectiveCitationCeiling,
      frozenFamilies
    });
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
      remediatedProjectedQualityDelta,
      remediatedProjectedAverageQualityScore,
      appliedCitationRiskPenalty: penalty,
      remediatedKeepOrDoNotActivate,
      failingGates
    };
  });

  const knownRows = buildKnownRows({
    r48Report,
    r52Report,
    frozenFamilies,
    byDocId
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
        const frontierPenalty = toNumber(row.frontier?.projectedCitationTopDocumentShare, 0)
          ? citationRiskPenalty({
              row: row.frontier,
              effectiveCitationCeiling: baselineLiveMetrics.effectiveCitationCeiling,
              frozenFamilies
            })
          : 0;
        const remediatedPred = toNumber(row.simulatedQualityDelta, 0) - frontierPenalty;
        return Math.abs(remediatedPred - toNumber(row.actualQualityDeltaIfKnown, 0));
      })
    ).toFixed(4)
  );

  const baselineFrontierSummary = {
    candidatesScanned: baselineRows.length,
    safeCount: toNumber(r39Report?.safeCandidateCount, 0),
    blockedCount: toNumber(r39Report?.blockedCandidateCount, baselineRows.length)
  };
  const newlySafeSimulatedCandidates = remediatedRows
    .filter((row) => String(row.keepOrDoNotActivate || "do_not_activate") !== "keep")
    .filter((row) => row.remediatedKeepOrDoNotActivate === "keep")
    .map((row) => ({
      documentId: String(row.documentId || ""),
      title: String(row.title || ""),
      familyLabel: String(row.documentFamilyLabel || ""),
      remediatedProjectedQualityDelta: row.remediatedProjectedQualityDelta,
      appliedCitationRiskPenalty: row.appliedCitationRiskPenalty
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
    blockerCounts: Object.fromEntries(
      Object.entries(
        stillBlockedCandidates.reduce((acc, row) => {
          for (const gate of row.failingGates || []) acc[gate] = (acc[gate] || 0) + 1;
          return acc;
        }, {})
      ).sort((a, b) => a[0].localeCompare(b[0]))
    )
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

  const simulatedNextSafeCandidatesFromR46 = (r46Report?.safeSingleCandidates || []).map((row) => String(row?.documentId || ""));
  const affectedR46Candidates = remediatedRows
    .filter((row) => simulatedNextSafeCandidatesFromR46.includes(String(row.documentId || "")))
    .map((row) => ({
      documentId: String(row.documentId || ""),
      baselineProjectedQualityDelta: toNumber(row.projectedQualityDelta, 0),
      remediatedProjectedQualityDelta: toNumber(row.remediatedProjectedQualityDelta, 0),
      appliedCitationRiskPenalty: toNumber(row.appliedCitationRiskPenalty, 0),
      remediatedKeepOrDoNotActivate: String(row.remediatedKeepOrDoNotActivate || "do_not_activate")
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const safetyChecksPass = Object.entries(querySafetyChecks)
    .filter(([, value]) => typeof value === "boolean")
    .every(([, value]) => value);
  const recommendedNextStep =
    newlySafeSimulatedCandidates.length > 0 && safetyChecksPass
      ? "proceed_to_controlled_dry_run_activation_rehearsal_for_top_remediated_candidate"
      : "no_safe_reopen_under_experiment_2_move_to_r54_experiment_3";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R57",
    summary: {
      candidatesScanned: baselineRows.length,
      simulatedNextSafeCandidatesFromR46Count: simulatedNextSafeCandidatesFromR46.length,
      affectedR46CandidatesCount: affectedR46Candidates.length,
      newlySafeSimulatedCandidateCount: newlySafeSimulatedCandidates.length
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
  lines.push("# Retrieval R57 Remediation Experiment (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
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
      `- ${row.documentId} | family=${row.familyLabel} | remediatedDelta=${row.remediatedProjectedQualityDelta} | penalty=${row.appliedCitationRiskPenalty}`
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
  const [r54, r39, r48, r52, r53, r46] = await Promise.all([
    readJson(path.resolve(reportsDir, r54Name)),
    readJson(path.resolve(reportsDir, r39Name)),
    readJson(path.resolve(reportsDir, r48Name)),
    readJson(path.resolve(reportsDir, r52Name)),
    readJson(path.resolve(reportsDir, r53Name)),
    readJsonIfExists(path.resolve(reportsDir, r46Name))
  ]);

  const report = buildR57RemediationExperiment({
    r54Report: r54,
    r39Report: r39,
    r48Report: r48,
    r52Report: r52,
    r53Report: r53,
    r46Report: r46
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        experimentImplemented: report.experimentImplemented?.experimentId || "",
        newlySafeSimulatedCandidateCount: report.summary.newlySafeSimulatedCandidateCount,
        stillBlockedCandidateCount: report.remediatedFrontierSummary.blockedCount,
        baselinePredictionError: report.baselinePredictionError,
        remediatedPredictionError: report.remediatedPredictionError,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R57 remediation experiment report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
