import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r54Name = process.env.RETRIEVAL_R55_R54_REPORT_NAME || "retrieval-r54-remediation-plan-report.json";
const r52Name = process.env.RETRIEVAL_R55_R52_REPORT_NAME || "retrieval-r52-frozen-family-postmortem-report.json";
const r48Name = process.env.RETRIEVAL_R55_R48_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const outputJsonName = process.env.RETRIEVAL_R55_REPORT_NAME || "retrieval-r55-remediation-experiment-report.json";
const outputMdName = process.env.RETRIEVAL_R55_MARKDOWN_NAME || "retrieval-r55-remediation-experiment-report.md";

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

function pickFirstExperiment(r54Report) {
  return (r54Report?.recommendedExperiments || [])[0] || null;
}

function buildFrozenCandidateRows({ r48Report, r52Report, frozenFamilies }) {
  const rows = new Map();

  for (const row of r48Report?.simulationVsRealityRows || []) {
    const familyLabel = String(row?.documentFamilyLabel || "unknown");
    if (!frozenFamilies.includes(familyLabel)) continue;
    const docId = String(row?.documentId || "");
    rows.set(docId, {
      documentId: docId,
      familyLabel,
      simulatedQualityDelta: toNumber(row?.simulatedQualityDelta, 0),
      actualQualityDeltaIfKnown:
        row?.actualKnownQualityDelta === null || row?.actualKnownQualityDelta === undefined
          ? null
          : toNumber(row?.actualKnownQualityDelta, 0),
      source: "r48"
    });
  }

  const r52Family = String(r52Report?.frozenFamilyLabel || "");
  const includeR52Family = frozenFamilies.includes(r52Family);
  if (includeR52Family) {
    for (const row of r52Report?.candidateRows || []) {
      const docId = String(row?.documentId || "");
      const existing = rows.get(docId) || {};
      rows.set(docId, {
        documentId: docId,
        familyLabel: r52Family || String(existing.familyLabel || "unknown"),
        simulatedQualityDelta: toNumber(row?.simulatedQualityDelta, toNumber(existing.simulatedQualityDelta, 0)),
        actualQualityDeltaIfKnown:
          row?.actualQualityDeltaIfKnown === null || row?.actualQualityDeltaIfKnown === undefined
            ? existing.actualQualityDeltaIfKnown ?? null
            : toNumber(row?.actualQualityDeltaIfKnown, 0),
        source: existing.source ? `${existing.source}+r52` : "r52"
      });
    }
  }

  return Array.from(rows.values()).sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));
}

function computeFamilyErrorBudgets(candidateRows, frozenFamilies) {
  const knownRows = candidateRows.filter((row) => row.actualQualityDeltaIfKnown !== null);
  const globalBudget = avg(
    knownRows.map((row) => Math.max(0, toNumber(row.simulatedQualityDelta, 0) - toNumber(row.actualQualityDeltaIfKnown, 0)))
  );

  const budgets = {};
  for (const family of frozenFamilies) {
    const familyKnown = knownRows.filter((row) => String(row.familyLabel) === String(family));
    budgets[family] = familyKnown.length
      ? avg(familyKnown.map((row) => Math.max(0, toNumber(row.simulatedQualityDelta, 0) - toNumber(row.actualQualityDeltaIfKnown, 0))))
      : globalBudget;
  }
  return { budgets, globalBudget };
}

function applyExperiment({ experiment, candidateRows, errorBudgets }) {
  if (!experiment || experiment.experimentId !== "r54_exp_01_predictor_error_budget") {
    throw new Error("R55 requires first R54 experiment r54_exp_01_predictor_error_budget");
  }

  return candidateRows.map((row) => {
    const familyBudget = toNumber(errorBudgets[row.familyLabel], toNumber(errorBudgets.globalBudget, 0));
    const remediatedPredictedQualityDelta = Number((toNumber(row.simulatedQualityDelta, 0) - familyBudget).toFixed(4));
    const baselinePredictionErrorIfKnown =
      row.actualQualityDeltaIfKnown === null
        ? null
        : Number(Math.abs(toNumber(row.simulatedQualityDelta, 0) - toNumber(row.actualQualityDeltaIfKnown, 0)).toFixed(4));
    const remediatedPredictionErrorIfKnown =
      row.actualQualityDeltaIfKnown === null
        ? null
        : Number(Math.abs(remediatedPredictedQualityDelta - toNumber(row.actualQualityDeltaIfKnown, 0)).toFixed(4));

    const statusBefore = toNumber(row.simulatedQualityDelta, 0) > 0 ? "predicted_gain" : "predicted_non_gain";
    const statusAfter = remediatedPredictedQualityDelta > 0 ? "predicted_gain" : "predicted_non_gain";

    return {
      ...row,
      appliedErrorBudget: familyBudget,
      remediatedPredictedQualityDelta,
      baselinePredictionErrorIfKnown,
      remediatedPredictionErrorIfKnown,
      statusBefore,
      statusAfter
    };
  });
}

function buildQueryRegressionChanges({ queryRegressionPatterns, candidatesWithRemediation }) {
  const citationLikeQueries = (queryRegressionPatterns || [])
    .filter((row) => /citation_/i.test(String(row?.queryId || "")))
    .map((row) => String(row.queryId));

  const baselineRiskScore = Number(
    candidatesWithRemediation
      .map((row) => Math.max(0, toNumber(row.simulatedQualityDelta, 0)))
      .reduce((sum, value) => sum + value, 0)
      .toFixed(4)
  );
  const remediatedRiskScore = Number(
    candidatesWithRemediation
      .map((row) => Math.max(0, toNumber(row.remediatedPredictedQualityDelta, 0)))
      .reduce((sum, value) => sum + value, 0)
      .toFixed(4)
  );

  return {
    citationQueryIds: citationLikeQueries,
    baselineCitationRiskScore: baselineRiskScore,
    remediatedCitationRiskScore: remediatedRiskScore,
    riskReduction: Number((baselineRiskScore - remediatedRiskScore).toFixed(4))
  };
}

function buildFamilyRiskPatternChanges({ frozenFamilies, candidatesWithRemediation, errorBudgets }) {
  return frozenFamilies
    .map((familyLabel) => {
      const familyRows = candidatesWithRemediation.filter((row) => String(row.familyLabel) === String(familyLabel));
      return {
        familyLabel,
        candidateCount: familyRows.length,
        baselineSimulatedDeltaAvg: avg(familyRows.map((row) => toNumber(row.simulatedQualityDelta, 0))),
        remediatedPredictedDeltaAvg: avg(familyRows.map((row) => toNumber(row.remediatedPredictedQualityDelta, 0))),
        appliedErrorBudget: toNumber(errorBudgets[familyLabel], 0),
        knownLiveOutcomeCount: familyRows.filter((row) => row.actualQualityDeltaIfKnown !== null).length
      };
    })
    .sort((a, b) => String(a.familyLabel).localeCompare(String(b.familyLabel)));
}

function buildCandidateOutcomeChanges(candidatesWithRemediation) {
  return candidatesWithRemediation
    .map((row) => ({
      documentId: row.documentId,
      familyLabel: row.familyLabel,
      simulatedQualityDelta: row.simulatedQualityDelta,
      remediatedPredictedQualityDelta: row.remediatedPredictedQualityDelta,
      actualQualityDeltaIfKnown: row.actualQualityDeltaIfKnown,
      baselinePredictionErrorIfKnown: row.baselinePredictionErrorIfKnown,
      remediatedPredictionErrorIfKnown: row.remediatedPredictionErrorIfKnown,
      statusBefore: row.statusBefore,
      statusAfter: row.statusAfter
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));
}

export function buildR55RemediationExperiment({ r54Report, r52Report, r48Report }) {
  const experimentImplemented = pickFirstExperiment(r54Report);
  if (!experimentImplemented) {
    throw new Error("R54 report does not contain recommendedExperiments[0]");
  }

  const frozenFamilies = unique([
    ...(r54Report?.frozenFamilies || []),
    String(r52Report?.frozenFamilyLabel || "")
  ]);

  const frozenCandidates = buildFrozenCandidateRows({ r48Report, r52Report, frozenFamilies });
  const { budgets, globalBudget } = computeFamilyErrorBudgets(frozenCandidates, frozenFamilies);
  const candidatesWithRemediation = applyExperiment({
    experiment: experimentImplemented,
    candidateRows: frozenCandidates,
    errorBudgets: { ...budgets, globalBudget }
  });

  const knownRows = candidatesWithRemediation.filter((row) => row.actualQualityDeltaIfKnown !== null);
  const baselinePredictionError = Number(avg(knownRows.map((row) => row.baselinePredictionErrorIfKnown)).toFixed(4));
  const remediatedPredictionError = Number(avg(knownRows.map((row) => row.remediatedPredictionErrorIfKnown)).toFixed(4));
  const improvement = Number((baselinePredictionError - remediatedPredictionError).toFixed(4));
  const improvementRate =
    baselinePredictionError > 0 ? Number((improvement / baselinePredictionError).toFixed(4)) : 0;
  const materiallyImproved = improvement >= 1 || improvementRate >= 0.2;

  const queryRegressionChanges = buildQueryRegressionChanges({
    queryRegressionPatterns: r54Report?.queryRegressionPatterns || [],
    candidatesWithRemediation
  });

  const familyRiskPatternChanges = buildFamilyRiskPatternChanges({
    frozenFamilies,
    candidatesWithRemediation,
    errorBudgets: budgets
  });

  const candidateOutcomeChanges = buildCandidateOutcomeChanges(candidatesWithRemediation);

  const recommendedNextStep = materiallyImproved
    ? "proceed_to_second_dry_run_validation_phase_for_experiment_1"
    : "prediction_error_not_materially_improved_try_next_r54_experiment";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R55",
    summary: {
      candidatesEvaluated: frozenCandidates.length,
      knownOutcomeCandidates: knownRows.length,
      materiallyImproved
    },
    experimentImplemented,
    frozenFamiliesEvaluated: frozenFamilies,
    baselinePredictionError,
    remediatedPredictionError,
    queryRegressionChanges,
    familyRiskPatternChanges,
    candidateOutcomeChanges,
    recommendedNextStep
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R55 Remediation Experiment (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push(`- experimentImplemented: ${report.experimentImplemented?.experimentId || ""}`);
  lines.push(`- baselinePredictionError: ${report.baselinePredictionError}`);
  lines.push(`- remediatedPredictionError: ${report.remediatedPredictionError}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Frozen Families Evaluated");
  for (const family of report.frozenFamiliesEvaluated || []) lines.push(`- ${family}`);
  if (!(report.frozenFamiliesEvaluated || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Query Regression Changes");
  for (const [key, value] of Object.entries(report.queryRegressionChanges || {})) {
    if (Array.isArray(value)) lines.push(`- ${key}: ${value.join(", ")}`);
    else lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Family Risk Pattern Changes");
  for (const row of report.familyRiskPatternChanges || []) {
    lines.push(
      `- ${row.familyLabel}: baselineAvg=${row.baselineSimulatedDeltaAvg}, remediatedAvg=${row.remediatedPredictedDeltaAvg}, appliedErrorBudget=${row.appliedErrorBudget}, knownOutcomes=${row.knownLiveOutcomeCount}`
    );
  }
  if (!(report.familyRiskPatternChanges || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Candidate Outcome Changes");
  for (const row of report.candidateOutcomeChanges || []) {
    lines.push(
      `- ${row.documentId}: simDelta=${row.simulatedQualityDelta}, remediatedDelta=${row.remediatedPredictedQualityDelta}, actualDelta=${row.actualQualityDeltaIfKnown}, statusBefore=${row.statusBefore}, statusAfter=${row.statusAfter}`
    );
  }
  if (!(report.candidateOutcomeChanges || []).length) lines.push("- none");
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, or runtime/trust/admission/provenance mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r54, r52, r48] = await Promise.all([
    readJson(path.resolve(reportsDir, r54Name)),
    readJson(path.resolve(reportsDir, r52Name)),
    readJson(path.resolve(reportsDir, r48Name))
  ]);

  const report = buildR55RemediationExperiment({ r54Report: r54, r52Report: r52, r48Report: r48 });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        experimentImplemented: report.experimentImplemented?.experimentId || "",
        baselinePredictionError: report.baselinePredictionError,
        remediatedPredictionError: report.remediatedPredictionError,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R55 remediation experiment report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
