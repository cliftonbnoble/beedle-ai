import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r48Name = process.env.RETRIEVAL_R54_R48_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const r52Name = process.env.RETRIEVAL_R54_R52_REPORT_NAME || "retrieval-r52-frozen-family-postmortem-report.json";
const r53Name = process.env.RETRIEVAL_R54_R53_REPORT_NAME || "retrieval-r53-frontier-policy-lock-report.json";
const r44ActivationName = process.env.RETRIEVAL_R54_R44_ACTIVATION_REPORT_NAME || "retrieval-r44-single-activation-report.json";
const r47ActivationName = process.env.RETRIEVAL_R54_R47_ACTIVATION_REPORT_NAME || "retrieval-r47-single-activation-report.json";
const r51ActivationName =
  process.env.RETRIEVAL_R54_R51_ACTIVATION_REPORT_NAME || "retrieval-r51-single-probe-activation-report.json";
const outputJsonName = process.env.RETRIEVAL_R54_REPORT_NAME || "retrieval-r54-remediation-plan-report.json";
const outputMdName = process.env.RETRIEVAL_R54_MARKDOWN_NAME || "retrieval-r54-remediation-plan-report.md";

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

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function buildKnownFailedActivations({ activationReports = [] }) {
  return activationReports
    .filter(Boolean)
    .map((report) => {
      const before = toNumber(report?.beforeLiveMetrics?.averageQualityScore, 0);
      const after = toNumber(report?.afterLiveMetrics?.averageQualityScore, 0);
      const qualityDelta = Number((after - before).toFixed(2));
      return {
        phase: String(report?.summary?.activationBatchId || report?.activationBatchId || ""),
        docActivatedExact: String(report?.docActivatedExact || ""),
        keepOrRollbackDecision: String(report?.keepOrRollbackDecision || ""),
        qualityDelta,
        anomalyFlags: unique(report?.anomalyFlags || []),
        freezeDecision: String(report?.freezeDecision || ""),
        freezeReason: String(report?.freezeReason || "")
      };
    })
    .filter((row) => row.keepOrRollbackDecision === "rollback_batch")
    .sort((a, b) => String(a.docActivatedExact).localeCompare(String(b.docActivatedExact)));
}

function buildSimulationVsLiveErrorBreakdown({ r48Report, r52Report }) {
  const rows = Array.isArray(r48Report?.simulationVsRealityRows) ? r48Report.simulationVsRealityRows : [];
  const byFamily = new Map();
  for (const row of rows) {
    const family = String(row?.documentFamilyLabel || "unknown");
    if (!byFamily.has(family)) byFamily.set(family, []);
    byFamily.get(family).push(row);
  }

  const familyRows = Array.from(byFamily.entries())
    .map(([familyLabel, familyRows]) => {
      const errors = familyRows.map((row) => Math.abs(toNumber(row?.qualityPredictionError, 0)));
      const avgAbsError =
        errors.length > 0 ? Number((errors.reduce((sum, value) => sum + value, 0) / errors.length).toFixed(2)) : 0;
      const knownLiveRows = familyRows.filter((row) => row?.actualKnownQualityDelta !== null && row?.actualKnownQualityDelta !== undefined);
      const missCount = knownLiveRows.filter((row) => toNumber(row.actualKnownQualityDelta, 0) < -0.5).length;
      const missRate = knownLiveRows.length ? Number((missCount / knownLiveRows.length).toFixed(4)) : 0;
      return {
        familyLabel,
        candidateCount: familyRows.length,
        knownLiveOutcomeCount: knownLiveRows.length,
        avgAbsolutePredictionError: avgAbsError,
        knownLiveMissRate: missRate
      };
    })
    .sort((a, b) => {
      if (b.avgAbsolutePredictionError !== a.avgAbsolutePredictionError) {
        return b.avgAbsolutePredictionError - a.avgAbsolutePredictionError;
      }
      return String(a.familyLabel).localeCompare(String(b.familyLabel));
    });

  const frozenFamilyLabel = String(r52Report?.frozenFamilyLabel || "");
  const frozenFamilyErrorRow = familyRows.find((row) => row.familyLabel === frozenFamilyLabel) || null;
  return {
    byFamily: familyRows,
    frozenFamilyErrorRow
  };
}

function buildQueryRegressionPatterns(r52Report) {
  const rows = Array.isArray(r52Report?.queryLevelRegressionBreakdown) ? r52Report.queryLevelRegressionBreakdown : [];
  return rows
    .filter((row) => toNumber(row?.qualityDelta, 0) < 0)
    .map((row) => ({
      queryId: String(row?.queryId || ""),
      query: String(row?.query || ""),
      qualityDelta: toNumber(row?.qualityDelta, 0),
      activatedDocTop10Hits: toNumber(row?.activatedDocTop10Hits, 0),
      topDocumentShareBefore: toNumber(row?.topDocumentShareBefore, 0),
      topDocumentShareAfter: toNumber(row?.topDocumentShareAfter, 0)
    }))
    .sort((a, b) => {
      if (a.qualityDelta !== b.qualityDelta) return a.qualityDelta - b.qualityDelta;
      return String(a.queryId).localeCompare(String(b.queryId));
    });
}

function buildCandidateRemediationLevers() {
  return [
    {
      leverId: "predictor_calibration_adjustments",
      description: "Calibrate simulation quality deltas with historical simulation-vs-live error bounds per family."
    },
    {
      leverId: "citation_intent_sensitivity_modeling",
      description: "Introduce citation-intent risk weight in frontier predictor to discount fragile citation-query gains."
    },
    {
      leverId: "family_level_exclusion_features",
      description: "Apply deterministic family freeze rules when a family has proven live quality misprediction."
    },
    {
      leverId: "section_chunk_monoculture_penalties",
      description: "Increase frontier-risk penalty when chunk-type or section-label monoculture is detected."
    },
    {
      leverId: "fallback_family_overestimation_penalties",
      description: "Downweight fallback-heavy families whose simulated gains historically overestimate live performance."
    },
    {
      leverId: "document_insertion_effect_modeling",
      description: "Add insertion-effect term to predictor for top-rank displacement risk after adding one document."
    }
  ];
}

function buildRecommendedExperiments() {
  return [
    {
      experimentId: "r54_exp_01_predictor_error_budget",
      hypothesis: "Family-specific error budgets will reduce false-safe single-doc recommendations.",
      expectedBenefit: "Lower live quality regression risk from simulation overestimation.",
      risk: "May reduce candidate throughput by classifying more docs as blocked.",
      requiredCodeAreas: [
        "scripts/retrieval-r36-single-safe-frontier-report.mjs",
        "scripts/retrieval-r38-single-frontier-refresh-report.mjs",
        "scripts/retrieval-r48-frontier-quality-audit-report.mjs"
      ],
      validationPlan:
        "Backtest against known R47/R51 failures and confirm failed families are blocked while known clean baselines remain unchanged."
    },
    {
      experimentId: "r54_exp_02_citation_intent_risk_feature",
      hypothesis: "Adding citation-intent sensitivity to frontier scoring will catch hidden query-level regressions.",
      expectedBenefit: "Fewer citation_rule_direct and citation_ordinance_direct surprises in live QA.",
      risk: "Could over-penalize documents that are otherwise useful for citation retrieval.",
      requiredCodeAreas: [
        "scripts/retrieval-r39-frontier-blocker-breakdown-report.mjs",
        "scripts/retrieval-r40-citation-gate-sensitivity-report.mjs",
        "scripts/retrieval-r48-frontier-quality-audit-report.mjs"
      ],
      validationPlan:
        "Run dry-run frontier scans and verify near-miss candidates with citation-intent regressions are downgraded before activation planning."
    },
    {
      experimentId: "r54_exp_03_insertion_effect_probe",
      hypothesis: "Modeling document insertion effects will align simulation with live rerank displacement behavior.",
      expectedBenefit: "Closer live/simulation parity for single-doc activation rehearsals.",
      risk: "Adds complexity to predictor and may require larger historical sample.",
      requiredCodeAreas: [
        "scripts/retrieval-r48-frontier-quality-audit-report.mjs",
        "scripts/retrieval-r52-frozen-family-postmortem-report.mjs",
        "scripts/retrieval-r53-frontier-policy-lock-report.mjs"
      ],
      validationPlan:
        "Replay R47 and R51 candidate docs in dry-run and require prediction-error reduction before unfreezing any family."
    }
  ];
}

export function buildR54RemediationPlan({
  r48Report,
  r52Report,
  r53Report,
  activationReports
}) {
  const frozenFamilies = unique([
    ...(r53Report?.frozenFamilies || []),
    String(r52Report?.frozenFamilyLabel || "")
  ]);

  const knownFailedActivations = buildKnownFailedActivations({ activationReports });
  const simulationVsLiveErrorBreakdown = buildSimulationVsLiveErrorBreakdown({ r48Report, r52Report });
  const queryRegressionPatterns = buildQueryRegressionPatterns(r52Report);
  const familyRiskPatterns = {
    frozenFamilies,
    familyRiskFactors: unique(r52Report?.familyRiskFactors || []),
    guardrailHitCounts: r53Report?.guardrailHitCounts || {}
  };

  const candidateRemediationLevers = buildCandidateRemediationLevers();
  const recommendedExperiments = buildRecommendedExperiments();
  const recommendedNextStep =
    "stop_all_activations_until_at_least_one_remediation_experiment_is_implemented_and_revalidated";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R54",
    summary: {
      frozenFamilyCount: frozenFamilies.length,
      knownFailedActivationCount: knownFailedActivations.length,
      queryRegressionPatternCount: queryRegressionPatterns.length,
      experimentCount: recommendedExperiments.length
    },
    frozenFamilies,
    knownFailedActivations,
    simulationVsLiveErrorBreakdown,
    queryRegressionPatterns,
    familyRiskPatterns,
    candidateRemediationLevers,
    recommendedExperiments,
    recommendedNextStep
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R54 Remediation Plan (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push(`- frozenFamilies: ${(report.frozenFamilies || []).join(", ") || "<none>"}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Known Failed Activations");
  for (const row of report.knownFailedActivations || []) {
    lines.push(
      `- ${row.docActivatedExact} | decision=${row.keepOrRollbackDecision} | qualityDelta=${row.qualityDelta} | anomalyFlags=${(row.anomalyFlags || []).join(",")}`
    );
  }
  if (!(report.knownFailedActivations || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Query Regression Patterns");
  for (const row of report.queryRegressionPatterns || []) {
    lines.push(
      `- ${row.queryId}: qualityDelta=${row.qualityDelta}, activatedDocTop10Hits=${row.activatedDocTop10Hits}, topDocumentShareBefore=${row.topDocumentShareBefore}, topDocumentShareAfter=${row.topDocumentShareAfter}`
    );
  }
  if (!(report.queryRegressionPatterns || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Family Risk Patterns");
  for (const item of report.familyRiskPatterns?.familyRiskFactors || []) lines.push(`- ${item}`);
  lines.push("");

  lines.push("## Candidate Remediation Levers");
  for (const row of report.candidateRemediationLevers || []) lines.push(`- ${row.leverId}: ${row.description}`);
  lines.push("");

  lines.push("## Recommended Experiments");
  for (const row of report.recommendedExperiments || []) {
    lines.push(`- ${row.experimentId}`);
    lines.push(`  hypothesis: ${row.hypothesis}`);
    lines.push(`  expectedBenefit: ${row.expectedBenefit}`);
    lines.push(`  risk: ${row.risk}`);
    lines.push(`  requiredCodeAreas: ${(row.requiredCodeAreas || []).join(", ")}`);
    lines.push(`  validationPlan: ${row.validationPlan}`);
  }
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, trust, admission, provenance, or runtime mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r48, r52, r53, r44, r47, r51] = await Promise.all([
    readJson(path.resolve(reportsDir, r48Name)),
    readJson(path.resolve(reportsDir, r52Name)),
    readJson(path.resolve(reportsDir, r53Name)),
    readJsonIfExists(path.resolve(reportsDir, r44ActivationName)),
    readJsonIfExists(path.resolve(reportsDir, r47ActivationName)),
    readJsonIfExists(path.resolve(reportsDir, r51ActivationName))
  ]);

  const report = buildR54RemediationPlan({
    r48Report: r48,
    r52Report: r52,
    r53Report: r53,
    activationReports: [r44, r47, r51]
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        frozenFamilyCount: report.summary.frozenFamilyCount,
        knownFailedActivationCount: report.summary.knownFailedActivationCount,
        queryRegressionPatternCount: report.summary.queryRegressionPatternCount,
        experimentCount: report.summary.experimentCount,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R54 remediation plan report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
