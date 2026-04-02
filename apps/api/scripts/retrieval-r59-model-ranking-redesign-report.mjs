import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r54Name = process.env.RETRIEVAL_R59_R54_REPORT_NAME || "retrieval-r54-remediation-plan-report.json";
const r55Name = process.env.RETRIEVAL_R59_R55_REPORT_NAME || "retrieval-r55-remediation-experiment-report.json";
const r57Name = process.env.RETRIEVAL_R59_R57_REPORT_NAME || "retrieval-r57-remediation-experiment-report.json";
const r58Name = process.env.RETRIEVAL_R59_R58_REPORT_NAME || "retrieval-r58-remediation-experiment-report.json";
const r39Name = process.env.RETRIEVAL_R59_R39_REPORT_NAME || "retrieval-r39-frontier-blocker-breakdown-report.json";
const outputJsonName = process.env.RETRIEVAL_R59_REPORT_NAME || "retrieval-r59-model-ranking-redesign-report.json";
const outputMdName = process.env.RETRIEVAL_R59_MARKDOWN_NAME || "retrieval-r59-model-ranking-redesign-report.md";

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

function buildPrioritizedChanges({ frozenFamilies }) {
  return [
    {
      changeId: "r59_change_01_family_error_budget_v2",
      workstream: "predictor_calibration_changes",
      problemAddressed: "Simulation quality gains overpredict live outcomes for frozen fallback families.",
      expectedBenefit: "Reduce false-safe frontier classifications and prevent quality-regressive activations.",
      implementationRisk: "medium",
      codeAreasLikelyAffected: [
        "scripts/retrieval-r48-frontier-quality-audit-report.mjs",
        "scripts/retrieval-r56-remediation-validation-report.mjs",
        "scripts/retrieval-r58-remediation-experiment-report.mjs"
      ],
      validationPlan:
        "Backtest on known failed activations and require prediction error reduction >= 50% while preserving all non-regression safety checks.",
      prerequisiteOrDependency: "Needs frozen-family historical error table keyed by documentFamilyLabel."
    },
    {
      changeId: "r59_change_02_live_sim_parity_contract",
      workstream: "simulation_live_parity_changes",
      problemAddressed: "Simulation omits insertion-side effects that live ranking introduces for citation-sensitive queries.",
      expectedBenefit: "Converge simulation ordering with live ranking behavior before activation planning.",
      implementationRisk: "medium",
      codeAreasLikelyAffected: [
        "scripts/retrieval-r31-r28-pair-simulation-report.mjs",
        "scripts/retrieval-r48-frontier-quality-audit-report.mjs",
        "src/services/search.ts"
      ],
      validationPlan:
        "Run parity audit over frozen families and require max live/sim citation-query delta <= 0.02 before re-enabling activation rehearsals.",
      prerequisiteOrDependency: "Depends on stable ranking explanation fields and deterministic tie-breaking."
    },
    {
      changeId: "r59_change_03_citation_intent_feature_bundle",
      workstream: "ranking_feature_changes",
      problemAddressed: "Citation intent remains sensitive to document insertion despite repeat penalties.",
      expectedBenefit: "Better top-rank stability for citation_rule_direct and citation_ordinance_direct queries.",
      implementationRisk: "high",
      codeAreasLikelyAffected: [
        "src/services/search.ts",
        "scripts/retrieval-live-search-qa-report.mjs",
        "scripts/retrieval-r29-citation-parity-report.mjs"
      ],
      validationPlan:
        "Offline replay harness: prove citation top-document concentration remains within ceiling and no quality regressions on baseline trusted corpus.",
      prerequisiteOrDependency: "Requires parity contract from r59_change_02."
    },
    {
      changeId: "r59_change_04_family_risk_lock_v2",
      workstream: "candidate_family_risk_controls",
      problemAddressed: "Current family freeze lock is binary and does not encode residual risk for near-miss families.",
      expectedBenefit: "Stricter pre-activation filtering with explicit family risk tiers and mandatory probe prerequisites.",
      implementationRisk: "low",
      codeAreasLikelyAffected: [
        "scripts/retrieval-r53-frontier-policy-lock-report.mjs",
        "scripts/retrieval-r49-family-freeze-report.mjs",
        "scripts/retrieval-r38-single-frontier-refresh-report.mjs"
      ],
      validationPlan:
        "Dry-run frontier refresh must show zero candidates classified safe unless risk tier and probe criteria are fully satisfied.",
      prerequisiteOrDependency: `Frozen families in scope: ${frozenFamilies.join(", ") || "<none>"}.`
    },
    {
      changeId: "r59_change_05_activation_protocol_upgrade",
      workstream: "validation_protocol_changes",
      problemAddressed: "Average-score-only prechecks hide query-level regressions that cause live quality failures.",
      expectedBenefit: "Activation readiness decisions based on strict query-level and family-level go/no-go checks.",
      implementationRisk: "medium",
      codeAreasLikelyAffected: [
        "scripts/retrieval-r50-family-probe-plan-report.mjs",
        "scripts/retrieval-r51-single-probe-activation-write.mjs",
        "scripts/retrieval-r46-single-activation-write.mjs"
      ],
      validationPlan:
        "Require query-level no-regression matrix and family-probe pass criteria prior to any single-doc activation write.",
      prerequisiteOrDependency: "Depends on calibrated predictor and parity outputs from changes 01-03."
    }
  ];
}

function buildRedesignWorkstreams(changes) {
  const groups = new Map();
  for (const change of changes) {
    const key = String(change.workstream);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(change.changeId);
  }
  return Array.from(groups.entries())
    .map(([workstreamId, changeIds]) => ({ workstreamId, changeIds: changeIds.sort((a, b) => a.localeCompare(b)) }))
    .sort((a, b) => a.workstreamId.localeCompare(b.workstreamId));
}

function buildValidationProtocol() {
  return {
    phases: [
      {
        phaseId: "r59_validation_phase_1_offline_backtest",
        passCriteria: [
          "prediction_error_reduction_at_least_50_percent_vs_r58_baseline",
          "frozen_family_false_safe_rate_equals_0",
          "all_non_regression_safety_checks_true"
        ]
      },
      {
        phaseId: "r59_validation_phase_2_live_sim_parity",
        passCriteria: [
          "citation_query_live_sim_delta_within_0_02",
          "same_document_concentration_not_worse_than_baseline",
          "query_level_regression_count_equals_0_on_baseline_corpus"
        ]
      },
      {
        phaseId: "r59_validation_phase_3_activation_rehearsal_dry_run",
        passCriteria: [
          "at_least_one_newly_safe_simulated_candidate",
          "query_level_no_regression_matrix_all_green",
          "family_risk_tier_allows_probe"
        ]
      }
    ],
    stopCondition: "do_not_resume_activation_until_all_three_phases_pass"
  };
}

function buildActivationResumeCriteria() {
  return {
    goCriteria: [
      "newly_safe_simulated_candidate_count_greater_than_0",
      "prediction_error_materially_reduced_and_stable_across_frozen_families",
      "citation_concentration_low_signal_provenance_anchor_zero_trusted_out_of_corpus_checks_all_pass",
      "query_level_regression_matrix_has_no_critical_failures",
      "family_probe_policy_explicitly_allows_target_family"
    ],
    noGoCriteria: [
      "newly_safe_simulated_candidate_count_equals_0",
      "any_frozen_family_still_exhibits_live_quality_misprediction",
      "any_non_regression_safety_check_fails",
      "live_sim_parity_contract_not_met"
    ]
  };
}

export function buildR59RedesignReport({ r54Report, r55Report, r57Report, r58Report, r39Report }) {
  const frozenFamilies = unique(r54Report?.frozenFamilies || []);
  const changes = buildPrioritizedChanges({ frozenFamilies });
  const redesignWorkstreams = buildRedesignWorkstreams(changes);
  const validationProtocol = buildValidationProtocol();
  const activationResumeCriteria = buildActivationResumeCriteria();

  const failureModeSummary = {
    knownFrozenFamilies: frozenFamilies,
    frontierBlockedCount: toNumber(r58Report?.summary?.stillBlockedCandidateCount, toNumber(r39Report?.blockedCandidateCount, 0)),
    reopenedCandidatesAcrossExperiments: {
      exp1: toNumber(r55Report?.summary?.materiallyImproved ? 0 : 0, 0),
      exp2: toNumber(r57Report?.summary?.newlySafeSimulatedCandidateCount, 0),
      exp3: toNumber(r58Report?.summary?.newlySafeSimulatedCandidateCount, 0)
    },
    predictionErrorTrend: {
      r55: {
        baseline: toNumber(r55Report?.baselinePredictionError, 0),
        remediated: toNumber(r55Report?.remediatedPredictionError, 0)
      },
      r57: {
        baseline: toNumber(r57Report?.baselinePredictionError, 0),
        remediated: toNumber(r57Report?.remediatedPredictionError, 0)
      },
      r58: {
        baseline: toNumber(r58Report?.baselinePredictionError, 0),
        remediated: toNumber(r58Report?.remediatedPredictionError, 0)
      }
    },
    dominantBlockerFamilies: r39Report?.blockerFamilyCounts || {}
  };

  const summary = {
    phase: "R59",
    readOnly: true,
    activationWorkStopped: true,
    prioritizedChangeCount: changes.length,
    workstreamCount: redesignWorkstreams.length,
    baselineBlockedCandidates: toNumber(r39Report?.blockedCandidateCount, 0),
    latestBlockedCandidates: toNumber(r58Report?.summary?.stillBlockedCandidateCount, 0)
  };

  const recommendedNextStep =
    "implement_r59_change_01_and_r59_change_02_first_then_rerun_dry_run_validation_before_any_activation_attempts";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R59",
    summary,
    failureModeSummary,
    redesignWorkstreams,
    prioritizedChanges: changes,
    validationProtocol,
    activationResumeCriteria,
    recommendedNextStep
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R59 Model/Ranking Redesign Plan (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Failure Mode Summary");
  for (const [key, value] of Object.entries(report.failureModeSummary || {})) {
    if (typeof value === "object" && value !== null) lines.push(`- ${key}: ${JSON.stringify(value)}`);
    else lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Redesign Workstreams");
  for (const row of report.redesignWorkstreams || []) {
    lines.push(`- ${row.workstreamId}: ${row.changeIds.join(", ")}`);
  }
  lines.push("");

  lines.push("## Prioritized Changes");
  for (const row of report.prioritizedChanges || []) {
    lines.push(`- ${row.changeId} [${row.workstream}]`);
    lines.push(`  problemAddressed: ${row.problemAddressed}`);
    lines.push(`  expectedBenefit: ${row.expectedBenefit}`);
    lines.push(`  implementationRisk: ${row.implementationRisk}`);
    lines.push(`  codeAreasLikelyAffected: ${(row.codeAreasLikelyAffected || []).join(", ")}`);
    lines.push(`  validationPlan: ${row.validationPlan}`);
    lines.push(`  prerequisiteOrDependency: ${row.prerequisiteOrDependency}`);
  }
  lines.push("");

  lines.push("## Activation Resume Criteria");
  lines.push(`- goCriteria: ${(report.activationResumeCriteria?.goCriteria || []).join(" | ")}`);
  lines.push(`- noGoCriteria: ${(report.activationResumeCriteria?.noGoCriteria || []).join(" | ")}`);
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, trust/admission/provenance gate, or runtime ranking mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r54, r55, r57, r58, r39] = await Promise.all([
    readJson(path.resolve(reportsDir, r54Name)),
    readJson(path.resolve(reportsDir, r55Name)),
    readJson(path.resolve(reportsDir, r57Name)),
    readJson(path.resolve(reportsDir, r58Name)),
    readJson(path.resolve(reportsDir, r39Name))
  ]);

  const report = buildR59RedesignReport({
    r54Report: r54,
    r55Report: r55,
    r57Report: r57,
    r58Report: r58,
    r39Report: r39
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        prioritizedChangeCount: report.summary.prioritizedChangeCount,
        workstreamCount: report.summary.workstreamCount,
        baselineBlockedCandidates: report.summary.baselineBlockedCandidates,
        latestBlockedCandidates: report.summary.latestBlockedCandidates,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R59 redesign report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
