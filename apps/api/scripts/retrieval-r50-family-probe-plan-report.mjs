import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r49ReportName = process.env.RETRIEVAL_R50_R49_REPORT_NAME || "retrieval-r49-family-freeze-report.json";
const outputJsonName = process.env.RETRIEVAL_R50_REPORT_NAME || "retrieval-r50-family-probe-plan-report.json";
const outputMdName = process.env.RETRIEVAL_R50_MARKDOWN_NAME || "retrieval-r50-family-probe-plan-report.md";

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

async function readJsonStrict(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function buildCandidateOrder({ probeCandidateIds = [], nextBestCandidateIfAny = null }) {
  const ids = unique(probeCandidateIds);
  const nextBestId = String(nextBestCandidateIfAny?.documentId || "");
  const first = nextBestId && ids.includes(nextBestId) ? [nextBestId] : [];
  const rest = ids.filter((id) => id !== nextBestId);
  return [...first, ...rest];
}

export function buildR50FamilyProbePlan(r49Report) {
  const eligibleFamilies = Array.isArray(r49Report?.eligibleFamilies) ? r49Report.eligibleFamilies : [];
  const probeFamily = eligibleFamilies
    .slice()
    .sort((a, b) => {
      const aq = Number(a?.simulatedQualityDeltaAvg || 0);
      const bq = Number(b?.simulatedQualityDeltaAvg || 0);
      if (bq !== aq) return bq - aq;
      return String(a?.familyLabel || "").localeCompare(String(b?.familyLabel || ""));
    })[0] || null;

  const familyLabel = String(probeFamily?.familyLabel || "");

  const remainingIds = unique(r49Report?.remainingCandidateIds || []);
  const probeCandidateIdsFromRows = unique(
    remainingIds.filter((id) => {
      const row = (r49Report?.candidateRows || []).find((candidate) => String(candidate?.documentId || "") === String(id));
      return row ? String(row?.documentFamilyLabel || "") === familyLabel : false;
    })
  );
  const probeCandidateIds = probeCandidateIdsFromRows.length ? probeCandidateIdsFromRows : remainingIds;

  const candidateOrder = buildCandidateOrder({
    probeCandidateIds,
    nextBestCandidateIfAny: r49Report?.nextBestCandidateIfAny || null
  });

  const hardGates = {
    qualityFloor: "post_activation_averageQualityScore >= pre_activation_averageQualityScore - 0.5",
    dynamicFloorCitationCeiling: "citationTopDocumentShare <= effectiveCitationCeiling(dynamic_floor)",
    lowSignalStructuralShareNotWorsened: "post_activation_lowSignalStructuralShare <= pre_activation_lowSignalStructuralShare",
    zeroTrustedResultQueries: "zeroTrustedResultQueryCount == 0",
    outOfCorpusHits: "outOfCorpusHitQueryCount == 0",
    provenanceCompleteness: "provenanceCompletenessAverage == 1",
    citationAnchorCoverage: "citationAnchorCoverageAverage == 1"
  };

  const stopConditions = [
    "first_candidate_fails_any_hard_gate",
    "first_candidate_has_any_anomaly_flag",
    "rollback_verification_failed_for_first_candidate"
  ];

  const successCriteria = [
    "first_candidate_passes_all_hard_gates",
    "first_candidate_has_no_anomaly_flags",
    "rollback_not_triggered_for_first_candidate",
    "post_activation_metrics_within_hard_gates"
  ];

  const failureCriteria = [
    "any_hard_gate_failure_on_first_candidate",
    "any_non_manifest_touch_on_first_candidate",
    "any_provenance_or_anchor_regression",
    "rollback_required_or_verification_failed"
  ];

  const probeStages = [
    {
      stageId: "stage_1_single_doc_probe",
      description: "Activate first candidate only, run full hard-gate QA, and evaluate anomalies.",
      candidateIds: candidateOrder.slice(0, 1),
      requiredPass: true,
      onFail: "freeze_family_pending_model_change",
      onPass: "continue_to_stage_2_observation"
    },
    {
      stageId: "stage_2_observation_evaluation",
      description: "Observe post-activation quality and concentration deltas; confirm no hidden drift.",
      candidateIds: candidateOrder.slice(0, 1),
      requiredPass: true,
      onFail: "freeze_family_pending_model_change",
      onPass: "optional_stage_3_second_doc"
    },
    {
      stageId: "stage_3_optional_second_doc",
      description: "Only if stage 1 and stage 2 pass with zero anomalies, consider second candidate activation.",
      candidateIds: candidateOrder.slice(1, 2),
      requiredPass: false,
      gateCondition: "stage_1_and_stage_2_passed_without_anomaly",
      onFail: "freeze_family_pending_model_change",
      onPass: "family_probe_passed"
    }
  ];

  const recommendedProbeStrategy = {
    mode: "single_then_observe_then_optional_second",
    conservativeRule: "if_first_candidate_fails_any_hard_gate_freeze_family_pending_model_change",
    freezeOnFirstFailure: true,
    allowSecondCandidateOnlyIfFirstPassesCleanly: true
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R50",
    summary: {
      sourceR49SafeFamiliesEvaluated: Number(r49Report?.safeFamiliesEvaluated || 0),
      sourceR49FrozenFamilies: Number((r49Report?.frozenFamilies || []).length),
      sourceR49EligibleFamilies: Number((r49Report?.eligibleFamilies || []).length),
      plannedProbeCandidateCount: probeCandidateIds.length
    },
    probeFamilyLabel: familyLabel,
    probeCandidateIds,
    candidateOrder,
    hardGates,
    stopConditions,
    successCriteria,
    failureCriteria,
    probeStages,
    recommendedProbeStrategy,
    proceedToRealActivation: false,
    rationale:
      "Planning-only phase. Family has no real activation outcomes yet; probe must run in staged sequence with first-failure freeze rule before any expansion."
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R50 Family Probe Plan (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push(`- probeFamilyLabel: ${report.probeFamilyLabel}`);
  lines.push(`- probeCandidateIds: ${(report.probeCandidateIds || []).join(", ") || "<none>"}`);
  lines.push(`- candidateOrder: ${(report.candidateOrder || []).join(" -> ") || "<none>"}`);
  lines.push(`- proceedToRealActivation: ${report.proceedToRealActivation}`);
  lines.push("");

  lines.push("## Hard Gates");
  for (const [key, value] of Object.entries(report.hardGates || {})) lines.push(`- ${key}: ${value}`);
  lines.push("");

  lines.push("## Stop Conditions");
  for (const item of report.stopConditions || []) lines.push(`- ${item}`);
  lines.push("");

  lines.push("## Probe Stages");
  for (const stage of report.probeStages || []) {
    lines.push(`- ${stage.stageId}: ${stage.description}`);
    lines.push(`  candidates: ${(stage.candidateIds || []).join(", ") || "<none>"}`);
    lines.push(`  onFail: ${stage.onFail}`);
    lines.push(`  onPass: ${stage.onPass}`);
  }
  lines.push("");

  lines.push("## Recommendation");
  lines.push(`- ${report.recommendedProbeStrategy?.conservativeRule || ""}`);
  lines.push(`- rationale: ${report.rationale}`);
  lines.push("");
  lines.push("- Dry-run only. No activation, rollback, trust, admission, or ranking changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const r49Path = path.resolve(reportsDir, r49ReportName);
  const r49Report = await readJsonStrict(r49Path);

  const report = buildR50FamilyProbePlan(r49Report);

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);

  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        probeFamilyLabel: report.probeFamilyLabel,
        probeCandidateIds: report.probeCandidateIds,
        candidateOrder: report.candidateOrder,
        proceedToRealActivation: report.proceedToRealActivation,
        recommendedNextStep: "family_probe_plan_ready"
      },
      null,
      2
    )
  );
  console.log(`R50 family probe plan report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
