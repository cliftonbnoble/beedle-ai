import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r48ReportName = process.env.RETRIEVAL_R53_R48_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const r49ReportName = process.env.RETRIEVAL_R53_R49_REPORT_NAME || "retrieval-r49-family-freeze-report.json";
const r52ReportName = process.env.RETRIEVAL_R53_R52_REPORT_NAME || "retrieval-r52-frozen-family-postmortem-report.json";
const outputJsonName = process.env.RETRIEVAL_R53_REPORT_NAME || "retrieval-r53-frontier-policy-lock-report.json";
const outputMdName = process.env.RETRIEVAL_R53_MARKDOWN_NAME || "retrieval-r53-frontier-policy-lock-report.md";

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((value) => String(value)))).sort((a, b) => a.localeCompare(b));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function hasMonoculture(profile = []) {
  if (!Array.isArray(profile) || profile.length <= 1) return true;
  const top = Number(profile[0]?.count || 0);
  const second = Number(profile[1]?.count || 0);
  return top >= 3 && second <= 1;
}

function hasFallbackOverestimationRisk(row = {}) {
  const family = String(row?.documentFamilyLabel || "");
  return /analysis_reasoning\+none/.test(family) || /low_signal_heavy::short::analysis_reasoning\+holding_disposition/.test(family);
}

function buildGuardrails() {
  return [
    {
      id: "hasQueryLevelRegressionRisk",
      description: "Family has proven citation-intent query regressions in live probe outcomes."
    },
    {
      id: "hasChunkTypeMonocultureRisk",
      description: "Candidate chunk profile is dominated by one chunk type."
    },
    {
      id: "hasSectionLabelMonocultureRisk",
      description: "Candidate section-label profile is dominated by body-only segmentation."
    },
    {
      id: "hasFallbackFamilyOverestimationRisk",
      description: "Family pattern matches known fallback overestimation signatures from R52 postmortem."
    },
    {
      id: "hasCitationIntentSensitivityRisk",
      description: "Family has elevated citation-intent sensitivity from live query regressions."
    }
  ];
}

export function buildR53PolicyLock({ r48Report, r49Report, r52Report }) {
  const currentCandidates = Array.isArray(r48Report?.candidateRows) ? r48Report.candidateRows : [];

  const frozenFamilies = unique([
    ...((r49Report?.frozenFamilies || []).map((row) => row?.familyLabel)),
    String(r52Report?.frozenFamilyLabel || "")
  ]);

  const citationRegressionQueries = new Set(
    (r52Report?.queryLevelRegressionBreakdown || [])
      .filter((row) => Number(row?.qualityDelta || 0) < 0)
      .map((row) => String(row?.queryId || ""))
      .filter(Boolean)
  );
  const hasCitationRegressionSignal = citationRegressionQueries.has("citation_rule_direct") || citationRegressionQueries.has("citation_ordinance_direct");

  const candidateRows = currentCandidates
    .map((row) => {
      const familyLabel = String(row?.documentFamilyLabel || "unknown");
      const isFrozenFamily = frozenFamilies.includes(familyLabel);

      const hasQueryLevelRegressionRisk = isFrozenFamily && hasCitationRegressionSignal;
      const hasChunkTypeMonocultureRisk = hasMonoculture(row?.chunkTypeProfile || []);
      const hasSectionLabelMonocultureRisk = hasMonoculture(row?.sectionLabelProfile || []);
      const hasFallbackFamilyOverestimationRisk = hasFallbackOverestimationRisk(row);
      const hasCitationIntentSensitivityRisk = isFrozenFamily || hasCitationRegressionSignal;

      const guardrailFlags = {
        hasQueryLevelRegressionRisk,
        hasChunkTypeMonocultureRisk,
        hasSectionLabelMonocultureRisk,
        hasFallbackFamilyOverestimationRisk,
        hasCitationIntentSensitivityRisk
      };

      const triggeredGuardrails = Object.entries(guardrailFlags)
        .filter(([, value]) => value)
        .map(([key]) => key)
        .sort((a, b) => a.localeCompare(b));

      const excludedByR53Policy =
        isFrozenFamily ||
        (hasFallbackFamilyOverestimationRisk && hasChunkTypeMonocultureRisk && hasSectionLabelMonocultureRisk && hasCitationIntentSensitivityRisk);

      return {
        documentId: String(row?.documentId || ""),
        familyLabel,
        excludedByR53Policy,
        triggeredGuardrails,
        previousStatus: "safe_frontier_candidate",
        recommendedDisposition: excludedByR53Policy ? "exclude_until_model_change" : "eligible_for_future_probe_after_revalidation",
        ...guardrailFlags
      };
    })
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const newlyExcludedCandidateIds = candidateRows.filter((row) => row.excludedByR53Policy).map((row) => row.documentId);
  const remainingEligibleCandidateIds = candidateRows.filter((row) => !row.excludedByR53Policy).map((row) => row.documentId);

  const guardrailHitCounts = countBy(candidateRows.flatMap((row) => row.triggeredGuardrails));
  guardrailHitCounts.excludedByR53Policy = newlyExcludedCandidateIds.length;

  const recommendedNextStep =
    remainingEligibleCandidateIds.length === 0
      ? "no_remaining_safe_candidates_model_or_ranking_work_required_before_new_activations"
      : "revalidate_remaining_candidates_with_stricter_family_probe_checks_before_any_activation";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R53",
    summary: {
      sourceSafeCandidateCount: Number(r48Report?.safeCandidatesEvaluated || 0),
      frozenFamilyCount: frozenFamilies.length
    },
    frozenFamilies,
    newlyExcludedCandidateCount: newlyExcludedCandidateIds.length,
    newlyExcludedCandidateIds,
    remainingEligibleCandidateCount: remainingEligibleCandidateIds.length,
    remainingEligibleCandidateIds,
    predictorGuardrails: buildGuardrails(),
    guardrailHitCounts,
    candidateRows,
    recommendedNextStep
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R53 Frontier Policy Lock Report (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push(`- frozenFamilies: ${(report.frozenFamilies || []).join(", ") || "<none>"}`);
  lines.push(`- newlyExcludedCandidateCount: ${report.newlyExcludedCandidateCount}`);
  lines.push(`- remainingEligibleCandidateCount: ${report.remainingEligibleCandidateCount}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Predictor Guardrails");
  for (const row of report.predictorGuardrails || []) {
    lines.push(`- ${row.id}: ${row.description}`);
  }
  lines.push("");

  lines.push("## Guardrail Hit Counts");
  for (const [k, v] of Object.entries(report.guardrailHitCounts || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Candidate Rows");
  for (const row of report.candidateRows || []) {
    lines.push(
      `- ${row.documentId} | family=${row.familyLabel} | excludedByR53Policy=${row.excludedByR53Policy} | triggeredGuardrails=${(row.triggeredGuardrails || []).join(",")}`
    );
  }
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, trust, admission, provenance, or runtime mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r48, r49, r52] = await Promise.all([
    readJson(path.resolve(reportsDir, r48ReportName)),
    readJson(path.resolve(reportsDir, r49ReportName)),
    readJson(path.resolve(reportsDir, r52ReportName))
  ]);

  const report = buildR53PolicyLock({ r48Report: r48, r49Report: r49, r52Report: r52 });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        frozenFamilies: report.frozenFamilies,
        newlyExcludedCandidateCount: report.newlyExcludedCandidateCount,
        remainingEligibleCandidateCount: report.remainingEligibleCandidateCount,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R53 policy lock report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
