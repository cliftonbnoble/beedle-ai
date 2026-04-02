import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r48ReportName = process.env.RETRIEVAL_R49_R48_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const outputJsonName = process.env.RETRIEVAL_R49_REPORT_NAME || "retrieval-r49-family-freeze-report.json";
const outputMdName = process.env.RETRIEVAL_R49_MARKDOWN_NAME || "retrieval-r49-family-freeze-report.md";

async function readJsonStrict(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((value) => String(value)))).sort((a, b) => a.localeCompare(b));
}

function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + Number(value || 0), 0) / values.length).toFixed(4));
}

function byFamily(candidateRows = []) {
  const groups = new Map();
  for (const row of candidateRows || []) {
    const familyLabel = String(row?.documentFamilyLabel || "unknown");
    if (!groups.has(familyLabel)) groups.set(familyLabel, []);
    groups.get(familyLabel).push(row);
  }
  return groups;
}

function decideFamily(familyRows = []) {
  const knownRows = familyRows.filter((row) => row.actualKnownQualityDelta !== null);
  const knownOutcomeCount = knownRows.length;
  const knownMissCount = knownRows.filter((row) => Number(row.actualKnownQualityDelta || 0) < -0.5).length;
  const knownMissRate = knownOutcomeCount ? Number((knownMissCount / knownOutcomeCount).toFixed(4)) : 0;

  const simulatedQualityDeltaAvg = avg(familyRows.map((row) => row.simulatedQualityDelta));
  const projectedCitationTopDocumentShareAvg = avg(familyRows.map((row) => row.projectedCitationTopDocumentShare));
  const projectedLowSignalStructuralShareAvg = avg(familyRows.map((row) => row.projectedLowSignalStructuralShare));

  let riskLabel = "medium";
  if (knownOutcomeCount >= 1 && knownMissRate === 1) riskLabel = "high";
  else if (knownOutcomeCount === 0) riskLabel = "unknown";
  else if (knownMissRate === 0) riskLabel = "low";

  let decision = "eligible_for_family_probe";
  let decisionReason = "no_proven_live_failure_in_family";
  if (knownOutcomeCount >= 1 && knownMissRate === 1) {
    decision = "freeze_family";
    decisionReason = "proven_live_quality_misprediction";
  } else if (knownOutcomeCount === 0) {
    decision = "hold_for_probe_plan";
    decisionReason = "no_real_outcome_observed_for_family";
  }

  return {
    familySize: familyRows.length,
    knownRealOutcomeCount: knownOutcomeCount,
    knownMissRate,
    simulatedQualityDeltaAvg,
    projectedCitationTopDocumentShareAvg,
    projectedLowSignalStructuralShareAvg,
    riskLabel,
    decision,
    decisionReason
  };
}

export function buildR49FamilyFreezeReport(r48Report) {
  const candidateRows = Array.isArray(r48Report?.candidateRows) ? r48Report.candidateRows : [];
  const grouped = byFamily(candidateRows);

  const familyRows = Array.from(grouped.entries())
    .map(([familyLabel, rows]) => ({
      familyLabel,
      ...decideFamily(rows)
    }))
    .sort((a, b) => {
      const order = { freeze_family: 0, hold_for_probe_plan: 1, eligible_for_family_probe: 2 };
      const ao = order[a.decision] ?? 99;
      const bo = order[b.decision] ?? 99;
      if (ao !== bo) return ao - bo;
      if (b.familySize !== a.familySize) return b.familySize - a.familySize;
      return String(a.familyLabel).localeCompare(String(b.familyLabel));
    });

  const frozenFamilies = familyRows.filter((row) => row.decision === "freeze_family");
  const eligibleFamilies = familyRows.filter((row) => row.decision !== "freeze_family");

  const frozenSet = new Set(frozenFamilies.map((row) => row.familyLabel));
  const excludedCandidateIds = unique(
    candidateRows.filter((row) => frozenSet.has(String(row.documentFamilyLabel || "unknown"))).map((row) => row.documentId)
  );
  const remainingCandidateRows = candidateRows.filter((row) => !frozenSet.has(String(row.documentFamilyLabel || "unknown")));
  const remainingCandidateIds = unique(remainingCandidateRows.map((row) => row.documentId));

  const nextBestFamilyIfAny =
    eligibleFamilies
      .slice()
      .sort((a, b) => {
        if (b.simulatedQualityDeltaAvg !== a.simulatedQualityDeltaAvg) return b.simulatedQualityDeltaAvg - a.simulatedQualityDeltaAvg;
        if (a.projectedCitationTopDocumentShareAvg !== b.projectedCitationTopDocumentShareAvg) {
          return a.projectedCitationTopDocumentShareAvg - b.projectedCitationTopDocumentShareAvg;
        }
        return String(a.familyLabel).localeCompare(String(b.familyLabel));
      })[0] || null;

  const nextBestCandidateIfAny =
    remainingCandidateRows
      .slice()
      .sort((a, b) => {
        const aq = Number(a.simulatedQualityDelta || 0);
        const bq = Number(b.simulatedQualityDelta || 0);
        if (bq !== aq) return bq - aq;
        const ac = Number(a.projectedCitationTopDocumentShare || 0);
        const bc = Number(b.projectedCitationTopDocumentShare || 0);
        if (ac !== bc) return ac - bc;
        return String(a.documentId).localeCompare(String(b.documentId));
      })[0] || null;

  const onlyUnknownEligibleFamilies =
    eligibleFamilies.length > 0 && eligibleFamilies.every((row) => Number(row.knownRealOutcomeCount || 0) === 0);

  const activationRecommendation = onlyUnknownEligibleFamilies ? "no" : nextBestCandidateIfAny ? "yes" : "no";
  const recommendedNextStep = onlyUnknownEligibleFamilies
    ? "no_activation_until_family_probe_plan"
    : nextBestCandidateIfAny
      ? `safe_single_doc_activation_candidate:${nextBestCandidateIfAny.documentId}`
      : "do_not_activate_any_more_singles";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R49",
    summary: {
      sourceR48SafeCandidateCount: Number(r48Report?.safeCandidatesEvaluated || 0),
      sourceR48QualityMispredictionCount: Number(r48Report?.qualityMispredictionCount || 0)
    },
    safeFamiliesEvaluated: familyRows.length,
    frozenFamilies,
    eligibleFamilies,
    familyRows,
    excludedCandidateIds,
    remainingCandidateIds,
    nextBestFamilyIfAny,
    nextBestCandidateIfAny,
    activationRecommendation,
    recommendedNextStep
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R49 Family Freeze Report (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push(`- safeFamiliesEvaluated: ${report.safeFamiliesEvaluated}`);
  lines.push(`- frozenFamilies: ${(report.frozenFamilies || []).length}`);
  lines.push(`- eligibleFamilies: ${(report.eligibleFamilies || []).length}`);
  lines.push(`- activationRecommendation: ${report.activationRecommendation}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Family Rows");
  for (const row of report.familyRows || []) {
    lines.push(
      `- ${row.familyLabel} | size=${row.familySize} | knownOutcomes=${row.knownRealOutcomeCount} | missRate=${row.knownMissRate} | simDeltaAvg=${row.simulatedQualityDeltaAvg} | decision=${row.decision} (${row.decisionReason})`
    );
  }
  if (!(report.familyRows || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Candidate Sets");
  lines.push(`- excludedCandidateIds: ${(report.excludedCandidateIds || []).join(", ") || "<none>"}`);
  lines.push(`- remainingCandidateIds: ${(report.remainingCandidateIds || []).join(", ") || "<none>"}`);
  lines.push("");

  lines.push("## Next Best");
  lines.push(`- nextBestFamilyIfAny: ${report.nextBestFamilyIfAny?.familyLabel || ""}`);
  lines.push(`- nextBestCandidateIfAny: ${report.nextBestCandidateIfAny?.documentId || ""}`);
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, or policy mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const r48ReportPath = path.resolve(reportsDir, r48ReportName);
  const r48Report = await readJsonStrict(r48ReportPath);

  const report = buildR49FamilyFreezeReport(r48Report);

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);

  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        safeFamiliesEvaluated: report.safeFamiliesEvaluated,
        frozenFamilies: (report.frozenFamilies || []).length,
        eligibleFamilies: (report.eligibleFamilies || []).length,
        activationRecommendation: report.activationRecommendation,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R49 family freeze report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
