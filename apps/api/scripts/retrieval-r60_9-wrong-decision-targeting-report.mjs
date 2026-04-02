import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r60_8Name = process.env.RETRIEVAL_R60_9_SOURCE_REPORT_NAME || "retrieval-r60_8-query-normalization-report.json";
const outputJsonName =
  process.env.RETRIEVAL_R60_9_REPORT_NAME || "retrieval-r60_9-wrong-decision-targeting-report.json";
const outputMdName = process.env.RETRIEVAL_R60_9_MARKDOWN_NAME || "retrieval-r60_9-wrong-decision-targeting-report.md";

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function countBy(values = []) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCounts(obj = {}) {
  return Object.entries(obj)
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([key, count]) => ({ key, count }));
}

function missReasonByIntent(intent) {
  switch (String(intent || "")) {
    case "citation_direct":
      return "citation_terms_underweighted";
    case "procedural_history":
      return "procedural_terms_underweighted";
    case "findings":
      return "findings_terms_underweighted";
    case "issue_holding_disposition":
      return "disposition_terms_underweighted";
    case "authority_lookup":
      return "authority_terms_underweighted";
    default:
      return "mixed_query_weighting_gap";
  }
}

function refinementCandidatesForIntent(pkg, intent) {
  const cands = [];
  if (pkg?.citationFocusedQuery) cands.push(pkg.citationFocusedQuery);
  if (pkg?.proceduralQuery) cands.push(pkg.proceduralQuery);
  if (pkg?.findingsCredibilityQuery) cands.push(pkg.findingsCredibilityQuery);
  if (pkg?.dispositionQuery) cands.push(pkg.dispositionQuery);

  if (intent === "citation_direct") {
    cands.push("rule 37.8 authority discussion legal standard");
    cands.push("ordinance 37.2 authority analysis holding");
  } else if (intent === "procedural_history") {
    cands.push("procedural history hearing notice continuance timeline");
  } else if (intent === "findings") {
    cands.push("findings of fact credibility evidence weight testimony");
  } else if (intent === "issue_holding_disposition") {
    cands.push("issue presented holding disposition final order");
  } else if (intent === "authority_lookup") {
    cands.push("authority discussion ordinance rule legal standard analysis");
  } else {
    cands.push("analysis reasoning legal standard application");
  }

  return unique(cands).slice(0, 5);
}

function likelyWhyFalsePositiveWon({ intent, falsePositiveSections, legalConceptTerms }) {
  const sections = new Set((falsePositiveSections || []).map(String));
  const legalTerms = (legalConceptTerms || []).map(String);
  if (intent === "citation_direct" && !legalTerms.some((t) => /rule|ordinance|citation/i.test(t))) {
    return "citation_terms_underweighted_relative_to_generic_analysis_terms";
  }
  if (intent === "procedural_history" && sections.has("analysis_reasoning")) {
    return "procedural_signals_drowned_out_by_analysis_reasoning_matches";
  }
  if (intent === "findings" && sections.has("analysis_reasoning")) {
    return "findings_credibility_terms_not_strong_enough_against_general_analysis_hits";
  }
  if (intent === "issue_holding_disposition" && sections.has("analysis_reasoning")) {
    return "disposition_order_terms_underweighted_against_analysis_hits";
  }
  if (intent === "authority_lookup" && sections.has("analysis_reasoning")) {
    return "authority_terms_not_specific_enough_to_outrank_analysis_sections";
  }
  return "mixed_query_weighting_gap_across_intent_and_section_signals";
}

function choosePrimaryMissReason(rows) {
  const reasons = rows.map((row) => row.primaryMissReason);
  const counts = countBy(reasons);
  const ordered = Object.entries(counts).sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return a[0].localeCompare(b[0]);
  });
  if (!ordered.length) return "mixed_query_weighting_gap";
  if (ordered.length > 1 && ordered[0][1] === ordered[1][1]) return "mixed_query_weighting_gap";
  return ordered[0][0];
}

export function buildR60_9WrongDecisionTargetingReport(r60_8Report) {
  const taskEvalById = new Map((r60_8Report?.taskEvaluations || []).map((row) => [String(row?.queryId || ""), row]));
  const normalizedById = new Map((r60_8Report?.normalizedPackages || []).map((row) => [String(row?.queryId || ""), row]));
  const wrongIds = new Set((r60_8Report?.wrongDecisionTaskIds || []).map(String));

  const wrongRows = (r60_8Report?.taskEvaluations || [])
    .filter((row) => wrongIds.has(String(row?.queryId || "")))
    .map((row) => {
      const queryId = String(row?.queryId || "");
      const pkg = normalizedById.get(queryId) || {};
      const bestVariant = (row?.variantRows || []).find((v) => v.variantType === row.bestVariantType) || (row?.variantRows || [])[0] || {};
      const expectedDecisionIds = (row?.expectedDecisionIds || []).map(String);
      const returnedDecisionIds = (bestVariant?.topReturnedDecisionIds || []).map(String);
      const expectedSectionTypes = unique((row?.expectedSectionTypes || []).map(String));
      const returnedSectionTypes = unique((bestVariant?.topReturnedSectionTypes || []).map(String));
      const topFalsePositiveDecisionIds = returnedDecisionIds.filter((id) => !expectedDecisionIds.includes(id));
      const falsePositiveChunkTypes = returnedSectionTypes.filter((type) => !expectedSectionTypes.includes(type));
      const primaryMissReason = missReasonByIntent(row?.intent);

      return {
        queryId,
        originalQuery: String(pkg?.originalQuery || ""),
        adoptedNormalizedQuery: String(bestVariant?.query || pkg?.normalizedQuery || pkg?.compressedKeywordQuery || ""),
        expectedDecisionIds,
        returnedDecisionIds,
        topFalsePositiveDecisionIds,
        topReturnedSectionTypes: returnedSectionTypes,
        dominantFalsePositiveChunkTypes: falsePositiveChunkTypes.length ? falsePositiveChunkTypes : returnedSectionTypes,
        likelyWhyFalsePositiveWon: likelyWhyFalsePositiveWon({
          intent: row?.intent,
          falsePositiveSections: falsePositiveChunkTypes,
          legalConceptTerms: pkg?.legalConceptTerms || []
        }),
        queryRefinementCandidates: refinementCandidatesForIntent(pkg, row?.intent),
        intent: String(row?.intent || ""),
        primaryMissReason
      };
    })
    .sort((a, b) => a.queryId.localeCompare(b.queryId));

  const recoveredButWrongTaskCount = wrongRows.length;
  const dominantWrongDecisionPatterns = sortCounts(countBy(wrongRows.map((row) => row.likelyWhyFalsePositiveWon)));
  const dominantFalsePositiveChunkTypes = sortCounts(
    countBy(wrongRows.flatMap((row) => row.dominantFalsePositiveChunkTypes || []))
  );
  const dominantFalsePositiveSectionTypes = sortCounts(
    countBy(wrongRows.flatMap((row) => row.topReturnedSectionTypes || []))
  );
  const bestRefinementPatterns = sortCounts(
    countBy(wrongRows.flatMap((row) => (row.queryRefinementCandidates || []).slice(0, 2)))
  );

  const candidateIntentSpecificRewriteRules = Object.entries(
    wrongRows.reduce((acc, row) => {
      const key = row.intent || "unknown";
      if (!acc[key]) acc[key] = new Set();
      for (const cand of row.queryRefinementCandidates || []) acc[key].add(cand);
      return acc;
    }, {})
  )
    .map(([intent, ruleSet]) => ({
      intent,
      rewriteRules: Array.from(ruleSet).sort((a, b) => a.localeCompare(b)).slice(0, 5)
    }))
    .sort((a, b) => a.intent.localeCompare(b.intent));

  const recommendedNextStep =
    recoveredButWrongTaskCount > 0
      ? "apply_intent_specific_query_weighting_templates_then_rerun_r60_8_and_r60_7"
      : "no_wrong_decision_tasks_detected_keep_current_normalization_profile";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.9",
    recoveredButWrongTaskCount,
    dominantWrongDecisionPatterns,
    dominantFalsePositiveChunkTypes,
    dominantFalsePositiveSectionTypes,
    bestRefinementPatterns,
    candidateIntentSpecificRewriteRules,
    primaryMissReasonClassification: choosePrimaryMissReason(wrongRows),
    recommendedNextStep,
    perTaskRows: wrongRows
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.9 Wrong-Decision Targeting (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- recoveredButWrongTaskCount: ${report.recoveredButWrongTaskCount}`);
  lines.push(`- primaryMissReasonClassification: ${report.primaryMissReasonClassification}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Dominant Wrong Decision Patterns");
  for (const row of report.dominantWrongDecisionPatterns || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.dominantWrongDecisionPatterns || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Dominant False Positive Chunk Types");
  for (const row of report.dominantFalsePositiveChunkTypes || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.dominantFalsePositiveChunkTypes || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Candidate Intent-Specific Rewrite Rules");
  for (const row of report.candidateIntentSpecificRewriteRules || []) {
    lines.push(`- ${row.intent}: ${(row.rewriteRules || []).join(" | ")}`);
  }
  if (!(report.candidateIntentSpecificRewriteRules || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const source = await fs.readFile(path.resolve(reportsDir, r60_8Name), "utf8");
  const report = buildR60_9WrongDecisionTargetingReport(JSON.parse(source));

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        recoveredButWrongTaskCount: report.recoveredButWrongTaskCount,
        primaryMissReasonClassification: report.primaryMissReasonClassification,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.9 wrong-decision targeting report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
