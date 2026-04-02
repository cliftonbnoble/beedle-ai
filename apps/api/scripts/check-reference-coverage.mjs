import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const minOrdinanceSections = Number(process.env.MIN_ORDINANCE_SECTIONS || "15");
const minRulesSections = Number(process.env.MIN_RULES_SECTIONS || "10");

async function main() {
  const response = await fetch(`${apiBase}/admin/references`);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`Failed to fetch references: ${response.status} ${JSON.stringify(body)}`);
  }

  const ordinanceCount = body?.summary?.ordinance_section_count || 0;
  const rulesCount = body?.summary?.rules_section_count || 0;
  const unresolvedCrosswalk = Array.isArray(body?.unresolved_crosswalks) ? body.unresolved_crosswalks.length : 0;
  const parserOrdinance = body?.coverage_report?.ordinance?.parser_used || "unknown";
  const parserRules = body?.coverage_report?.rules?.parser_used || "unknown";
  const readinessFromAdmin = body?.readiness_status || {};

  const readiness = {
    ordinanceCoverageOk:
      typeof readinessFromAdmin.ordinance_coverage_ok === "boolean"
        ? readinessFromAdmin.ordinance_coverage_ok
        : ordinanceCount >= minOrdinanceSections,
    rulesCoverageOk:
      typeof readinessFromAdmin.rules_coverage_ok === "boolean" ? readinessFromAdmin.rules_coverage_ok : rulesCount >= minRulesSections,
    crosswalkResolvable:
      typeof readinessFromAdmin.crosswalk_resolvable === "boolean" ? readinessFromAdmin.crosswalk_resolvable : unresolvedCrosswalk === 0,
    countsConsistent:
      typeof readinessFromAdmin.counts_consistent === "boolean"
        ? readinessFromAdmin.counts_consistent
        : (body?.coverage_report?.ordinance?.parsed_section_count ?? ordinanceCount) === ordinanceCount &&
          (body?.coverage_report?.rules?.parsed_section_count ?? rulesCount) === rulesCount,
    criticalCitationsOk:
      typeof readinessFromAdmin.critical_citations_ok === "boolean" ? readinessFromAdmin.critical_citations_ok : undefined,
    crosswalkCandidatesMeaningful:
      typeof readinessFromAdmin.crosswalk_candidates_meaningful === "boolean" ? readinessFromAdmin.crosswalk_candidates_meaningful : undefined
  };

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    thresholds: {
      min_ordinance_sections: minOrdinanceSections,
      min_rules_sections: minRulesSections
    },
    counts: {
      ordinance_sections: ordinanceCount,
      rules_sections: rulesCount,
      unresolved_crosswalk: unresolvedCrosswalk
    },
    parsers: {
      ordinance: parserOrdinance,
      rules: parserRules
    },
    consistency: {
      admin_summary_ordinance: ordinanceCount,
      admin_summary_rules: rulesCount,
      coverage_report_ordinance: body?.coverage_report?.ordinance?.parsed_section_count ?? null,
      coverage_report_rules: body?.coverage_report?.rules?.parsed_section_count ?? null
    },
    readiness
  };

  const outPath = path.resolve(process.cwd(), "reports", "reference-coverage-report.json");
  await fs.writeFile(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.log(`Report written to ${outPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
