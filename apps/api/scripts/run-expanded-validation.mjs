import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

async function fetchJson(pathname, init) {
  const response = await fetch(`${apiBase}${pathname}`, init);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  return { status: response.status, body };
}

function hasCitationSupport(payload) {
  const citationIds = new Set((payload.citations || []).map((row) => row.id));
  return (payload.paragraph_support || []).every((row) => (row.citation_ids || []).every((id) => citationIds.has(id)));
}

async function main() {
  const scenarios = [
    {
      name: "zoning_variance",
      findings_text: "Applicant requests lot-coverage variance with neighborhood notice evidence and mitigation plan.",
      law_text: "Rule 3.1 notice requirements and Ordinance 77-19 variance criteria apply.",
      index_codes: ["IC-104"],
      rules_sections: ["Rule 3.1"],
      ordinance_sections: ["Ordinance 77-19"],
      issue_tags: ["variance", "lot coverage"]
    },
    {
      name: "mixed_support",
      findings_text: "Record is mixed: compliance evidence conflicts with inspection notes and timeline is incomplete.",
      law_text: "Applicable enforcement provisions are referenced but exact sections are uncertain.",
      issue_tags: ["compliance", "penalty"]
    }
  ];

  const results = [];

  for (const scenario of scenarios) {
    const conclusions = await fetchJson("/api/draft/conclusions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(scenario)
    });
    const template = await fetchJson("/api/draft/template", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        case_type: scenario.name === "zoning_variance" ? "zoning_variance" : "general",
        template_mode: "lightly_contextualized",
        findings_text: scenario.findings_text,
        law_text: scenario.law_text
      })
    });

    results.push({
      name: scenario.name,
      conclusionsStatus: conclusions.status,
      templateStatus: template.status,
      conclusionsConfidence: conclusions.body?.confidence || null,
      limitations: conclusions.body?.limitations || [],
      paragraphSupportCount: (conclusions.body?.paragraph_support || []).length,
      citationSupportValid: conclusions.status === 200 ? hasCitationSupport(conclusions.body) : false,
      templateCaseType: template.body?.case_type || null,
      templateSections: (template.body?.template_sections || []).length
    });
  }

  const summary = {
    totalScenarios: results.length,
    passing: results.filter((row) => row.conclusionsStatus === 200 && row.templateStatus === 200 && row.citationSupportValid).length,
    withLowConfidence: results.filter((row) => row.conclusionsConfidence === "low").length
  };

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    summary,
    results
  };

  const outputPath = path.resolve(process.cwd(), "reports", "expanded-drafting-validation-report.json");
  await fs.writeFile(outputPath, JSON.stringify(report, null, 2));
  console.log("Expanded drafting/template validation summary:");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Detailed report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
