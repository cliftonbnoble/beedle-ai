import fs from "node:fs/promises";
import path from "node:path";
import { TOPIC_FAMILIES } from "./provisional-topic-candidate-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const sourceAcquisitionPath = path.resolve(reportsDir, "provisional-topic-source-acquisition-report.json");
const healthReportPath = path.resolve(reportsDir, "retrieval-health-report.json");
const reportPath = path.resolve(reportsDir, "provisional-topic-acquisition-list-report.json");
const markdownPath = path.resolve(reportsDir, "provisional-topic-acquisition-list-report.md");

const TOPIC_INTAKE_PLANS = {
  cooling: {
    priority: "high",
    targetNewDecisions: 12,
    searchPrompts: ["cooling apartment", "air conditioning unit", "insufficient cooling", "overheating apartment"],
    expectedTerms: TOPIC_FAMILIES.cooling,
    userValue: "Adds housing-condition evidence beyond heat/hot-water so natural-language cooling complaints can surface real decisions."
  },
  ventilation: {
    priority: "high",
    targetNewDecisions: 10,
    searchPrompts: ["poor ventilation apartment", "lack of ventilation", "air flow complaint", "bathroom exhaust issue"],
    expectedTerms: TOPIC_FAMILIES.ventilation,
    userValue: "Improves retrieval for stale-air, exhaust, and airflow questions that judges may ask in plain language."
  },
  mold: {
    priority: "high",
    targetNewDecisions: 12,
    searchPrompts: ["mold apartment decision", "mildew housing services", "moisture intrusion tenant complaint", "water damage mold unit"],
    expectedTerms: TOPIC_FAMILIES.mold,
    userValue: "Fills a current zero-result topic family with real condition evidence likely to matter in habitability research."
  }
};

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function buildAcquisitionEntries(sourceReport, healthReport) {
  const zeroFamilies = new Set(
    (healthReport?.queries || [])
      .filter((row) => Number(row.totalResults || 0) === 0)
      .map((row) => String(row.id || ""))
  );

  return Object.entries(TOPIC_INTAKE_PLANS).map(([topic, plan]) => {
    const blockedDocs = (sourceReport?.blockedTopicDocs || []).filter((row) => row.heuristic?.strongestTopic === topic);
    return {
      topic,
      priority: plan.priority,
      targetNewDecisions: plan.targetNewDecisions,
      currentlyZeroResults: zeroFamilies.has(topic),
      searchPrompts: plan.searchPrompts,
      expectedTerms: plan.expectedTerms,
      userValue: plan.userValue,
      localBlockedExamples: blockedDocs.slice(0, 3).map((row) => ({
        id: row.id,
        title: row.title,
        blockers: row.heuristic?.blockers || []
      })),
      acquisitionRecommendation: zeroFamilies.has(topic)
        ? "Acquire new real decision sources; do not spend more cycles reprocessing current blocked docs."
        : "Coverage exists; acquire only if stronger diversity is still needed."
    };
  });
}

function toMarkdown(report) {
  const lines = [
    "# Provisional Topic Acquisition List",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Current health report: \`${path.basename(report.inputs.healthReportPath)}\``,
    `- Current source acquisition report: \`${path.basename(report.inputs.sourceAcquisitionPath)}\``,
    `- Gated reprocess policy: \`preserved\``,
    ""
  ];

  lines.push("## Goal");
  lines.push("");
  lines.push("- Acquire new real decisions for missing topic families without over-specializing the ranker.");
  lines.push("- Keep the current gated reprocess policy so structurally bad local docs do not churn the pipeline.");
  lines.push("- Re-run the same retrieval health report after each import batch.");
  lines.push("");

  for (const entry of report.entries) {
    lines.push(`## ${entry.topic}`);
    lines.push("");
    lines.push(`- Priority: \`${entry.priority}\``);
    lines.push(`- Current zero results: \`${entry.currentlyZeroResults}\``);
    lines.push(`- Target new decisions: \`${entry.targetNewDecisions}\``);
    lines.push(`- User value: ${entry.userValue}`);
    lines.push(`- Acquisition recommendation: ${entry.acquisitionRecommendation}`);
    lines.push(`- Search prompts: ${(entry.searchPrompts || []).map((value) => `\`${value}\``).join(", ")}`);
    lines.push(`- Expected terms: ${(entry.expectedTerms || []).map((value) => `\`${value}\``).join(", ")}`);
    if (entry.localBlockedExamples?.length) {
      lines.push("- Local blocked examples:");
      for (const example of entry.localBlockedExamples) {
        lines.push(`  - \`${example.id}\` | ${example.title} | blockers=${(example.blockers || []).join(", ")}`);
      }
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [sourceReport, healthReport] = await Promise.all([readJson(sourceAcquisitionPath), readJson(healthReportPath)]);

  const report = {
    generatedAt: new Date().toISOString(),
    inputs: {
      sourceAcquisitionPath,
      healthReportPath
    },
    policy: {
      gatedReprocessPreserved: true,
      recallGuardrail: "Favor decision-level breadth and chunk-level quality; do not hard-filter normal search into narrow zero-return behavior."
    },
    entries: buildAcquisitionEntries(sourceReport, healthReport)
  };

  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(markdownPath, toMarkdown(report))
  ]);

  console.log(JSON.stringify({ entries: report.entries.length, gatedReprocessPreserved: true }, null, 2));
  console.log(`Provisional topic acquisition list JSON report written to ${reportPath}`);
  console.log(`Provisional topic acquisition list Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
