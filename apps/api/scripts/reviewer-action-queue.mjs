import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerActionQueue } from "./reviewer-action-queue-utils.mjs";

const splitInput = process.env.REVIEWER_ACTION_QUEUE_SPLIT_INPUT || "./reports/reviewer-split-packets.json";
const simInput = process.env.REVIEWER_ACTION_QUEUE_SIM_INPUT || "./reports/reviewer-decision-sim.json";
const evidenceInput = process.env.REVIEWER_ACTION_QUEUE_EVIDENCE_INPUT || "./reports/reviewer-legal-evidence.json";
const reportName = process.env.REVIEWER_ACTION_QUEUE_REPORT_NAME || "reviewer-action-queue.json";
const markdownName = process.env.REVIEWER_ACTION_QUEUE_MARKDOWN_NAME || reportName.replace(/\.json$/i, ".md");

async function readJson(relativePath) {
  const abs = path.resolve(process.cwd(), relativePath);
  const raw = await fs.readFile(abs, "utf8");
  return JSON.parse(raw);
}

function sectionLines(title, rows) {
  const lines = [];
  lines.push(`## ${title}`);
  if (!rows.length) {
    lines.push("- none");
    lines.push("");
    return lines;
  }
  for (const row of rows) {
    lines.push(
      `- #${row.queueOrder} | ${row.documentId} | ${row.title} | batch=${row.batchKey} | subBucket=${row.subBucket || "<none>"} | posture=${row.recommendedReviewerPosture}`
    );
    lines.push(`  - lane=${row.priorityLane} | disposition=${row.recommendedSimulatedDisposition} | legalEscalation=${row.requiresLegalEscalation}`);
    lines.push(`  - rootCause=${row.rootCauseSummary} | contextClass=${row.contextClass}`);
    lines.push(`  - evidence=${row.topEvidenceSnippet ? row.topEvidenceSnippet.slice(0, 180) : "<none>"}`);
  }
  lines.push("");
  return lines;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Action Queue");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- totalQueueRows: ${report.summary.totalQueueRows}`);
  lines.push(`- splitBatchesRepresented: ${report.summary.splitBatchesRepresented}`);
  lines.push("- splitCoverage:");
  for (const item of report.summary.splitCoverage || []) {
    lines.push(`  - ${item.batchKey}: ${item.queueRowCount}/${item.expectedDocCount} (${item.coverageStatus})`);
  }
  if (!(report.summary.splitCoverage || []).length) lines.push("  - none");
  lines.push("- countsByPriorityLane:");
  for (const item of report.summary.countsByPriorityLane || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- countsByRecommendedReviewerPosture:");
  for (const item of report.summary.countsByRecommendedReviewerPosture || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- countsBySubBucket:");
  for (const item of report.summary.countsBySubBucket || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- countsByBlocked37xFamily:");
  for (const item of report.summary.countsByBlocked37xFamily || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("");

  lines.push("## Top 20 Docs To Review First");
  for (const row of report.summary.top20DocsToReviewFirst || []) {
    lines.push(
      `- #${row.queueOrder} | ${row.documentId} | ${row.title} | lane=${row.priorityLane} | posture=${row.recommendedReviewerPosture} | batch=${row.batchKey} | subBucket=${row.subBucket || "<none>"}`
    );
  }
  if (!(report.summary.top20DocsToReviewFirst || []).length) lines.push("- none");
  lines.push("");

  lines.push(...sectionLines("Review-First Queue", report.reviewFirstQueue || []));
  lines.push(...sectionLines("Review-After Queue", report.reviewAfterQueue || []));
  lines.push(...sectionLines("Hold-Blocked Queue", report.holdBlockedQueue || []));

  return lines.join("\n");
}

async function main() {
  const splitReport = await readJson(splitInput);
  const decisionReport = await readJson(simInput);
  const evidenceReport = await readJson(evidenceInput);

  const queue = buildReviewerActionQueue(splitReport, decisionReport, evidenceReport);
  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    input: {
      splitInputPathUsed: path.resolve(process.cwd(), splitInput),
      simInputPathUsed: path.resolve(process.cwd(), simInput),
      evidenceInputPathUsed: path.resolve(process.cwd(), evidenceInput)
    },
    ...queue
  };

  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, toMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${jsonPath}`);
  console.log(`Markdown action queue written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
