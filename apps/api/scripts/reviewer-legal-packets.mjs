import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerLegalPackets } from "./reviewer-legal-packets-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number(process.env.REVIEWER_LEGAL_PACKET_LIMIT || "1200");
const reportName = process.env.REVIEWER_LEGAL_PACKET_REPORT_NAME || "reviewer-legal-packets.json";
const markdownName = process.env.REVIEWER_LEGAL_PACKET_MARKDOWN_NAME || reportName.replace(/\.json$/i, ".md");

async function fetchJson(endpoint) {
  const response = await fetch(`${apiBase}${endpoint}`);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    body = { raw };
  }
  return { status: response.status, body };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Legal Packets");
  lines.push("");
  lines.push("## Cross-Batch Summary");
  lines.push(`- blocked batches: ${report.summary.blockedBatchCount}`);
  lines.push(`- blocked docs: ${report.summary.blockedDocCount}`);
  lines.push("");
  lines.push("### Largest Blocked Legal Batches");
  for (const item of report.summary.largestBlockedLegalBatches) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | family=${item.blocked37xFamily.join(", ") || "<none>"}`);
  }
  if (!report.summary.largestBlockedLegalBatches.length) lines.push("- none");
  lines.push("");
  lines.push("### Citation Families Causing Most Blocked Docs");
  for (const item of report.summary.citationFamiliesCausingMostBlockedDocs) {
    lines.push(`- ${item.family}: ${item.docCount}`);
  }
  if (!report.summary.citationFamiliesCausingMostBlockedDocs.length) lines.push("- none");
  lines.push("");
  lines.push("### Top Root Causes Across Blocked Legal Batches");
  for (const item of report.summary.topRootCausesAcrossBlockedBatches) {
    lines.push(`- ${item.rootCause}: ${item.count}`);
  }
  if (!report.summary.topRootCausesAcrossBlockedBatches.length) lines.push("- none");
  lines.push("");
  lines.push("### Docs Per Blocked37x Family");
  for (const item of report.summary.docsPerBlocked37xFamily) {
    lines.push(`- ${item.family}: ${item.docCount}`);
  }
  if (!report.summary.docsPerBlocked37xFamily.length) lines.push("- none");
  lines.push("");
  lines.push("### Suggested Review Order");
  for (const item of report.summary.suggestedReviewOrder) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | posture=${item.recommendedDecisionPosture} | rationale=${item.rationale}`);
  }
  if (!report.summary.suggestedReviewOrder.length) lines.push("- none");
  lines.push("");
  lines.push("## Batch Packets");
  for (const packet of report.packets) {
    lines.push(`### ${packet.batchKey}`);
    lines.push(`- docCount: ${packet.docCount}`);
    lines.push(`- blocked37xFamily: ${packet.blocked37xFamily.join(", ") || "<none>"}`);
    lines.push(`- unresolvedTriageBuckets: ${packet.unresolvedTriageBuckets.join(", ") || "<none>"}`);
    lines.push(`- dominantBlockerPattern: ${packet.dominantBlockerPattern}`);
    lines.push(`- dominantCitationFamily: ${packet.dominantCitationFamily || "<none>"}`);
    lines.push(`- issueAppearanceLikely: ${packet.issueAppearanceLikely}`);
    lines.push(`- recommendedDecisionPosture: ${packet.recommendedDecisionPosture}`);
    lines.push(`- recommendedReviewerAction: ${packet.recommendedReviewerAction}`);
    lines.push("- sampleDocs:");
    for (const doc of packet.sampleDocs) lines.push(`  - ${doc.documentId} | ${doc.title}`);
    lines.push("- topRawCitationStrings:");
    for (const item of packet.topRawCitationStrings.slice(0, 8)) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("- topNormalizedValues:");
    for (const item of packet.topNormalizedValues.slice(0, 8)) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("- topRootCauses:");
    for (const item of packet.topRootCauses.slice(0, 8)) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("- recommendedReviewerQuestions:");
    for (const q of packet.recommendedReviewerQuestions) lines.push(`  - ${q}`);
    lines.push("- reviewChecklist:");
    for (const step of packet.reviewChecklist) lines.push(`  - ${step}`);
    lines.push("");
  }
  return lines.join("\n");
}

async function main() {
  const response = await fetchJson(`/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=${Math.max(1, limit)}`);
  if (response.status !== 200) {
    throw new Error(`failed to fetch reviewer queue: ${response.status} ${JSON.stringify(response.body)}`);
  }
  const rows = Array.isArray(response.body?.rows) ? response.body.rows : [];
  const packets = buildReviewerLegalPackets(rows);
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    readOnly: true,
    rowsAnalyzed: rows.length,
    ...packets
  };
  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, toMarkdown(report));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${jsonPath}`);
  console.log(`Markdown packet written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
