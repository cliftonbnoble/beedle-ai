import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerLegalEvidencePackets } from "./reviewer-legal-evidence-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number(process.env.REVIEWER_LEGAL_EVIDENCE_LIMIT || "1200");
const reportName = process.env.REVIEWER_LEGAL_EVIDENCE_REPORT_NAME || "reviewer-legal-evidence.json";
const markdownName = process.env.REVIEWER_LEGAL_EVIDENCE_MARKDOWN_NAME || reportName.replace(/\.json$/i, ".md");

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
  lines.push("# Reviewer Legal Evidence Packets");
  lines.push("");
  lines.push("## Cross-Batch Summary");
  lines.push(`- blocked batches: ${report.summary.blockedBatchCount}`);
  lines.push(`- blocked docs: ${report.summary.blockedDocCount}`);
  lines.push("");
  lines.push("### Most Repeated Blocked Citation/Context Pairs");
  for (const item of report.summary.mostRepeatedBlockedCitationContextPairs) {
    lines.push(`- ${item.citation} | ${item.context} | ${item.count}`);
  }
  if (!report.summary.mostRepeatedBlockedCitationContextPairs.length) lines.push("- none");
  lines.push("");
  lines.push("### Batches With Strongest Ordinance-Like Evidence");
  for (const item of report.summary.batchesWithStrongestOrdinanceLikeEvidence) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | ordinanceLike=${item.ordinanceLike}`);
  }
  if (!report.summary.batchesWithStrongestOrdinanceLikeEvidence.length) lines.push("- none");
  lines.push("");
  lines.push("### Batches With Strongest Rules-Like Evidence");
  for (const item of report.summary.batchesWithStrongestRulesLikeEvidence) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | rulesLike=${item.rulesLike}`);
  }
  if (!report.summary.batchesWithStrongestRulesLikeEvidence.length) lines.push("- none");
  lines.push("");
  lines.push("### Batches Too Ambiguous For Manual Context Correction");
  for (const item of report.summary.batchesTooAmbiguousForManualContextCorrection) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | ambiguous=${item.ambiguous} | noUsefulContext=${item.noUsefulContext}`);
  }
  if (!report.summary.batchesTooAmbiguousForManualContextCorrection.length) lines.push("- none");
  lines.push("");
  lines.push("### Suggested Human Review Order Using Context Evidence");
  for (const item of report.summary.suggestedHumanReviewOrderUsingContextEvidence) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | appearance=${item.issueAppearanceLikely} | posture=${item.recommendedReviewerPosture}`);
  }
  if (!report.summary.suggestedHumanReviewOrderUsingContextEvidence.length) lines.push("- none");
  lines.push("");
  lines.push("## Batch Evidence Packets");
  for (const packet of report.packets) {
    lines.push(`### ${packet.batchKey}`);
    lines.push(`- docCount: ${packet.docCount}`);
    lines.push(`- blocked37xFamily: ${packet.blocked37xFamily.join(", ") || "<none>"}`);
    lines.push(`- unresolvedTriageBuckets: ${packet.unresolvedTriageBuckets.join(", ") || "<none>"}`);
    lines.push(`- dominantBlockerPattern: ${packet.dominantBlockerPattern}`);
    lines.push(`- dominantCitationFamily: ${packet.dominantCitationFamily || "<none>"}`);
    lines.push(`- issueAppearanceLikely: ${packet.issueAppearanceLikely}`);
    lines.push(`- recommendedReviewerPosture: ${packet.recommendedReviewerPosture}`);
    lines.push(`- contextSummary: ordinance=${packet.contextSummary.ordinanceLike}, rules=${packet.contextSummary.rulesLike}, ambiguous=${packet.contextSummary.ambiguous}, none=${packet.contextSummary.noUsefulContext}`);
    lines.push("- topRawCitationStringsByCount:");
    for (const item of packet.topRawCitationStringsByCount.slice(0, 8)) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("- topNormalizedValuesByCount:");
    for (const item of packet.topNormalizedValuesByCount.slice(0, 8)) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("- topRootCausesByCount:");
    for (const item of packet.topRootCausesByCount.slice(0, 8)) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("- representativeEvidence:");
    const snippets = packet.patternEvidence.flatMap((pattern) => pattern.representativeSnippets).slice(0, 8);
    for (const snippet of snippets) {
      lines.push(
        `  - ${snippet.documentId} | ${snippet.rawCitation} | ${snippet.contextClass} | ${snippet.localTextSnippet.slice(0, 200)}`
      );
    }
    if (!snippets.length) lines.push("  - none");
    lines.push("- reviewChecklist:");
    for (const step of packet.reviewChecklist) lines.push(`  - ${step}`);
    lines.push("- reviewerNotesTemplate:");
    for (const note of packet.reviewerNotesTemplate) lines.push(`  - ${note}`);
    lines.push("");
  }
  return lines.join("\n");
}

async function main() {
  const exportResponse = await fetchJson(`/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=${Math.max(1, limit)}`);
  if (exportResponse.status !== 200) {
    throw new Error(`failed to fetch reviewer export: ${exportResponse.status} ${JSON.stringify(exportResponse.body)}`);
  }
  const rows = Array.isArray(exportResponse.body?.rows) ? exportResponse.body.rows : [];
  const docIds = Array.from(new Set(rows.map((row) => String(row.documentId || "")).filter(Boolean)));
  const docSourceById = new Map();
  for (const docId of docIds) {
    const detail = await fetchJson(`/admin/ingestion/documents/${docId}`);
    if (detail.status !== 200) continue;
    const chunks = Array.isArray(detail.body?.chunks) ? detail.body.chunks : [];
    const sections = Array.isArray(detail.body?.sections) ? detail.body.sections : [];
    const chunkBlocks = chunks.map((chunk) => String(chunk.chunkText || ""));
    const sectionBlocks = sections.flatMap((section) =>
      Array.isArray(section.paragraphs) ? section.paragraphs.map((p) => String(p.text || "")) : []
    );
    docSourceById.set(docId, {
      title: String(detail.body?.title || ""),
      textBlocks: [...chunkBlocks, ...sectionBlocks]
    });
  }

  const packets = buildReviewerLegalEvidencePackets(rows, docSourceById);
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
  console.log(`Markdown evidence written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
