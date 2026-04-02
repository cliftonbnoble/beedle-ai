import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerSplitPackets } from "./reviewer-split-packets-utils.mjs";
import { buildReviewerLegalEvidencePackets } from "./reviewer-legal-evidence-utils.mjs";
import { buildReviewerDecisionSimulation } from "./reviewer-decision-sim-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const simInput = process.env.REVIEWER_SPLIT_SIM_INPUT || "./reports/reviewer-decision-sim.json";
const evidenceInput = process.env.REVIEWER_SPLIT_EVIDENCE_INPUT || "./reports/reviewer-legal-evidence.json";
const queueInput = process.env.REVIEWER_SPLIT_QUEUE_INPUT || "./reports/reviewer-batch-export.json";
const reportName = process.env.REVIEWER_SPLIT_REPORT_NAME || "reviewer-split-packets.json";
const markdownName = process.env.REVIEWER_SPLIT_MARKDOWN_NAME || reportName.replace(/\.json$/i, ".md");

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

async function readJsonIfExists(relativePath) {
  const absolute = path.resolve(process.cwd(), relativePath);
  try {
    const raw = await fs.readFile(absolute, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function buildLiveEvidence() {
  const exportResponse = await fetchJson("/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=1200");
  if (exportResponse.status !== 200) {
    throw new Error(`failed to fetch reviewer export queue: ${exportResponse.status} ${JSON.stringify(exportResponse.body)}`);
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
  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    ...buildReviewerLegalEvidencePackets(rows, docSourceById)
  };
}

async function loadQueueRows() {
  const fromFile = await readJsonIfExists(queueInput);
  if (Array.isArray(fromFile?.rows)) return fromFile.rows;
  const response = await fetchJson("/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=1200");
  if (response.status === 200 && Array.isArray(response.body?.rows)) return response.body.rows;
  return [];
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Split Packets");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- splitBatchesFound: ${report.summary.splitBatchesFound}`);
  lines.push(`- splitBatchDocTotal: ${report.summary.splitBatchDocTotal}`);
  lines.push(`- excludedNonSplitBatches: ${report.summary.excludedNonSplitBatches}`);
  lines.push("- splitBatchKeys:");
  for (const key of report.summary.splitBatchKeys || []) lines.push(`  - ${key}`);
  if (!(report.summary.splitBatchKeys || []).length) lines.push("  - none");
  lines.push("");
  lines.push("## Split Batches");
  for (const batch of report.splitBatches || []) {
    lines.push(`### ${batch.batchKey}`);
    lines.push(`- docCount: ${batch.docCount}`);
    lines.push(`- blocked37xFamily: ${(batch.blocked37xFamily || []).join(", ") || "<none>"}`);
    lines.push(`- splitReason: ${batch.splitReason}`);
    lines.push(`- splitConfidence: ${batch.splitConfidence}`);
    lines.push(`- proposedSubBuckets: ${(batch.proposedSubBuckets || []).join(", ") || "<none>"}`);
    lines.push(`- coverageStatus: ${batch.coverageStatus}`);
    lines.push(`- coveredDocCount: ${batch.coveredDocCount}`);
    lines.push(`- uncoveredDocCount: ${batch.uncoveredDocCount}`);
    lines.push(`- duplicateDocCount: ${batch.duplicateDocCount}`);
    lines.push(`- syntheticUnmappedDocCount: ${batch.syntheticUnmappedDocCount}`);
    lines.push(`- uniqueAssignedDocCount: ${batch.uniqueAssignedDocCount}`);
    lines.push(`- coverageWarnings: ${(batch.coverageWarnings || []).join(", ") || "<none>"}`);
    lines.push(`- doNotAutoApply: ${batch.doNotAutoApply}`);
    lines.push("- subBucketCounts:");
    for (const item of batch.subBucketCounts || []) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("- docSimulationOutcomeCounts:");
    for (const item of batch.docSimulationOutcomeCounts || []) lines.push(`  - ${item.key}: ${item.count}`);
    lines.push("");
    lines.push("#### Sub-Bucket Packets");
    for (const packet of batch.subBucketPackets || []) {
      lines.push(`- ${packet.subBucket} | docs=${packet.docCount} | posture=${packet.recommendedReviewerPosture} | doNotAutoApply=${packet.doNotAutoApply}`);
      lines.push("  - sampleDocs:");
      for (const doc of packet.sampleDocs || []) lines.push(`    - ${doc.documentId} | ${doc.title}`);
      lines.push("  - representativeEvidence:");
      for (const evidence of packet.representativeCitationContextEvidence || []) {
        lines.push(
          `    - ${evidence.documentId} | ${evidence.rawCitation} | ${evidence.contextClass} | ${String(evidence.localTextSnippet || "").slice(0, 180)}`
        );
      }
      if (!(packet.representativeCitationContextEvidence || []).length) lines.push("    - none");
    }
    lines.push("");
  }
  if (!(report.splitBatches || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Excluded Non-Split Batches");
  for (const batch of report.excludedNonSplitBatches || []) {
    lines.push(`- ${batch.batchKey} | ${batch.recommendedSimulatedDisposition} | docs=${batch.docCount}`);
  }
  if (!(report.excludedNonSplitBatches || []).length) lines.push("- none");
  return lines.join("\n");
}

async function main() {
  let simReport = await readJsonIfExists(simInput);
  let evidenceReport = await readJsonIfExists(evidenceInput);
  const queueRows = await loadQueueRows();

  if (!simReport || !Array.isArray(simReport.batches)) {
    if (!evidenceReport || !Array.isArray(evidenceReport.packets)) {
      evidenceReport = await buildLiveEvidence();
    }
    simReport = {
      generatedAt: new Date().toISOString(),
      readOnly: true,
      ...buildReviewerDecisionSimulation(evidenceReport)
    };
  }
  if (!evidenceReport || !Array.isArray(evidenceReport.packets)) {
    evidenceReport = null;
  }

  const splitPackets = buildReviewerSplitPackets(simReport, evidenceReport, queueRows);
  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    input: {
      simInputPathUsed: path.resolve(process.cwd(), simInput),
      evidenceInputPathUsed: path.resolve(process.cwd(), evidenceInput),
      queueInputPathUsed: path.resolve(process.cwd(), queueInput),
      queueRowsLoaded: queueRows.length
    },
    ...splitPackets
  };
  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, toMarkdown(report));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${jsonPath}`);
  console.log(`Markdown split packet written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
