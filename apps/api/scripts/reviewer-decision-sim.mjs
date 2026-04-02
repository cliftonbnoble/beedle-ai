import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerDecisionSimulation } from "./reviewer-decision-sim-utils.mjs";
import { buildReviewerLegalEvidencePackets } from "./reviewer-legal-evidence-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const evidenceInput = process.env.REVIEWER_DECISION_SIM_INPUT || "";
const reportName = process.env.REVIEWER_DECISION_SIM_REPORT_NAME || "reviewer-decision-sim.json";
const markdownName = process.env.REVIEWER_DECISION_SIM_MARKDOWN_NAME || reportName.replace(/\.json$/i, ".md");

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
  lines.push("# Reviewer Decision Simulation");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- batchCount: ${report.summary.batchCount}`);
  lines.push("- dispositionCounts:");
  for (const item of report.summary.dispositionCounts || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- confidenceCounts:");
  for (const item of report.summary.confidenceCounts || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push(`- batchesRecommendedToSplit: ${report.summary.batchesRecommendedToSplit}`);
  lines.push("- splitHeuristicReasons:");
  for (const item of report.summary.splitHeuristicReasons || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- docSimulationOutcomeCounts:");
  for (const item of report.summary.docSimulationOutcomeCounts || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("");
  lines.push("## Largest Split Candidates");
  for (const item of report.summary.largestSplitCandidates || []) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | ${item.splitReason}`);
  }
  if (!(report.summary.largestSplitCandidates || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Tomorrow Morning Review Plan");
  for (const item of report.tomorrowMorningReviewPlan || []) {
    lines.push(`- ${item.batchKey} | ${item.recommendedSimulatedDisposition} | ${item.confidenceLevel} | docs=${item.docCount}`);
  }
  if (!(report.tomorrowMorningReviewPlan || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Avoid For Now");
  for (const item of report.avoidForNow || []) {
    lines.push(`- ${item.batchKey} | ${item.recommendedSimulatedDisposition} | ${item.confidenceLevel} | ${item.rationale}`);
  }
  if (!(report.avoidForNow || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Top Batches Most Likely To Yield Safe Reviewer Progress");
  for (const item of report.topBatchesMostLikelyToYieldSafeReviewerProgress || []) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | ${item.confidenceLevel}`);
  }
  if (!(report.topBatchesMostLikelyToYieldSafeReviewerProgress || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Top Batches Least Likely To Yield Safe Progress");
  for (const item of report.topBatchesLeastLikelyToYieldSafeProgress || []) {
    lines.push(`- ${item.batchKey} | docs=${item.docCount} | ${item.confidenceLevel}`);
  }
  if (!(report.topBatchesLeastLikelyToYieldSafeProgress || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Large 37.3+37.7 Recommendation");
  if (report.large3737SplitRecommendation) {
    lines.push(`- batchKey: ${report.large3737SplitRecommendation.batchKey}`);
    lines.push(`- shouldSplit: ${report.large3737SplitRecommendation.shouldSplit}`);
    lines.push(`- proposedSubBuckets: ${(report.large3737SplitRecommendation.proposedSubBuckets || []).join(", ") || "<none>"}`);
    lines.push(`- rationale: ${report.large3737SplitRecommendation.rationale}`);
  } else {
    lines.push("- not found in current simulation input");
  }
  lines.push("");
  lines.push("## Batch Decisions");
  for (const batch of report.batches || []) {
    lines.push(`### ${batch.batchKey}`);
    lines.push(`- disposition: ${batch.recommendedSimulatedDisposition}`);
    lines.push(`- confidence: ${batch.confidenceLevel}`);
    lines.push(`- splitRecommended: ${batch.splitRecommended}`);
    lines.push(`- splitReason: ${batch.splitReason}`);
    lines.push(`- splitConfidence: ${batch.splitConfidence}`);
    lines.push(`- proposedSubBuckets: ${(batch.proposedSubBuckets || []).join(", ") || "<none>"}`);
    lines.push(`- subBucketCounts: ${JSON.stringify(batch.subBucketCounts || [])}`);
    lines.push(`- rationale: ${batch.rationale}`);
    lines.push("- doc-level simulation:");
    for (const doc of batch.docSimulations || []) lines.push(`  - ${doc.documentId} | ${doc.simulatedRecommendation} | ${doc.contextSignal}`);
    lines.push("");
  }
  return lines.join("\n");
}

async function loadEvidenceInput() {
  if (evidenceInput) {
    const abs = path.resolve(process.cwd(), evidenceInput);
    return JSON.parse(await fs.readFile(abs, "utf8"));
  }
  const response = await fetchJson("/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=1200");
  if (response.status !== 200) {
    throw new Error(`failed to fetch live reviewer export for evidence bootstrap: ${response.status} ${JSON.stringify(response.body)}`);
  }
  const rows = Array.isArray(response.body?.rows) ? response.body.rows : [];
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

async function main() {
  const evidence = await loadEvidenceInput();
  if (!Array.isArray(evidence?.packets)) {
    throw new Error("REVIEWER_DECISION_SIM_INPUT must contain evidence report shape with packets[]");
  }
  const simulation = buildReviewerDecisionSimulation(evidence);
  const report = {
    apiBase,
    inputSource: evidenceInput ? path.resolve(process.cwd(), evidenceInput) : "provided_object",
    ...simulation
  };
  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, toMarkdown(report));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${jsonPath}`);
  console.log(`Markdown simulation written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
