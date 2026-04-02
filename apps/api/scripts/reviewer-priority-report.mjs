import fs from "node:fs/promises";
import path from "node:path";
import { buildPrioritizedReviewerBatches } from "./reviewer-priority-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number(process.env.REVIEWER_PRIORITY_LIMIT || "1200");
const reportName = process.env.REVIEWER_PRIORITY_REPORT_NAME || "reviewer-priority-report.json";
const markdownName = process.env.REVIEWER_PRIORITY_MARKDOWN_NAME || reportName.replace(/\.json$/i, ".md");

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
  lines.push("# Reviewer Priority Queue");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- Total rows analyzed: ${report.summary.rowsAnalyzed}`);
  lines.push(`- Total prioritized batches: ${report.summary.totalBatches}`);
  lines.push(`- review_now: ${report.summary.buckets.review_now}`);
  lines.push(`- review_next: ${report.summary.buckets.review_next}`);
  lines.push(`- review_later: ${report.summary.buckets.review_later}`);
  lines.push(`- blocked_legal_adjudication: ${report.summary.buckets.blocked_legal_adjudication}`);
  lines.push("");
  lines.push("## Top 10 Batches To Review First");
  for (const batch of report.top10Batches) {
    lines.push(
      `- ${batch.batchKey} | docs=${batch.docCount} | effort=${batch.estimatedReviewerEffort} | risk=${batch.reviewerRiskLevel} | action=${batch.recommendedReviewerAction}`
    );
  }
  if (!report.top10Batches.length) lines.push("- none");
  lines.push("");
  lines.push("## Largest Low-Effort Batches");
  for (const batch of report.largestLowEffortBatches) {
    lines.push(`- ${batch.batchKey} | docs=${batch.docCount} | unresolved=${batch.unresolvedTriageBuckets.join(", ") || "<none>"}`);
  }
  if (!report.largestLowEffortBatches.length) lines.push("- none");
  lines.push("");
  lines.push("## Batches Blocked By Unsafe 37.x Families");
  for (const batch of report.blocked37xBatches) {
    lines.push(`- ${batch.batchKey} | family=${batch.blocked37xFamily.join(", ")} | docs=${batch.docCount}`);
  }
  if (!report.blocked37xBatches.length) lines.push("- none");
  lines.push("");
  lines.push("## Batches Needing Legal-Context Adjudication");
  for (const batch of report.legalContextBatches) {
    lines.push(`- ${batch.batchKey} | buckets=${batch.unresolvedTriageBuckets.join(", ") || "<none>"} | docs=${batch.docCount}`);
  }
  if (!report.legalContextBatches.length) lines.push("- none");
  return lines.join("\n");
}

async function main() {
  const exportResp = await fetchJson(`/admin/ingestion/reviewer-export?realOnly=1&format=json&limit=${Math.max(1, limit)}`);
  if (exportResp.status !== 200) {
    throw new Error(`failed to fetch reviewer export queue: ${exportResp.status} ${JSON.stringify(exportResp.body)}`);
  }
  const rows = Array.isArray(exportResp.body?.rows) ? exportResp.body.rows : [];
  const ranked = buildPrioritizedReviewerBatches(rows);
  const top10 = ranked.prioritizedBatches.slice(0, 10);
  const largestLowEffort = ranked.prioritizedBatches
    .filter((batch) => batch.estimatedReviewerEffort === "low" && batch.priorityBucket !== "blocked_legal_adjudication")
    .sort((a, b) => b.docCount - a.docCount || a.batchKey.localeCompare(b.batchKey))
    .slice(0, 10);
  const blocked37xBatches = ranked.prioritizedBatches.filter((batch) => batch.blocked37xFamily.some((family) => ["37.3", "37.7", "37.9"].includes(family)));
  const legalContextBatches = ranked.prioritizedBatches.filter((batch) =>
    batch.priorityBucket === "blocked_legal_adjudication" || batch.unresolvedTriageBuckets.includes("cross_context_ambiguous")
  );

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    readOnly: true,
    summary: {
      rowsAnalyzed: rows.length,
      totalBatches: ranked.prioritizedBatches.length,
      buckets: {
        review_now: ranked.buckets.review_now.length,
        review_next: ranked.buckets.review_next.length,
        review_later: ranked.buckets.review_later.length,
        blocked_legal_adjudication: ranked.buckets.blocked_legal_adjudication.length
      }
    },
    top10Batches: top10,
    largestLowEffortBatches: largestLowEffort,
    blocked37xBatches,
    legalContextBatches,
    prioritizedBatches: ranked.prioritizedBatches
  };

  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, toMarkdown(report));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${jsonPath}`);
  console.log(`Markdown summary written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
