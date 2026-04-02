import fs from "node:fs/promises";
import path from "node:path";
import {
  buildBatchGroups,
  citationFamilyFromIssue,
  classifyDocUnresolvedTriage,
  classifyUnresolvedIssueBucket
} from "./unresolved-review-triage-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const listLimit = Number(process.env.TRIAGE_LIST_LIMIT || "300");
const topLimit = Number(process.env.TRIAGE_TOP_LIMIT || "120");
const reportName = process.env.TRIAGE_REPORT_NAME || "staged-real-unresolved-triage-report.json";

async function fetchJson(endpoint, init) {
  const response = await fetch(`${apiBase}${endpoint}`, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    body = { raw };
  }
  return { status: response.status, body };
}

function summarizeFamilies(rows) {
  const byFamily = new Map();
  for (const row of rows) {
    for (const issue of row.referenceIssues) {
      const family = citationFamilyFromIssue(issue);
      if (!family) continue;
      const entry = byFamily.get(family) || {
        citation_family: family,
        occurrence_count: 0,
        affected_docs: [],
        bucket_counts: new Map()
      };
      entry.occurrence_count += 1;
      if (!entry.affected_docs.find((d) => d.id === row.id)) {
        entry.affected_docs.push({ id: row.id, title: row.title });
      }
      const bucket = issue.bucket;
      entry.bucket_counts.set(bucket, (entry.bucket_counts.get(bucket) || 0) + 1);
      byFamily.set(family, entry);
    }
  }
  return Array.from(byFamily.values())
    .map((entry) => {
      const sortedBuckets = Array.from(entry.bucket_counts.entries()).sort((a, b) => b[1] - a[1]);
      const dominantRootCause = sortedBuckets[0]?.[0] || "unknown";
      const safeToBatchReview = !["unsafe_37x_structural_block", "cross_context_ambiguous", "structurally_blocked_not_found"].includes(
        dominantRootCause
      );
      const recommendedInstruction = safeToBatchReview
        ? `Batch review ${entry.citation_family} for consistent reviewer cleanup using source-backed edits only.`
        : `Keep ${entry.citation_family} blocked for manual legal review; do not batch auto-fix.`;
      return {
        citation_family: entry.citation_family,
        occurrence_count: entry.occurrence_count,
        affected_real_docs: entry.affected_docs,
        dominant_root_cause: dominantRootCause,
        safeToBatchReview,
        recommended_reviewer_instruction: recommendedInstruction
      };
    })
    .sort((a, b) => b.occurrence_count - a.occurrence_count);
}

function countBuckets(rows) {
  const counts = new Map();
  for (const row of rows) {
    for (const bucket of row.unresolvedBuckets || []) {
      counts.set(bucket, (counts.get(bucket) || 0) + 1);
    }
  }
  return Array.from(counts.entries())
    .map(([bucket, count]) => ({ bucket, count }))
    .sort((a, b) => b.count - a.count);
}

async function main() {
  const list = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=unresolvedLeverageDesc&limit=${Math.max(1, listLimit)}`
  );
  if (list.status !== 200) {
    throw new Error(`failed to list staged real docs: ${list.status} ${JSON.stringify(list.body)}`);
  }
  const stagedRealDocs = (list.body.documents || []).slice(0, Math.max(1, topLimit));
  const rows = [];

  for (const doc of stagedRealDocs) {
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    if (detail.status !== 200) continue;
    const triage = classifyDocUnresolvedTriage(doc, detail.body);
    const referenceIssues = (detail.body.referenceIssues || []).map((issue) => ({
      referenceType: issue.referenceType,
      rawValue: issue.rawValue,
      normalizedValue: issue.normalizedValue,
      message: issue.message,
      severity: issue.severity,
      bucket: null
    }));
    const duplicateCounts = new Map();
    for (const issue of referenceIssues) {
      const key = `${issue.referenceType}::${String(issue.normalizedValue || issue.rawValue || "").toLowerCase().replace(/\s+/g, "")}`;
      duplicateCounts.set(key, (duplicateCounts.get(key) || 0) + 1);
    }
    for (const issue of referenceIssues) {
      issue.bucket = classifyUnresolvedIssueBucket(issue, { duplicateCounts });
    }
    rows.push({
      id: doc.id,
      title: doc.title,
      unresolvedReferenceCount: detail.body.unresolvedReferenceCount || 0,
      unresolvedBuckets: triage.unresolvedBuckets,
      topRecommendedReviewerAction: triage.topRecommendedReviewerAction,
      estimatedReviewerEffort: triage.estimatedReviewerEffort,
      candidateManualFixes: triage.candidateManualFixes,
      recurringCitationFamilies: triage.recurringCitationFamilies,
      referenceIssues
    });
  }

  const batchGroups = buildBatchGroups(rows);
  const enriched = rows.map((row) => ({
    ...row,
    canBatchReviewWith: batchGroups.get(row.id) || []
  }));
  const recurringFamilies = summarizeFamilies(enriched);

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    dryRunOnly: true,
    summary: {
      staged_real_docs_analyzed: enriched.length,
      bucket_counts: countBuckets(enriched),
      confirmation_only_candidates: 0,
      confirmation_plus_one_manual_fix_candidates: 0,
      structurally_blocked_docs: enriched.length
    },
    recurring_citation_families: recurringFamilies,
    staged_real_docs: enriched.map((row) => ({
      id: row.id,
      title: row.title,
      unresolvedBuckets: row.unresolvedBuckets,
      topRecommendedReviewerAction: row.topRecommendedReviewerAction,
      estimatedReviewerEffort: row.estimatedReviewerEffort,
      candidateManualFixes: row.candidateManualFixes,
      recurringCitationFamilies: row.recurringCitationFamilies,
      canBatchReviewWith: row.canBatchReviewWith
    }))
  };

  const outputPath = path.resolve(process.cwd(), "reports", reportName);
  await fs.writeFile(outputPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
