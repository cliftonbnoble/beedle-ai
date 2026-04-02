import fs from "node:fs/promises";
import path from "node:path";
import {
  classifyReviewerReadinessCandidate,
  isSafeForMetadataAutoConfirmation,
  splitReviewerReadinessDocs
} from "./reviewer-readiness-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dryRun = process.env.REVIEWER_DRY_RUN !== "0";
const listLimit = Number(process.env.REVIEWER_LIST_LIMIT || "250");
const confirmLimit = Number(process.env.REVIEWER_CONFIRM_LIMIT || "12");
const reportName = process.env.REVIEWER_REPORT_NAME || "reviewer-readiness-report.json";
const reprocessFirst = process.env.REVIEWER_REPROCESS_FIRST === "1";

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

function blockerBreakdown(docs) {
  const counts = new Map();
  for (const doc of docs || []) {
    for (const blocker of doc.approvalReadiness?.blockers || []) {
      counts.set(blocker, (counts.get(blocker) || 0) + 1);
    }
  }
  return Array.from(counts.entries())
    .map(([blocker, count]) => ({ blocker, count }))
    .sort((a, b) => b.count - a.count);
}

function summarizeBucket(items) {
  const real = items.filter((item) => !item.isLikelyFixture);
  const fixture = items.filter((item) => item.isLikelyFixture);
  return {
    total: items.length,
    real_docs: real.length,
    fixture_docs: fixture.length
  };
}

function projectDoc(doc) {
  return {
    id: doc.id,
    title: doc.title,
    isLikelyFixture: Boolean(doc.isLikelyFixture),
    reviewerRiskLevel: doc.reviewerRiskLevel,
    reviewerReady: Boolean(doc.reviewerReady),
    reviewerReadyReasons: doc.reviewerReadyReasons || [],
    reviewerRequiredActions: doc.reviewerRequiredActions || [],
    metadataConfirmationWouldUnlock: Boolean(doc.metadataConfirmationWouldUnlock),
    unresolvedBlockersAfterConfirmation: doc.unresolvedBlockersAfterConfirmation || [],
    approvalBlockers: doc.approvalReadiness?.blockers || [],
    unresolvedReferenceCount: doc.unresolvedReferenceCount || 0,
    warningCount: doc.warningCount || 0,
    extractionConfidence: doc.extractionConfidence || 0
  };
}

async function main() {
  const staged = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&sort=reviewerReadinessDesc&limit=${Math.max(1, listLimit)}`
  );
  if (staged.status !== 200) {
    throw new Error(`failed to list staged docs: ${staged.status} ${JSON.stringify(staged.body)}`);
  }
  const stagedDocs = staged.body.documents || [];
  const beforeBreakdown = blockerBreakdown(stagedDocs.filter((doc) => !doc.isLikelyFixture));
  const buckets = splitReviewerReadinessDocs(stagedDocs);

  const confirmationOnlyReal = buckets.confirmation_only_candidates.filter((doc) => !doc.isLikelyFixture);
  const applyCandidates = confirmationOnlyReal.slice(0, Math.max(0, confirmLimit));
  const applied = [];
  const skipped = [];

  if (!dryRun) {
    for (const doc of applyCandidates) {
      if (reprocessFirst) {
        await fetchJson(`/admin/ingestion/documents/${doc.id}/reprocess`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}"
        });
      }
      const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
      if (detail.status !== 200) {
        skipped.push({ id: doc.id, title: doc.title, reason: "detail_fetch_failed", status: detail.status });
        continue;
      }
      if (!isSafeForMetadataAutoConfirmation(doc, detail.body)) {
        skipped.push({ id: doc.id, title: doc.title, reason: "safety_checks_failed_after_detail_review" });
        continue;
      }
      const refs = detail.body.validReferences || {};
      const payload = {
        index_codes: refs.indexCodes || [],
        rules_sections: refs.rulesSections || [],
        ordinance_sections: refs.ordinanceSections || [],
        case_number: detail.body.caseNumber || null,
        decision_date: detail.body.decisionDate || null,
        author_name: detail.body.authorName || null,
        outcome_label: detail.body.outcomeLabel || "unclear",
        confirm_required_metadata: true
      };
      const update = await fetchJson(`/admin/ingestion/documents/${doc.id}/metadata`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      if (update.status !== 200) {
        skipped.push({ id: doc.id, title: doc.title, reason: "metadata_update_failed", status: update.status });
        continue;
      }
      applied.push({
        id: doc.id,
        title: doc.title,
        action: "metadata_confirmed_only",
        validatedReferenceCounts: {
          indexCodes: (refs.indexCodes || []).length,
          rulesSections: (refs.rulesSections || []).length,
          ordinanceSections: (refs.ordinanceSections || []).length
        }
      });
    }
  }

  const afterStaged = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&sort=reviewerReadinessDesc&limit=${Math.max(1, listLimit)}`
  );
  const afterDocs = afterStaged.status === 200 ? afterStaged.body.documents || [] : [];
  const afterBreakdown = blockerBreakdown(afterDocs.filter((doc) => !doc.isLikelyFixture));

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    dryRun,
    listLimit,
    confirmLimit,
    reprocessFirst,
    summary: {
      staged_docs_analyzed: stagedDocs.length,
      confirmation_only_candidates: summarizeBucket(buckets.confirmation_only_candidates),
      confirmation_plus_one_manual_fix_candidates: summarizeBucket(buckets.confirmation_plus_one_manual_fix_candidates),
      structurally_blocked_docs: summarizeBucket(buckets.structurally_blocked_docs),
      metadata_confirmed_in_apply: applied.length,
      apply_skipped: skipped.length
    },
    before_real_blocker_breakdown: beforeBreakdown,
    after_real_blocker_breakdown: afterBreakdown,
    confirmation_only_candidates: buckets.confirmation_only_candidates.map(projectDoc),
    confirmation_plus_one_manual_fix_candidates: buckets.confirmation_plus_one_manual_fix_candidates.map(projectDoc),
    structurally_blocked_docs: buckets.structurally_blocked_docs.map(projectDoc),
    apply: {
      applied,
      skipped,
      applied_real_docs: applied.length,
      applied_fixture_docs: 0
    },
    notes: [
      "Apply mode only confirms metadata for confirmation_only real-doc candidates.",
      "No auto-approval is performed by this script.",
      "Docs with unresolved 37.3/37.7/37.9 or cross-context ambiguity remain blocked."
    ]
  };

  const outputPath = path.resolve(process.cwd(), "reports", reportName);
  await fs.writeFile(outputPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ...report.summary,
        report: outputPath
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
