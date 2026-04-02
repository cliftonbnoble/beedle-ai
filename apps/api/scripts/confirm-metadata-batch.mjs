import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dryRun = process.env.CONFIRM_DRY_RUN !== "0";
const limit = Number(process.env.CONFIRM_LIMIT || "40");
const reportName = process.env.CONFIRM_REPORT_NAME || "pilot-metadata-confirm-report.json";
const reprocessFirst = process.env.CONFIRM_REPROCESS_FIRST !== "0";

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
  for (const doc of docs) {
    for (const blocker of doc.approvalReadiness?.blockers || []) {
      counts.set(blocker, (counts.get(blocker) || 0) + 1);
    }
  }
  return Array.from(counts.entries())
    .map(([blocker, count]) => ({ blocker, count }))
    .sort((a, b) => b.count - a.count);
}

function safeForBatchConfirm(doc) {
  const blockers = doc.approvalReadiness?.blockers || [];
  const recoverableBlockers = new Set(["metadata_not_confirmed", "qc_gate_not_passed", "unresolved_references_above_threshold"]);
  const blockersRecoverable = blockers.length > 0 && blockers.every((item) => recoverableBlockers.has(item));
  return (
    blockersRecoverable &&
    !doc.isLikelyFixture &&
    (doc.criticalExceptionCount || 0) === 0 &&
    (doc.unresolvedReferenceCount || 0) <= 5 &&
    (doc.warningCount || 0) <= 8 &&
    (doc.extractionConfidence || 0) >= 0.5 &&
    !doc.missingRulesDetection &&
    !doc.missingOrdinanceDetection
  );
}

async function main() {
  const staged = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=${Math.max(1, limit * 8)}`
  );
  if (staged.status !== 200) {
    throw new Error(`failed to list staged docs: ${staged.status} ${JSON.stringify(staged.body)}`);
  }
  const docs = staged.body.documents || [];
  const beforeBlockers = blockerBreakdown(docs);
  const candidates = docs.filter(safeForBatchConfirm).slice(0, Math.max(1, limit));

  const confirmed = [];
  const failed = [];
  if (!dryRun) {
    for (const doc of candidates) {
      if (reprocessFirst) {
        await fetchJson(`/admin/ingestion/documents/${doc.id}/reprocess`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}"
        });
      }
      const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
      if (detail.status !== 200) {
        failed.push({ id: doc.id, title: doc.title, reason: "detail_fetch_failed", status: detail.status });
        continue;
      }
      const indexCodes =
        (detail.body.validReferences?.indexCodes || []).length > 0 ? detail.body.validReferences.indexCodes : detail.body.indexCodes || [];
      const rulesSections =
        (detail.body.validReferences?.rulesSections || []).length > 0
          ? detail.body.validReferences.rulesSections
          : detail.body.rulesSections || [];
      const ordinanceSections =
        (detail.body.validReferences?.ordinanceSections || []).length > 0
          ? detail.body.validReferences.ordinanceSections
          : detail.body.ordinanceSections || [];
      const confirmRequired = indexCodes.length > 0 && rulesSections.length > 0 && ordinanceSections.length > 0;
      const payload = {
        index_codes: indexCodes,
        rules_sections: rulesSections,
        ordinance_sections: ordinanceSections,
        case_number: detail.body.caseNumber || null,
        decision_date: detail.body.decisionDate || null,
        author_name: detail.body.authorName || null,
        outcome_label: detail.body.outcomeLabel || "unclear",
        confirm_required_metadata: confirmRequired
      };
      const update = await fetchJson(`/admin/ingestion/documents/${doc.id}/metadata`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      if (update.status === 200) {
        const postDetail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
        confirmed.push({
          id: doc.id,
          title: doc.title,
          confirmRequired,
          indexCodes: indexCodes.length,
          rulesSections: rulesSections.length,
          ordinanceSections: ordinanceSections.length,
          postBlockers: postDetail.status === 200 ? postDetail.body?.approvalReadiness?.blockers || [] : []
        });
      } else {
        failed.push({ id: doc.id, title: doc.title, reason: "metadata_update_failed", status: update.status, body: update.body });
      }
    }
  }
  const post = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=${Math.max(1, limit * 8)}`
  );
  const afterDocs = post.status === 200 ? post.body.documents || [] : [];
  const afterBlockers = blockerBreakdown(afterDocs);

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    dryRun,
    limit,
    reprocessFirst,
    summary: {
      staged_considered: docs.length,
      safe_batch_candidates: candidates.length,
      real_candidates: candidates.filter((doc) => !doc.isLikelyFixture).length,
      reviewed_top_real_docs: docs.length,
      confirmed: confirmed.length,
      failed: failed.length
    },
    before_blocker_breakdown: beforeBlockers,
    after_blocker_breakdown: afterBlockers,
    candidates: candidates.map((doc) => ({
      id: doc.id,
      title: doc.title,
      approvalReadiness: doc.approvalReadiness,
      warningCount: doc.warningCount,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      extractionConfidence: doc.extractionConfidence
    })),
    confirmed,
    failed
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
