import fs from "node:fs/promises";
import path from "node:path";
import { classifyDocForensics } from "./staged-blocker-forensics-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const topLimit = Number(process.env.FORENSICS_TOP_LIMIT || "25");
const listLimit = Number(process.env.FORENSICS_LIST_LIMIT || "200");
const reportName = process.env.FORENSICS_REPORT_NAME || "staged-real-blocker-forensics-report.json";

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

function countBy(items, keyFn) {
  const map = new Map();
  for (const item of items) {
    const key = keyFn(item);
    map.set(key, (map.get(key) || 0) + 1);
  }
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count);
}

async function main() {
  const list = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=${Math.max(1, listLimit)}`
  );
  if (list.status !== 200) {
    throw new Error(`failed to list staged real docs: ${list.status} ${JSON.stringify(list.body)}`);
  }
  const docs = (list.body.documents || []).slice(0, Math.max(1, topLimit));
  const rows = [];
  for (const doc of docs) {
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    if (detail.status !== 200) continue;
    const forensic = classifyDocForensics(doc, detail.body);
    rows.push({
      id: doc.id,
      title: doc.title,
      score: doc.approvalReadiness?.score ?? 0,
      blockers: doc.approvalReadiness?.blockers || [],
      blockerCategory: forensic.blockerCategory,
      unresolvedReferenceCount: detail.body.unresolvedReferenceCount || 0,
      unresolvedDetail: forensic.unresolvedDetail,
      validatedReferencesPresent: forensic.validatedReferencesPresent,
      indexCodeSource: forensic.indexCodeSource,
      metadataConfirmationHelps: forensic.metadataConfirmationHelps,
      reviewer_unlockable: forensic.reviewerUnlockable,
      safe_after_manual_confirmation: forensic.safeAfterManualConfirmation,
      recommended_next_action: forensic.recommendedNextAction
    });
  }

  const unresolvedRootCauses = rows.flatMap((row) => row.unresolvedDetail.map((issue) => issue.rootCause));
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    topLimit,
    summary: {
      docs_analyzed: rows.length,
      reviewer_unlockable: rows.filter((row) => row.reviewer_unlockable).length,
      safe_after_manual_confirmation: rows.filter((row) => row.safe_after_manual_confirmation).length
    },
    aggregate: {
      blocker_categories: countBy(rows, (row) => row.blockerCategory),
      recommended_actions: countBy(rows, (row) => row.recommended_next_action),
      unresolved_root_causes: countBy(unresolvedRootCauses, (value) => value)
    },
    docs: rows
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

