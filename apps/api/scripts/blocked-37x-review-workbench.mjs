import fs from "node:fs/promises";
import path from "node:path";
import { buildBlocked37xDocView, groupByBatchKey } from "./blocked-37x-workbench-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const listLimit = Number(process.env.BLOCKED_37X_LIST_LIMIT || "300");
const topLimit = Number(process.env.BLOCKED_37X_TOP_LIMIT || "150");
const includeFixtures = process.env.BLOCKED_37X_INCLUDE_FIXTURES === "1";
const reportName = process.env.BLOCKED_37X_REPORT_NAME || "blocked-37x-review-workbench-report.json";

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

async function verifyCitation(citation) {
  const response = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: [citation] })
  });
  if (response.status !== 200) return null;
  return response.body?.checks?.[0] || null;
}

async function main() {
  const list = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=${includeFixtures ? "0" : "1"}&blocked37xOnly=1&sort=blocked37xBatchKeyAsc&limit=${Math.max(1, listLimit)}`
  );
  if (list.status !== 200) {
    throw new Error(`failed to list blocked 37.x staged docs: ${list.status} ${JSON.stringify(list.body)}`);
  }
  const docs = (list.body.documents || []).slice(0, Math.max(1, topLimit));
  const rows = [];

  for (const doc of docs) {
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    if (detail.status !== 200) continue;
    rows.push(buildBlocked37xDocView(doc, detail.body));
  }

  const families = ["37.3", "37.7", "37.9"];
  const byFamily = [];
  for (const family of families) {
    const affected = rows.filter((row) => row.blocked37xReferences.some((ref) => ref.family === family));
    const verify = await verifyCitation(family);
    byFamily.push({
      citationFamily: family,
      affected_count: affected.length,
      affected_docs: affected.map((row) => ({
        id: row.id,
        title: row.title,
        currentExtractedReferenceTypes: Array.from(new Set(row.blocked37xReferences.filter((r) => r.family === family).map((r) => r.referenceType))),
        exactUnresolvedReferences: row.blocked37xReferences.filter((ref) => ref.family === family).map((ref) => ({
          rawValue: ref.rawValue,
          normalizedValue: ref.normalizedValue,
          message: ref.message,
          reason: ref.reason
        })),
        blockedReason: row.blocked37xReason,
        safestReviewerAction: row.blocked37xReviewerHint,
        blocked37xSafeToBatchReview: row.blocked37xSafeToBatchReview,
        blocked37xBatchKey: row.blocked37xBatchKey
      })),
      ordinance_candidates: verify?.ordinance_matches || [],
      rules_candidates: verify?.rules_matches || [],
      blocked_reason: affected.some((row) => row.blocked37xReason === "cross_context_ambiguous")
        ? "cross_context_ambiguous"
        : affected.length > 0
          ? "unsafe_37x_structural_block"
          : "none",
      safeToBatchReview: affected.length > 1 && affected.every((row) => row.blocked37xSafeToBatchReview),
      recommendedReviewerInstruction:
        affected.length > 0
          ? "Keep blocked; use candidates for manual legal review only. No auto-resolution."
          : "No affected staged real docs in current slice."
    });
  }

  const batchGroups = groupByBatchKey(rows);
  const batchKeyGroups = Array.from(batchGroups.entries())
    .map(([batchKey, items]) => ({
      batchKey,
      count: items.length,
      docIds: items.map((item) => item.id),
      docTitles: items.map((item) => item.title),
      blockedReason: items[0]?.blocked37xReason || "none",
      safeToBatchReview: items.every((item) => item.blocked37xSafeToBatchReview)
    }))
    .sort((a, b) => b.count - a.count);

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    realOnlyDefault: !includeFixtures,
    summary: {
      staged_docs_analyzed: rows.length,
      blocked_families: families,
      blocked37x_docs: rows.length,
      batch_key_groups: batchKeyGroups.length
    },
    grouped_by_family: byFamily,
    grouped_by_batch_key: batchKeyGroups,
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
