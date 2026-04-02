function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

const LANE_ORDER = { review_first: 0, review_after: 1, hold_blocked: 2 };

export const WORKSHEET_COLUMNS = [
  "queueOrder",
  "priorityLane",
  "batchKey",
  "subBucket",
  "documentId",
  "title",
  "blocked37xFamily",
  "recommendedReviewerPosture",
  "recommendedSimulatedDisposition",
  "rootCauseSummary",
  "contextClass",
  "topEvidenceSnippet",
  "suggestedDecisionOptions",
  "requiresLegalEscalation",
  "doNotAutoApply",
  "reviewerDecision",
  "reviewerDecisionReason",
  "reviewerCitationContext",
  "reviewerEvidenceUsed",
  "reviewerNotes",
  "escalateToLegal",
  "keepBlocked",
  "possibleManualContextFix",
  "reviewedBy",
  "reviewedAt"
];

function evidenceMap(evidenceReport) {
  const byBatchDoc = new Map();
  for (const packet of evidenceReport?.packets || []) {
    const batchKey = String(packet.batchKey || "");
    if (!batchKey) continue;
    const docMap = new Map();
    const snippets = (packet.patternEvidence || []).flatMap((pattern) => pattern.representativeSnippets || []);
    for (const snippet of snippets) {
      const docId = String(snippet.documentId || "");
      if (!docId || docMap.has(docId)) continue;
      docMap.set(docId, {
        contextClass: String(snippet.contextClass || ""),
        rootCause: String(snippet.rootCause || ""),
        localTextSnippet: String(snippet.localTextSnippet || "")
      });
    }
    byBatchDoc.set(batchKey, docMap);
  }
  return byBatchDoc;
}

function simMap(simReport) {
  return new Map((simReport?.batches || []).map((batch) => [String(batch.batchKey || ""), String(batch.recommendedSimulatedDisposition || "")]));
}

function normalizeRow(row, evidenceByBatchDoc, dispositionByBatch) {
  const batchKey = String(row.batchKey || "");
  const documentId = String(row.documentId || "");
  const docEvidence = evidenceByBatchDoc.get(batchKey)?.get(documentId);
  const blocked37x = Array.isArray(row.blocked37xFamily) ? row.blocked37xFamily : [];
  const options = Array.isArray(row.suggestedDecisionOptions) ? row.suggestedDecisionOptions : [];

  return {
    queueOrder: Number(row.queueOrder || 0),
    priorityLane: String(row.priorityLane || "review_after"),
    batchKey,
    subBucket: row.subBucket == null ? "" : String(row.subBucket),
    documentId,
    title: String(row.title || ""),
    blocked37xFamily: blocked37x.join(";"),
    recommendedReviewerPosture: String(row.recommendedReviewerPosture || ""),
    recommendedSimulatedDisposition: String(row.recommendedSimulatedDisposition || dispositionByBatch.get(batchKey) || ""),
    rootCauseSummary: String(row.rootCauseSummary || docEvidence?.rootCause || ""),
    contextClass: String(row.contextClass || docEvidence?.contextClass || ""),
    topEvidenceSnippet: String(row.topEvidenceSnippet || docEvidence?.localTextSnippet || ""),
    suggestedDecisionOptions: options.join(";"),
    requiresLegalEscalation: Boolean(row.requiresLegalEscalation),
    doNotAutoApply: row.doNotAutoApply !== false,
    reviewerDecision: "",
    reviewerDecisionReason: "",
    reviewerCitationContext: "",
    reviewerEvidenceUsed: "",
    reviewerNotes: "",
    escalateToLegal: "",
    keepBlocked: "",
    possibleManualContextFix: "",
    reviewedBy: "",
    reviewedAt: ""
  };
}

function compareRows(a, b) {
  const laneA = LANE_ORDER[a.priorityLane] ?? 9;
  const laneB = LANE_ORDER[b.priorityLane] ?? 9;
  if (laneA !== laneB) return laneA - laneB;
  if (a.queueOrder !== b.queueOrder) return a.queueOrder - b.queueOrder;
  if (a.batchKey !== b.batchKey) return a.batchKey.localeCompare(b.batchKey);
  if (a.subBucket !== b.subBucket) return a.subBucket.localeCompare(b.subBucket);
  return a.documentId.localeCompare(b.documentId);
}

function dedupeRows(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = `${row.batchKey}::${row.subBucket || "<none>"}::${row.documentId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function formatReviewerWorksheetCsv(report) {
  const lines = [];
  lines.push(WORKSHEET_COLUMNS.join(","));
  for (const row of report.rows || []) {
    lines.push(WORKSHEET_COLUMNS.map((col) => csvEscape(row[col])).join(","));
  }
  return lines.join("\n");
}

export function formatReviewerWorksheetMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Worksheet Export");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- worksheetRowCount: ${report.summary.worksheetRowCount}`);
  lines.push(`- actionQueueRowCount: ${report.summary.actionQueueRowCount}`);
  lines.push(`- rowCountMatchesActionQueue: ${report.summary.rowCountMatchesActionQueue}`);
  lines.push("- splitCoverage:");
  for (const item of report.summary.splitCoverage || []) {
    lines.push(`  - ${item.batchKey}: ${item.queueRowCount}/${item.expectedDocCount} (${item.coverageStatus})`);
  }
  if (!(report.summary.splitCoverage || []).length) lines.push("  - none");
  lines.push("- countsByPriorityLane:");
  for (const item of report.summary.countsByPriorityLane || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- countsByRecommendedReviewerPosture:");
  for (const item of report.summary.countsByRecommendedReviewerPosture || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- countsByBlocked37xFamily:");
  for (const item of report.summary.countsByBlocked37xFamily || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push(`- rowsRequiringLegalEscalation: ${report.summary.rowsRequiringLegalEscalation}`);
  lines.push("");

  lines.push("## Top 20 Items");
  for (const row of report.summary.top20WorksheetRows || []) {
    lines.push(`- #${row.queueOrder} | ${row.documentId} | ${row.title} | lane=${row.priorityLane} | posture=${row.recommendedReviewerPosture}`);
  }
  if (!(report.summary.top20WorksheetRows || []).length) lines.push("- none");
  lines.push("");

  const sections = [
    ["Review First worksheet rows", report.reviewFirstRows || []],
    ["Review After worksheet rows", report.reviewAfterRows || []],
    ["Hold Blocked worksheet rows", report.holdBlockedRows || []]
  ];

  for (const [title, rows] of sections) {
    lines.push(`## ${title}`);
    if (!rows.length) {
      lines.push("- none");
      lines.push("");
      continue;
    }
    for (const row of rows) {
      lines.push(`- #${row.queueOrder} | ${row.documentId} | ${row.title} | batch=${row.batchKey} | subBucket=${row.subBucket || "<none>"}`);
      lines.push(`  - posture=${row.recommendedReviewerPosture} | disposition=${row.recommendedSimulatedDisposition} | legalEscalation=${row.requiresLegalEscalation}`);
      lines.push(`  - evidence=${row.topEvidenceSnippet ? row.topEvidenceSnippet.slice(0, 180) : "<none>"}`);
    }
    lines.push("");
  }

  lines.push("## Reviewer instructions");
  lines.push("- Fill only reviewer-entry columns in CSV/JSON; do not edit system-generated fields.");
  lines.push("- Keep unresolved unsafe 37.3 / 37.7 / 37.9 cases blocked unless legal context review explicitly clears them.");
  lines.push("- Use evidence snippets and context class before selecting a reviewer decision.");
  lines.push("");
  lines.push("## Do not auto-apply reminder");
  lines.push("- This worksheet is read-only planning support. No approvals, metadata changes, citation writes, or QC threshold changes are performed.");

  return lines.join("\n");
}

export function buildReviewerWorksheetExport(actionQueueReport, evidenceReport = null, decisionSimReport = null) {
  const sourceRows = Array.isArray(actionQueueReport?.rows) ? actionQueueReport.rows : [];
  const evidenceByBatchDoc = evidenceMap(evidenceReport || {});
  const dispositionByBatch = simMap(decisionSimReport || {});

  const normalized = sourceRows.map((row) => normalizeRow(row, evidenceByBatchDoc, dispositionByBatch));
  const rows = dedupeRows(normalized).sort(compareRows);

  const splitCoverage = Array.isArray(actionQueueReport?.summary?.splitCoverage)
    ? actionQueueReport.summary.splitCoverage.map((item) => ({
        batchKey: String(item.batchKey || ""),
        expectedDocCount: Number(item.expectedDocCount || 0),
        queueRowCount: Number(item.queueRowCount || 0),
        coverageStatus: String(item.coverageStatus || "unknown")
      }))
    : [];

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    columns: WORKSHEET_COLUMNS,
    summary: {
      worksheetRowCount: rows.length,
      actionQueueRowCount: sourceRows.length,
      rowCountMatchesActionQueue: rows.length === sourceRows.length,
      splitCoverage,
      countsByPriorityLane: countBy(rows.map((row) => row.priorityLane)),
      countsByRecommendedReviewerPosture: countBy(rows.map((row) => row.recommendedReviewerPosture)),
      countsByBlocked37xFamily: countBy(
        rows.flatMap((row) => {
          const split = String(row.blocked37xFamily || "")
            .split(";")
            .map((item) => item.trim())
            .filter(Boolean);
          return split.length ? split : ["<none>"];
        })
      ),
      rowsRequiringLegalEscalation: rows.filter((row) => row.requiresLegalEscalation).length,
      top20WorksheetRows: rows.slice(0, 20).map((row) => ({
        queueOrder: row.queueOrder,
        priorityLane: row.priorityLane,
        batchKey: row.batchKey,
        subBucket: row.subBucket || null,
        documentId: row.documentId,
        title: row.title,
        recommendedReviewerPosture: row.recommendedReviewerPosture
      }))
    },
    reviewFirstRows: rows.filter((row) => row.priorityLane === "review_first"),
    reviewAfterRows: rows.filter((row) => row.priorityLane === "review_after"),
    holdBlockedRows: rows.filter((row) => row.priorityLane === "hold_blocked"),
    rows
  };
}
