const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

function splitFamily(value) {
  if (Array.isArray(value)) return value.map((v) => String(v).trim()).filter(Boolean);
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function simulatedAction(row) {
  const decision = String(row.reviewerDecision || "").trim();
  if (!decision) return "no_action";
  if (decision === "keep_blocked") return "keep_blocked";
  if (decision === "escalate_to_legal_context_review") return "escalate_to_legal_context_review";
  if (decision === "possible_manual_context_fix_but_no_auto_apply") return "possible_manual_context_fix_but_no_auto_apply";
  if (decision === "manual_no_action") return "no_action";
  return "no_action";
}

function rowIsUnusual(row, action) {
  if (action === "possible_manual_context_fix_but_no_auto_apply") return true;
  if (action === "escalate_to_legal_context_review") return true;
  const notes = String(row.reviewerDecisionReason || "").toLowerCase();
  return notes.includes("unusual") || notes.includes("exception") || notes.includes("override");
}

function normalizeRow(row) {
  return {
    rowNumber: row.rowNumber ?? null,
    queueOrder: Number(row.queueOrder || 0),
    documentId: String(row.documentId || ""),
    title: String(row.title || ""),
    batchKey: String(row.batchKey || ""),
    subBucket: row.subBucket == null ? "" : String(row.subBucket),
    priorityLane: String(row.priorityLane || ""),
    reviewerDecision: String(row.reviewerDecision || ""),
    blocked37xFamily: splitFamily(row.blocked37xFamily),
    doNotAutoApply: row.doNotAutoApply !== false,
    validationState: String(row.validationState || ""),
    validRow: row.validRow === true || String(row.validationState || "").startsWith("valid_") || String(row.validationState || "") === "blank_unreviewed"
  };
}

function compareRows(a, b) {
  const qa = Number(a.queueOrder || 0);
  const qb = Number(b.queueOrder || 0);
  if (qa !== qb) return qa - qb;
  const ra = Number(a.rowNumber || 0);
  const rb = Number(b.rowNumber || 0);
  if (ra !== rb) return ra - rb;
  if (a.batchKey !== b.batchKey) return a.batchKey.localeCompare(b.batchKey);
  if (a.subBucket !== b.subBucket) return a.subBucket.localeCompare(b.subBucket);
  return a.documentId.localeCompare(b.documentId);
}

export function buildReviewerImportSimulation(validateReport) {
  const sourceRows = Array.isArray(validateReport?.rows) ? validateReport.rows : [];
  const rows = sourceRows.map(normalizeRow).sort(compareRows);

  const simulationRows = rows.map((row) => {
    const action = simulatedAction(row);
    const hasUnsafe = row.blocked37xFamily.some((f) => UNSAFE_37X.has(f));
    const unsafeNonAutoApplyOk = !hasUnsafe || row.doNotAutoApply === true;
    return {
      ...row,
      simulatedImportAction: action,
      hasUnsafeBlocked37xFamily: hasUnsafe,
      unsafeNonAutoApplyOk,
      requiresSpecialAttention: rowIsUnusual(row, action)
    };
  });

  const specialAttentionRows = simulationRows.filter((row) => row.requiresSpecialAttention);
  const manualContextFixCandidates = simulationRows.filter((row) => row.simulatedImportAction === "possible_manual_context_fix_but_no_auto_apply");
  const legalEscalationCandidates = simulationRows.filter((row) => row.simulatedImportAction === "escalate_to_legal_context_review");
  const unsafeRows = simulationRows.filter((row) => row.hasUnsafeBlocked37xFamily);

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      totalRows: simulationRows.length,
      validRows: simulationRows.filter((row) => row.validRow).length,
      invalidRows: simulationRows.filter((row) => !row.validRow).length,
      keepBlockedRows: simulationRows.filter((row) => row.simulatedImportAction === "keep_blocked").length,
      escalateRows: legalEscalationCandidates.length,
      manualContextFixRows: manualContextFixCandidates.length,
      noActionRows: simulationRows.filter((row) => row.simulatedImportAction === "no_action").length,
      unsafeBlocked37xRows: unsafeRows.length,
      unsafeBlocked37xNonAutoApplyConfirmed: unsafeRows.every((row) => row.unsafeNonAutoApplyOk),
      specialAttentionRowCount: specialAttentionRows.length
    },
    countsByReviewerDecision: countBy(simulationRows.map((row) => row.reviewerDecision || "<blank>")),
    countsByBlocked37xFamily: countBy(
      simulationRows.flatMap((row) => (row.blocked37xFamily.length ? row.blocked37xFamily : ["<none>"]))
    ),
    countsByBatchKey: countBy(simulationRows.map((row) => row.batchKey || "<none>")),
    countsByPriorityLane: countBy(simulationRows.map((row) => row.priorityLane || "<none>")),
    countsBySimulatedImportAction: countBy(simulationRows.map((row) => row.simulatedImportAction)),
    specialAttentionRows,
    manualContextFixCandidates,
    legalEscalationCandidates,
    unusualValidRows: simulationRows.filter((row) => row.validRow && row.requiresSpecialAttention),
    rows: simulationRows
  };
}

export function formatReviewerImportSimulationMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Import Simulation (Read-Only)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push("");

  lines.push("## Counts by reviewerDecision");
  for (const item of report.countsByReviewerDecision || []) lines.push(`- ${item.key}: ${item.count}`);
  if (!(report.countsByReviewerDecision || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts by blocked37xFamily");
  for (const item of report.countsByBlocked37xFamily || []) lines.push(`- ${item.key}: ${item.count}`);
  if (!(report.countsByBlocked37xFamily || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts by batchKey");
  for (const item of report.countsByBatchKey || []) lines.push(`- ${item.key}: ${item.count}`);
  if (!(report.countsByBatchKey || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts by priorityLane");
  for (const item of report.countsByPriorityLane || []) lines.push(`- ${item.key}: ${item.count}`);
  if (!(report.countsByPriorityLane || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Special attention rows");
  for (const row of report.specialAttentionRows || []) {
    lines.push(`- row=${row.rowNumber ?? "n/a"} | #${row.queueOrder} | ${row.documentId} | ${row.title} | action=${row.simulatedImportAction}`);
  }
  if (!(report.specialAttentionRows || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Manual context fix candidates");
  for (const row of report.manualContextFixCandidates || []) {
    lines.push(`- row=${row.rowNumber ?? "n/a"} | #${row.queueOrder} | ${row.documentId} | ${row.batchKey}`);
  }
  if (!(report.manualContextFixCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Legal escalation candidates");
  for (const row of report.legalEscalationCandidates || []) {
    lines.push(`- row=${row.rowNumber ?? "n/a"} | #${row.queueOrder} | ${row.documentId} | ${row.batchKey}`);
  }
  if (!(report.legalEscalationCandidates || []).length) lines.push("- none");
  lines.push("");

  lines.push("- Unsafe 37.3/37.7/37.9 rows remain non-auto-apply in simulation.");
  lines.push("- This report is read-only and does not mutate any data.");

  return lines.join("\n");
}
