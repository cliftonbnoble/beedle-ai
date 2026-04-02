const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

function splitFamilies(value) {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function laneRank(lane) {
  if (lane === "review_first") return 0;
  if (lane === "review_after") return 1;
  if (lane === "hold_blocked") return 2;
  return 3;
}

function normalizeRow(raw) {
  return {
    ...raw,
    queueOrder: Number(raw.queueOrder || 0),
    priorityLane: String(raw.priorityLane || "review_after"),
    batchKey: String(raw.batchKey || ""),
    subBucket: raw.subBucket == null ? "" : String(raw.subBucket),
    documentId: String(raw.documentId || ""),
    title: String(raw.title || ""),
    blocked37xFamily: String(raw.blocked37xFamily || ""),
    recommendedReviewerPosture: String(raw.recommendedReviewerPosture || ""),
    recommendedSimulatedDisposition: String(raw.recommendedSimulatedDisposition || ""),
    rootCauseSummary: String(raw.rootCauseSummary || ""),
    contextClass: String(raw.contextClass || ""),
    topEvidenceSnippet: String(raw.topEvidenceSnippet || ""),
    suggestedDecisionOptions: String(raw.suggestedDecisionOptions || ""),
    requiresLegalEscalation: String(raw.requiresLegalEscalation || "").toLowerCase() === "true" || raw.requiresLegalEscalation === true,
    doNotAutoApply: true,
    reviewerDecision: String(raw.reviewerDecision || ""),
    reviewerDecisionReason: String(raw.reviewerDecisionReason || ""),
    reviewerCitationContext: String(raw.reviewerCitationContext || ""),
    reviewerEvidenceUsed: String(raw.reviewerEvidenceUsed || ""),
    reviewerNotes: String(raw.reviewerNotes || ""),
    escalateToLegal: String(raw.escalateToLegal || ""),
    keepBlocked: String(raw.keepBlocked || ""),
    possibleManualContextFix: String(raw.possibleManualContextFix || ""),
    reviewedBy: String(raw.reviewedBy || ""),
    reviewedAt: String(raw.reviewedAt || "")
  };
}

function rowKey(row) {
  return `${row.batchKey}::${row.subBucket || "<none>"}::${row.documentId}`;
}

function hasReviewerDecision(row) {
  return String(row.reviewerDecision || "").trim().length > 0;
}

function isUnsafeFamily(row) {
  return splitFamilies(row.blocked37xFamily).some((family) => UNSAFE_37X.has(family));
}

function hasHighSignalManualFix(row) {
  if (row.recommendedReviewerPosture !== "possible_manual_context_fix_but_no_auto_apply") return false;
  if (isUnsafeFamily(row)) return false;
  if (!row.batchKey.startsWith("family:37.2+37.8")) return false;
  if (row.contextClass !== "likely_ordinance_wording") return false;
  return String(row.topEvidenceSnippet || "").trim().length >= 40;
}

function applyKeepBlocked(row, reason, generatedAt) {
  return {
    ...row,
    reviewerDecision: "keep_blocked",
    reviewerDecisionReason: reason,
    reviewerCitationContext: row.reviewerCitationContext || row.contextClass || "",
    reviewerEvidenceUsed: row.reviewerEvidenceUsed || (row.topEvidenceSnippet ? row.topEvidenceSnippet.slice(0, 220) : ""),
    reviewerNotes: row.reviewerNotes || `Autofill policy: ${reason}. Read-only recommendation only.`,
    escalateToLegal: "",
    keepBlocked: "TRUE",
    possibleManualContextFix: "",
    reviewedBy: row.reviewedBy || "autofill_bot",
    reviewedAt: row.reviewedAt || generatedAt,
    doNotAutoApply: true
  };
}

function applyManualContextFix(row, reason, generatedAt) {
  return {
    ...row,
    reviewerDecision: "possible_manual_context_fix_but_no_auto_apply",
    reviewerDecisionReason: reason,
    reviewerCitationContext: row.reviewerCitationContext || "ordinance",
    reviewerEvidenceUsed: row.reviewerEvidenceUsed || (row.topEvidenceSnippet ? row.topEvidenceSnippet.slice(0, 220) : "source-context"),
    reviewerNotes: row.reviewerNotes || "Autofill high-signal manual context fix candidate (no auto-apply).",
    escalateToLegal: "",
    keepBlocked: "",
    possibleManualContextFix: "TRUE",
    reviewedBy: row.reviewedBy || "autofill_bot",
    reviewedAt: row.reviewedAt || generatedAt,
    doNotAutoApply: true
  };
}

function buildWorksheetSummary(rows, actionQueueRowCount, splitCoverage = []) {
  return {
    worksheetRowCount: rows.length,
    actionQueueRowCount,
    rowCountMatchesActionQueue: rows.length === actionQueueRowCount,
    splitCoverage,
    countsByPriorityLane: countBy(rows.map((row) => row.priorityLane)),
    countsByRecommendedReviewerPosture: countBy(rows.map((row) => row.recommendedReviewerPosture)),
    countsByBlocked37xFamily: countBy(
      rows.flatMap((row) => {
        const families = splitFamilies(row.blocked37xFamily);
        return families.length ? families : ["<none>"];
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
  };
}

function sortRows(rows) {
  return [...rows].sort((a, b) => {
    const lr = laneRank(a.priorityLane) - laneRank(b.priorityLane);
    if (lr !== 0) return lr;
    if (a.queueOrder !== b.queueOrder) return a.queueOrder - b.queueOrder;
    if (a.batchKey !== b.batchKey) return a.batchKey.localeCompare(b.batchKey);
    if (a.subBucket !== b.subBucket) return a.subBucket.localeCompare(b.subBucket);
    return a.documentId.localeCompare(b.documentId);
  });
}

export function buildReviewerDecisionAutofill({
  worksheetRows,
  actionQueueSummary = null,
  generatedAt = new Date().toISOString()
}) {
  const normalized = sortRows((worksheetRows || []).map(normalizeRow));
  const autofilledRows = [];
  const exceptionRows = [];
  const prefilledRows = [];
  const policyCounts = new Map();

  const addPolicy = (policy) => {
    policyCounts.set(policy, (policyCounts.get(policy) || 0) + 1);
  };

  for (const row of normalized) {
    let output = row;
    let policy = "";
    let confidence = "none";

    if (hasReviewerDecision(row)) {
      policy = "existing_reviewer_decision_preserved";
      confidence = "high";
      output = { ...row, doNotAutoApply: true };
      autofilledRows.push({
        rowKey: rowKey(row),
        queueOrder: row.queueOrder,
        documentId: row.documentId,
        batchKey: row.batchKey,
        policy,
        confidence,
        autofillDecision: row.reviewerDecision
      });
      addPolicy(policy);
      prefilledRows.push(output);
      continue;
    }

    if (row.subBucket === "not_found_hold") {
      policy = "auto_keep_blocked_not_found_hold";
      confidence = "high";
      output = applyKeepBlocked(row, "No useful resolvable citation found (not_found_hold).", generatedAt);
    } else if (row.subBucket === "insufficient_context_hold") {
      policy = "auto_keep_blocked_insufficient_context_hold";
      confidence = "high";
      output = applyKeepBlocked(row, "Insufficient context to safely relabel citation (insufficient_context_hold).", generatedAt);
    } else if (isUnsafeFamily(row)) {
      policy = "auto_keep_blocked_unsafe_37x_family";
      confidence = "high";
      output = applyKeepBlocked(row, "Unsafe blocked 37.x family requires blocked status pending legal context review.", generatedAt);
    } else if (hasHighSignalManualFix(row)) {
      policy = "auto_manual_context_fix_high_signal_safe_family";
      confidence = "medium";
      output = applyManualContextFix(row, "High-signal ordinance-like evidence in safe family (no auto-apply).", generatedAt);
    } else {
      policy = "exception_low_confidence_or_contradictory_signal";
      confidence = "low";
      exceptionRows.push({
        rowKey: rowKey(row),
        queueOrder: row.queueOrder,
        documentId: row.documentId,
        batchKey: row.batchKey,
        reason: "confidence_too_low_or_signal_not_strong_enough",
        contextClass: row.contextClass,
        subBucket: row.subBucket || "<none>",
        blocked37xFamily: splitFamilies(row.blocked37xFamily)
      });
      addPolicy(policy);
      prefilledRows.push({ ...row, doNotAutoApply: true });
      continue;
    }

    addPolicy(policy);
    prefilledRows.push(output);
    autofilledRows.push({
      rowKey: rowKey(row),
      queueOrder: row.queueOrder,
      documentId: row.documentId,
      batchKey: row.batchKey,
      policy,
      confidence,
      autofillDecision: output.reviewerDecision
    });
  }

  const dedupExceptions = new Map();
  for (const item of exceptionRows) {
    if (!dedupExceptions.has(item.rowKey)) dedupExceptions.set(item.rowKey, item);
  }
  const exceptionList = Array.from(dedupExceptions.values()).sort((a, b) => a.queueOrder - b.queueOrder || a.rowKey.localeCompare(b.rowKey));

  const prefilled = sortRows(prefilledRows);
  const exceptionWorksheetRows = sortRows(prefilled.filter((row) => exceptionList.some((e) => e.rowKey === rowKey(row))));

  const splitCoverage = Array.isArray(actionQueueSummary?.splitCoverage) ? actionQueueSummary.splitCoverage : [];
  const actionQueueRowCount = Number(actionQueueSummary?.totalQueueRows || prefilled.length);

  const policyCountsArray = Array.from(policyCounts.entries())
    .map(([policy, count]) => ({ policy, count }))
    .sort((a, b) => b.count - a.count || a.policy.localeCompare(b.policy));

  return {
    generatedAt,
    readOnly: true,
    summary: {
      totalRows: prefilled.length,
      autofilledCount: autofilledRows.length,
      exceptionCount: exceptionList.length,
      rowsLeftForManualReview: exceptionList.length,
      manualReviewReductionAchieved: prefilled.length ? ((prefilled.length - exceptionList.length) / prefilled.length).toFixed(3) : "0.000"
    },
    policyCounts: policyCountsArray,
    countsByAutofillDecision: countBy(autofilledRows.map((row) => row.autofillDecision || "<none>")),
    countsByConfidence: countBy(autofilledRows.map((row) => row.confidence || "none")),
    autofilledRows,
    exceptionRows: exceptionList,
    rowsLeftForManualReview: exceptionList.length,
    prefilledWorksheet: {
      generatedAt,
      readOnly: true,
      summary: buildWorksheetSummary(prefilled, actionQueueRowCount, splitCoverage),
      reviewFirstRows: prefilled.filter((row) => row.priorityLane === "review_first"),
      reviewAfterRows: prefilled.filter((row) => row.priorityLane === "review_after"),
      holdBlockedRows: prefilled.filter((row) => row.priorityLane === "hold_blocked"),
      rows: prefilled
    },
    exceptionsWorksheet: {
      generatedAt,
      readOnly: true,
      summary: buildWorksheetSummary(exceptionWorksheetRows, actionQueueRowCount, splitCoverage),
      reviewFirstRows: exceptionWorksheetRows.filter((row) => row.priorityLane === "review_first"),
      reviewAfterRows: exceptionWorksheetRows.filter((row) => row.priorityLane === "review_after"),
      holdBlockedRows: exceptionWorksheetRows.filter((row) => row.priorityLane === "hold_blocked"),
      rows: exceptionWorksheetRows
    }
  };
}

export function formatReviewerDecisionAutofillMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Decision Autofill (Read-Only)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- totalRows: ${report.summary.totalRows}`);
  lines.push(`- autofilledCount: ${report.summary.autofilledCount}`);
  lines.push(`- exceptionCount: ${report.summary.exceptionCount}`);
  lines.push(`- rowsLeftForManualReview: ${report.summary.rowsLeftForManualReview}`);
  lines.push(`- manualReviewReductionAchieved: ${report.summary.manualReviewReductionAchieved}`);
  lines.push("");

  lines.push("## Autofilled rows by policy");
  for (const item of report.policyCounts || []) lines.push(`- ${item.policy}: ${item.count}`);
  if (!(report.policyCounts || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Counts by autofill decision");
  for (const item of report.countsByAutofillDecision || []) lines.push(`- ${item.key}: ${item.count}`);
  if (!(report.countsByAutofillDecision || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Exception rows");
  for (const item of report.exceptionRows || []) {
    lines.push(`- #${item.queueOrder} | ${item.documentId} | ${item.batchKey} | ${item.reason}`);
  }
  if (!(report.exceptionRows || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Manual review reduction achieved");
  lines.push(`- Only ${report.summary.exceptionCount} rows remain in exception worksheet for manual review.`);
  lines.push("- All autofilled rows remain doNotAutoApply=true and read-only.");

  return lines.join("\n");
}
