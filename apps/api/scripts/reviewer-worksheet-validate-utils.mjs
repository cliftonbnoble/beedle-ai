import fs from "node:fs/promises";
import { WORKSHEET_COLUMNS } from "./reviewer-worksheet-export-utils.mjs";

const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);
const ALLOWED_DECISIONS = new Set([
  "",
  "keep_blocked",
  "escalate_to_legal_context_review",
  "possible_manual_context_fix_but_no_auto_apply",
  "manual_no_action"
]);

function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function parseCsvRows(text) {
  const lines = String(text || "").replace(/\r/g, "").split("\n");
  const nonEmpty = lines.filter((line) => line.trim().length > 0);
  if (!nonEmpty.length) return { headers: [], rows: [] };
  const headers = parseCsvLine(nonEmpty[0]).map((h) => h.trim());
  const rows = [];
  for (let i = 1; i < nonEmpty.length; i += 1) {
    const values = parseCsvLine(nonEmpty[i]);
    const row = { rowNumber: i + 1 };
    headers.forEach((header, idx) => {
      row[header] = values[idx] ?? "";
    });
    rows.push(row);
  }
  return { headers, rows };
}

function boolFromCell(value, strict, errors, key) {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return false;
  if (["1", "true", "yes", "y"].includes(raw)) return true;
  if (["0", "false", "no", "n"].includes(raw)) return false;
  if (strict) errors.push(`invalid_boolean:${key}`);
  return false;
}

function splitFamilies(value) {
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeRow(rawRow) {
  const row = {};
  for (const col of WORKSHEET_COLUMNS) row[col] = rawRow[col] ?? "";
  row.rowNumber = rawRow.rowNumber ?? null;
  return row;
}

function hasReviewerInput(row) {
  const keys = [
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
  return keys.some((key) => String(row[key] || "").trim().length > 0);
}

function validateRow(row, strict) {
  const reasons = [];
  const decision = String(row.reviewerDecision || "").trim();
  const blockedFamilies = splitFamilies(row.blocked37xFamily);
  const hasUnsafe37x = blockedFamilies.some((family) => UNSAFE_37X.has(family));

  if (!ALLOWED_DECISIONS.has(decision)) {
    return {
      validationState: "invalid_unknown_value",
      reasons: ["unknown_reviewerDecision"]
    };
  }

  if (!hasReviewerInput(row) && !decision) {
    return { validationState: "blank_unreviewed", reasons: [] };
  }

  if (!decision) {
    return { validationState: "invalid_missing_reviewer_decision", reasons: ["reviewerDecision_required_when_row_filled"] };
  }

  const boolErrors = [];
  const escalate = boolFromCell(row.escalateToLegal, strict, boolErrors, "escalateToLegal");
  const keepBlocked = boolFromCell(row.keepBlocked, strict, boolErrors, "keepBlocked");
  const manualFix = boolFromCell(row.possibleManualContextFix, strict, boolErrors, "possibleManualContextFix");
  if (boolErrors.length) {
    return { validationState: "invalid_unknown_value", reasons: boolErrors };
  }

  const trueFlags = [escalate, keepBlocked, manualFix].filter(Boolean).length;
  if (trueFlags > 1) {
    return { validationState: "invalid_conflicting_reviewer_flags", reasons: ["multiple_reviewer_flags_true"] };
  }

  const reason = String(row.reviewerDecisionReason || "").trim();
  const citationContext = String(row.reviewerCitationContext || "").trim();
  const evidenceUsed = String(row.reviewerEvidenceUsed || "").trim();

  if (decision === "possible_manual_context_fix_but_no_auto_apply") {
    if (hasUnsafe37x) {
      return { validationState: "invalid_illegal_decision_for_blocked37x", reasons: ["unsafe_37x_cannot_use_manual_context_fix"] };
    }
    if (!reason) return { validationState: "invalid_missing_required_reason", reasons: ["reviewerDecisionReason_required"] };
    if (!citationContext || !evidenceUsed) {
      return {
        validationState: "invalid_missing_evidence_for_manual_context_fix",
        reasons: ["reviewerCitationContext_and_reviewerEvidenceUsed_required"]
      };
    }
    if (!manualFix) return { validationState: "invalid_conflicting_reviewer_flags", reasons: ["possibleManualContextFix_flag_required"] };
    return { validationState: "valid_possible_manual_context_fix", reasons: [] };
  }

  if (decision === "keep_blocked") {
    if (!keepBlocked) return { validationState: "invalid_conflicting_reviewer_flags", reasons: ["keepBlocked_flag_required"] };
    return { validationState: "valid_keep_blocked", reasons: [] };
  }

  if (decision === "escalate_to_legal_context_review") {
    if (!escalate) return { validationState: "invalid_conflicting_reviewer_flags", reasons: ["escalateToLegal_flag_required"] };
    return { validationState: "valid_escalate_to_legal_context_review", reasons: [] };
  }

  if (decision === "manual_no_action") {
    return { validationState: "valid_manual_no_action", reasons: [] };
  }

  return { validationState: "invalid_unknown_value", reasons: ["unhandled_decision_state"] };
}

function isValidState(state) {
  return state.startsWith("valid_") || state === "blank_unreviewed";
}

function readyForDryRun(state) {
  return state === "valid_keep_blocked" || state === "valid_escalate_to_legal_context_review" || state === "valid_possible_manual_context_fix" || state === "valid_manual_no_action";
}

export async function loadReviewerWorksheetInput(inputPath) {
  const raw = await fs.readFile(inputPath, "utf8");
  const lower = String(inputPath || "").toLowerCase();
  if (lower.endsWith(".csv")) {
    const parsed = parseCsvRows(raw);
    return {
      inputFormat: "csv",
      headers: parsed.headers,
      rows: parsed.rows.map(normalizeRow)
    };
  }
  if (lower.endsWith(".json")) {
    const parsed = JSON.parse(raw);
    const rows = Array.isArray(parsed?.rows) ? parsed.rows : Array.isArray(parsed) ? parsed : [];
    return {
      inputFormat: "json",
      headers: WORKSHEET_COLUMNS,
      rows: rows.map(normalizeRow)
    };
  }
  throw new Error("REVIEWER_WORKSHEET_INPUT must be .csv or .json");
}

export function validateReviewerWorksheetRows(rows, strict = true) {
  const validated = rows.map((row) => {
    const result = validateRow(row, strict);
    return {
      ...row,
      validationState: result.validationState,
      validationReasons: result.reasons,
      validRow: isValidState(result.validationState),
      readyForDryRunComparison: readyForDryRun(result.validationState)
    };
  });

  const blankRows = validated.filter((row) => row.validationState === "blank_unreviewed");
  const validRows = validated.filter((row) => row.validationState.startsWith("valid_"));
  const invalidRows = validated.filter((row) => row.validationState.startsWith("invalid_"));
  const rowsStillBlocked = validated.filter(
    (row) => row.validationState === "blank_unreviewed" || row.validationState === "valid_keep_blocked"
  );
  const rowsReady = validated.filter((row) => row.readyForDryRunComparison);

  return {
    rows: validated,
    summary: {
      totalRows: validated.length,
      blankRows: blankRows.length,
      validRows: validRows.length,
      invalidRows: invalidRows.length,
      countsByValidationState: countBy(validated.map((row) => row.validationState)),
      countsByPriorityLane: countBy(validated.map((row) => String(row.priorityLane || "<none>"))),
      countsByReviewerDecision: countBy(validated.map((row) => String(row.reviewerDecision || "<blank>"))),
      rowsReadyForDryRunComparison: rowsReady.length,
      rowsStillBlocked: rowsStillBlocked.length,
      rowsRequiringMoreReviewerInput: invalidRows.length
    },
    guidance: {
      rowsMissingEvidence: invalidRows.filter((row) => row.validationState === "invalid_missing_evidence_for_manual_context_fix"),
      rowsWithConflictingFlags: invalidRows.filter((row) => row.validationState === "invalid_conflicting_reviewer_flags"),
      rowsAttemptingUnsafeBlocked37xActions: invalidRows.filter((row) => row.validationState === "invalid_illegal_decision_for_blocked37x"),
      rowsFullyReviewComplete: rowsReady
    }
  };
}

export function formatReviewerWorksheetValidationMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Worksheet Validation");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) {
    if (Array.isArray(v)) continue;
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("- countsByValidationState:");
  for (const item of report.summary.countsByValidationState || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- countsByPriorityLane:");
  for (const item of report.summary.countsByPriorityLane || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("- countsByReviewerDecision:");
  for (const item of report.summary.countsByReviewerDecision || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("");

  const sections = [
    ["Valid reviewed rows", report.rows.filter((row) => row.validationState.startsWith("valid_"))],
    ["Invalid rows", report.rows.filter((row) => row.validationState.startsWith("invalid_"))],
    ["Blank rows", report.rows.filter((row) => row.validationState === "blank_unreviewed")],
    ["Rows ready for dry-run comparison", report.rows.filter((row) => row.readyForDryRunComparison)]
  ];

  for (const [title, rows] of sections) {
    lines.push(`## ${title}`);
    if (!rows.length) {
      lines.push("- none");
      lines.push("");
      continue;
    }
    for (const row of rows.slice(0, 200)) {
      lines.push(`- row=${row.rowNumber ?? "n/a"} | ${row.documentId} | ${row.title} | state=${row.validationState}`);
      if (row.validationReasons?.length) lines.push(`  - reasons: ${row.validationReasons.join(", ")}`);
    }
    lines.push("");
  }

  lines.push("## Reviewer follow-up checklist");
  lines.push(`- Missing evidence rows: ${report.guidance.rowsMissingEvidence.length}`);
  lines.push(`- Conflicting flag rows: ${report.guidance.rowsWithConflictingFlags.length}`);
  lines.push(`- Unsafe blocked 37.x action attempts: ${report.guidance.rowsAttemptingUnsafeBlocked37xActions.length}`);
  lines.push(`- Fully review-complete rows: ${report.guidance.rowsFullyReviewComplete.length}`);
  lines.push("");
  lines.push("- This validator is read-only and performs no mutation.");

  return lines.join("\n");
}
