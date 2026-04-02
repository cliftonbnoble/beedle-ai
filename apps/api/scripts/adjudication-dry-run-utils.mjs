import fs from "node:fs/promises";

export const ALLOWED_REVIEWER_DECISIONS = ["", "no_action", "confirm_metadata", "citation_fix", "escalate", "do_not_approve"];
export const ALLOWED_CITATION_ACTION_TYPES = ["", "replace_citation", "normalize_format", "drop_reference", "relabel_context"];
export const ADJUDICATION_TEMPLATE_FIELDS = [
  "documentId",
  "title",
  "batchKey",
  "reviewerDecision",
  "reviewerNotes",
  "citationActionType",
  "citationOriginal",
  "citationReplacement",
  "confirmMetadata",
  "escalate",
  "doNotApprove",
  "reviewedBy",
  "reviewedAt"
];
export const ADJUDICATION_ROW_STATES = [
  "blank_template_row",
  "reviewed_no_action",
  "reviewed_confirm_metadata",
  "reviewed_citation_fix_supported",
  "reviewed_citation_fix_blocked",
  "reviewed_escalate",
  "reviewed_do_not_approve",
  "invalid_conflicting_flags",
  "invalid_missing_required_fields",
  "invalid_title_mismatch",
  "invalid_unknown_decision",
  "invalid_unknown_action_type"
];
const SAFE_ORDINANCE_BASES = new Set(["37.1", "37.2", "37.8"]);
const UNSAFE_BLOCKED_BASES = new Set(["37.3", "37.7", "37.9"]);

function normalizeCitation(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/^ordinance/, "")
    .replace(/^rule/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

function citationBase(value) {
  return normalizeCitation(value).replace(/\([a-z0-9]+\)/g, "");
}

function normalizeTitleWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function stripTitlePunctuation(value) {
  return normalizeTitleWhitespace(value).replace(/[^\p{L}\p{N}\s]/gu, "");
}

export function analyzeTitleComparison(inputTitle, canonicalTitle) {
  const rawInput = String(inputTitle || "");
  const rawCanonical = String(canonicalTitle || "");
  const normalizedInputTitle = normalizeTitleWhitespace(rawInput);
  const normalizedCanonicalTitle = normalizeTitleWhitespace(rawCanonical);
  const exactMatch = rawInput === rawCanonical;
  const normalizedExact = normalizedInputTitle === normalizedCanonicalTitle;
  const caseInsensitiveMatch = normalizedInputTitle.toLowerCase() === normalizedCanonicalTitle.toLowerCase();
  const punctuationNormalizedInput = stripTitlePunctuation(rawInput).toLowerCase();
  const punctuationNormalizedCanonical = stripTitlePunctuation(rawCanonical).toLowerCase();
  const punctuationOnlyMatch = punctuationNormalizedInput === punctuationNormalizedCanonical;
  const trimmedInput = rawInput.trim();
  const trimmedCanonical = rawCanonical.trim();
  const duplicateSpacingOnlyDifference = trimmedInput.replace(/\s+/g, " ") === trimmedCanonical.replace(/\s+/g, " ");

  let mismatchReason = "exact_match";
  if (!exactMatch) {
    if (normalizedExact && (rawInput !== trimmedInput || rawCanonical !== trimmedCanonical)) mismatchReason = "trailing_whitespace";
    else if (normalizedExact && duplicateSpacingOnlyDifference) mismatchReason = "duplicate_spacing";
    else if (caseInsensitiveMatch) mismatchReason = "case_only_difference";
    else if (punctuationOnlyMatch) mismatchReason = "punctuation_only_difference";
    else mismatchReason = "true_semantic_mismatch";
  }
  return {
    inputTitle: rawInput,
    canonicalDocumentTitle: rawCanonical,
    normalizedInputTitle,
    normalizedCanonicalTitle,
    mismatchReason,
    exactMatch,
    normalizedExact
  };
}

function parseBooleanToken(value, strict) {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw.length) return { ok: true, value: false };
  if (["1", "true", "yes", "y"].includes(raw)) return { ok: true, value: true };
  if (["0", "false", "no", "n"].includes(raw)) return { ok: true, value: false };
  if (strict) return { ok: false, reason: `invalid_boolean:${value}` };
  return { ok: true, value: false, coerced: true };
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") {
        cur += "\"";
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

export function parseCsvRows(text) {
  const lines = String(text || "")
    .replace(/\r/g, "")
    .split("\n");
  const nonEmptyLines = lines.filter((line) => line.trim().length > 0);
  const parseWarnings = [];
  if (nonEmptyLines.length === 0) {
    parseWarnings.push("input_file_empty");
    return { rows: [], headers: [], blankRowsSkipped: 0, parseWarnings };
  }
  const linesForParse = nonEmptyLines;
  const headers = parseCsvLine(linesForParse[0]).map((h) => h.trim());
  const headerSet = new Set(headers);
  const expectedSet = new Set(ADJUDICATION_TEMPLATE_FIELDS);
  const hasExpectedHeaders = ADJUDICATION_TEMPLATE_FIELDS.every((field) => headerSet.has(field));
  if (!hasExpectedHeaders) parseWarnings.push("unsupported_csv_header_set");
  if (linesForParse.length === 1) parseWarnings.push("template_has_headers_only");
  const rows = [];
  let blankRowsSkipped = 0;
  for (let i = 1; i < linesForParse.length; i += 1) {
    const values = parseCsvLine(linesForParse[i]);
    const row = {};
    headers.forEach((key, idx) => {
      row[key] = values[idx] ?? "";
    });
    const hasAnyValue = headers.some((key) => String(row[key] ?? "").trim().length > 0);
    if (!hasAnyValue) {
      blankRowsSkipped += 1;
      continue;
    }
    rows.push({ rowNumber: i + 1, ...row });
  }
  if (rows.length === 0 && linesForParse.length > 1) parseWarnings.push("rows_filtered_as_blank");
  return { rows, headers, blankRowsSkipped, parseWarnings };
}

export async function loadAdjudicationInput(inputPath) {
  const raw = await fs.readFile(inputPath, "utf8");
  const lower = String(inputPath || "").toLowerCase();
  const diagnostics = {
    inputBytes: Buffer.byteLength(raw, "utf8"),
    detectedFormat: lower.endsWith(".csv") ? "csv" : lower.endsWith(".json") ? "json" : "unknown",
    detectedHeaders: [],
    actualHeaderRow: "",
    expectedHeaderRow: ADJUDICATION_TEMPLATE_FIELDS.join(","),
    missingHeaders: [],
    extraHeaders: [],
    topLevelKeys: [],
    detectedRootType: "unknown",
    expectedRootTypes: ["array", "object.rows", "object.adjudication.rows"],
    rowExtractionPathUsed: "none",
    blankRowsSkipped: 0,
    parseWarnings: []
  };
  if (lower.endsWith(".csv")) {
    const parsed = parseCsvRows(raw);
    diagnostics.detectedHeaders = parsed.headers;
    diagnostics.actualHeaderRow = parsed.headers.join(",");
    diagnostics.rowExtractionPathUsed = "csv_rows";
    diagnostics.blankRowsSkipped = parsed.blankRowsSkipped;
    diagnostics.parseWarnings = parsed.parseWarnings;
    const headerSet = new Set(parsed.headers);
    diagnostics.missingHeaders = ADJUDICATION_TEMPLATE_FIELDS.filter((field) => !headerSet.has(field));
    diagnostics.extraHeaders = parsed.headers.filter((field) => !ADJUDICATION_TEMPLATE_FIELDS.includes(field));
    if (parsed.rows.length === 0 && !diagnostics.parseWarnings.includes("template_has_headers_only")) {
      diagnostics.parseWarnings.push("no_adjudication_rows_present");
    }
    return { inputFormat: "csv", rows: parsed.rows, diagnostics };
  }
  if (lower.endsWith(".json")) {
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      diagnostics.parseWarnings.push("parse_failed_no_rows");
      return { inputFormat: "json", rows: [], diagnostics };
    }
    diagnostics.detectedRootType = Array.isArray(parsed) ? "array" : parsed === null ? "null" : typeof parsed;
    diagnostics.topLevelKeys = parsed && typeof parsed === "object" && !Array.isArray(parsed) ? Object.keys(parsed) : [];
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed) && typeof parsed.error === "string") {
      diagnostics.parseWarnings.push("parse_failed_no_rows");
      diagnostics.parseWarnings.push("export_error_payload");
      diagnostics.rowExtractionPathUsed = "error_payload";
      return { inputFormat: "json", rows: [], diagnostics };
    }
    let rows = [];
    if (Array.isArray(parsed)) {
      rows = parsed;
      diagnostics.rowExtractionPathUsed = "root_array";
    } else if (Array.isArray(parsed?.rows)) {
      rows = parsed.rows;
      diagnostics.rowExtractionPathUsed = "rows";
    } else if (Array.isArray(parsed?.adjudication?.rows)) {
      rows = parsed.adjudication.rows;
      diagnostics.rowExtractionPathUsed = "adjudication.rows";
    } else {
      diagnostics.parseWarnings.push("unsupported_json_shape");
      rows = [];
    }
    const normalized = rows.map((row, idx) => ({ rowNumber: idx + 1, ...row }));
    const blankFiltered = normalized.filter((row) =>
      ADJUDICATION_TEMPLATE_FIELDS.some((field) => String(row[field] ?? "").trim().length > 0)
    );
    diagnostics.blankRowsSkipped = normalized.length - blankFiltered.length;
    if (diagnostics.blankRowsSkipped > 0) diagnostics.parseWarnings.push("rows_filtered_as_blank");
    if (blankFiltered.length === 0 && diagnostics.parseWarnings.length === 0) diagnostics.parseWarnings.push("no_adjudication_rows_present");
    return {
      inputFormat: "json",
      rows: blankFiltered,
      diagnostics
    };
  }
  throw new Error("ADJUDICATION_INPUT must be .csv or .json");
}

export async function fetchDocumentSnapshots(fetchJson, documentIds) {
  const uniqueIds = Array.from(new Set((documentIds || []).filter(Boolean)));
  const out = new Map();
  for (const documentId of uniqueIds) {
    const response = await fetchJson(`/admin/ingestion/documents/${documentId}`);
    if (response.status === 200) out.set(documentId, response.body);
  }
  return out;
}

function validateRowShape(row, strict) {
  const reasons = [];
  const decision = String(row.reviewerDecision ?? "").trim();
  const action = String(row.citationActionType ?? "").trim();
  const missingDocumentId = !String(row.documentId ?? "").trim();
  const missingBatchKey = !String(row.batchKey ?? "").trim();
  if (missingDocumentId) reasons.push("missing_documentId");
  const invalidDecision = !ALLOWED_REVIEWER_DECISIONS.includes(decision);
  const invalidAction = !ALLOWED_CITATION_ACTION_TYPES.includes(action);
  if (invalidDecision) reasons.push(`invalid_reviewerDecision:${decision}`);
  if (invalidAction) reasons.push(`invalid_citationActionType:${action}`);
  if ((row.citationOriginal || row.citationReplacement) && !action) reasons.push("citation_fields_without_action");
  if (action && action !== "drop_reference" && !String(row.citationOriginal || "").trim()) reasons.push("missing_citationOriginal");
  if (["replace_citation", "normalize_format", "relabel_context"].includes(action) && !String(row.citationReplacement || "").trim()) {
    reasons.push("missing_citationReplacement");
  }

  const confirmMetadata = parseBooleanToken(row.confirmMetadata, strict);
  const escalate = parseBooleanToken(row.escalate, strict);
  const doNotApprove = parseBooleanToken(row.doNotApprove, strict);
  if (!confirmMetadata.ok) reasons.push(confirmMetadata.reason);
  if (!escalate.ok) reasons.push(escalate.reason);
  if (!doNotApprove.ok) reasons.push(doNotApprove.reason);

  const parsedFlags = {
    confirmMetadata: confirmMetadata.value === true,
    escalate: escalate.value === true,
    doNotApprove: doNotApprove.value === true
  };
  if (parsedFlags.escalate && parsedFlags.doNotApprove) reasons.push("conflicting_escalate_and_doNotApprove");
  if (decision === "confirm_metadata" && parsedFlags.doNotApprove) reasons.push("conflicting_confirm_and_doNotApprove");

  return {
    reasons,
    parsedFlags,
    decision,
    action,
    missingDocumentId,
    missingBatchKey,
    invalidDecision,
    invalidAction
  };
}

function hasBlockedUnsafeContext(doc) {
  const buckets = doc?.unresolvedBuckets || [];
  return buckets.some((bucket) =>
    ["unsafe_37x_structural_block", "cross_context_ambiguous", "structurally_blocked_not_found"].includes(bucket)
  );
}

function safeReplacementCandidate(row, doc) {
  const action = String(row.citationActionType || "").trim();
  if (!["replace_citation", "normalize_format"].includes(action)) return false;
  const original = String(row.citationOriginal || "");
  const replacement = String(row.citationReplacement || "");
  const base = citationBase(replacement);
  if (UNSAFE_BLOCKED_BASES.has(base)) return false;
  if (String(action) === "relabel_context") return false;
  if (!replacement.trim()) return false;

  const validRefs = doc?.validReferences || { indexCodes: [], rulesSections: [], ordinanceSections: [] };
  const normalizedValid = new Set(
    [...(validRefs.indexCodes || []), ...(validRefs.rulesSections || []), ...(validRefs.ordinanceSections || [])].map(normalizeCitation)
  );
  if (normalizedValid.has(normalizeCitation(replacement))) return true;
  if (SAFE_ORDINANCE_BASES.has(base) && /^ordinance37\./.test(normalizeCitation(original))) return true;
  return false;
}

export function simulateAdjudicationRow(row, doc, strict) {
  const shape = validateRowShape(row, strict);
  const title = String(row.title || "");
  const reasons = [...shape.reasons];
  const decisionMissing = !shape.decision && !shape.parsedFlags.confirmMetadata && !shape.parsedFlags.escalate && !shape.parsedFlags.doNotApprove;
  const citationFixSelected = shape.decision === "citation_fix" || shape.action.length > 0;
  const citationReplacementMissing =
    citationFixSelected &&
    ["replace_citation", "normalize_format", "relabel_context"].includes(shape.action) &&
    !String(row.citationReplacement || "").trim();
  const rowMeta = {
    citationFixSelected,
    citationReplacementMissing
  };

  if (shape.invalidDecision) {
    return {
      rowState: "invalid_unknown_decision",
      outcome: "invalid_row",
      reasons,
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  if (shape.invalidAction) {
    return {
      rowState: "invalid_unknown_action_type",
      outcome: "unsupported_action",
      reasons,
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  if (shape.missingDocumentId || (citationFixSelected && decisionMissing) || citationReplacementMissing) {
    return {
      rowState: "invalid_missing_required_fields",
      outcome: "invalid_row",
      reasons: [...reasons, ...(decisionMissing && citationFixSelected ? ["missing_reviewerDecision"] : [])],
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }

  const matchedDocumentFound = Boolean(doc);
  const matchedTitle = !doc ? false : !title.trim() || analyzeTitleComparison(title, String(doc.title || "")).exactMatch;

  if (!matchedDocumentFound) {
    return {
      rowState: "invalid_missing_required_fields",
      outcome: "document_not_found",
      reasons: [...reasons, "document_not_found"],
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  if (!matchedTitle && title.length > 0) {
    return {
      rowState: "invalid_title_mismatch",
      outcome: "title_mismatch",
      reasons: [...reasons, "title_mismatch"],
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  if (reasons.some((r) => r.startsWith("conflicting_"))) {
    return {
      rowState: "invalid_conflicting_flags",
      outcome: "conflicting_flags",
      reasons,
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }

  const isEmpty =
    !shape.decision &&
    !shape.action &&
    !shape.parsedFlags.confirmMetadata &&
    !shape.parsedFlags.escalate &&
    !shape.parsedFlags.doNotApprove &&
    !String(row.citationOriginal || "").trim() &&
    !String(row.citationReplacement || "").trim();
  if (isEmpty) {
    const blankReasons = [...reasons];
    if (shape.missingBatchKey) blankReasons.push("missing_batchKey_on_blank_template_row");
    return {
      rowState: "blank_template_row",
      outcome: "incomplete_row",
      reasons: [...blankReasons, "no_adjudication_fields_set"],
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }

  if (shape.missingBatchKey) {
    return {
      rowState: "invalid_missing_required_fields",
      outcome: "invalid_row",
      reasons: [...reasons, "missing_batchKey_on_reviewed_row"],
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }

  if (shape.action === "relabel_context") {
    return {
      rowState: "reviewed_citation_fix_blocked",
      outcome: "unsupported_action",
      reasons: [...reasons, "relabel_context_not_simulated"],
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }

  const structurallyBlocked = hasBlockedUnsafeContext(doc);
  if (shape.decision === "escalate" || shape.parsedFlags.escalate) {
    return {
      rowState: "reviewed_escalate",
      outcome: "no_action",
      reasons,
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  if (shape.decision === "do_not_approve" || shape.parsedFlags.doNotApprove) {
    return {
      rowState: "reviewed_do_not_approve",
      outcome: "no_action",
      reasons,
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  if (shape.decision === "no_action") {
    return {
      rowState: "reviewed_no_action",
      outcome: "no_action",
      reasons,
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  if (shape.decision === "confirm_metadata" || shape.parsedFlags.confirmMetadata) {
    const safeConfirm = Boolean(doc.metadataConfirmationWouldUnlock) && !structurallyBlocked;
    return {
      rowState: "reviewed_confirm_metadata",
      outcome: safeConfirm ? "metadata_confirmation_candidate" : "blocked_structural",
      reasons: safeConfirm ? reasons : [...reasons, "metadata_confirmation_not_safe_for_structural_block"],
      safeToApplyInFuture: safeConfirm,
      ...rowMeta
    };
  }
  if (shape.decision === "citation_fix" || citationFixSelected) {
    if (structurallyBlocked) {
      return {
        rowState: "reviewed_citation_fix_blocked",
        outcome: "blocked_structural",
        reasons: [...reasons, "structural_unresolved_blockers_present"],
        safeToApplyInFuture: false,
        ...rowMeta
      };
    }
    const safeFix = safeReplacementCandidate(row, doc);
    if (safeFix) {
      return {
        rowState: "reviewed_citation_fix_supported",
        outcome: "one_safe_manual_fix_candidate",
        reasons,
        safeToApplyInFuture: true,
        ...rowMeta
      };
    }
    return {
      rowState: "reviewed_citation_fix_blocked",
      outcome: "blocked_structural",
      reasons: [...reasons, "no_safe_simulated_path"],
      safeToApplyInFuture: false,
      ...rowMeta
    };
  }
  return {
    rowState: "invalid_missing_required_fields",
    outcome: "invalid_row",
    reasons: [...reasons, "missing_reviewerDecision"],
    safeToApplyInFuture: false,
    ...rowMeta
  };
}
