import fs from "node:fs/promises";
import path from "node:path";
import {
  ADJUDICATION_ROW_STATES,
  ALLOWED_CITATION_ACTION_TYPES,
  ALLOWED_REVIEWER_DECISIONS,
  analyzeTitleComparison,
  fetchDocumentSnapshots,
  loadAdjudicationInput,
  simulateAdjudicationRow
} from "./adjudication-dry-run-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const inputPath = process.env.ADJUDICATION_INPUT;
const strictMode = process.env.ADJUDICATION_STRICT === "1";
const reportName = process.env.ADJUDICATION_REPORT_NAME || "adjudication-dry-run-report.json";
const reportFormat = (process.env.ADJUDICATION_REPORT_FORMAT || "json").toLowerCase();
const markdownReportName =
  process.env.ADJUDICATION_MARKDOWN_REPORT_NAME || reportName.replace(/\.json$/i, ".md");
const includeCanonicalTitleHints = process.env.ADJUDICATION_INCLUDE_CANONICAL_TITLE_HINTS === "1";

async function fetchJson(endpoint) {
  const response = await fetch(`${apiBase}${endpoint}`);
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

function unresolvedRootCause(issue) {
  const message = String(issue?.message || "");
  const type = String(issue?.referenceType || "");
  const normalized = String(issue?.normalizedValue || issue?.rawValue || "").toLowerCase();
  if (/malformed|invalid|unable to parse|unparseable|format/i.test(message)) return "malformed";
  if (
    /cross[_\s-]?context/i.test(message) ||
    (type === "rules_section" && normalized.includes("37.")) ||
    (type === "ordinance_section" && normalized.includes("rule"))
  ) {
    return "cross_context";
  }
  if (/duplicate|redundant/i.test(message)) return "duplicate";
  if (/parent|related/i.test(message)) return "parent_child";
  return "not_found";
}

function summarizeCountsByState(perRowResults) {
  const map = new Map(ADJUDICATION_ROW_STATES.map((state) => [state, 0]));
  for (const row of perRowResults) {
    map.set(row.rowState, (map.get(row.rowState) || 0) + 1);
  }
  return Array.from(map.entries()).map(([key, count]) => ({ key, count }));
}

function toMarkdownReport(report) {
  const lines = [];
  lines.push("# Adjudication Dry-Run Summary");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- Rows loaded: ${report.summary.rowsLoaded}`);
  lines.push(`- Matched documents: ${report.summary.matchedDocuments}`);
  lines.push(`- Blank template rows: ${report.summary.blankTemplateRows}`);
  lines.push(`- Actionable reviewed rows: ${report.summary.actionableReviewedRows}`);
  lines.push(`- Supported action rows: ${report.summary.supportedActionRows}`);
  lines.push(`- Blocked action rows: ${report.summary.blockedActionRows}`);
  lines.push(`- Invalid reviewed rows: ${report.summary.invalidReviewedRows}`);
  if (report.summary.zeroRowReason) lines.push(`- Zero-row reason: ${report.summary.zeroRowReason}`);
  lines.push("");
  lines.push("## Export Contract Warnings");
  if ((report.exportContractWarnings || []).length === 0) {
    lines.push("- none");
  } else {
    for (const warning of report.exportContractWarnings || []) lines.push(`- ${warning}`);
  }
  lines.push(`- Rows with missing batchKey: ${report.debug.rowsMissingBatchKeyCount}`);
  if (
    report.debug.rowsMissingBatchKeyCount > 0 &&
    report.summary.invalidReviewedRows === 0 &&
    report.summary.blankTemplateRows === report.debug.rowsMissingBatchKeyCount
  ) {
    lines.push("- Missing batchKey appears only on blank template rows (non-fatal).");
  }
  lines.push("");
  lines.push("## Actionable Reviewed Rows");
  for (const row of report.per_row_results.filter((r) =>
    ["reviewed_confirm_metadata", "reviewed_citation_fix_supported", "reviewed_no_action", "reviewed_escalate", "reviewed_do_not_approve"].includes(r.rowState)
  )) {
    lines.push(`- ${row.documentId} | ${row.title} | ${row.rowState} | next: ${row.simulatedNextAction}`);
  }
  if (!report.per_row_results.some((r) =>
    ["reviewed_confirm_metadata", "reviewed_citation_fix_supported", "reviewed_no_action", "reviewed_escalate", "reviewed_do_not_approve"].includes(r.rowState)
  )) lines.push("- none");
  lines.push("");
  lines.push("## Blocked Reviewed Rows");
  for (const row of report.per_row_results.filter((r) => r.rowState === "reviewed_citation_fix_blocked")) {
    lines.push(`- ${row.documentId} | ${row.title} | reasons: ${(row.reasons || []).join("; ")}`);
  }
  if (!report.per_row_results.some((r) => r.rowState === "reviewed_citation_fix_blocked")) lines.push("- none");
  lines.push("");
  lines.push("## Invalid Rows");
  for (const row of report.invalid_rows) {
    lines.push(`- row ${row.sourceRowNumber}: ${row.documentId || "<missing>"} | ${row.rowState} | ${(row.reasons || []).join("; ")}`);
  }
  if (!report.invalid_rows.length) lines.push("- none");
  lines.push("");
  lines.push("## Title Mismatch Details");
  for (const row of report.reviewer_completion_checklist.rowsWithTitleMismatchDetails || []) {
    lines.push(
      `- row ${row.sourceRowNumber}: ${row.documentId} | reason=${row.mismatchReason} | input="${row.inputTitle}" | canonical="${row.canonicalDocumentTitle}"`
    );
  }
  if (!(report.reviewer_completion_checklist.rowsWithTitleMismatchDetails || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Blank Template Rows");
  lines.push(`- count: ${report.summary.blankTemplateRows}`);
  lines.push("");
  lines.push("## Next Operator Steps");
  lines.push(`- Fill reviewerDecision for ${report.reviewer_completion_checklist.rowsNeedingReviewerDecisionCount} rows.`);
  lines.push(`- Fill citationReplacement for ${report.reviewer_completion_checklist.rowsNeedingCitationReplacementCount} citation_fix rows.`);
  lines.push(`- Resolve ${report.reviewer_completion_checklist.rowsWithConflictingFlagsCount} conflicting-flag rows.`);
  lines.push(`- Review ${report.reviewer_completion_checklist.rowsBlockedUnsafe37xOrCrossContextCount} blocked unsafe/cross-context rows (remain blocked).`);
  return lines.join("\n");
}

async function main() {
  if (!inputPath) {
    throw new Error("ADJUDICATION_INPUT is required");
  }
  const absoluteInput = path.resolve(process.cwd(), inputPath);
  const input = await loadAdjudicationInput(absoluteInput);
  const rows = input.rows || [];
  const documentIds = rows.map((row) => String(row.documentId || "")).filter(Boolean);
  const docMap = await fetchDocumentSnapshots(fetchJson, documentIds);

  const perRowResults = rows.map((row) => {
    const documentId = String(row.documentId || "");
    const doc = docMap.get(documentId);
    const simulation = simulateAdjudicationRow(row, doc, strictMode);
    const reviewerDecision = String(row.reviewerDecision || "");
    const citationActionType = String(row.citationActionType || "");
    const exactUnresolvedReferences = (doc?.referenceIssues || []).map((issue) => ({
      referenceType: issue.referenceType,
      rawValue: issue.rawValue,
      normalizedValue: issue.normalizedValue,
      rootCause: unresolvedRootCause(issue),
      message: issue.message
    }));
    const titleDiagnostics = analyzeTitleComparison(String(row.title || ""), String(doc?.title || ""));
    return {
      sourceRowNumber: row.rowNumber ?? null,
      documentId,
      title: String(row.title || ""),
      batchKey: String(row.batchKey || ""),
      reviewerDecision,
      citationActionType,
      parsedFlags: {
        confirmMetadata: String(row.confirmMetadata || ""),
        escalate: String(row.escalate || ""),
        doNotApprove: String(row.doNotApprove || "")
      },
      matchedDocumentFound: Boolean(doc),
      matchedTitle: !doc ? false : !String(row.title || "").trim() || titleDiagnostics.exactMatch,
      ...(includeCanonicalTitleHints || simulation.rowState === "invalid_title_mismatch"
        ? {
            inputTitle: titleDiagnostics.inputTitle,
            canonicalDocumentTitle: titleDiagnostics.canonicalDocumentTitle,
            normalizedInputTitle: titleDiagnostics.normalizedInputTitle,
            normalizedCanonicalTitle: titleDiagnostics.normalizedCanonicalTitle,
            mismatchReason: titleDiagnostics.mismatchReason
          }
        : {}),
      exactUnresolvedReferences,
      validatedReferencesPresent: doc
        ? {
            indexCodes: doc?.validReferences?.indexCodes || [],
            rulesSections: doc?.validReferences?.rulesSections || [],
            ordinanceSections: doc?.validReferences?.ordinanceSections || []
          }
        : { indexCodes: [], rulesSections: [], ordinanceSections: [] },
      metadataConfirmationWouldUnlock: Boolean(doc?.metadataConfirmationWouldUnlock),
      rowState: simulation.rowState,
      outcome: simulation.outcome,
      reasons: simulation.reasons,
      citationFixSelected: simulation.citationFixSelected,
      citationReplacementMissing: simulation.citationReplacementMissing,
      simulatedNextAction:
        simulation.outcome === "metadata_confirmation_candidate"
          ? "metadata_confirmable_if_future_apply_enabled"
          : simulation.outcome === "one_safe_manual_fix_candidate"
            ? "one_safe_fix_candidate_if_future_apply_enabled"
            : "remain_read_only_review",
      safeToApplyInFuture: simulation.safeToApplyInFuture
    };
  });

  const countsByOutcome = countBy(perRowResults, (row) => row.outcome);
  const countsByRowState = summarizeCountsByState(perRowResults);
  const countsByDecision = countBy(perRowResults, (row) => row.reviewerDecision || "<blank>");
  const countsByActionType = countBy(perRowResults, (row) => row.citationActionType || "<blank>");
  const countsByBatchKey = countBy(perRowResults, (row) => row.batchKey || "<blank>");

  const invalidRows = perRowResults.filter((row) => String(row.rowState || "").startsWith("invalid_"));
  const blankTemplateRows = perRowResults.filter((row) => row.rowState === "blank_template_row");
  const metadataCandidates = perRowResults.filter((row) => row.rowState === "reviewed_confirm_metadata" && row.safeToApplyInFuture);
  const safeFixCandidates = perRowResults.filter((row) => row.rowState === "reviewed_citation_fix_supported");
  const stillBlocked = perRowResults.filter((row) => row.rowState === "reviewed_citation_fix_blocked");
  const unresolvedConflicts = perRowResults.filter((row) => ["conflicting_flags", "unsupported_action", "title_mismatch"].includes(row.outcome));
  const actionableReviewedRows = perRowResults.filter((row) =>
    ["reviewed_no_action", "reviewed_confirm_metadata", "reviewed_citation_fix_supported", "reviewed_escalate", "reviewed_do_not_approve"].includes(row.rowState)
  );
  const supportedActionRows = perRowResults.filter((row) =>
    ["reviewed_confirm_metadata", "reviewed_citation_fix_supported", "reviewed_escalate", "reviewed_do_not_approve", "reviewed_no_action"].includes(row.rowState)
  );
  const blockedActionRows = stillBlocked;
  const invalidReviewedRows = invalidRows;
  const rowsMissingBatchKey = perRowResults.filter((row) => !String(row.batchKey || "").trim());
  const exportContractWarnings = [];
  if (rowsMissingBatchKey.length > 0) {
    if (rowsMissingBatchKey.every((row) => row.rowState === "blank_template_row")) {
      exportContractWarnings.push("missing_batchKey_on_blank_template_rows_only");
    } else {
      exportContractWarnings.push("missing_batchKey_on_reviewed_rows_present");
    }
  }

  const reviewerCompletionChecklist = {
    rowsStillBlankCount: blankTemplateRows.length,
    rowsStillBlank: blankTemplateRows.map((row) => ({ sourceRowNumber: row.sourceRowNumber, documentId: row.documentId, title: row.title })),
    rowsNeedingReviewerDecisionCount: perRowResults.filter(
      (row) => row.rowState === "blank_template_row" || row.reasons.includes("missing_reviewerDecision")
    ).length,
    rowsNeedingReviewerDecision: perRowResults
      .filter((row) => row.rowState === "blank_template_row" || row.reasons.includes("missing_reviewerDecision"))
      .map((row) => ({ sourceRowNumber: row.sourceRowNumber, documentId: row.documentId, title: row.title })),
    rowsNeedingCitationReplacementCount: perRowResults.filter((row) => row.citationFixSelected && row.citationReplacementMissing).length,
    rowsNeedingCitationReplacement: perRowResults
      .filter((row) => row.citationFixSelected && row.citationReplacementMissing)
      .map((row) => ({ sourceRowNumber: row.sourceRowNumber, documentId: row.documentId, title: row.title })),
    rowsWithConflictingFlagsCount: perRowResults.filter((row) => row.rowState === "invalid_conflicting_flags").length,
    rowsWithConflictingFlags: perRowResults
      .filter((row) => row.rowState === "invalid_conflicting_flags")
      .map((row) => ({ sourceRowNumber: row.sourceRowNumber, documentId: row.documentId, title: row.title, reasons: row.reasons })),
    rowsWithTitleMismatchCount: perRowResults.filter((row) => row.rowState === "invalid_title_mismatch").length,
    rowsWithTitleMismatch: perRowResults
      .filter((row) => row.rowState === "invalid_title_mismatch")
      .map((row) => ({ sourceRowNumber: row.sourceRowNumber, documentId: row.documentId, title: row.title, reasons: row.reasons })),
    rowsWithTitleMismatchDetails: perRowResults
      .filter((row) => row.rowState === "invalid_title_mismatch")
      .map((row) => ({
        sourceRowNumber: row.sourceRowNumber,
        documentId: row.documentId,
        title: row.title,
        inputTitle: row.inputTitle ?? String(row.title || ""),
        canonicalDocumentTitle: row.canonicalDocumentTitle ?? "",
        normalizedInputTitle: row.normalizedInputTitle ?? "",
        normalizedCanonicalTitle: row.normalizedCanonicalTitle ?? "",
        mismatchReason: row.mismatchReason ?? "true_semantic_mismatch",
        reasons: row.reasons
      })),
    rowsBlockedUnsafe37xOrCrossContextCount: perRowResults.filter((row) =>
      (row.reasons || []).some((reason) =>
        [
          "structural_unresolved_blockers_present",
          "no_safe_simulated_path",
          "metadata_confirmation_not_safe_for_structural_block"
        ].includes(reason)
      )
    ).length,
    rowsBlockedUnsafe37xOrCrossContext: perRowResults
      .filter((row) =>
        (row.reasons || []).some((reason) =>
          [
            "structural_unresolved_blockers_present",
            "no_safe_simulated_path",
            "metadata_confirmation_not_safe_for_structural_block"
          ].includes(reason)
        )
      )
      .map((row) => ({ sourceRowNumber: row.sourceRowNumber, documentId: row.documentId, title: row.title, rowState: row.rowState, reasons: row.reasons })),
    rowsWithMissingBatchKeyCount: rowsMissingBatchKey.length,
    rowsWithMissingBatchKey: rowsMissingBatchKey.map((row) => ({
      sourceRowNumber: row.sourceRowNumber,
      documentId: row.documentId,
      title: row.title,
      rowState: row.rowState,
      reasons: row.reasons
    }))
  };

  const report = {
    generatedAt: new Date().toISOString(),
    inputFile: absoluteInput,
    inputFormat: input.inputFormat,
    strictMode,
    inputBytes: input.diagnostics?.inputBytes || 0,
    detectedFormat: input.diagnostics?.detectedFormat || input.inputFormat,
    detectedHeaders: input.diagnostics?.detectedHeaders || [],
    actualHeaderRow: input.diagnostics?.actualHeaderRow || "",
    expectedHeaderRow: input.diagnostics?.expectedHeaderRow || "",
    missingHeaders: input.diagnostics?.missingHeaders || [],
    extraHeaders: input.diagnostics?.extraHeaders || [],
    topLevelKeys: input.diagnostics?.topLevelKeys || [],
    detectedRootType: input.diagnostics?.detectedRootType || "unknown",
    expectedRootTypes: input.diagnostics?.expectedRootTypes || [],
    rowExtractionPathUsed: input.diagnostics?.rowExtractionPathUsed || "unknown",
    blankRowsSkipped: input.diagnostics?.blankRowsSkipped || 0,
    parseWarnings: input.diagnostics?.parseWarnings || [],
    allowedReviewerDecisions: ALLOWED_REVIEWER_DECISIONS,
    allowedCitationActionTypes: ALLOWED_CITATION_ACTION_TYPES,
    includeCanonicalTitleHints,
    summary: {
      rowsLoaded: rows.length,
      matchedDocuments: perRowResults.filter((row) => row.matchedDocumentFound).length,
      invalidRows: invalidRows.length,
      blankTemplateRows: blankTemplateRows.length,
      actionableReviewedRows: actionableReviewedRows.length,
      supportedActionRows: supportedActionRows.length,
      blockedActionRows: blockedActionRows.length,
      invalidReviewedRows: invalidReviewedRows.length,
      metadataConfirmationCandidates: metadataCandidates.length,
      safeFixCandidates: safeFixCandidates.length,
      stillBlocked: stillBlocked.length,
      zeroRowReason:
        rows.length === 0
          ? (input.diagnostics?.parseWarnings || []).find((warning) =>
              [
                "input_file_empty",
                "template_has_headers_only",
                "no_adjudication_rows_present",
                "unsupported_json_shape",
                "unsupported_csv_header_set",
                "parse_failed_no_rows",
                "rows_filtered_as_blank"
              ].includes(warning)
            ) || "unknown_zero_row_reason"
          : null
    },
    counts_by_outcome: countsByOutcome,
    counts_by_row_state: countsByRowState,
    counts_by_reviewerDecision: countsByDecision,
    counts_by_citationActionType: countsByActionType,
    counts_by_batchKey: countsByBatchKey,
    exportContractWarnings,
    valid_rows: perRowResults.filter((row) => !invalidRows.includes(row)),
    invalid_rows: invalidRows,
    simulated_metadata_confirmation_candidates: metadataCandidates,
    simulated_safe_fix_candidates: safeFixCandidates,
    still_blocked_docs: stillBlocked,
    unresolved_conflicts: unresolvedConflicts,
    reviewer_completion_checklist: reviewerCompletionChecklist,
    debug: {
      rowsMissingBatchKeyCount: rowsMissingBatchKey.length,
      rowsMissingBatchKey: rowsMissingBatchKey.slice(0, 20).map((row) => ({
        sourceRowNumber: row.sourceRowNumber,
        documentId: row.documentId,
        title: row.title,
        rowState: row.rowState,
        reasons: row.reasons
      }))
    },
    per_row_results: perRowResults,
    readOnly: true
  };
  const reportsDir = path.resolve(process.cwd(), "reports");
  const outputPath = path.resolve(reportsDir, reportName);
  if (reportFormat === "markdown") {
    await fs.writeFile(outputPath, toMarkdownReport(report));
  } else if (reportFormat === "both") {
    await fs.writeFile(outputPath, JSON.stringify(report, null, 2));
    const markdownPath = path.resolve(reportsDir, markdownReportName);
    await fs.writeFile(markdownPath, toMarkdownReport(report));
  } else {
    await fs.writeFile(outputPath, JSON.stringify(report, null, 2));
  }
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${outputPath}`);
  if (reportFormat === "both") {
    console.log(`Markdown report written to ${path.resolve(reportsDir, markdownReportName)}`);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
