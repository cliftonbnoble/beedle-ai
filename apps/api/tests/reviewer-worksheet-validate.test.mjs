import { test } from "node:test";
import assert from "node:assert/strict";
import {
  validateReviewerWorksheetRows,
  formatReviewerWorksheetValidationMarkdown
} from "../scripts/reviewer-worksheet-validate-utils.mjs";

function baseRow(overrides = {}) {
  return {
    rowNumber: 2,
    queueOrder: 1,
    priorityLane: "review_first",
    batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
    subBucket: "insufficient_context_hold",
    documentId: "doc_1",
    title: "Doc 1",
    blocked37xFamily: "37.3;37.7",
    recommendedReviewerPosture: "keep_blocked",
    recommendedSimulatedDisposition: "split_batch_before_review",
    rootCauseSummary: "insufficient_context",
    contextClass: "no_useful_context",
    topEvidenceSnippet: "",
    suggestedDecisionOptions: "keep_blocked;escalate_to_legal_context_review",
    requiresLegalEscalation: false,
    doNotAutoApply: true,
    reviewerDecision: "",
    reviewerDecisionReason: "",
    reviewerCitationContext: "",
    reviewerEvidenceUsed: "",
    reviewerNotes: "",
    escalateToLegal: "",
    keepBlocked: "",
    possibleManualContextFix: "",
    reviewedBy: "",
    reviewedAt: "",
    ...overrides
  };
}

test("blank worksheet rows remain valid", () => {
  const report = validateReviewerWorksheetRows([baseRow()], true);
  assert.equal(report.summary.blankRows, 1);
  assert.equal(report.rows[0].validationState, "blank_unreviewed");
});

test("filled keep_blocked rows validate", () => {
  const row = baseRow({ reviewerDecision: "keep_blocked", keepBlocked: "true", reviewerDecisionReason: "still unsafe" });
  const report = validateReviewerWorksheetRows([row], true);
  assert.equal(report.rows[0].validationState, "valid_keep_blocked");
});

test("filled escalate rows validate", () => {
  const row = baseRow({ reviewerDecision: "escalate_to_legal_context_review", escalateToLegal: "true", reviewerDecisionReason: "needs legal" });
  const report = validateReviewerWorksheetRows([row], true);
  assert.equal(report.rows[0].validationState, "valid_escalate_to_legal_context_review");
});

test("manual context fix requires reason/context/evidence", () => {
  const row = baseRow({ blocked37xFamily: "", reviewerDecision: "possible_manual_context_fix_but_no_auto_apply", possibleManualContextFix: "true" });
  const report = validateReviewerWorksheetRows([row], true);
  assert.equal(report.rows[0].validationState, "invalid_missing_required_reason");

  const row2 = baseRow({
    blocked37xFamily: "",
    reviewerDecision: "possible_manual_context_fix_but_no_auto_apply",
    possibleManualContextFix: "true",
    reviewerDecisionReason: "supported",
    reviewerCitationContext: "ordinance context",
    reviewerEvidenceUsed: "snippet"
  });
  const report2 = validateReviewerWorksheetRows([row2], true);
  assert.equal(report2.rows[0].validationState, "valid_possible_manual_context_fix");
});

test("conflicting flags are invalid", () => {
  const row = baseRow({ reviewerDecision: "keep_blocked", keepBlocked: "true", escalateToLegal: "true" });
  const report = validateReviewerWorksheetRows([row], true);
  assert.equal(report.rows[0].validationState, "invalid_conflicting_reviewer_flags");
});

test("blocked 37.3/37.7/37.9 cannot become unsafe manual context fix", () => {
  const row = baseRow({
    reviewerDecision: "possible_manual_context_fix_but_no_auto_apply",
    possibleManualContextFix: "true",
    reviewerDecisionReason: "try fix",
    reviewerCitationContext: "context",
    reviewerEvidenceUsed: "evidence"
  });
  const report = validateReviewerWorksheetRows([row], true);
  assert.equal(report.rows[0].validationState, "invalid_illegal_decision_for_blocked37x");
});

test("deterministic output", () => {
  const rows = [
    baseRow(),
    baseRow({ rowNumber: 3, documentId: "doc_2", reviewerDecision: "keep_blocked", keepBlocked: "true" })
  ];
  const one = validateReviewerWorksheetRows(rows, true);
  const two = validateReviewerWorksheetRows(rows, true);
  assert.deepEqual(one, two);
});

test("read-only transformation only", () => {
  const rows = [baseRow()];
  const clone = JSON.parse(JSON.stringify(rows));
  const report = validateReviewerWorksheetRows(rows, true);
  assert.deepEqual(rows, clone);
  assert.equal(report.rows.length, 1);
  assert.ok(formatReviewerWorksheetValidationMarkdown(report).includes("# Reviewer Worksheet Validation"));
});
