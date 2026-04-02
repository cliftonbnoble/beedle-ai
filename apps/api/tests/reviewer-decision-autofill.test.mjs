import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerDecisionAutofill } from "../scripts/reviewer-decision-autofill-utils.mjs";

function baseRow(overrides = {}) {
  return {
    queueOrder: 1,
    priorityLane: "review_after",
    batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
    subBucket: "not_found_hold",
    documentId: "doc_1",
    title: "Doc 1",
    blocked37xFamily: "37.3;37.7",
    recommendedReviewerPosture: "keep_blocked",
    recommendedSimulatedDisposition: "split_batch_before_review",
    rootCauseSummary: "not_found",
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

function run(rows) {
  return buildReviewerDecisionAutofill({
    worksheetRows: rows,
    actionQueueSummary: { totalQueueRows: rows.length, splitCoverage: [] },
    generatedAt: "2026-03-08T12:00:00.000Z"
  });
}

test("autofill not_found_hold => keep_blocked", () => {
  const report = run([baseRow({ subBucket: "not_found_hold" })]);
  const row = report.prefilledWorksheet.rows[0];
  assert.equal(row.reviewerDecision, "keep_blocked");
  assert.equal(row.keepBlocked, "TRUE");
});

test("autofill insufficient_context_hold => keep_blocked", () => {
  const report = run([baseRow({ subBucket: "insufficient_context_hold", rootCauseSummary: "insufficient_context" })]);
  const row = report.prefilledWorksheet.rows[0];
  assert.equal(row.reviewerDecision, "keep_blocked");
  assert.equal(row.keepBlocked, "TRUE");
});

test("unsafe 37.3 / 37.7 / 37.9 rows never become unsafe manual-fix/approval", () => {
  const report = run([
    baseRow({ blocked37xFamily: "37.3" }),
    baseRow({ blocked37xFamily: "37.7", documentId: "doc_2", queueOrder: 2 }),
    baseRow({ blocked37xFamily: "37.9", documentId: "doc_3", queueOrder: 3 })
  ]);
  for (const row of report.prefilledWorksheet.rows) {
    assert.equal(row.reviewerDecision, "keep_blocked");
    assert.equal(row.doNotAutoApply, true);
  }
});

test("high-signal safe rows can be prefilled conservatively when allowed", () => {
  const report = run([
    baseRow({
      batchKey: "family:37.2+37.8",
      blocked37xFamily: "",
      subBucket: "",
      recommendedReviewerPosture: "possible_manual_context_fix_but_no_auto_apply",
      contextClass: "likely_ordinance_wording",
      topEvidenceSnippet: "Ordinance Sections: 37.2(r); 37.8(e)(7) with clear ordinance context and enough detail for manual review candidate.",
      documentId: "doc_9"
    })
  ]);
  const row = report.prefilledWorksheet.rows[0];
  assert.equal(row.reviewerDecision, "possible_manual_context_fix_but_no_auto_apply");
  assert.equal(row.possibleManualContextFix, "TRUE");
  assert.equal(row.doNotAutoApply, true);
});

test("exception rows are emitted deterministically", () => {
  const rows = [
    baseRow({
      batchKey: "family:unknown",
      blocked37xFamily: "",
      subBucket: "",
      recommendedReviewerPosture: "possible_manual_context_fix_but_no_auto_apply",
      contextClass: "mixed_ambiguous_wording",
      topEvidenceSnippet: "short"
    })
  ];
  const one = run(rows);
  const two = run(rows);
  delete one.generatedAt;
  delete two.generatedAt;
  assert.deepEqual(one, two);
  assert.equal(one.exceptionRows.length, 1);
  assert.equal(one.exceptionsWorksheet.rows.length, 1);
});

test("output remains read-only", () => {
  const rows = [baseRow()];
  const clone = JSON.parse(JSON.stringify(rows));
  const report = run(rows);
  assert.equal(report.readOnly, true);
  assert.equal(report.prefilledWorksheet.readOnly, true);
  assert.equal(report.exceptionsWorksheet.readOnly, true);
  assert.deepEqual(rows, clone);
});
