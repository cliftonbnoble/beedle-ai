import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerDecisionCompare, formatReviewerDecisionCompareMarkdown } from "../scripts/reviewer-decision-compare-utils.mjs";

function validateReport(rows) {
  return { rows };
}

function row(overrides = {}) {
  return {
    rowNumber: 2,
    queueOrder: 1,
    documentId: "doc_1",
    title: "Doc 1",
    batchKey: "b1",
    subBucket: "s1",
    priorityLane: "review_first",
    reviewerDecision: "keep_blocked",
    recommendedReviewerPosture: "keep_blocked",
    recommendedSimulatedDisposition: "keep_blocked",
    blocked37xFamily: "37.3;37.7",
    doNotAutoApply: true,
    requiresLegalEscalation: false,
    readyForDryRunComparison: true,
    ...overrides
  };
}

function queueReport() {
  return {
    rows: [
      {
        queueOrder: 1,
        documentId: "doc_1",
        title: "Doc 1",
        batchKey: "b1",
        subBucket: "s1",
        recommendedReviewerPosture: "keep_blocked",
        recommendedSimulatedDisposition: "keep_blocked",
        blocked37xFamily: ["37.3", "37.7"],
        requiresLegalEscalation: false
      }
    ]
  };
}

function simReport() {
  return {
    batches: [{ batchKey: "b1", recommendedSimulatedDisposition: "keep_blocked" }]
  };
}

test("matching keep_blocked row compares correctly", () => {
  const report = buildReviewerDecisionCompare(validateReport([row()]), queueReport(), simReport());
  assert.equal(report.rows.length, 1);
  assert.equal(report.rows[0].comparisonOutcome, "conservative_policy_match");
  assert.equal(report.rows[0].conservativePolicyMatch, true);
  assert.equal(report.summary.rowsStillBlocked, 1);
  assert.equal(report.summary.rowsConservativePolicyMatches, 1);
});

test("matching escalate row compares correctly", () => {
  const r = row({ reviewerDecision: "escalate_to_legal_context_review", recommendedReviewerPosture: "escalate_to_legal_context_review", recommendedSimulatedDisposition: "escalate_to_legal_context_review", blocked37xFamily: "" });
  const report = buildReviewerDecisionCompare(validateReport([r]), queueReport(), simReport());
  assert.equal(report.rows[0].comparisonOutcome, "requires_legal_attention");
  assert.equal(report.summary.rowsEscalatedToLegal, 1);
});

test("manual context fix candidate compares correctly", () => {
  const r = row({ reviewerDecision: "possible_manual_context_fix_but_no_auto_apply", recommendedReviewerPosture: "possible_manual_context_fix_but_no_auto_apply", recommendedSimulatedDisposition: "possible_manual_context_fix_but_no_auto_apply", blocked37xFamily: "" });
  const report = buildReviewerDecisionCompare(validateReport([r]), queueReport(), simReport());
  assert.equal(report.rows[0].comparisonOutcome, "manual_context_fix_candidate");
  assert.equal(report.summary.rowsMarkedManualContextFix, 1);
});

test("more conservative reviewer decisions are classified correctly", () => {
  const r = row({ reviewerDecision: "keep_blocked", recommendedReviewerPosture: "possible_manual_context_fix_but_no_auto_apply", recommendedSimulatedDisposition: "possible_manual_context_fix_but_no_auto_apply", blocked37xFamily: "" });
  const report = buildReviewerDecisionCompare(validateReport([r]), queueReport(), simReport());
  assert.equal(report.rows[0].moreConservativeThanRecommendation, true);
});

test("less conservative reviewer decisions are classified correctly", () => {
  const r = row({ reviewerDecision: "possible_manual_context_fix_but_no_auto_apply", recommendedReviewerPosture: "keep_blocked", recommendedSimulatedDisposition: "keep_blocked", blocked37xFamily: "" });
  const report = buildReviewerDecisionCompare(validateReport([r]), queueReport(), simReport());
  assert.equal(report.rows[0].lessConservativeThanRecommendation, true);
  assert.equal(report.summary.rowsLessConservativeThanRecommendation, 1);
  assert.equal(report.summary.rowsTrueDivergencesNeedingHumanAttention, 1);
});

test("blocked 37.3/37.7/37.9 rows always remain doNotAutoApply", () => {
  const r = row({ reviewerDecision: "keep_blocked", blocked37xFamily: "37.3;37.9", doNotAutoApply: true });
  const report = buildReviewerDecisionCompare(validateReport([r]), queueReport(), simReport());
  assert.equal(report.rows[0].doNotAutoApply, true);
  assert.ok(report.rows[0].blocked37xFamily.includes("37.3"));
});

test("non-unsafe keep_blocked uses standard blocked outcome", () => {
  const r = row({ batchKey: "b2", subBucket: "s2", blocked37xFamily: "", reviewerDecision: "keep_blocked" });
  const report = buildReviewerDecisionCompare(validateReport([r]), queueReport(), simReport());
  assert.equal(report.rows[0].comparisonOutcome, "blocked_remains_blocked");
});

test("deterministic output", () => {
  const rows = [row({ rowNumber: 3, documentId: "doc_b", batchKey: "b2", subBucket: "s2" }), row({ rowNumber: 2, documentId: "doc_a", batchKey: "b1", subBucket: "s1" })];
  const one = buildReviewerDecisionCompare(validateReport(rows), queueReport(), simReport());
  const two = buildReviewerDecisionCompare(validateReport(rows), queueReport(), simReport());
  delete one.generatedAt;
  delete two.generatedAt;
  assert.deepEqual(one, two);
});

test("read-only transformation only", () => {
  const input = validateReport([row()]);
  const inputClone = JSON.parse(JSON.stringify(input));
  const report = buildReviewerDecisionCompare(input, queueReport(), simReport());
  assert.equal(report.rows.length, 1);
  assert.equal(report.rows[0].doNotAutoApply, true);
  assert.deepEqual(input, inputClone);
  assert.ok(formatReviewerDecisionCompareMarkdown(report).includes("# Reviewer Decision Compare"));
});
