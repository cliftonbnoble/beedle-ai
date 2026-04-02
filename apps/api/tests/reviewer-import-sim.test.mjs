import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerImportSimulation, formatReviewerImportSimulationMarkdown } from "../scripts/reviewer-import-sim-utils.mjs";

function row(overrides = {}) {
  return {
    rowNumber: 2,
    queueOrder: 1,
    documentId: "doc_1",
    title: "Doc 1",
    batchKey: "b1",
    subBucket: "s1",
    priorityLane: "hold_blocked",
    reviewerDecision: "keep_blocked",
    blocked37xFamily: "37.3;37.7",
    doNotAutoApply: true,
    validationState: "valid_keep_blocked",
    validRow: true,
    ...overrides
  };
}

test("deterministic output", () => {
  const input = { rows: [row(), row({ rowNumber: 3, queueOrder: 2, documentId: "doc_2", reviewerDecision: "possible_manual_context_fix_but_no_auto_apply", blocked37xFamily: "", validationState: "valid_possible_manual_context_fix" })] };
  const one = buildReviewerImportSimulation(input);
  const two = buildReviewerImportSimulation(input);
  delete one.generatedAt;
  delete two.generatedAt;
  assert.deepEqual(one, two);
});

test("unsafe blocked 37.x rows remain non-auto-apply", () => {
  const report = buildReviewerImportSimulation({ rows: [row()] });
  assert.equal(report.summary.unsafeBlocked37xRows, 1);
  assert.equal(report.summary.unsafeBlocked37xNonAutoApplyConfirmed, true);
  assert.equal(report.rows[0].unsafeNonAutoApplyOk, true);
  assert.equal(report.rows[0].simulatedImportAction, "keep_blocked");
});

test("manual context fix rows are isolated", () => {
  const report = buildReviewerImportSimulation({
    rows: [
      row({ blocked37xFamily: "", reviewerDecision: "possible_manual_context_fix_but_no_auto_apply", validationState: "valid_possible_manual_context_fix" }),
      row({ rowNumber: 3, queueOrder: 2, documentId: "doc_2", reviewerDecision: "keep_blocked", validationState: "valid_keep_blocked" })
    ]
  });
  assert.equal(report.manualContextFixCandidates.length, 1);
  assert.equal(report.specialAttentionRows.length, 1);
});

test("read-only behavior", () => {
  const input = { rows: [row()] };
  const clone = JSON.parse(JSON.stringify(input));
  const report = buildReviewerImportSimulation(input);
  assert.equal(report.readOnly, true);
  assert.deepEqual(input, clone);
  assert.ok(formatReviewerImportSimulationMarkdown(report).includes("# Reviewer Import Simulation (Read-Only)"));
});
