import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildReviewerWorksheetExport,
  formatReviewerWorksheetCsv,
  formatReviewerWorksheetMarkdown
} from "../scripts/reviewer-worksheet-export-utils.mjs";

function actionQueueReport() {
  return {
    summary: {
      splitCoverage: [
        {
          batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
          expectedDocCount: 2,
          queueRowCount: 2,
          coverageStatus: "complete"
        }
      ]
    },
    rows: [
      {
        queueOrder: 2,
        priorityLane: "review_after",
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        subBucket: "insufficient_context_hold",
        documentId: "doc_2",
        title: "Doc 2",
        blocked37xFamily: ["37.3", "37.7"],
        recommendedReviewerPosture: "keep_blocked",
        recommendedSimulatedDisposition: "split_batch_before_review",
        rootCauseSummary: "insufficient_context",
        contextClass: "no_useful_context",
        topEvidenceSnippet: "",
        suggestedDecisionOptions: ["keep_blocked"],
        requiresLegalEscalation: false,
        doNotAutoApply: true
      },
      {
        queueOrder: 1,
        priorityLane: "review_first",
        batchKey: "37.7::ordinance_section::unsafe_37x_structural_block",
        subBucket: null,
        documentId: "doc_1",
        title: "Doc 1",
        blocked37xFamily: ["37.7"],
        recommendedReviewerPosture: "possible_manual_context_fix_but_no_auto_apply",
        recommendedSimulatedDisposition: "possible_manual_context_fix_but_no_auto_apply",
        rootCauseSummary: "cross_context",
        contextClass: "likely_ordinance_wording",
        topEvidenceSnippet: "usable evidence",
        suggestedDecisionOptions: ["possible_manual_context_fix_but_no_auto_apply", "keep_blocked"],
        requiresLegalEscalation: false,
        doNotAutoApply: true
      },
      {
        queueOrder: 3,
        priorityLane: "hold_blocked",
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        subBucket: "not_found_hold",
        documentId: "doc_3",
        title: "Doc 3",
        blocked37xFamily: ["37.3", "37.7"],
        recommendedReviewerPosture: "keep_blocked",
        recommendedSimulatedDisposition: "split_batch_before_review",
        rootCauseSummary: "not_found",
        contextClass: "no_useful_context",
        topEvidenceSnippet: "",
        suggestedDecisionOptions: ["keep_blocked", "escalate_to_legal_context_review"],
        requiresLegalEscalation: true,
        doNotAutoApply: true
      }
    ]
  };
}

function evidenceReport() {
  return {
    packets: [
      {
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        patternEvidence: [
          {
            representativeSnippets: [
              {
                documentId: "doc_2",
                rootCause: "cross_context",
                contextClass: "mixed_ambiguous_wording",
                localTextSnippet: "snippet for doc2"
              }
            ]
          }
        ]
      }
    ]
  };
}

function simReport() {
  return {
    batches: [
      {
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        recommendedSimulatedDisposition: "split_batch_before_review"
      }
    ]
  };
}

test("deterministic worksheet ordering", () => {
  const one = buildReviewerWorksheetExport(actionQueueReport(), evidenceReport(), simReport());
  const two = buildReviewerWorksheetExport(actionQueueReport(), evidenceReport(), simReport());
  delete one.generatedAt;
  delete two.generatedAt;
  assert.deepEqual(one, two);
  assert.equal(one.rows[0].priorityLane, "review_first");
  assert.equal(one.rows[1].priorityLane, "review_after");
  assert.equal(one.rows[2].priorityLane, "hold_blocked");
});

test("row count matches action queue row count", () => {
  const report = buildReviewerWorksheetExport(actionQueueReport(), evidenceReport(), simReport());
  assert.equal(report.summary.worksheetRowCount, 3);
  assert.equal(report.summary.actionQueueRowCount, 3);
  assert.equal(report.summary.rowCountMatchesActionQueue, true);
});

test("split coverage preserved", () => {
  const report = buildReviewerWorksheetExport(actionQueueReport(), evidenceReport(), simReport());
  assert.equal(report.summary.splitCoverage.length, 1);
  assert.equal(report.summary.splitCoverage[0].coverageStatus, "complete");
});

test("no duplicate worksheet rows by documentId+batchKey+subBucket", () => {
  const withDupe = actionQueueReport();
  withDupe.rows.push({ ...withDupe.rows[0] });
  const report = buildReviewerWorksheetExport(withDupe, evidenceReport(), simReport());
  assert.equal(report.rows.length, 3);
});

test("reviewer blank columns present", () => {
  const report = buildReviewerWorksheetExport(actionQueueReport(), evidenceReport(), simReport());
  const row = report.rows[0];
  assert.equal(row.reviewerDecision, "");
  assert.equal(row.reviewerDecisionReason, "");
  assert.equal(row.reviewerCitationContext, "");
  assert.equal(row.reviewerEvidenceUsed, "");
  assert.equal(row.reviewerNotes, "");
  assert.equal(row.escalateToLegal, "");
  assert.equal(row.keepBlocked, "");
  assert.equal(row.possibleManualContextFix, "");
  assert.equal(row.reviewedBy, "");
  assert.equal(row.reviewedAt, "");
});

test("markdown/json/csv shapes are stable", () => {
  const report = buildReviewerWorksheetExport(actionQueueReport(), evidenceReport(), simReport());
  const csv = formatReviewerWorksheetCsv(report);
  const markdown = formatReviewerWorksheetMarkdown(report);
  const header = String(csv).split(/\r?\n/, 1)[0] || "";
  assert.ok(header.includes("queueOrder"));
  assert.ok(header.includes("reviewerDecision"));
  assert.ok(markdown.includes("# Reviewer Worksheet Export"));
  assert.ok(markdown.includes("## Summary"));
  assert.ok(markdown.includes("## Top 20 Items"));
});

test("read-only transformation only", () => {
  const queue = actionQueueReport();
  const evidence = evidenceReport();
  const sim = simReport();
  const queueClone = JSON.parse(JSON.stringify(queue));
  const evidenceClone = JSON.parse(JSON.stringify(evidence));
  const simClone = JSON.parse(JSON.stringify(sim));
  const report = buildReviewerWorksheetExport(queue, evidence, sim);
  assert.equal(report.readOnly, true);
  assert.deepEqual(queue, queueClone);
  assert.deepEqual(evidence, evidenceClone);
  assert.deepEqual(sim, simClone);
});
