import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerActionQueue } from "../scripts/reviewer-action-queue-utils.mjs";

function splitReport() {
  return {
    splitBatches: [
      {
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        docCount: 4,
        blocked37xFamily: ["37.3", "37.7"],
        subBucketPackets: [
          {
            subBucket: "likely_ordinance_manual_review",
            recommendedReviewerPosture: "possible_manual_context_fix_but_no_auto_apply",
            doNotAutoApply: true,
            docs: [
              { documentId: "doc_1", title: "Doc 1", contextSignal: "likely_ordinance_wording", simulatedRecommendation: "manual_context_review_candidate" }
            ]
          },
          {
            subBucket: "insufficient_context_hold",
            recommendedReviewerPosture: "keep_blocked",
            doNotAutoApply: true,
            docs: [
              { documentId: "doc_2", title: "Doc 2", contextSignal: "no_useful_context", simulatedRecommendation: "insufficient_context" }
            ]
          },
          {
            subBucket: "not_found_hold",
            recommendedReviewerPosture: "keep_blocked",
            doNotAutoApply: true,
            docs: [
              { documentId: "doc_3", title: "Doc 3", contextSignal: "no_useful_context", simulatedRecommendation: "keep_blocked" },
              { documentId: "doc_4", title: "Doc 4", contextSignal: "no_useful_context", simulatedRecommendation: "keep_blocked" }
            ]
          }
        ]
      }
    ]
  };
}

function decisionReport() {
  return {
    batches: [
      {
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        recommendedSimulatedDisposition: "split_batch_before_review",
        blocked37xFamily: ["37.3", "37.7"]
      },
      {
        batchKey: "37.7::ordinance_section::unsafe_37x_structural_block",
        recommendedSimulatedDisposition: "possible_manual_context_fix_but_no_auto_apply",
        blocked37xFamily: ["37.7"],
        docSimulations: [{ documentId: "doc_9", title: "Doc 9", contextSignal: "likely_ordinance_wording" }]
      }
    ]
  };
}

function evidenceReport() {
  return {
    packets: [
      {
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        reviewerNotesTemplate: ["note A", "note B"],
        blocked37xFamily: ["37.3", "37.7"],
        patternEvidence: [
          {
            representativeSnippets: [
              {
                documentId: "doc_1",
                rootCause: "cross_context",
                contextClass: "likely_ordinance_wording",
                localTextSnippet: "ordinance-like context",
                rawCitation: "Ordinance 37.7"
              },
              {
                documentId: "doc_3",
                rootCause: "not_found",
                contextClass: "no_useful_context",
                localTextSnippet: "",
                rawCitation: "Ordinance 37.3"
              }
            ]
          }
        ]
      },
      {
        batchKey: "37.7::ordinance_section::unsafe_37x_structural_block",
        reviewerNotesTemplate: ["note C"],
        blocked37xFamily: ["37.7"],
        patternEvidence: [
          {
            representativeSnippets: [
              {
                documentId: "doc_9",
                rootCause: "cross_context",
                contextClass: "likely_ordinance_wording",
                localTextSnippet: "usable ordinance evidence",
                rawCitation: "Ordinance 37.7"
              }
            ]
          }
        ]
      }
    ]
  };
}

test("deterministic ordering", () => {
  const one = buildReviewerActionQueue(splitReport(), decisionReport(), evidenceReport());
  const two = buildReviewerActionQueue(splitReport(), decisionReport(), evidenceReport());
  delete one.generatedAt;
  delete two.generatedAt;
  assert.deepEqual(one, two);
});

test("split sub-bucket rows included", () => {
  const report = buildReviewerActionQueue(splitReport(), decisionReport(), evidenceReport());
  const splitRows = report.rows.filter((row) => row.batchKey === "37.3+37.7::ordinance_section::unsafe_37x_structural_block");
  assert.equal(splitRows.length, 4);
  assert.ok(splitRows.some((row) => row.subBucket === "insufficient_context_hold"));
  assert.ok(splitRows.some((row) => row.subBucket === "not_found_hold"));
});

test("full queue coverage of split packets", () => {
  const report = buildReviewerActionQueue(splitReport(), decisionReport(), evidenceReport());
  const coverage = report.summary.splitCoverage.find((item) => item.batchKey === "37.3+37.7::ordinance_section::unsafe_37x_structural_block");
  assert.ok(coverage);
  assert.equal(coverage.expectedDocCount, 4);
  assert.equal(coverage.queueRowCount, 4);
  assert.equal(coverage.coverageStatus, "complete");
});

test("no duplicate doc rows in same queue export", () => {
  const report = buildReviewerActionQueue(splitReport(), decisionReport(), evidenceReport());
  const keys = report.rows.map((row) => `${row.batchKey}|${row.subBucket || ""}|${row.documentId}`);
  assert.equal(keys.length, new Set(keys).size);
});

test("evidence attached when available", () => {
  const report = buildReviewerActionQueue(splitReport(), decisionReport(), evidenceReport());
  const row = report.rows.find((item) => item.documentId === "doc_1");
  assert.ok(row);
  assert.equal(row.contextClass, "likely_ordinance_wording");
  assert.equal(row.topEvidenceSnippet, "ordinance-like context");
});

test("unsafe 37.x rows are forced to conservative keep_blocked posture", () => {
  const report = buildReviewerActionQueue(splitReport(), decisionReport(), evidenceReport());
  const unsafeRows = report.rows.filter((row) => (row.blocked37xFamily || []).includes("37.7"));
  assert.ok(unsafeRows.length > 0);
  for (const row of unsafeRows) {
    assert.equal(row.recommendedReviewerPosture, "keep_blocked");
    assert.equal(row.recommendedSimulatedDisposition, "keep_blocked");
    assert.equal(row.doNotAutoApply, true);
  }
});

test("read-only transformation only", () => {
  const split = splitReport();
  const decision = decisionReport();
  const evidence = evidenceReport();
  const splitClone = JSON.parse(JSON.stringify(split));
  const decisionClone = JSON.parse(JSON.stringify(decision));
  const evidenceClone = JSON.parse(JSON.stringify(evidence));
  const report = buildReviewerActionQueue(split, decision, evidence);
  assert.equal(report.readOnly, true);
  assert.deepEqual(split, splitClone);
  assert.deepEqual(decision, decisionClone);
  assert.deepEqual(evidence, evidenceClone);
});
