import { test } from "node:test";
import assert from "node:assert/strict";
import { buildPrioritizedReviewerBatches } from "../scripts/reviewer-priority-utils.mjs";

function makeRow(overrides = {}) {
  return {
    documentId: "doc_1",
    title: "Doc",
    batchKey: "b1",
    reviewerRiskLevel: "medium",
    estimatedReviewerEffort: "medium",
    unresolvedTriageBuckets: "",
    blocked37xFamily: "",
    recurringCitationFamily: "",
    blockers: "unresolved_references_above_threshold",
    topRecommendedReviewerAction: "Review",
    safeToBatchReview: true,
    unresolvedCount: 2,
    ...overrides
  };
}

test("deterministic batch ranking", () => {
  const rows = [
    makeRow({ documentId: "doc_b", title: "B", batchKey: "k2", estimatedReviewerEffort: "low" }),
    makeRow({ documentId: "doc_a", title: "A", batchKey: "k1", estimatedReviewerEffort: "low" }),
    makeRow({ documentId: "doc_c", title: "C", batchKey: "k3", estimatedReviewerEffort: "medium" })
  ];
  const one = buildPrioritizedReviewerBatches(rows);
  const two = buildPrioritizedReviewerBatches(rows);
  assert.deepEqual(
    one.prioritizedBatches.map((b) => b.batchKey),
    two.prioritizedBatches.map((b) => b.batchKey)
  );
});

test("blocked unsafe 37.x batches rank after safer batches", () => {
  const rows = [
    makeRow({ documentId: "doc_safe", batchKey: "safe", estimatedReviewerEffort: "low", unresolvedTriageBuckets: "duplicate_or_redundant_reference", blocked37xFamily: "" }),
    makeRow({ documentId: "doc_blocked", batchKey: "blocked", estimatedReviewerEffort: "low", unresolvedTriageBuckets: "unsafe_37x_structural_block", blocked37xFamily: "37.3" })
  ];
  const ranked = buildPrioritizedReviewerBatches(rows).prioritizedBatches;
  assert.equal(ranked[0].batchKey, "safe");
  assert.equal(ranked[ranked.length - 1].batchKey, "blocked");
  assert.equal(ranked[ranked.length - 1].priorityBucket, "blocked_legal_adjudication");
});

test("low-effort safe batches rank ahead of ambiguous batches", () => {
  const rows = [
    makeRow({ documentId: "doc_safe", batchKey: "safe_low", estimatedReviewerEffort: "low", unresolvedTriageBuckets: "duplicate_or_redundant_reference" }),
    makeRow({ documentId: "doc_amb", batchKey: "ambiguous", estimatedReviewerEffort: "low", unresolvedTriageBuckets: "cross_context_ambiguous", safeToBatchReview: false })
  ];
  const ranked = buildPrioritizedReviewerBatches(rows).prioritizedBatches;
  assert.equal(ranked[0].batchKey, "safe_low");
  assert.equal(ranked[0].priorityBucket, "review_now");
  assert.equal(ranked[1].batchKey, "ambiguous");
  assert.equal(ranked[1].priorityBucket, "review_later");
});

test("priority report utilities are read-only transformations", () => {
  const source = [makeRow({ documentId: "doc_1", batchKey: "k1" })];
  const clone = JSON.parse(JSON.stringify(source));
  const ranked = buildPrioritizedReviewerBatches(source);
  assert.ok(Array.isArray(ranked.prioritizedBatches));
  assert.deepEqual(source, clone);
});
