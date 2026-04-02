import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerLegalPackets } from "../scripts/reviewer-legal-packets-utils.mjs";

function row(overrides = {}) {
  return {
    documentId: "doc_1",
    title: "Doc 1",
    batchKey: "37.3::ordinance_section::unsafe_37x_structural_block",
    blocked37xFamily: "37.3",
    unresolvedTriageBuckets: "unsafe_37x_structural_block",
    recurringCitationFamily: "37.3",
    blockers: "unresolved_references_above_threshold",
    reviewerRiskLevel: "high",
    estimatedReviewerEffort: "high",
    topRecommendedReviewerAction: "Manual legal review.",
    exactUnresolvedReferences: [
      { referenceType: "ordinance_section", rawValue: "Ordinance 37.3", normalizedValue: "37.3", rootCause: "cross_context", message: "Ambiguous context" }
    ],
    ...overrides
  };
}

test("deterministic legal packet generation", () => {
  const rows = [row({ documentId: "doc_b", title: "B" }), row({ documentId: "doc_a", title: "A" })];
  const one = buildReviewerLegalPackets(rows);
  const two = buildReviewerLegalPackets(rows);
  assert.deepEqual(one, two);
});

test("unsafe 37.x families remain flagged as blocked legal packets", () => {
  const report = buildReviewerLegalPackets([
    row({ batchKey: "k1", blocked37xFamily: "37.3" }),
    row({ documentId: "doc_2", batchKey: "k2", blocked37xFamily: "37.7" }),
    row({ documentId: "doc_3", batchKey: "k3", blocked37xFamily: "37.9" })
  ]);
  const families = report.summary.docsPerBlocked37xFamily.map((item) => item.family);
  assert.ok(families.includes("37.3"));
  assert.ok(families.includes("37.7"));
  assert.ok(families.includes("37.9"));
  assert.ok(report.packets.every((packet) => ["keep_blocked", "escalate_to_legal_context_review", "possible_manual_context_fix_but_no_auto_apply"].includes(packet.recommendedDecisionPosture)));
});

test("aggregate root-cause summaries are stable", () => {
  const report = buildReviewerLegalPackets([
    row({
      batchKey: "k1",
      exactUnresolvedReferences: [
        { referenceType: "ordinance_section", rawValue: "Ordinance 37.3", normalizedValue: "37.3", rootCause: "cross_context", message: "Ambiguous context" },
        { referenceType: "ordinance_section", rawValue: "Ordinance 37.3", normalizedValue: "37.3", rootCause: "cross_context", message: "Ambiguous context" }
      ]
    }),
    row({
      documentId: "doc_2",
      batchKey: "k2",
      exactUnresolvedReferences: [{ referenceType: "ordinance_section", rawValue: "Ordinance 37.9", normalizedValue: "37.9", rootCause: "not_found", message: "Not found" }]
    })
  ]);
  const root = report.summary.topRootCausesAcrossBlockedBatches.map((item) => item.rootCause);
  assert.equal(root[0], "cross_context");
  assert.ok(root.includes("not_found"));
});

test("legal packet generation remains read-only transformation", () => {
  const source = [row({ documentId: "doc_1", batchKey: "k1" })];
  const clone = JSON.parse(JSON.stringify(source));
  const report = buildReviewerLegalPackets(source);
  assert.ok(Array.isArray(report.packets));
  assert.deepEqual(source, clone);
});
