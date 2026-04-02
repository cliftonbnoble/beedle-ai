import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerLegalEvidencePackets, classifyEvidenceContext } from "../scripts/reviewer-legal-evidence-utils.mjs";

function makeRow(overrides = {}) {
  return {
    documentId: "doc_1",
    title: "Doc 1",
    batchKey: "37.3::ordinance_section::unsafe_37x_structural_block",
    blocked37xFamily: "37.3",
    unresolvedTriageBuckets: "unsafe_37x_structural_block",
    recurringCitationFamily: "37.3",
    blockers: "unresolved_references_above_threshold",
    exactUnresolvedReferences: [
      {
        referenceType: "ordinance_section",
        rawValue: "Ordinance 37.3",
        normalizedValue: "37.3",
        rootCause: "cross_context",
        message: "Rules reference not found"
      }
    ],
    ...overrides
  };
}

function makeSources() {
  return new Map([
    [
      "doc_1",
      {
        title: "Doc 1",
        textBlocks: [
          "The Rent Ordinance section 37.3 is discussed in this paragraph.",
          "No other relevant context."
        ]
      }
    ],
    [
      "doc_2",
      {
        title: "Doc 2",
        textBlocks: ["Under Rule 37.7 in the Rules and Regulations, this applies."]
      }
    ]
  ]);
}

test("deterministic snippet extraction and grouping", () => {
  const rows = [makeRow(), makeRow({ documentId: "doc_2", title: "Doc 2", batchKey: "37.7::ordinance_section::unsafe_37x_structural_block", blocked37xFamily: "37.7", unresolvedTriageBuckets: "unsafe_37x_structural_block", exactUnresolvedReferences: [{ referenceType: "ordinance_section", rawValue: "Ordinance 37.7", normalizedValue: "37.7", rootCause: "cross_context", message: "Ambiguous" }] })];
  const one = buildReviewerLegalEvidencePackets(rows, makeSources());
  const two = buildReviewerLegalEvidencePackets(rows, makeSources());
  assert.deepEqual(one, two);
});

test("unsafe 37.x families remain blocked", () => {
  const report = buildReviewerLegalEvidencePackets([makeRow()], makeSources());
  assert.equal(report.summary.blockedBatchCount, 1);
  assert.ok(report.packets[0].blocked37xFamily.includes("37.3"));
  assert.ok(["keep_blocked", "escalate_to_legal_context_review", "possible_manual_context_fix_but_no_auto_apply"].includes(report.packets[0].recommendedReviewerPosture));
});

test("context classification is conservative and never auto-resolves", () => {
  assert.equal(classifyEvidenceContext("Rent Ordinance section 37.3").contextClass, "likely_ordinance_wording");
  assert.equal(classifyEvidenceContext("Rule 37.7 in the Rules and Regulations").contextClass, "likely_rules_wording");
  assert.equal(classifyEvidenceContext("Rule 37.3 under the Ordinance").contextClass, "mixed_ambiguous_wording");
  const report = buildReviewerLegalEvidencePackets([makeRow()], makeSources());
  assert.notEqual(report.packets[0].recommendedReviewerPosture, "auto_resolve");
});

test("evidence packet generation is read-only transformation", () => {
  const rows = [makeRow()];
  const clone = JSON.parse(JSON.stringify(rows));
  const report = buildReviewerLegalEvidencePackets(rows, makeSources());
  assert.ok(Array.isArray(report.packets));
  assert.deepEqual(rows, clone);
});
