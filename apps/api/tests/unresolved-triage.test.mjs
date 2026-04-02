import { test } from "node:test";
import assert from "node:assert/strict";
import { buildBatchGroups, classifyDocUnresolvedTriage, classifyUnresolvedIssueBucket } from "../scripts/unresolved-review-triage-utils.mjs";

test("cross-context citations stay blocked", () => {
  const bucket = classifyUnresolvedIssueBucket({
    referenceType: "rules_section",
    rawValue: "Rule 37.2",
    normalizedValue: "37.2",
    message: "Unknown rules references (manual review): Rule 37.2",
    severity: "warning"
  });
  assert.equal(bucket, "cross_context_ambiguous");
});

test("unsafe 37.3/37.7/37.9 stay structurally blocked", () => {
  const citations = ["37.3", "37.7", "37.9"];
  for (const citation of citations) {
    const bucket = classifyUnresolvedIssueBucket({
      referenceType: "ordinance_section",
      rawValue: `Ordinance ${citation}`,
      normalizedValue: `ordinance${citation}`,
      message: "Unknown ordinance references",
      severity: "warning"
    });
    assert.equal(bucket, "unsafe_37x_structural_block");
  }
});

test("duplicate references are triaged as duplicate_or_redundant_reference", () => {
  const issue = {
    referenceType: "ordinance_section",
    rawValue: "Ordinance 37.2",
    normalizedValue: "ordinance37.2",
    message: "Unknown ordinance references",
    severity: "warning"
  };
  const duplicateCounts = new Map([["ordinance_section::37.2", 2]]);
  const bucket = classifyUnresolvedIssueBucket(issue, { duplicateCounts });
  assert.equal(bucket, "duplicate_or_redundant_reference");
});

test("batch review grouping only groups materially identical patterns", () => {
  const rows = [
    { id: "a", unresolvedBuckets: ["duplicate_or_redundant_reference"], recurringCitationFamilies: ["37.2"] },
    { id: "b", unresolvedBuckets: ["duplicate_or_redundant_reference"], recurringCitationFamilies: ["37.2"] },
    { id: "c", unresolvedBuckets: ["cross_context_ambiguous"], recurringCitationFamilies: ["37.2"] }
  ];
  const groups = buildBatchGroups(rows);
  assert.deepEqual(groups.get("a"), ["b"]);
  assert.deepEqual(groups.get("b"), ["a"]);
  assert.equal(groups.has("c"), false);
});

test("doc triage exposes blocked status when unsafe family exists", () => {
  const triage = classifyDocUnresolvedTriage(
    { id: "doc1" },
    {
      referenceIssues: [
        {
          referenceType: "ordinance_section",
          rawValue: "Ordinance 37.3",
          normalizedValue: "ordinance37.3",
          message: "Unknown ordinance references",
          severity: "warning"
        }
      ]
    }
  );
  assert.equal(triage.status, "blocked");
  assert.ok(triage.unresolvedBuckets.includes("unsafe_37x_structural_block"));
  assert.equal(triage.estimatedReviewerEffort, "high");
});
