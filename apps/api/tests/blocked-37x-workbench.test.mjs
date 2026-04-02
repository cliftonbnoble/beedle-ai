import { test } from "node:test";
import assert from "node:assert/strict";
import { buildBlocked37xDocView, extractBlockedFamily } from "../scripts/blocked-37x-workbench-utils.mjs";

test("extractBlockedFamily detects only blocked families 37.3/37.7/37.9", () => {
  assert.equal(extractBlockedFamily("Ordinance 37.3"), "37.3");
  assert.equal(extractBlockedFamily("Ordinance 37.7"), "37.7");
  assert.equal(extractBlockedFamily("Ordinance 37.9"), "37.9");
  assert.equal(extractBlockedFamily("Ordinance 37.2"), null);
});

test("blocked 37.3 remains blocked and not safe for auto resolution", () => {
  const row = buildBlocked37xDocView(
    { id: "doc1", title: "Doc 1" },
    {
      referenceIssues: [
        {
          referenceType: "ordinance_section",
          rawValue: "Ordinance 37.3",
          normalizedValue: "ordinance37.3",
          message: "Unknown ordinance reference",
          severity: "warning"
        }
      ]
    }
  );
  assert.equal(row.blocked37xReferences.length, 1);
  assert.equal(row.blocked37xReason, "unsafe_37x_structural_block");
  assert.equal(row.blocked37xSafeToBatchReview, true);
});

test("cross-context 37.7 remains blocked as ambiguous", () => {
  const row = buildBlocked37xDocView(
    { id: "doc2", title: "Doc 2" },
    {
      referenceIssues: [
        {
          referenceType: "rules_section",
          rawValue: "Rule 37.7",
          normalizedValue: "37.7",
          message: "Unknown rules reference",
          severity: "warning"
        }
      ]
    }
  );
  assert.equal(row.blocked37xReason, "cross_context_ambiguous");
  assert.equal(row.blocked37xSafeToBatchReview, false);
});
