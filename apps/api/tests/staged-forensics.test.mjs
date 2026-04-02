import { test } from "node:test";
import assert from "node:assert/strict";
import { classifyDocForensics, classifyUnresolvedIssue } from "../scripts/staged-blocker-forensics-utils.mjs";

test("unresolved root-cause classification handles duplicates and malformed", () => {
  const context = {
    validSets: {
      index_code: new Set(),
      rules_section: new Set(["1.11"]),
      ordinance_section: new Set(["37.3(a)(1)"])
    },
    issueCounts: new Map([
      ["rules_section::1.11", 2],
      ["ordinance_section::37.3(a)(2)", 1]
    ])
  };
  const duplicate = classifyUnresolvedIssue({ referenceType: "rules_section", normalizedValue: "1.11", message: "Unknown" }, context);
  const malformed = classifyUnresolvedIssue(
    { referenceType: "ordinance_section", normalizedValue: "37.3(a)(2)", message: "Malformed citation token" },
    context
  );
  assert.equal(duplicate, "duplicate");
  assert.equal(malformed, "malformed");
});

test("reviewer_unlockable classification requires conservative preconditions", () => {
  const doc = {
    approvalReadiness: { blockers: ["metadata_not_confirmed", "unresolved_references_above_threshold"] }
  };
  const detail = {
    unresolvedReferenceCount: 2,
    missingRulesDetection: false,
    missingOrdinanceDetection: false,
    failedQcRequirements: [],
    criticalExceptionReferences: [],
    extractionWarnings: ["Index codes inferred from validated references: 13"],
    validReferences: { indexCodes: ["13"], rulesSections: ["I-1.11"], ordinanceSections: ["37.3(a)(1)"] },
    referenceIssues: [{ referenceType: "rules_section", normalizedValue: "37.2", message: "Unknown rules reference", severity: "warning" }]
  };
  const forensic = classifyDocForensics(doc, detail);
  assert.equal(forensic.reviewerUnlockable, true);
  assert.equal(forensic.safeAfterManualConfirmation, true);
});

test("no unsafe auto-promotion classification: missing ordinance stays structurally blocked", () => {
  const doc = {
    approvalReadiness: { blockers: ["qc_gate_not_passed", "metadata_not_confirmed", "missing_ordinance_detection"] }
  };
  const detail = {
    unresolvedReferenceCount: 1,
    missingRulesDetection: false,
    missingOrdinanceDetection: true,
    failedQcRequirements: ["missing_ordinance_section"],
    criticalExceptionReferences: [],
    validReferences: { indexCodes: ["13"], rulesSections: ["I-1.11"], ordinanceSections: [] },
    referenceIssues: [{ referenceType: "ordinance_section", normalizedValue: "37.7", message: "Unknown ordinance reference", severity: "warning" }]
  };
  const forensic = classifyDocForensics(doc, detail);
  assert.equal(forensic.reviewerUnlockable, false);
  assert.equal(forensic.safeAfterManualConfirmation, false);
  assert.equal(forensic.blockerCategory, "missing_required_detection");
});

