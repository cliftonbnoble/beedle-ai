import { test } from "node:test";
import assert from "node:assert/strict";
import { classifyUnresolvedReferenceRisk, isConservativeRemediationCandidate } from "../scripts/staged-remediation-utils.mjs";

test("unresolved triage marks low-risk residue conservatively", () => {
  const detail = {
    unresolvedReferenceCount: 3,
    validReferences: {
      indexCodes: ["13"],
      rulesSections: ["I-1.11"],
      ordinanceSections: ["37.3(a)(1)"]
    },
    missingRulesDetection: false,
    missingOrdinanceDetection: false,
    failedQcRequirements: [],
    criticalExceptionReferences: [],
    referenceIssues: [
      { referenceType: "rules_section", severity: "warning", message: "Unknown rules reference: Rule 37.2" },
      { referenceType: "ordinance_section", severity: "warning", message: "Unknown ordinance reference: Ordinance 37.7" }
    ]
  };
  const triage = classifyUnresolvedReferenceRisk(detail);
  assert.equal(triage.lowRiskResidue, true);
  assert.equal(triage.category, "low_risk_residue");
});

test("unresolved triage rejects risky unresolved sets", () => {
  const detail = {
    unresolvedReferenceCount: 3,
    validReferences: { indexCodes: ["13"], rulesSections: ["I-1.11"], ordinanceSections: ["37.3(a)(1)"] },
    missingRulesDetection: false,
    missingOrdinanceDetection: false,
    failedQcRequirements: [],
    criticalExceptionReferences: [],
    referenceIssues: [{ referenceType: "index_code", severity: "warning", message: "Unknown index code 9999" }]
  };
  const triage = classifyUnresolvedReferenceRisk(detail);
  assert.equal(triage.lowRiskResidue, false);
  assert.equal(triage.category, "risky_unresolved");
});

test("conservative remediation candidate selection is narrow", () => {
  const doc = {
    isLikelyFixture: false,
    extractionConfidence: 0.8,
    approvalReadiness: { blockers: ["metadata_not_confirmed", "unresolved_references_above_threshold"] }
  };
  const detail = {
    unresolvedReferenceCount: 2,
    extractionConfidence: 0.8,
    validReferences: { indexCodes: ["13"], rulesSections: ["I-1.11"], ordinanceSections: ["37.3(a)(1)"] },
    missingRulesDetection: false,
    missingOrdinanceDetection: false,
    failedQcRequirements: [],
    criticalExceptionReferences: [],
    referenceIssues: [{ referenceType: "rules_section", severity: "warning", message: "Unknown rules reference: Rule 37.2" }]
  };
  const result = isConservativeRemediationCandidate(doc, detail);
  assert.equal(result.eligible, true);
});

test("no global QC relaxation: unsupported blockers remain ineligible", () => {
  const doc = {
    isLikelyFixture: false,
    extractionConfidence: 0.9,
    approvalReadiness: { blockers: ["missing_ordinance_detection"] }
  };
  const detail = {
    unresolvedReferenceCount: 1,
    extractionConfidence: 0.9,
    validReferences: { indexCodes: ["13"], rulesSections: ["I-1.11"], ordinanceSections: [] },
    missingRulesDetection: false,
    missingOrdinanceDetection: true,
    failedQcRequirements: ["missing_ordinance_section"],
    criticalExceptionReferences: [],
    referenceIssues: []
  };
  const result = isConservativeRemediationCandidate(doc, detail);
  assert.equal(result.eligible, false);
  assert.equal(result.reason, "unsupported_blockers");
});

