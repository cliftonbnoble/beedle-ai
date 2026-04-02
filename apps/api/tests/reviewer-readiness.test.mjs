import { test } from "node:test";
import assert from "node:assert/strict";
import {
  classifyReviewerReadinessCandidate,
  isSafeForMetadataAutoConfirmation,
  splitReviewerReadinessDocs
} from "../scripts/reviewer-readiness-utils.mjs";

test("safe confirmation-only candidate is classified and eligible for auto confirmation", () => {
  const doc = {
    id: "doc_real_1",
    isLikelyFixture: false,
    reviewerReady: true,
    reviewerReadyReasons: ["metadata_confirmation_only_blocker"],
    metadataConfirmationWouldUnlock: true,
    unresolvedBlockersAfterConfirmation: [],
    approvalReadiness: { blockers: ["metadata_not_confirmed"] }
  };
  const detail = {
    validReferences: {
      indexCodes: ["13"],
      rulesSections: ["I-1.11"],
      ordinanceSections: ["37.2"]
    },
    criticalExceptionReferences: [],
    unresolvedBlockersAfterConfirmation: [],
    referenceIssues: []
  };
  assert.equal(classifyReviewerReadinessCandidate(doc), "confirmation_only");
  assert.equal(isSafeForMetadataAutoConfirmation(doc, detail), true);
});

test("candidate requiring one manual citation fix is classified but not auto-confirmed", () => {
  const doc = {
    id: "doc_real_2",
    isLikelyFixture: false,
    reviewerReady: true,
    reviewerReadyReasons: ["confirmation_plus_one_manual_citation_fix"],
    metadataConfirmationWouldUnlock: false,
    unresolvedBlockersAfterConfirmation: ["unresolved_references_above_threshold"],
    approvalReadiness: { blockers: ["metadata_not_confirmed", "unresolved_references_above_threshold"] }
  };
  assert.equal(classifyReviewerReadinessCandidate(doc), "confirmation_plus_one_manual_fix");
  assert.equal(isSafeForMetadataAutoConfirmation(doc, { validReferences: {} }), false);
});

test("structurally blocked doc remains blocked when unsafe 37.x unresolved exists", () => {
  const doc = {
    id: "doc_real_3",
    isLikelyFixture: false,
    reviewerReady: false,
    reviewerReadyReasons: ["unsafe_37x_unresolved_present"],
    metadataConfirmationWouldUnlock: false,
    unresolvedBlockersAfterConfirmation: ["unresolved_references_above_threshold"],
    approvalReadiness: { blockers: ["metadata_not_confirmed", "unresolved_references_above_threshold"] }
  };
  const detail = {
    validReferences: {
      indexCodes: ["13"],
      rulesSections: ["I-1.11"],
      ordinanceSections: ["37.2"]
    },
    criticalExceptionReferences: [],
    unresolvedBlockersAfterConfirmation: ["unresolved_references_above_threshold"],
    referenceIssues: [{ referenceType: "ordinance_section", normalizedValue: "ordinance37.3", message: "Unknown ordinance reference" }]
  };
  assert.equal(classifyReviewerReadinessCandidate(doc), "structurally_blocked");
  assert.equal(isSafeForMetadataAutoConfirmation(doc, detail), false);
});

test("split report keeps fixture docs out of real confirmation-only counts", () => {
  const docs = [
    {
      id: "doc_real",
      isLikelyFixture: false,
      reviewerReady: true,
      reviewerReadyReasons: ["metadata_confirmation_only_blocker"],
      metadataConfirmationWouldUnlock: true,
      unresolvedBlockersAfterConfirmation: [],
      approvalReadiness: { blockers: ["metadata_not_confirmed"] }
    },
    {
      id: "doc_fixture",
      isLikelyFixture: true,
      reviewerReady: true,
      reviewerReadyReasons: ["metadata_confirmation_only_blocker"],
      metadataConfirmationWouldUnlock: true,
      unresolvedBlockersAfterConfirmation: [],
      approvalReadiness: { blockers: ["metadata_not_confirmed"] }
    }
  ];
  const split = splitReviewerReadinessDocs(docs);
  const realOnlyCount = split.confirmation_only_candidates.filter((doc) => !doc.isLikelyFixture).length;
  assert.equal(split.confirmation_only_candidates.length, 2);
  assert.equal(realOnlyCount, 1);
});
