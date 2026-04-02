import test from "node:test";
import assert from "node:assert/strict";
import { buildR53PolicyLock } from "../scripts/retrieval-r53-frontier-policy-lock-report.mjs";

test("R53 excludes frozen families and similarly risky monoculture fallback candidates", () => {
  const r48 = {
    safeCandidatesEvaluated: 3,
    candidateRows: [
      {
        documentId: "doc_a",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
        chunkTypeProfile: [{ key: "analysis_reasoning", count: 12 }],
        sectionLabelProfile: [{ key: "body", count: 12 }]
      },
      {
        documentId: "doc_b",
        documentFamilyLabel: "other_family",
        chunkTypeProfile: [{ key: "analysis_reasoning", count: 8 }, { key: "procedural_history", count: 4 }],
        sectionLabelProfile: [{ key: "analysis", count: 8 }, { key: "procedural", count: 4 }]
      },
      {
        documentId: "doc_c",
        documentFamilyLabel: "low_signal_heavy::short::analysis_reasoning+holding_disposition",
        chunkTypeProfile: [{ key: "analysis_reasoning", count: 6 }],
        sectionLabelProfile: [{ key: "body", count: 6 }]
      }
    ]
  };

  const r49 = {
    frozenFamilies: [{ familyLabel: "low_signal_heavy::short::analysis_reasoning+holding_disposition" }]
  };

  const r52 = {
    frozenFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
    queryLevelRegressionBreakdown: [
      { queryId: "citation_rule_direct", qualityDelta: -4.33 },
      { queryId: "citation_ordinance_direct", qualityDelta: -4.33 }
    ]
  };

  const out = buildR53PolicyLock({ r48Report: r48, r49Report: r49, r52Report: r52 });

  assert.equal(out.newlyExcludedCandidateCount, 2);
  assert.deepEqual(out.newlyExcludedCandidateIds, ["doc_a", "doc_c"]);
  assert.deepEqual(out.remainingEligibleCandidateIds, ["doc_b"]);
  assert.ok(out.frozenFamilies.includes("low_signal_absent::medium::analysis_reasoning+none"));
  assert.ok(out.frozenFamilies.includes("low_signal_heavy::short::analysis_reasoning+holding_disposition"));
  assert.ok(out.guardrailHitCounts.excludedByR53Policy >= 2);
});

test("R53 recommends model/ranking work when no candidates remain", () => {
  const r48 = {
    safeCandidatesEvaluated: 1,
    candidateRows: [
      {
        documentId: "doc_x",
        documentFamilyLabel: "family_x",
        chunkTypeProfile: [{ key: "analysis_reasoning", count: 10 }],
        sectionLabelProfile: [{ key: "body", count: 10 }]
      }
    ]
  };
  const r49 = { frozenFamilies: [{ familyLabel: "family_x" }] };
  const r52 = { frozenFamilyLabel: "family_x", queryLevelRegressionBreakdown: [] };

  const out = buildR53PolicyLock({ r48Report: r48, r49Report: r49, r52Report: r52 });
  assert.equal(out.remainingEligibleCandidateCount, 0);
  assert.equal(out.recommendedNextStep, "no_remaining_safe_candidates_model_or_ranking_work_required_before_new_activations");
});
