import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerDecisionSimulation } from "../scripts/reviewer-decision-sim-utils.mjs";

function packet(overrides = {}) {
  return {
    batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
    docCount: 60,
    blocked37xFamily: ["37.3", "37.7"],
    contextSummary: { ordinanceLike: 5, rulesLike: 8, ambiguous: 21, noUsefulContext: 30 },
    issueAppearanceLikely: "mixed",
    topRootCausesByCount: [{ key: "cross_context", count: 30 }, { key: "not_found", count: 10 }],
    sampleDocs: [
      { documentId: "doc_1", title: "Doc 1" },
      { documentId: "doc_2", title: "Doc 2" }
    ],
    patternEvidence: [
      {
        representativeSnippets: [
          { documentId: "doc_1", contextClass: "mixed_ambiguous_wording" },
          { documentId: "doc_2", contextClass: "no_useful_context" }
        ]
      }
    ],
    ...overrides
  };
}

test("deterministic batch recommendation output", () => {
  const input = { packets: [packet(), packet({ batchKey: "37.7::ordinance_section::unsafe_37x_structural_block", docCount: 9, blocked37xFamily: ["37.7"], issueAppearanceLikely: "ordinance citation ambiguity", contextSummary: { ordinanceLike: 12, rulesLike: 2, ambiguous: 3, noUsefulContext: 1 } })] };
  const one = buildReviewerDecisionSimulation(input);
  const two = buildReviewerDecisionSimulation(input);
  delete one.generatedAt;
  delete two.generatedAt;
  assert.deepEqual(one, two);
});

test("conservative handling of mixed/unsafe 37.x batches", () => {
  const report = buildReviewerDecisionSimulation({ packets: [packet()] });
  assert.equal(report.batches[0].recommendedSimulatedDisposition, "keep_blocked");
  assert.equal(report.batches[0].splitRecommended, false);
  assert.equal(report.batches[0].splitConfidence, "high");
  assert.ok(report.batches[0].splitHeuristicReasons.includes("unsafe_37x_conservative_default"));
});

test("split recommendation for large mixed batches is stable", () => {
  const report = buildReviewerDecisionSimulation({ packets: [packet()] });
  assert.equal(report.large3737SplitRecommendation?.shouldSplit, false);
  assert.ok(Array.isArray(report.large3737SplitRecommendation?.proposedSubBuckets));
  assert.equal(report.large3737SplitRecommendation?.proposedSubBuckets.length, 0);
});

test("small strong unsafe 37.7 batch defaults keep_blocked", () => {
  const report = buildReviewerDecisionSimulation({
    packets: [
      packet({
        batchKey: "37.7::ordinance_section::unsafe_37x_structural_block",
        docCount: 4,
        blocked37xFamily: ["37.7"],
        issueAppearanceLikely: "ordinance citation ambiguity",
        contextSummary: { ordinanceLike: 8, rulesLike: 0, ambiguous: 1, noUsefulContext: 0 },
        patternEvidence: [{ representativeSnippets: [{ documentId: "doc_1", contextClass: "likely_ordinance_wording" }] }]
      })
    ]
  });
  assert.equal(report.batches[0].recommendedSimulatedDisposition, "keep_blocked");
  assert.equal(report.batches[0].splitRecommended, false);
});

test("safe non-unsafe family can remain manual review candidate", () => {
  const report = buildReviewerDecisionSimulation({
    packets: [
      packet({
        batchKey: "family:37.2+37.8",
        docCount: 4,
        blocked37xFamily: [],
        issueAppearanceLikely: "ordinance citation ambiguity",
        contextSummary: { ordinanceLike: 8, rulesLike: 0, ambiguous: 1, noUsefulContext: 0 },
        patternEvidence: [{ representativeSnippets: [{ documentId: "doc_1", contextClass: "likely_ordinance_wording" }] }]
      })
    ]
  });
  assert.equal(report.batches[0].recommendedSimulatedDisposition, "possible_manual_context_fix_but_no_auto_apply");
});

test("doc-level simulated decisions never imply auto-apply", () => {
  const report = buildReviewerDecisionSimulation({ packets: [packet()] });
  const allowed = new Set(["keep_blocked", "manual_context_review_candidate", "escalate", "insufficient_context"]);
  for (const doc of report.batches[0].docSimulations) {
    assert.ok(allowed.has(doc.simulatedRecommendation));
    assert.notEqual(doc.simulatedRecommendation, "auto_apply");
  }
});

test("proposed sub-buckets are deterministic", () => {
  const report = buildReviewerDecisionSimulation({ packets: [packet()] });
  const first = report.batches[0].proposedSubBuckets;
  const second = buildReviewerDecisionSimulation({ packets: [packet()] }).batches[0].proposedSubBuckets;
  assert.deepEqual(first, second);
});

test("report generation remains read-only transformation", () => {
  const source = { packets: [packet()] };
  const clone = JSON.parse(JSON.stringify(source));
  const report = buildReviewerDecisionSimulation(source);
  assert.equal(report.readOnly, true);
  assert.deepEqual(source, clone);
});
