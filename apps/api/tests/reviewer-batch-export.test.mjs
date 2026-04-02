import { test } from "node:test";
import assert from "node:assert/strict";
import { applyReviewerExportFilters, buildAdjudicationTemplate, chunkIdsForQuery, stableBatchGrouping } from "../scripts/reviewer-batch-export-utils.mjs";

test("export defaults to real docs only", () => {
  const rows = [
    { documentId: "real1", isRealDoc: true, unresolvedTriageBuckets: [], blocked37xFamily: [] },
    { documentId: "fix1", isRealDoc: false, unresolvedTriageBuckets: [], blocked37xFamily: [] }
  ];
  const filtered = applyReviewerExportFilters(rows, {});
  assert.deepEqual(filtered.map((row) => row.documentId), ["real1"]);
});

test("blocked 37.3/37.7/37.9 remain export-only rows and are filterable", () => {
  const rows = [
    { documentId: "d1", isRealDoc: true, blocked37xFamily: ["37.3"], unresolvedTriageBuckets: ["unsafe_37x_structural_block"] },
    { documentId: "d2", isRealDoc: true, blocked37xFamily: ["37.7"], unresolvedTriageBuckets: ["unsafe_37x_structural_block"] },
    { documentId: "d3", isRealDoc: true, blocked37xFamily: ["37.9"], unresolvedTriageBuckets: ["unsafe_37x_structural_block"] }
  ];
  const only37_7 = applyReviewerExportFilters(rows, { blocked37xFamily: "37.7" });
  assert.deepEqual(only37_7.map((row) => row.documentId), ["d2"]);
});

test("batch grouping is stable and deterministic", () => {
  const rows = [
    { documentId: "d2", batchKey: "b" },
    { documentId: "d1", batchKey: "b" },
    { documentId: "d3", batchKey: "a" }
  ];
  const grouped = stableBatchGrouping(rows);
  assert.deepEqual(grouped, [
    { batchKey: "a", documentIds: ["d3"] },
    { batchKey: "b", documentIds: ["d1", "d2"] }
  ]);
});

test("adjudication template generation is read-only and does not mutate source rows", () => {
  const source = [{ documentId: "d1", title: "Doc 1", batchKey: "b1", reviewerDecision: "should_not_copy" }];
  const clone = JSON.parse(JSON.stringify(source));
  const template = buildAdjudicationTemplate(source);
  assert.equal(template.length, 1);
  assert.equal(template[0].reviewerDecision, "");
  assert.deepEqual(source, clone);
});

test("adjudication template preserves batchKey for grouped rows", () => {
  const template = buildAdjudicationTemplate([
    { documentId: "d1", title: "Doc 1", batchKey: "family:37.3" },
    { documentId: "d2", title: "Doc 2", batchKey: "bucket:cross_context_ambiguous" }
  ]);
  assert.equal(template[0].batchKey, "family:37.3");
  assert.equal(template[1].batchKey, "bucket:cross_context_ambiguous");
});

test("filters produce correct subset combinations", () => {
  const rows = [
    {
      documentId: "d1",
      isRealDoc: true,
      unresolvedTriageBuckets: ["cross_context_ambiguous"],
      blocked37xFamily: ["37.3"],
      estimatedReviewerEffort: "high",
      reviewerRiskLevel: "high",
      safeToBatchReview: false,
      batchKey: "k1"
    },
    {
      documentId: "d2",
      isRealDoc: true,
      unresolvedTriageBuckets: ["duplicate_or_redundant_reference"],
      blocked37xFamily: [],
      estimatedReviewerEffort: "low",
      reviewerRiskLevel: "medium",
      safeToBatchReview: true,
      batchKey: "k2"
    }
  ];
  const filtered = applyReviewerExportFilters(rows, {
    unresolvedTriageBucket: "duplicate_or_redundant_reference",
    estimatedReviewerEffort: "low",
    safeToBatchReviewOnly: true,
    batchKey: "k2"
  });
  assert.deepEqual(filtered.map((row) => row.documentId), ["d2"]);
});

test("large queue id chunking stays below sqlite variable thresholds", () => {
  const ids = Array.from({ length: 1200 }, (_, idx) => `doc_${idx}`);
  const chunks = chunkIdsForQuery(ids, 250);
  assert.equal(chunks.length, 5);
  assert.ok(chunks.every((chunk) => chunk.length <= 250));
  assert.equal(chunks.flat().length, 1200);
});
