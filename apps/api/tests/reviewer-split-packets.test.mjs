import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReviewerSplitPackets } from "../scripts/reviewer-split-packets-utils.mjs";

function simReport() {
  return {
    batches: [
      {
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        docCount: 60,
        blocked37xFamily: ["37.3", "37.7"],
        recommendedSimulatedDisposition: "split_batch_before_review",
        splitReason: "Large mixed batch.",
        splitConfidence: "high",
        proposedSubBuckets: ["insufficient_context_hold", "mixed_ambiguous_escalate"],
        docSimulations: [
          { documentId: "doc_1", title: "Doc 1", simulatedRecommendation: "insufficient_context", contextSignal: "no_useful_context" },
          { documentId: "doc_2", title: "Doc 2", simulatedRecommendation: "manual_context_review_candidate", contextSignal: "likely_ordinance_wording" },
          { documentId: "doc_3", title: "Doc 3", simulatedRecommendation: "escalate", contextSignal: "mixed_ambiguous_wording" }
        ]
      },
      {
        batchKey: "37.7::ordinance_section::unsafe_37x_structural_block",
        docCount: 9,
        blocked37xFamily: ["37.7"],
        recommendedSimulatedDisposition: "possible_manual_context_fix_but_no_auto_apply",
        splitReason: "No split heuristic triggered.",
        splitConfidence: "low",
        proposedSubBuckets: [],
        docSimulations: [{ documentId: "doc_4", title: "Doc 4", simulatedRecommendation: "manual_context_review_candidate", contextSignal: "likely_ordinance_wording" }]
      }
    ]
  };
}

function evidenceReport() {
  return {
    packets: [
      {
        batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
        patternEvidence: [
          {
            representativeSnippets: [
              {
                documentId: "doc_2",
                rawCitation: "Ordinance 37.7",
                normalizedValue: "37.7",
                referenceType: "ordinance_section",
                rootCause: "cross_context",
                localTextSnippet: "Rent Ordinance section 37.7 ...",
                contextClass: "likely_ordinance_wording"
              }
            ]
          }
        ]
      }
    ]
  };
}

function queueRows() {
  return Array.from({ length: 60 }, (_, idx) => ({
    documentId: `doc_${idx + 1}`,
    title: `Doc ${idx + 1}`,
    batchKey: "37.3+37.7::ordinance_section::unsafe_37x_structural_block",
    exactUnresolvedReferences: JSON.stringify([
      {
        referenceType: "ordinance_section",
        rawValue: idx % 2 === 0 ? "Ordinance 37.3" : "Ordinance 37.7",
        normalizedValue: idx % 2 === 0 ? "37.3" : "37.7",
        rootCause: "cross_context",
        message: "manual review"
      }
    ])
  }));
}

test("deterministic sub-bucket generation", () => {
  const one = buildReviewerSplitPackets(simReport(), evidenceReport(), queueRows());
  const two = buildReviewerSplitPackets(simReport(), evidenceReport(), queueRows());
  delete one.generatedAt;
  delete two.generatedAt;
  assert.deepEqual(one, two);
});

test("large 37.3+37.7 batch splits consistently", () => {
  const report = buildReviewerSplitPackets(simReport(), evidenceReport(), queueRows());
  assert.equal(report.splitBatches.length, 1);
  const batch = report.splitBatches[0];
  assert.equal(batch.batchKey, "37.3+37.7::ordinance_section::unsafe_37x_structural_block");
  assert.equal(batch.docCount, 60);
  assert.equal(batch.coveredDocCount, 60);
  assert.equal(batch.uncoveredDocCount, 0);
  assert.equal(batch.duplicateDocCount, 0);
  assert.equal(batch.coverageStatus, "complete");
  assert.equal(
    batch.subBucketCounts.reduce((sum, item) => sum + item.count, 0),
    batch.docCount
  );
  assert.ok(batch.subBucketCounts.some((item) => item.key === "insufficient_context_hold"));
});

test("smaller non-split batches are excluded", () => {
  const report = buildReviewerSplitPackets(simReport(), evidenceReport(), queueRows());
  assert.ok(report.excludedNonSplitBatches.some((batch) => batch.batchKey === "37.7::ordinance_section::unsafe_37x_structural_block"));
});

test("split packet generation remains read-only transformation", () => {
  const sim = simReport();
  const evidence = evidenceReport();
  const queue = queueRows();
  const simClone = JSON.parse(JSON.stringify(sim));
  const evidenceClone = JSON.parse(JSON.stringify(evidence));
  const queueClone = JSON.parse(JSON.stringify(queue));
  const report = buildReviewerSplitPackets(sim, evidence, queue);
  assert.equal(report.readOnly, true);
  assert.deepEqual(sim, simClone);
  assert.deepEqual(evidence, evidenceClone);
  assert.deepEqual(queue, queueClone);
});

test("deterministic fallback bucket assignment prevents dropped docs", () => {
  const report = buildReviewerSplitPackets(simReport(), evidenceReport(), []);
  const batch = report.splitBatches[0];
  assert.equal(batch.docCount, 60);
  assert.equal(batch.coveredDocCount, 60);
  assert.equal(batch.uncoveredDocCount, 0);
  assert.ok(batch.subBucketCounts.some((item) => item.key === "unclassified_hold"));
  assert.ok(batch.syntheticUnmappedDocCount > 0);
});

test("representative evidence present when available", () => {
  const report = buildReviewerSplitPackets(simReport(), evidenceReport(), queueRows());
  const batch = report.splitBatches[0];
  const likelyBucket = batch.subBucketPackets.find((packet) => packet.subBucket === "likely_ordinance_manual_review");
  assert.ok(likelyBucket);
  assert.ok((likelyBucket.representativeCitationContextEvidence || []).length > 0);
});
