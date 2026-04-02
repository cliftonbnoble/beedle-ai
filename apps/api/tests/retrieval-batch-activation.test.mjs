import test from "node:test";
import assert from "node:assert/strict";
import {
  buildBatchActivationArtifacts,
  compareLiveQa,
  validateBatchActivationOutcome
} from "../scripts/retrieval-batch-activation-utils.mjs";

function mkPreview(documentId, title, chunkIds) {
  return {
    document: {
      documentId,
      title,
      sourceLink: `https://example.test/${documentId}`
    },
    chunks: (chunkIds || []).map((chunkId, idx) => ({
      chunkId,
      chunkType: idx % 2 === 0 ? "analysis_reasoning" : "findings",
      retrievalPriority: "medium",
      hasCanonicalReferenceAlignment: true,
      citationAnchorStart: `${documentId}#p${idx + 1}`,
      citationAnchorEnd: `${documentId}#p${idx + 2}`,
      sourceText: `chunk ${chunkId} text`
    }))
  };
}

test("buildBatchActivationArtifacts includes only manifest docs and is deterministic", () => {
  const previews = [
    mkPreview("doc_a", "A", ["a1", "a2"]),
    mkPreview("doc_b", "B", ["b1"]),
    mkPreview("doc_c", "C", ["c1"])
  ];

  const one = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds: ["doc_b", "doc_a"],
    existingTrustedDocIds: ["doc_prev"]
  });
  const two = buildBatchActivationArtifacts({
    previews,
    nextBatchDocIds: ["doc_b", "doc_a"],
    existingTrustedDocIds: ["doc_prev"]
  });

  assert.deepEqual(one, two);
  assert.deepEqual(one.nextBatchDocIds, ["doc_a", "doc_b"]);
  assert.ok(one.payload.embeddingPayload.rows.every((row) => ["doc_a", "doc_b"].includes(row.documentId)));
  assert.ok(one.payload.searchPayload.rows.every((row) => ["doc_a", "doc_b"].includes(row.documentId)));
  assert.deepEqual(one.payload.activationManifest.documentsToActivate, ["doc_a", "doc_b"]);
  assert.ok(one.payload.activationManifest.chunksToActivate.length === 3);
  assert.deepEqual(one.payload.rollbackManifest.documentsToRemove, ["doc_a", "doc_b"]);
});

test("validateBatchActivationOutcome rejects non-batch writes and held/excluded/fixture leakage", () => {
  const corpusAdmissionById = new Map([
    ["doc_a", { corpusAdmissionStatus: "hold_for_repair_review", isLikelyFixture: false }],
    ["doc_b", { corpusAdmissionStatus: "hold_for_repair_review", isLikelyFixture: false }],
    ["doc_x", { corpusAdmissionStatus: "exclude_from_initial_corpus", isLikelyFixture: true }]
  ]);

  const validation = validateBatchActivationOutcome({
    batchDocIds: ["doc_a", "doc_b"],
    activationWriteReport: {
      summary: { provenanceFailuresCount: 0 },
      documentsActivated: [
        { documentId: "doc_a" },
        { documentId: "doc_b" },
        { documentId: "doc_x" }
      ],
      chunksActivated: [{ chunkId: "a1" }]
    },
    corpusAdmissionById,
    trustedBeforeIds: ["doc_prev"],
    trustedAfterIds: ["doc_prev", "doc_a", "doc_b"],
    beforeLiveQa: { summary: {} },
    afterLiveQa: {
      summary: {
        outOfCorpusHitQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1,
        zeroTrustedResultQueryCount: 0
      }
    }
  });

  assert.equal(validation.passed, false);
  assert.ok(validation.failures.includes("onlyManifestDocsActivated"));
  assert.ok(validation.failures.includes("activatedDocCountMatchesManifest"));
  assert.ok(validation.failures.includes("batchDocsRemainHeldScope"));
  assert.ok(validation.failures.includes("noExcludedDocsWritten"));
  assert.ok(validation.failures.includes("noFixtureDocsWritten"));
});

test("compareLiveQa reports expected deterministic deltas", () => {
  const comparison = compareLiveQa(
    {
      summary: {
        totalApiResultsAcrossQueries: 146,
        zeroTrustedResultQueryCount: 0,
        averageQualityScore: 65.37,
        outOfCorpusHitQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1
      }
    },
    {
      summary: {
        totalApiResultsAcrossQueries: 166,
        zeroTrustedResultQueryCount: 0,
        averageQualityScore: 67.11,
        outOfCorpusHitQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1
      }
    }
  );

  assert.equal(comparison.deltas.totalApiResultsAcrossQueries, 20);
  assert.equal(comparison.deltas.averageQualityScore, 1.74);
  assert.equal(comparison.deltas.zeroTrustedResultQueryCount, 0);
});
