import test from "node:test";
import assert from "node:assert/strict";
import {
  deriveActivationHealth,
  formatRetrievalActivationWriteMarkdown,
  validateActivationWriteReport
} from "../scripts/retrieval-activation-write-utils.mjs";

function baseReport(overrides = {}) {
  return {
    readOnly: false,
    summary: {
      attemptedTrustedDocumentCount: 2,
      attemptedTrustedChunkCount: 3,
      writtenEmbeddingRowCount: 3,
      writtenSearchRowCount: 3,
      activatedDocumentCount: 2,
      activatedChunkCount: 3,
      documentsRejectedCount: 0,
      chunksRejectedCount: 0,
      heldDocsWrittenCount: 0,
      excludedDocsWrittenCount: 0,
      fixtureDocsWrittenCount: 0,
      provenanceFailuresCount: 0,
      activationVerificationPassed: true,
      rollbackVerificationPassed: true,
      activationBatchId: "activation_abc"
    },
    writeCounts: {
      attemptedTrustedDocumentCount: 2,
      attemptedTrustedChunkCount: 3,
      writtenEmbeddingRowCount: 3,
      writtenSearchRowCount: 3,
      activatedDocumentCount: 2,
      activatedChunkCount: 3,
      documentsRejectedCount: 0,
      chunksRejectedCount: 0,
      heldDocsWrittenCount: 0,
      excludedDocsWrittenCount: 0,
      fixtureDocsWrittenCount: 0,
      provenanceFailuresCount: 0
    },
    verificationSummary: {
      onlyTrustedDocsWritten: true,
      noHeldDocsWritten: true,
      noExcludedDocsWritten: true,
      noFixtureDocsWritten: true,
      provenanceIntact: true,
      countsMatchManifestExpectations: true,
      activatedDocsQueryable: true
    },
    activationBatchSummary: {
      activationBatchId: "activation_abc",
      trustedDocumentCount: 2,
      trustedChunkCount: 3,
      trustSourceCounts: {
        baseline_admit_now: 1,
        promoted_after_enrichment: 1
      }
    },
    rollbackVerificationSummary: {
      rollbackManifestMatchesActivationSet: true,
      rollbackDocumentCount: 2,
      rollbackChunkCount: 3
    },
    documentsActivated: [
      { documentId: "doc_a", title: "A", trustSource: "baseline_admit_now", writeStatus: "written" },
      { documentId: "doc_b", title: "B", trustSource: "promoted_after_enrichment", writeStatus: "written" }
    ],
    chunksActivated: [
      {
        chunkId: "c1",
        documentId: "doc_a",
        chunkType: "authority_discussion",
        embeddingWriteStatus: "written",
        searchWriteStatus: "written",
        provenanceComplete: true
      },
      {
        chunkId: "c2",
        documentId: "doc_b",
        chunkType: "analysis_reasoning",
        embeddingWriteStatus: "written",
        searchWriteStatus: "written",
        provenanceComplete: true
      },
      {
        chunkId: "c3",
        documentId: "doc_b",
        chunkType: "holding_disposition",
        embeddingWriteStatus: "written",
        searchWriteStatus: "written",
        provenanceComplete: true
      }
    ],
    documentsRejectedFromWrite: [],
    chunksRejectedFromWrite: [],
    ...overrides
  };
}

test("trusted-only validation passes with clean report", () => {
  const report = baseReport();
  const checks = validateActivationWriteReport(report, {
    trustedDocIds: ["doc_a", "doc_b"],
    trustedChunkIds: ["c1", "c2", "c3"]
  });

  assert.equal(checks.onlyTrustedDocsWritten, true);
  assert.equal(checks.onlyTrustedChunksWritten, true);
  assert.equal(checks.noHeldOrExcludedOrFixtureWrites, true);
  assert.equal(checks.provenanceComplete, true);
  assert.equal(checks.rollbackMatchesWriteSet, true);
  assert.equal(checks.nonZeroWritesWhenAttempted, true);
  assert.equal(checks.activationVerificationPassed, true);
});

test("held/excluded/fixture and provenance failures are detected", () => {
  const report = baseReport({
    summary: {
      ...baseReport().summary,
      activationVerificationPassed: false
    },
    writeCounts: {
      ...baseReport().writeCounts,
      heldDocsWrittenCount: 1,
      excludedDocsWrittenCount: 1,
      fixtureDocsWrittenCount: 1,
      provenanceFailuresCount: 2
    },
    rollbackVerificationSummary: {
      rollbackManifestMatchesActivationSet: false,
      rollbackDocumentCount: 1,
      rollbackChunkCount: 1
    },
    documentsActivated: [{ documentId: "doc_bad", title: "bad", trustSource: "baseline_admit_now", writeStatus: "written" }],
    chunksActivated: [{ chunkId: "cx", documentId: "doc_bad", chunkType: "general_body", embeddingWriteStatus: "written", searchWriteStatus: "written", provenanceComplete: false }]
  });

  const checks = validateActivationWriteReport(report, {
    trustedDocIds: ["doc_a", "doc_b"],
    trustedChunkIds: ["c1", "c2", "c3"]
  });

  assert.equal(checks.onlyTrustedDocsWritten, false);
  assert.equal(checks.onlyTrustedChunksWritten, false);
  assert.equal(checks.noHeldOrExcludedOrFixtureWrites, false);
  assert.equal(checks.provenanceComplete, false);
  assert.equal(checks.rollbackMatchesWriteSet, false);
  assert.equal(checks.nonZeroWritesWhenAttempted, true);
  assert.equal(checks.activationVerificationPassed, false);
});

test("reproduces prior all-rejected bug pattern and marks activation unhealthy", () => {
  const report = baseReport({
    summary: {
      ...baseReport().summary,
      attemptedTrustedDocumentCount: 27,
      attemptedTrustedChunkCount: 135,
      activatedDocumentCount: 0,
      activatedChunkCount: 0,
      writtenEmbeddingRowCount: 0,
      writtenSearchRowCount: 0,
      documentsRejectedCount: 27,
      chunksRejectedCount: 135,
      activationVerificationPassed: true,
      rollbackVerificationPassed: false
    },
    writeCounts: {
      ...baseReport().writeCounts,
      attemptedTrustedDocumentCount: 27,
      attemptedTrustedChunkCount: 135,
      activatedDocumentCount: 0,
      activatedChunkCount: 0,
      writtenEmbeddingRowCount: 0,
      writtenSearchRowCount: 0,
      documentsRejectedCount: 27,
      chunksRejectedCount: 135
    },
    documentsActivated: [],
    chunksActivated: [],
    documentsRejectedFromWrite: Array.from({ length: 27 }, (_, i) => ({ documentId: `doc_${i}`, reason: "qc_not_passed" })),
    chunksRejectedFromWrite: Array.from({ length: 135 }, (_, i) => ({ chunkId: `drchk_${i}`, documentId: `doc_${i % 27}`, reason: "chunk_document_not_activated" })),
    rollbackVerificationSummary: {
      rollbackManifestMatchesActivationSet: false,
      rollbackDocumentCount: 27,
      rollbackChunkCount: 135
    }
  });

  const health = deriveActivationHealth(report);
  assert.equal(health.nonZeroWritesWhenAttempted, false);

  const checks = validateActivationWriteReport(report, {
    trustedDocIds: [],
    trustedChunkIds: []
  });
  assert.equal(checks.nonZeroWritesWhenAttempted, false);
  assert.equal(checks.activationVerificationPassed, false);
});

test("markdown output is deterministic and includes key sections", () => {
  const report = baseReport();
  const md1 = formatRetrievalActivationWriteMarkdown(report);
  const md2 = formatRetrievalActivationWriteMarkdown(report);

  assert.equal(md1, md2);
  assert.ok(md1.includes("# Retrieval Activation Write Report"));
  assert.ok(md1.includes("## Summary"));
  assert.ok(md1.includes("## Verification Summary"));
  assert.ok(md1.includes("## Activation Batch Summary"));
  assert.ok(md1.includes("## Rollback Verification Summary"));
});
