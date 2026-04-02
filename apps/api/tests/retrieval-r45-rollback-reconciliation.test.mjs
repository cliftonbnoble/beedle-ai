import test from "node:test";
import assert from "node:assert/strict";
import { buildFinalState, classifyRootCause } from "../scripts/retrieval-r45-rollback-reconciliation-report.mjs";

function rollbackResult(overrides = {}) {
  return {
    summary: {
      rollbackVerificationPassed: true
    },
    removalDetails: {
      docsMissingFromRollbackTarget: [],
      chunksMissingFromRollbackTarget: [],
      remainingActiveChunkIds: [],
      remainingDocumentIds: [],
      remainingDocumentIdsAnyBatch: [],
      ...overrides.removalDetails
    },
    ...overrides
  };
}

test("classifies manifest mismatch when manifest rows are missing", () => {
  const result = rollbackResult({
    removalDetails: {
      docsMissingFromRollbackTarget: ["doc_a"],
      remainingDocumentIds: ["doc_a"]
    }
  });
  assert.equal(classifyRootCause({ rollbackResult: result }), "manifest_mismatch");
});

test("classifies idempotent missing targets as verification bug only when rollback scope is already clear", () => {
  const result = rollbackResult({
    removalDetails: {
      docsMissingFromRollbackTarget: ["doc_a"],
      remainingDocumentIds: [],
      remainingActiveChunkIds: []
    }
  });
  assert.equal(classifyRootCause({ rollbackResult: result }), "verification_bug_only");
});

test("classifies orphaned chunk/doc states deterministically", () => {
  const orphanedChunk = rollbackResult({
    removalDetails: {
      remainingActiveChunkIds: ["ch_1"],
      remainingDocumentIds: []
    }
  });
  const orphanedDoc = rollbackResult({
    removalDetails: {
      remainingActiveChunkIds: [],
      remainingDocumentIds: ["doc_a"]
    }
  });
  assert.equal(classifyRootCause({ rollbackResult: orphanedChunk }), "orphaned_chunk_rows");
  assert.equal(classifyRootCause({ rollbackResult: orphanedDoc }), "orphaned_doc_row");
});

test("classifies verification bug only when rollback scope is clean but historical rows exist", () => {
  const result = rollbackResult({
    summary: { rollbackVerificationPassed: true },
    removalDetails: {
      remainingActiveChunkIds: [],
      remainingDocumentIds: [],
      remainingDocumentIdsAnyBatch: ["doc_prev"]
    }
  });
  assert.equal(classifyRootCause({ rollbackResult: result }), "verification_bug_only");
});

test("final state fields are deterministic and include required flags", () => {
  const result = rollbackResult({
    summary: { rollbackVerificationPassed: true },
    removalDetails: {
      remainingActiveChunkIds: [],
      remainingDocumentIds: []
    }
  });

  const state = buildFinalState({
    rollbackResult: result,
    compensatingRollbackApplied: false,
    rootCauseClassification: "verification_bug_only"
  });

  assert.deepEqual(state, {
    stateIsSafe: true,
    rollbackVerificationPassed: true,
    docStillActive: false,
    activeChunkCount: 0,
    nonManifestTouched: false,
    compensatingRollbackApplied: false,
    rootCauseClassification: "verification_bug_only"
  });
});
