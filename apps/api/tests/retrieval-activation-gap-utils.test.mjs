import test from "node:test";
import assert from "node:assert/strict";
import { evaluateDryRunForWrite } from "../scripts/lib/retrieval-activation-gap-utils.mjs";

function mkDryRunReport(overrides = {}) {
  return {
    summary: {
      attemptedTrustedDocumentCount: 2,
      attemptedTrustedChunkCount: 6,
      activatedDocumentCount: 2,
      activatedChunkCount: 6,
      rollbackVerificationPassed: true,
      activationVerificationPassed: true
    },
    writeCounts: {
      heldDocsWrittenCount: 0,
      excludedDocsWrittenCount: 0,
      fixtureDocsWrittenCount: 0,
      provenanceFailuresCount: 0
    },
    rollbackVerificationSummary: {
      rollbackManifestMatchesActivationSet: true
    },
    documentsActivated: [{ documentId: "doc_1" }, { documentId: "doc_2" }],
    chunksActivated: [{ chunkId: "c1" }, { chunkId: "c2" }, { chunkId: "c3" }, { chunkId: "c4" }, { chunkId: "c5" }, { chunkId: "c6" }],
    ...overrides
  };
}

test("evaluateDryRunForWrite allows write only when all trust checks pass", () => {
  const evaluation = evaluateDryRunForWrite(mkDryRunReport(), {
    documentsToActivate: ["doc_1", "doc_2"],
    chunksToActivate: ["c1", "c2", "c3", "c4", "c5", "c6"]
  });

  assert.equal(evaluation.canWrite, true);
  assert.deepEqual(evaluation.reasons, []);
  assert.equal(evaluation.verificationChecks.activationVerificationPassed, true);
});

test("evaluateDryRunForWrite blocks write when dry-run reproduces the zero-write failure pattern", () => {
  const evaluation = evaluateDryRunForWrite(
    mkDryRunReport({
      summary: {
        attemptedTrustedDocumentCount: 2,
        attemptedTrustedChunkCount: 6,
        activatedDocumentCount: 0,
        activatedChunkCount: 0,
        rollbackVerificationPassed: false,
        activationVerificationPassed: true
      },
      rollbackVerificationSummary: {
        rollbackManifestMatchesActivationSet: false
      },
      documentsActivated: [],
      chunksActivated: []
    }),
    {
      documentsToActivate: ["doc_1", "doc_2"],
      chunksToActivate: ["c1", "c2", "c3", "c4", "c5", "c6"]
    }
  );

  assert.equal(evaluation.canWrite, false);
  assert.ok(evaluation.reasons.includes("zero_writes_when_attempted"));
  assert.ok(evaluation.reasons.includes("rollback_manifest_mismatch"));
});

test("evaluateDryRunForWrite accepts rollback status from debug-report shape", () => {
  const evaluation = evaluateDryRunForWrite(
    {
      ...mkDryRunReport(),
      rollbackVerificationSummary: null,
      rollbackValidationStatus: {
        rollbackMatchesWriteSet: true
      }
    },
    {
      documentsToActivate: ["doc_1", "doc_2"],
      chunksToActivate: ["c1", "c2", "c3", "c4", "c5", "c6"]
    }
  );

  assert.equal(evaluation.canWrite, true);
  assert.equal(evaluation.verificationChecks.rollbackMatchesWriteSet, true);
});
