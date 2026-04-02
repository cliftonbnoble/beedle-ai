import test from "node:test";
import assert from "node:assert/strict";
import { analyzeReferenceIntegrity } from "../scripts/retrieval-r41-reference-integrity-report.mjs";

test("analyzeReferenceIntegrity identifies stale-loader mismatch and rerun safety", () => {
  const out = analyzeReferenceIntegrity({
    baselineRefMetrics: {
      averageQualityScore: 69,
      citationTopDocumentShare: 0.1667,
      lowSignalStructuralShare: 0,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    currentRefMetrics: {
      averageQualityScore: 22.38,
      citationTopDocumentShare: 0,
      lowSignalStructuralShare: 0,
      outOfCorpusHitQueryCount: 8,
      zeroTrustedResultQueryCount: 6,
      provenanceCompletenessAverage: 0.25,
      citationAnchorCoverageAverage: 0.25
    },
    sourceSets: {
      loaderTrustedDocIds: ["a", "b"],
      r34ReconstructedTrustedDocIds: ["a", "b", "c"],
      r36BaselineTrustedDocIds: ["a", "b", "c"],
      sourceOfTruthTrustedDocIds: ["a", "b", "c"],
      rollbackVerified: true,
      loaderUsedActivationWriteOnly: true
    },
    liveQaBySource: {
      loader: {
        averageQualityScore: 22.38,
        outOfCorpusHitQueryCount: 8,
        provenanceCompletenessAverage: 0.25,
        citationAnchorCoverageAverage: 0.25,
        zeroTrustedResultQueryCount: 6
      },
      truth: {
        averageQualityScore: 69,
        outOfCorpusHitQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1,
        zeroTrustedResultQueryCount: 0
      }
    }
  });

  assert.equal(out.staleArtifactDetected, true);
  assert.equal(out.mixedRollbackStateDetected, true);
  assert.equal(out.trustedSetMismatchDetected, true);
  assert.equal(out.qaInputMismatchDetected, true);
  assert.equal(out.realRuntimeRegressionDetected, false);
  assert.equal(out.canRerunR40Safely, true);
});

test("analyzeReferenceIntegrity marks real runtime regression when source-of-truth live QA is bad", () => {
  const out = analyzeReferenceIntegrity({
    baselineRefMetrics: { averageQualityScore: 69 },
    currentRefMetrics: { averageQualityScore: 69 },
    sourceSets: {
      loaderTrustedDocIds: ["a", "b", "c"],
      r34ReconstructedTrustedDocIds: ["a", "b", "c"],
      r36BaselineTrustedDocIds: ["a", "b", "c"],
      sourceOfTruthTrustedDocIds: ["a", "b", "c"],
      rollbackVerified: false,
      loaderUsedActivationWriteOnly: false
    },
    liveQaBySource: {
      loader: {
        averageQualityScore: 55,
        outOfCorpusHitQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1,
        zeroTrustedResultQueryCount: 0
      },
      truth: {
        averageQualityScore: 55,
        outOfCorpusHitQueryCount: 2,
        provenanceCompletenessAverage: 0.9,
        citationAnchorCoverageAverage: 1,
        zeroTrustedResultQueryCount: 0
      }
    }
  });

  assert.equal(out.realRuntimeRegressionDetected, true);
  assert.equal(out.canRerunR40Safely, false);
});
