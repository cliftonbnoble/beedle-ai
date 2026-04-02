import test from "node:test";
import assert from "node:assert/strict";
import {
  analyzeR39Frontier,
  classifyDocumentFamilyLabel,
  evaluateSingleGateCounterfactuals
} from "../scripts/retrieval-r39-frontier-blocker-breakdown-report.mjs";

test("classifyDocumentFamilyLabel is deterministic", () => {
  assert.equal(classifyDocumentFamilyLabel({ title: "Retrieval Messy Headings abc" }), "retrieval_messy_headings");
  assert.equal(classifyDocumentFamilyLabel({ title: "Retrieval Fallback xyz" }), "retrieval_fallback");
  assert.equal(
    classifyDocumentFamilyLabel({
      title: "Other",
      dominantFeatureDiagnosis: { sectionLabelProfile: [{ key: "QUESTIONS PRESENTED" }] }
    }),
    "section_questions_presented"
  );
});

test("evaluateSingleGateCounterfactuals isolates single-gate near misses", () => {
  assert.deepEqual(evaluateSingleGateCounterfactuals(["qualityNotMateriallyRegressed"]), {
    wouldPassIfOnlyQualityGateRelaxed: true,
    wouldPassIfOnlyCitationConcentrationRelaxed: false,
    wouldPassIfOnlyLowSignalGateRelaxed: false
  });
  assert.deepEqual(evaluateSingleGateCounterfactuals(["qualityNotMateriallyRegressed", "lowSignalStructuralShareNotWorsened"]), {
    wouldPassIfOnlyQualityGateRelaxed: false,
    wouldPassIfOnlyCitationConcentrationRelaxed: false,
    wouldPassIfOnlyLowSignalGateRelaxed: false
  });
});

test("analyzeR39Frontier computes blocker breakdown and near misses deterministically", () => {
  const report = analyzeR39Frontier({
    dataMode: "live",
    baselineLiveMetrics: { averageQualityScore: 69 },
    candidateRows: [
      {
        documentId: "doc_b",
        title: "Retrieval Fallback b",
        keepOrDoNotActivate: "do_not_activate",
        failingGates: ["qualityNotMateriallyRegressed"],
        blockerFamilies: ["quality_regression"],
        projectedAverageQualityScore: 68.2,
        projectedQualityDelta: -0.8,
        projectedCitationTopDocumentShare: 0.1,
        projectedLowSignalStructuralShare: 0.05,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        improvementSignals: [],
        regressionSignals: [],
        dominantFeatureDiagnosis: { sectionLabelProfile: [], chunkTypeMix: [] }
      },
      {
        documentId: "doc_a",
        title: "Retrieval Messy Headings a",
        keepOrDoNotActivate: "do_not_activate",
        failingGates: ["lowSignalStructuralShareNotWorsened", "citationTopDocumentShareAtOrBelowEffectiveCeiling"],
        blockerFamilies: ["low_signal_structural_share_increase", "citation_concentration_above_effective_ceiling"],
        projectedAverageQualityScore: 69.1,
        projectedQualityDelta: 0.1,
        projectedCitationTopDocumentShare: 0.2,
        projectedLowSignalStructuralShare: 0.1,
        projectedOutOfCorpusHitQueryCount: 0,
        projectedZeroTrustedResultQueryCount: 0,
        projectedProvenanceCompletenessAverage: 1,
        projectedCitationAnchorCoverageAverage: 1,
        improvementSignals: [],
        regressionSignals: [],
        dominantFeatureDiagnosis: { sectionLabelProfile: [], chunkTypeMix: [] }
      }
    ]
  });

  assert.equal(report.candidatesScanned, 2);
  assert.equal(report.safeCandidateCount, 0);
  assert.equal(report.blockedCandidateCount, 2);
  assert.equal(report.nearMissCandidateCount, 1);
  assert.equal(report.wouldPassIfOnlyQualityGateRelaxedCount, 1);
  assert.equal(report.wouldPassIfOnlyCitationConcentrationRelaxedCount, 0);
  assert.equal(report.wouldPassIfOnlyLowSignalGateRelaxedCount, 0);
  assert.equal(report.candidateRows[0].documentId, "doc_a");
  assert.equal(report.candidateRows[1].documentId, "doc_b");
});

