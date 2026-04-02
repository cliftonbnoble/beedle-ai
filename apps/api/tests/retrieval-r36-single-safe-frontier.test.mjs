import test from "node:test";
import assert from "node:assert/strict";
import {
  buildImprovementSignals,
  classifyBlockerFamilies,
  evaluateSingleDocGate,
  sortSafeSingleCandidates
} from "../scripts/retrieval-r36-single-safe-frontier-report.mjs";

test("evaluateSingleDocGate passes when all hard gates are satisfied", () => {
  const gate = evaluateSingleDocGate({
    baseline: {
      averageQualityScore: 69,
      citationTopDocumentShare: 0.1,
      lowSignalStructuralShare: 0.0167,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    expanded: {
      averageQualityScore: 69.4,
      citationTopDocumentShare: 0.1,
      lowSignalStructuralShare: 0.0167,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    effectiveCitationCeiling: 0.1667,
    qualityRegressionToleranceValue: 0.5
  });

  assert.equal(gate.keepOrDoNotActivate, "keep");
  assert.deepEqual(gate.failingGates, []);
});

test("evaluateSingleDocGate fails on low-signal increase and citation ceiling breach", () => {
  const gate = evaluateSingleDocGate({
    baseline: {
      averageQualityScore: 69,
      citationTopDocumentShare: 0.1,
      lowSignalStructuralShare: 0.0167,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    expanded: {
      averageQualityScore: 69.1,
      citationTopDocumentShare: 0.2,
      lowSignalStructuralShare: 0.0333,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    effectiveCitationCeiling: 0.1667,
    qualityRegressionToleranceValue: 0.5
  });

  assert.equal(gate.keepOrDoNotActivate, "do_not_activate");
  assert.ok(gate.failingGates.includes("citationTopDocumentShareAtOrBelowEffectiveCeiling"));
  assert.ok(gate.failingGates.includes("lowSignalStructuralShareNotWorsened"));
});

test("classifyBlockerFamilies groups expected blocker families", () => {
  const families = classifyBlockerFamilies([
    "lowSignalStructuralShareNotWorsened",
    "citationTopDocumentShareAtOrBelowEffectiveCeiling",
    "qualityNotMateriallyRegressed",
    "provenanceCompletenessOne"
  ]);

  assert.ok(families.includes("low_signal_structural_share_increase"));
  assert.ok(families.includes("citation_concentration_above_effective_ceiling"));
  assert.ok(families.includes("quality_regression"));
  assert.ok(families.includes("provenance_or_anchor_regression"));
});

test("sortSafeSingleCandidates is deterministic by quality gain then concentration risk then id", () => {
  const rows = [
    { documentId: "doc_c", metrics: { qualityDelta: 1.2, citationTopDocumentShareAfter: 0.09 } },
    { documentId: "doc_a", metrics: { qualityDelta: 1.2, citationTopDocumentShareAfter: 0.08 } },
    { documentId: "doc_b", metrics: { qualityDelta: 2.5, citationTopDocumentShareAfter: 0.1 } }
  ];
  const sorted = sortSafeSingleCandidates(rows);
  assert.deepEqual(
    sorted.map((row) => row.documentId),
    ["doc_b", "doc_a", "doc_c"]
  );
});

test("buildImprovementSignals returns conservative deterministic labels", () => {
  const signals = buildImprovementSignals({
    baseline: {
      averageQualityScore: 69,
      citationTopDocumentShare: 0.1,
      lowSignalStructuralShare: 0.0167,
      uniqueDocumentsPerQueryAvg: 2.1,
      uniqueChunkTypesPerQueryAvg: 1.8
    },
    expanded: {
      averageQualityScore: 70,
      citationTopDocumentShare: 0.08,
      lowSignalStructuralShare: 0.01,
      uniqueDocumentsPerQueryAvg: 2.5,
      uniqueChunkTypesPerQueryAvg: 2.0
    }
  });

  assert.deepEqual(signals, [
    "quality_improved",
    "citation_concentration_improved",
    "low_signal_structural_share_reduced",
    "document_diversity_improved",
    "chunk_type_diversity_improved"
  ]);
});
