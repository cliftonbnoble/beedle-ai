import test from "node:test";
import assert from "node:assert/strict";
import { buildR46FrontierReportFromRows } from "../scripts/retrieval-r46-single-frontier-report.mjs";

test("R46 frontier report ranks safe candidates deterministically", () => {
  const { report, manifest } = buildR46FrontierReportFromRows({
    baseline: {
      averageQualityScore: 69,
      citationTopDocumentShare: 0.2,
      lowSignalStructuralShare: 0
    },
    effectiveCitationCeiling: 0.2,
    configuredCitationCeilingValue: 0.1,
    trustedDocIds: ["doc_t1", "doc_t2"],
    candidateRows: [
      {
        documentId: "doc_b",
        projectedQualityDelta: 0.5,
        projectedCitationTopDocumentShare: 0.2,
        keepOrDoNotActivate: "keep",
        blockerFamilies: ["none"],
        failingGates: []
      },
      {
        documentId: "doc_a",
        projectedQualityDelta: 0.5,
        projectedCitationTopDocumentShare: 0.1,
        keepOrDoNotActivate: "keep",
        blockerFamilies: ["none"],
        failingGates: []
      },
      {
        documentId: "doc_c",
        projectedQualityDelta: -1,
        projectedCitationTopDocumentShare: 0.25,
        keepOrDoNotActivate: "do_not_activate",
        blockerFamilies: ["quality_regression"],
        failingGates: ["qualityNotMateriallyRegressed"]
      }
    ]
  });

  assert.equal(report.safeCandidateCount, 2);
  assert.equal(report.blockedCandidateCount, 1);
  assert.equal(report.nextSafeSingleDocId, "doc_a");
  assert.equal(report.activationRecommendation, "yes");
  assert.equal(manifest.nextSafeSingleDocId, "doc_a");
  assert.deepEqual(report.candidateRows.map((row) => row.documentId), ["doc_a", "doc_b", "doc_c"]);
});

test("R46 frontier report recommends no activation when no safe candidate exists", () => {
  const { report, manifest } = buildR46FrontierReportFromRows({
    baseline: { averageQualityScore: 69 },
    effectiveCitationCeiling: 0.2,
    candidateRows: [
      {
        documentId: "doc_x",
        keepOrDoNotActivate: "do_not_activate",
        blockerFamilies: ["citation_concentration_above_effective_ceiling"],
        failingGates: ["citationTopDocumentShareAtOrBelowEffectiveCeiling"]
      }
    ],
    trustedDocIds: ["doc_t1"]
  });

  assert.equal(report.nextSafeSingleDocId, "");
  assert.equal(report.activationRecommendation, "no");
  assert.equal(manifest.activationRecommendation, "no");
});
