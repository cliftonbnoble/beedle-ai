import test from "node:test";
import assert from "node:assert/strict";
import {
  buildOfflineFallbackArtifacts,
  buildQueryDeltaClassification,
  buildR38QualityRiskRanking,
  buildR38ReportFromRows
} from "../scripts/retrieval-r38-single-frontier-refresh-report.mjs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

test("buildR38QualityRiskRanking sorts by lowest projected quality-regression risk deterministically", () => {
  const ranked = buildR38QualityRiskRanking([
    {
      documentId: "doc_c",
      projectedQualityDelta: 0.25,
      projectedCitationTopDocumentShare: 0.12
    },
    {
      documentId: "doc_a",
      projectedQualityDelta: 0.25,
      projectedCitationTopDocumentShare: 0.11
    },
    {
      documentId: "doc_b",
      projectedQualityDelta: -0.5,
      projectedCitationTopDocumentShare: 0.08
    }
  ]);

  assert.deepEqual(
    ranked.map((row) => row.documentId),
    ["doc_a", "doc_c", "doc_b"]
  );
  assert.deepEqual(
    ranked.map((row) => row.rank),
    [1, 2, 3]
  );
});

test("buildQueryDeltaClassification deterministically separates query-level improvements and regressions", () => {
  const baselineByQuery = [
    { queryId: "q1", metrics: { qualityScore: 50, topDocumentShare: 0.2, resultCount: 10 } },
    { queryId: "q2", metrics: { qualityScore: 60, topDocumentShare: 0.1, resultCount: 10 } }
  ];
  const expandedByQuery = [
    { queryId: "q1", metrics: { qualityScore: 52, topDocumentShare: 0.2, resultCount: 10 } },
    { queryId: "q2", metrics: { qualityScore: 59.5, topDocumentShare: 0.1, resultCount: 10 } }
  ];

  const out = buildQueryDeltaClassification({ baselineByQuery, expandedByQuery, threshold: 0.2 });
  assert.deepEqual(out.queryLevelImprovements, ["q1"]);
  assert.deepEqual(out.queryLevelRegressions, ["q2"]);
  assert.equal(out.queryDeltaRows.length, 2);
  assert.equal(out.queryLevelNetDelta, 1.5);
});

test("buildR38ReportFromRows is deterministic in live mode ordering", () => {
  const rows = [
    {
      documentId: "doc_b",
      projectedAverageQualityScore: 69.2,
      projectedQualityDelta: 0.2,
      projectedCitationTopDocumentShare: 0.1,
      projectedLowSignalStructuralShare: 0.05,
      projectedOutOfCorpusHitQueryCount: 0,
      projectedZeroTrustedResultQueryCount: 0,
      projectedProvenanceCompletenessAverage: 1,
      projectedCitationAnchorCoverageAverage: 1,
      keepOrDoNotActivate: "keep",
      failingGates: [],
      blockerFamilies: ["none"],
      improvementSignals: [],
      regressionSignals: [],
      dominantFeatureDiagnosis: { chunkTypeMix: [], sectionLabelProfile: [] }
    },
    {
      documentId: "doc_a",
      projectedAverageQualityScore: 68.4,
      projectedQualityDelta: -0.8,
      projectedCitationTopDocumentShare: 0.12,
      projectedLowSignalStructuralShare: 0.05,
      projectedOutOfCorpusHitQueryCount: 0,
      projectedZeroTrustedResultQueryCount: 0,
      projectedProvenanceCompletenessAverage: 1,
      projectedCitationAnchorCoverageAverage: 1,
      keepOrDoNotActivate: "do_not_activate",
      failingGates: ["qualityNotMateriallyRegressed"],
      blockerFamilies: ["quality_regression"],
      improvementSignals: [],
      regressionSignals: ["gate_failed:qualityNotMateriallyRegressed"],
      dominantFeatureDiagnosis: { chunkTypeMix: [], sectionLabelProfile: [] }
    }
  ];

  const { report, manifest } = buildR38ReportFromRows({
    candidateRows: rows,
    trustedDocIds: ["doc_trusted"],
    baselineLiveMetrics: {
      trustedDocumentCount: 1,
      averageQualityScore: 69,
      citationTopDocumentShare: 0.1,
      effectiveCitationCeiling: 0.1667,
      lowSignalStructuralShare: 0.05,
      outOfCorpusHitQueryCount: 0,
      zeroTrustedResultQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1
    },
    configuredCitationCeilingValue: 0.1,
    effectiveCitationCeiling: 0.1667,
    dataMode: "live"
  });

  assert.equal(report.dataMode, "live");
  assert.equal(report.offlineFallbackUsed, false);
  assert.equal(report.nextSafeSingleDocId, "doc_b");
  assert.equal(manifest.nextSafeSingleDocId, "doc_b");
  assert.deepEqual(report.candidateRows.map((row) => row.documentId), ["doc_a", "doc_b"]);
});

test("buildOfflineFallbackArtifacts returns deterministic offline candidates from local artifacts", async () => {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), "r38-offline-"));

  await fs.writeFile(
    path.join(temp, "retrieval-corpus-admission-report.json"),
    JSON.stringify({
      documents: [
        { documentId: "doc_a", title: "A", corpusAdmissionStatus: "hold_for_repair_review", isLikelyFixture: false },
        { documentId: "doc_b", title: "B", corpusAdmissionStatus: "hold_for_repair_review", isLikelyFixture: false }
      ]
    })
  );
  await fs.writeFile(
    path.join(temp, "retrieval-r35-stability-report.json"),
    JSON.stringify({
      summary: {
        trustedDocumentCount: 1,
        averageQualityScore: 69,
        citationTopDocumentShare: 0.1,
        lowSignalStructuralShare: 0.05,
        outOfCorpusHitQueryCount: 0,
        zeroTrustedResultQueryCount: 0,
        provenanceCompletenessAverage: 1,
        citationAnchorCoverageAverage: 1
      }
    })
  );
  await fs.writeFile(
    path.join(temp, "retrieval-r36-single-safe-frontier-report.json"),
    JSON.stringify({
      safeSingleCandidates: [
        {
          documentId: "doc_a",
          keep_or_do_not_activate: "keep",
          metrics: {
            averageQualityScoreAfter: 69.4,
            qualityDelta: 0.4,
            citationTopDocumentShareAfter: 0.1,
            lowSignalStructuralShareAfter: 0.05,
            outOfCorpusHitQueryCountAfter: 0,
            zeroTrustedResultQueryCountAfter: 0,
            provenanceCompletenessAverageAfter: 1,
            citationAnchorCoverageAverageAfter: 1
          },
          failingGates: [],
          blockerFamilies: ["none"],
          improvementSignals: [],
          dominantFeatureDiagnosis: { chunkTypeMix: [], sectionLabelProfile: [] }
        }
      ],
      blockedSingleCandidates: []
    })
  );

  const out = await buildOfflineFallbackArtifacts({
    reportsDirPath: temp,
    configuredCitationCeilingValue: 0.1,
    trustedDocIds: ["doc_trusted"]
  });

  assert.equal(out.candidateRows.length, 2);
  assert.deepEqual(
    out.candidateRows.map((row) => row.documentId),
    ["doc_a", "doc_b"]
  );
  assert.ok(out.offlineFallbackInputs.includes("retrieval-corpus-admission-report.json"));
  assert.equal(out.baselineLiveMetrics.averageQualityScore, 69);
});
