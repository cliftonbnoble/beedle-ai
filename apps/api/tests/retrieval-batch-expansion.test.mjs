import test from "node:test";
import assert from "node:assert/strict";
import {
  evaluateExpansionGate,
  scoreExpansionCandidate,
  selectNextBatchCandidates,
  summarizeEvalAsQa
} from "../scripts/retrieval-batch-expansion-utils.mjs";

function mkRow(overrides = {}) {
  return {
    include: true,
    documentId: "doc_x",
    title: "Doc X",
    isLikelyFixture: false,
    corpusAdmissionStatus: "hold_for_repair_review",
    keyStats: {
      chunkTypeSpread: 3,
      chunksFlaggedMixedTopic: 0,
      usedFallbackChunking: false
    },
    preEnrichmentCanonicalAlignmentCount: 1,
    postEnrichmentCanonicalAlignmentCount: 3,
    referenceEnrichmentStrategies: ["heading_to_reference_propagation"],
    wouldPromoteIfCombinedNarrowRepairs: false,
    corpusAdmissionWarnings: [],
    ...overrides
  };
}

test("next-batch selection is deterministic and excludes trusted/admit/fixtures", () => {
  const corpusRows = [
    mkRow({ documentId: "doc_best", title: "Best" }),
    mkRow({ documentId: "doc_mid", title: "Mid", keyStats: { chunkTypeSpread: 2, chunksFlaggedMixedTopic: 1, usedFallbackChunking: true } }),
    mkRow({ documentId: "doc_fixture", isLikelyFixture: true }),
    mkRow({ documentId: "doc_admit", corpusAdmissionStatus: "admit_now" }),
    mkRow({ documentId: "doc_trusted" })
  ];

  const selectionA = selectNextBatchCandidates({
    corpusAdmissionRows: corpusRows,
    referenceEnrichmentRows: corpusRows,
    rehearsalRows: [{ documentId: "doc_best", promotionDeltaScore: 18 }, { documentId: "doc_mid", promotionDeltaScore: 3 }],
    trustedDocumentIds: ["doc_trusted"],
    batchSize: 2
  });

  const selectionB = selectNextBatchCandidates({
    corpusAdmissionRows: corpusRows,
    referenceEnrichmentRows: corpusRows,
    rehearsalRows: [{ documentId: "doc_best", promotionDeltaScore: 18 }, { documentId: "doc_mid", promotionDeltaScore: 3 }],
    trustedDocumentIds: ["doc_trusted"],
    batchSize: 2
  });

  assert.deepEqual(selectionA.selected, selectionB.selected);
  assert.deepEqual(
    selectionA.selected.map((row) => row.documentId),
    ["doc_best", "doc_mid"]
  );

  const excludedIds = new Set(selectionA.excluded.map((row) => row.documentId));
  assert.ok(excludedIds.has("doc_fixture"));
  assert.ok(excludedIds.has("doc_admit"));
  assert.ok(excludedIds.has("doc_trusted"));
});

test("scoring favors alignment gains and spread while penalizing fallback and mixed-topic", () => {
  const strong = scoreExpansionCandidate({
    admissionRow: mkRow({ keyStats: { chunkTypeSpread: 4, chunksFlaggedMixedTopic: 0, usedFallbackChunking: false } }),
    enrichmentRow: mkRow({ preEnrichmentCanonicalAlignmentCount: 0, postEnrichmentCanonicalAlignmentCount: 4 }),
    rehearsalRow: { promotionDeltaScore: 15 }
  });

  const weak = scoreExpansionCandidate({
    admissionRow: mkRow({ keyStats: { chunkTypeSpread: 1, chunksFlaggedMixedTopic: 3, usedFallbackChunking: true } }),
    enrichmentRow: mkRow({ preEnrichmentCanonicalAlignmentCount: 0, postEnrichmentCanonicalAlignmentCount: 1, corpusAdmissionWarnings: ["reference_alignment_below_preferred_corpus_threshold"] }),
    rehearsalRow: { promotionDeltaScore: 0 }
  });

  assert.ok(strong.candidateScore > weak.candidateScore);
  assert.ok(strong.candidateReasons.length > 0);
  assert.ok(weak.candidateBlockers.length > 0);
});

test("regression gate catches quality regressions and zero-result failures", () => {
  const gate = evaluateExpansionGate({
    baselineSummary: {
      averageQualityScore: 70
    },
    expandedSummary: {
      outOfCorpusHitQueryCount: 0,
      provenanceCompletenessAverage: 1,
      citationAnchorCoverageAverage: 1,
      zeroTrustedResultQueryCount: 2,
      averageQualityScore: 62
    },
    maxQualityRegression: 3
  });

  assert.equal(gate.passed, false);
  assert.ok(gate.failures.includes("zeroTrustedResultStillZero"));
  assert.ok(gate.failures.includes("noMaterialQualityRegression"));
});

test("eval summary remains deterministic and captures diversity/provenance fields", () => {
  const evalReport = {
    queryResults: [
      {
        queryId: "authority_ordinance",
        query: "ordinance 37.2",
        resultCount: 2,
        topResults: [
          {
            documentId: "doc1",
            chunkId: "c1",
            chunkType: "authority_discussion",
            score: 0.9,
            citationAnchorStart: "a",
            citationAnchorEnd: "b",
            sourceLink: "https://example/1"
          },
          {
            documentId: "doc2",
            chunkId: "c2",
            chunkType: "analysis_reasoning",
            score: 0.8,
            citationAnchorStart: "a2",
            citationAnchorEnd: "b2",
            sourceLink: "https://example/2"
          }
        ]
      }
    ]
  };

  const one = summarizeEvalAsQa(evalReport);
  const two = summarizeEvalAsQa(evalReport);

  assert.deepEqual(one, two);
  assert.equal(one.summary.zeroTrustedResultQueryCount, 0);
  assert.equal(one.summary.provenanceCompletenessAverage, 1);
  assert.equal(one.summary.citationAnchorCoverageAverage, 1);
});
