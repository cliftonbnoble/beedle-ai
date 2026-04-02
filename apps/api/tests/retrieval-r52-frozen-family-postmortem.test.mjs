import test from "node:test";
import assert from "node:assert/strict";
import { buildR52Postmortem } from "../scripts/retrieval-r52-frozen-family-postmortem-report.mjs";

test("R52 produces conservative freeze recommendation with simulation-live mismatch", () => {
  const r48 = {
    candidateRows: [
      {
        documentId: "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
        simulatedQualityDelta: 11.55,
        chunkTypeProfile: [{ key: "analysis_reasoning", count: 12 }],
        sectionLabelProfile: [{ key: "body", count: 12 }]
      },
      {
        documentId: "doc_496366f7-dea1-4246-8333-326f629cca57",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
        simulatedQualityDelta: 11.55,
        chunkTypeProfile: [{ key: "analysis_reasoning", count: 12 }],
        sectionLabelProfile: [{ key: "body", count: 12 }]
      }
    ]
  };

  const r50 = {
    probeFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
    probeCandidateIds: [
      "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
      "doc_496366f7-dea1-4246-8333-326f629cca57"
    ]
  };

  const r51Activation = {
    docActivatedExact: "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
    beforeLiveMetrics: { averageQualityScore: 69.02 },
    afterLiveMetrics: { averageQualityScore: 68.22 },
    freezeDecision: "freeze_family_pending_model_change",
    freezeReason: "hard_gate_failed,qualityNotMateriallyRegressed"
  };

  const r51LiveQa = {
    beforeQueryResults: [
      {
        queryId: "authority",
        query: "authority",
        metrics: { qualityScore: 70, topDocumentShare: 0.2 },
        topResults: [{ documentId: "doc_a", chunkType: "analysis_reasoning", citationAnchor: "A#p1" }]
      }
    ],
    afterQueryResults: [
      {
        queryId: "authority",
        query: "authority",
        metrics: { qualityScore: 66, topDocumentShare: 0.3 },
        topResults: [{ documentId: "doc_345fd497-a82c-40ca-a45d-c0aca1b17826", chunkType: "analysis_reasoning", citationAnchor: "B#p1" }]
      }
    ]
  };

  const out = buildR52Postmortem({
    r48Report: r48,
    r50Report: r50,
    r51ActivationReport: r51Activation,
    r51LiveQaReport: r51LiveQa
  });

  assert.equal(out.frozenFamilyLabel, "low_signal_absent::medium::analysis_reasoning+none");
  assert.equal(out.documentsAnalyzed, 2);
  assert.equal(out.recommendedFreezeDisposition, "freeze_family_pending_model_change");
  assert.equal(out.mayUnfreezeWithoutModelChange, false);

  const activated = out.candidateRows.find((row) => row.documentId === "doc_345fd497-a82c-40ca-a45d-c0aca1b17826");
  assert.equal(activated.actualQualityDeltaIfKnown, -0.8);
  assert.equal(activated.predictionError, 12.35);
  assert.ok(activated.familyRiskSignals.includes("proven_live_quality_regression"));
});
