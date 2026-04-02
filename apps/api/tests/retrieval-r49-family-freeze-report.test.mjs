import test from "node:test";
import assert from "node:assert/strict";
import { buildR49FamilyFreezeReport } from "../scripts/retrieval-r49-family-freeze-report.mjs";

test("R49 freezes family with proven live misprediction and stays conservative for unknown remaining family", () => {
  const r48 = {
    safeCandidatesEvaluated: 9,
    qualityMispredictionCount: 1,
    candidateRows: [
      {
        documentId: "doc_bad",
        documentFamilyLabel: "low_signal_heavy::short::analysis_reasoning+holding_disposition",
        simulatedQualityDelta: 11.96,
        projectedCitationTopDocumentShare: 0,
        projectedLowSignalStructuralShare: 0.05,
        actualKnownQualityDelta: -1.18
      },
      {
        documentId: "doc_u1",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
        simulatedQualityDelta: 11.5,
        projectedCitationTopDocumentShare: 0,
        projectedLowSignalStructuralShare: 0,
        actualKnownQualityDelta: null
      },
      {
        documentId: "doc_u2",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none",
        simulatedQualityDelta: 11.6,
        projectedCitationTopDocumentShare: 0,
        projectedLowSignalStructuralShare: 0,
        actualKnownQualityDelta: null
      }
    ]
  };

  const out = buildR49FamilyFreezeReport(r48);

  assert.equal(out.safeFamiliesEvaluated, 2);
  assert.equal(out.frozenFamilies.length, 1);
  assert.equal(out.frozenFamilies[0].familyLabel, "low_signal_heavy::short::analysis_reasoning+holding_disposition");
  assert.equal(out.frozenFamilies[0].decision, "freeze_family");
  assert.equal(out.frozenFamilies[0].decisionReason, "proven_live_quality_misprediction");

  assert.deepEqual(out.excludedCandidateIds, ["doc_bad"]);
  assert.deepEqual(out.remainingCandidateIds, ["doc_u1", "doc_u2"]);

  assert.equal(out.activationRecommendation, "no");
  assert.equal(out.recommendedNextStep, "no_activation_until_family_probe_plan");
});

test("R49 selects next best candidate when eligible family has known clean outcomes", () => {
  const r48 = {
    safeCandidatesEvaluated: 2,
    qualityMispredictionCount: 0,
    candidateRows: [
      {
        documentId: "doc_a",
        documentFamilyLabel: "family_clean",
        simulatedQualityDelta: 1.2,
        projectedCitationTopDocumentShare: 0.1,
        projectedLowSignalStructuralShare: 0,
        actualKnownQualityDelta: 0.3
      },
      {
        documentId: "doc_b",
        documentFamilyLabel: "family_clean",
        simulatedQualityDelta: 0.8,
        projectedCitationTopDocumentShare: 0.08,
        projectedLowSignalStructuralShare: 0,
        actualKnownQualityDelta: 0.1
      }
    ]
  };

  const out = buildR49FamilyFreezeReport(r48);
  assert.equal(out.frozenFamilies.length, 0);
  assert.equal(out.activationRecommendation, "yes");
  assert.equal(out.recommendedNextStep, "safe_single_doc_activation_candidate:doc_a");
});
