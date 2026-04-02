import test from "node:test";
import assert from "node:assert/strict";
import { buildR50FamilyProbePlan } from "../scripts/retrieval-r50-family-probe-plan-report.mjs";

test("R50 builds conservative probe plan and keeps proceedToRealActivation false", () => {
  const r49 = {
    safeFamiliesEvaluated: 2,
    frozenFamilies: [
      {
        familyLabel: "low_signal_heavy::short::analysis_reasoning+holding_disposition",
        decision: "freeze_family"
      }
    ],
    eligibleFamilies: [
      {
        familyLabel: "low_signal_absent::medium::analysis_reasoning+none",
        familySize: 2,
        knownRealOutcomeCount: 0,
        decision: "hold_for_probe_plan"
      }
    ],
    remainingCandidateIds: [
      "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
      "doc_496366f7-dea1-4246-8333-326f629cca57"
    ],
    nextBestCandidateIfAny: {
      documentId: "doc_345fd497-a82c-40ca-a45d-c0aca1b17826"
    },
    candidateRows: [
      {
        documentId: "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none"
      },
      {
        documentId: "doc_496366f7-dea1-4246-8333-326f629cca57",
        documentFamilyLabel: "low_signal_absent::medium::analysis_reasoning+none"
      }
    ]
  };

  const out = buildR50FamilyProbePlan(r49);

  assert.equal(out.probeFamilyLabel, "low_signal_absent::medium::analysis_reasoning+none");
  assert.deepEqual(out.probeCandidateIds, [
    "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
    "doc_496366f7-dea1-4246-8333-326f629cca57"
  ]);
  assert.deepEqual(out.candidateOrder, [
    "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
    "doc_496366f7-dea1-4246-8333-326f629cca57"
  ]);
  assert.equal(out.proceedToRealActivation, false);

  assert.equal(out.probeStages[0].stageId, "stage_1_single_doc_probe");
  assert.equal(out.probeStages[0].onFail, "freeze_family_pending_model_change");
  assert.equal(out.probeStages[2].stageId, "stage_3_optional_second_doc");
  assert.equal(out.recommendedProbeStrategy.allowSecondCandidateOnlyIfFirstPassesCleanly, true);
});
