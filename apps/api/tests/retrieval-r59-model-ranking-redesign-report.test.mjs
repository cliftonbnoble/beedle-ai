import test from "node:test";
import assert from "node:assert/strict";
import { buildR59RedesignReport } from "../scripts/retrieval-r59-model-ranking-redesign-report.mjs";

test("R59 produces deterministic redesign plan with required sections and stop/go criteria", () => {
  const r54 = {
    frozenFamilies: [
      "low_signal_absent::medium::analysis_reasoning+none",
      "low_signal_heavy::short::analysis_reasoning+holding_disposition"
    ]
  };
  const r55 = {
    baselinePredictionError: 12.745,
    remediatedPredictionError: 0
  };
  const r57 = {
    summary: { newlySafeSimulatedCandidateCount: 0 },
    baselinePredictionError: 12.745,
    remediatedPredictionError: 11.0888
  };
  const r58 = {
    summary: { stillBlockedCandidateCount: 60, newlySafeSimulatedCandidateCount: 0 },
    baselinePredictionError: 12.745,
    remediatedPredictionError: 11.3455
  };
  const r39 = {
    blockedCandidateCount: 60,
    blockerFamilyCounts: { citation_concentration_above_effective_ceiling: 60 }
  };

  const out = buildR59RedesignReport({
    r54Report: r54,
    r55Report: r55,
    r57Report: r57,
    r58Report: r58,
    r39Report: r39
  });

  assert.equal(out.phase, "R59");
  assert.equal(out.readOnly, true);
  assert.ok(Array.isArray(out.redesignWorkstreams));
  assert.ok(Array.isArray(out.prioritizedChanges));
  assert.equal(out.prioritizedChanges.length, 5);
  assert.ok(Array.isArray(out.validationProtocol?.phases));
  assert.ok(Array.isArray(out.activationResumeCriteria?.goCriteria));
  assert.ok(Array.isArray(out.activationResumeCriteria?.noGoCriteria));
  assert.match(out.recommendedNextStep, /implement_r59_change_01/);
});
