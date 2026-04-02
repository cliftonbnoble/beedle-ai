import test from "node:test";
import assert from "node:assert/strict";
import { deriveR51FamilyDecision } from "../scripts/retrieval-r51-single-probe-activation-write.mjs";

test("R51 freezes family when first candidate fails hard gate", () => {
  const out = deriveR51FamilyDecision({
    keepOrRollbackDecision: "rollback_batch",
    hardGateFailures: ["qualityNotMateriallyRegressed"],
    anomalyFlags: ["hard_gate_failed"]
  });

  assert.equal(out.freezeDecision, "freeze_family_pending_model_change");
  assert.equal(out.mayProceedToSecondCandidate, false);
  assert.ok(out.freezeReason.includes("qualityNotMateriallyRegressed"));
});

test("R51 allows possible second candidate only when first probe is fully clean", () => {
  const out = deriveR51FamilyDecision({
    keepOrRollbackDecision: "keep_batch_active",
    hardGateFailures: [],
    anomalyFlags: []
  });

  assert.equal(out.freezeDecision, "no_freeze");
  assert.equal(out.freezeReason, "");
  assert.equal(out.mayProceedToSecondCandidate, true);
});
