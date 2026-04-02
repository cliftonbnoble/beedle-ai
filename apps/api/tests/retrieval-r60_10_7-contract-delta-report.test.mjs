import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_10_7ContractDeltaReport } from "../scripts/retrieval-r60_10_7-contract-delta-report.mjs";

test("R60.10.7 reports remaining benchmark contract deltas", () => {
  const report = buildR60_10_7ContractDeltaReport();
  assert.equal(report.phase, "R60.10.7");
  assert.equal(report.scriptRows.length, 4);
  assert.ok(report.remainingMismatchCount > 0);
  assert.ok(report.exactFieldsStillDivergent.includes("query_type_mapping_divergence"));
  assert.equal(report.contractMismatchResolved, false);
});
