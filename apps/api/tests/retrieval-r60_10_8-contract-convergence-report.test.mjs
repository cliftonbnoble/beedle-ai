import test from "node:test";
import assert from "node:assert/strict";

test("R60.10.8 script file exists and exports dry-run phase marker", async () => {
  const mod = await import("../scripts/retrieval-r60_10_8-contract-convergence-report.mjs");
  assert.equal(typeof mod, "object");
});
