import { test } from "node:test";
import assert from "node:assert/strict";
import { buildContentProbe, buildLegalQueryVariants, buildSelfQueryVariants } from "../scripts/rollout-probe-utils.mjs";

test("low-signal content query is rejected and falls back safely", () => {
  const probe = buildContentProbe(
    {
      chunks: [
        { chunkText: "(See Attached Proof of Service List ), Hearing: July 22, 2019." },
        { chunkText: "CASE NO. L 182415 HEARING DATE: 07/22/2019" }
      ]
    },
    "L182415 Minute Order"
  );

  assert.equal(probe.skipped, true);
  assert.equal(probe.skippedReason, "low_signal_chunks_fallback_to_title");
  assert.equal(probe.query, "L182415 Minute Order");
});

test("legal query variants derive from validated references", () => {
  const legal = buildLegalQueryVariants({
    validReferences: {
      rulesSections: ["I-1.11"],
      ordinanceSections: ["37.3(a)(1)"],
      indexCodes: ["13"]
    }
  });

  assert.equal(legal.skipped, false);
  assert.ok(legal.variants.includes("I-1.11"));
  assert.ok(legal.variants.includes("1.11"));
  assert.ok(legal.variants.includes("Rule 1.11"));
  assert.ok(legal.variants.includes("Ordinance 37.3(a)(1)"));
});

test("self-query variants include normalized title/citation aliases", () => {
  const variants = buildSelfQueryVariants({
    title: "L180839 - Decision",
    citation: "L180839-DECISION"
  });

  assert.ok(variants.includes("L180839 - Decision"));
  assert.ok(variants.includes("L180839 Decision"));
  assert.ok(variants.includes("L180839-DECISION"));
  assert.ok(variants.includes("L180839 DECISION"));
});

