import test from "node:test";
import assert from "node:assert/strict";
import {
  buildCatalogMatchers,
  buildLinkedSectionMaps,
  detectIssueFamilies,
  inferCrosswalkCandidates,
  inferPhraseCandidates
} from "../scripts/lib/missing-index-audit-utils.mjs";

test("crosswalk and linked-section inference returns conservative canonical candidates", () => {
  const activeIndexRows = [
    {
      codeIdentifier: "G49",
      isReserved: false,
      linkedOrdinanceSections: ["27.1(a)"],
      linkedRulesSections: ["Rule 11.2"]
    }
  ];
  const linkedSectionMaps = buildLinkedSectionMaps(activeIndexRows);
  const crosswalkMaps = {
    byRules: new Map([["11.2", ["G49"]]]),
    byRulesBare: new Map(),
    byOrdinance: new Map([["27.1(a)", ["G49"]]])
  };

  const candidates = inferCrosswalkCandidates({
    rulesSections: ["Rule 11.2"],
    ordinanceSections: ["27.1(a)"],
    crosswalkMaps,
    linkedSectionMaps
  });

  assert.equal(candidates[0].code, "G49");
  assert.ok(candidates[0].score >= 8.5);
  assert.ok(candidates[0].evidence.some((item) => item.startsWith("crosswalk_rules:")));
  assert.ok(candidates[0].evidence.some((item) => item.startsWith("crosswalk_ordinance:")));
});

test("phrase inference requires strong exact language rather than loose token overlap", () => {
  const catalogMatchers = buildCatalogMatchers({
    sharedCatalog: [{ code: "G49", label: "Lack of heat", description: "Lack of heat" }],
    activeIndexRows: [{ codeIdentifier: "G49", family: "heat", label: "Lack of heat", description: "Lack of heat", isReserved: false }]
  });

  const strong = inferPhraseCandidates({
    normalizedText: "tenant complained about lack of heat during the winter season",
    findingsText: "the apartment had a lack of heat",
    conclusionsText: "",
    catalogMatchers
  });
  assert.equal(strong[0].code, "G49");
  assert.ok(strong[0].score >= 5.5);

  const weak = inferPhraseCandidates({
    normalizedText: "tenant discussed the heating bill and asked for repairs",
    findingsText: "",
    conclusionsText: "",
    catalogMatchers
  });
  assert.equal(weak.length, 0);
});

test("issue family detection clusters high-signal housing conditions", () => {
  const families = detectIssueFamilies("there was no heat, no hot water, and visible mold in the bedroom");
  assert.deepEqual(
    families.map((item) => item.family).slice(0, 3),
    ["heat", "hot_water", "mold"]
  );
});

test("issue family detection avoids substring false positives", () => {
  const families = detectIssueFamilies("the arbitrator reviewed the market-rate petition");
  assert.equal(families.some((item) => item.family === "rodent"), false);
});
