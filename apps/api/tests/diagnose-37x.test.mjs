import { test } from "node:test";
import assert from "node:assert/strict";
import { classify37xDiagnostic, extract37xKey } from "../scripts/citation-37x-diagnostics-utils.mjs";

test("extract37xKey picks recurring 37.x references", () => {
  const key = extract37xKey({ rawValue: "Ordinance 37.8", normalizedValue: "37.8" });
  assert.equal(key?.base, "37.8");
});

test("cross-context classification identifies alias candidates", () => {
  const out = classify37xDiagnostic({
    verifyCheck: {
      diagnostic: "parent_or_related_only",
      ordinance_matches: [{ citation: "37.8", heading: "Services" }],
      rules_matches: []
    },
    ordinanceIssueCount: 0,
    rulesIssueCount: 5,
    experimentalAliasMode: false
  });
  assert.equal(out.classification, "cross_context_alias_candidate");
});

test("parent/related in expected context is resolvable_by_normalization", () => {
  const out = classify37xDiagnostic({
    verifyCheck: {
      diagnostic: "parent_or_related_only",
      ordinance_matches: [{ citation: "37.3", heading: "General" }],
      rules_matches: []
    },
    ordinanceIssueCount: 4,
    rulesIssueCount: 0,
    experimentalAliasMode: false
  });
  assert.equal(out.classification, "resolvable_by_normalization");
});

test("not-found stays blocked and never resolves experimentally", () => {
  const out = classify37xDiagnostic({
    verifyCheck: {
      diagnostic: "not_found",
      ordinance_matches: [],
      rules_matches: []
    },
    ordinanceIssueCount: 2,
    rulesIssueCount: 0,
    experimentalAliasMode: true
  });
  assert.equal(out.classification, "truly_not_found");
  assert.equal(out.experimentalWouldResolve, false);
});

