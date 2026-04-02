import test from "node:test";
import assert from "node:assert/strict";
import {
  classifyWarning,
  evaluateStaleQcCandidate,
  selectStaleQcCandidates
} from "../scripts/lib/stale-qc-remediation-utils.mjs";

test("classifyWarning groups stale QC warnings into expected buckets", () => {
  assert.equal(classifyWarning("Index Codes not detected"), "stale_index_warning");
  assert.equal(
    classifyWarning("Unknown rules references (manual review): Rule 37.2, Rule 37.8"),
    "unknown_rules"
  );
  assert.equal(
    classifyWarning("Critical reference exception: 37.15 may be cross-context ambiguous (ordinance vs rules); manual QC required"),
    "critical_reference_exception"
  );
});

test("evaluateStaleQcCandidate accepts structurally complete stale-QC rows", () => {
  const result = evaluateStaleQcCandidate({
    qcPassed: 0,
    qcRequiredConfirmed: 0,
    extractionConfidence: 0.82,
    extractionWarningsJson: JSON.stringify([
      "Index Codes not detected",
      "Taxonomy suggestion is low-confidence; review case type during QC",
      "Unknown rules references (manual review): Rule 37.2, Rule 37.8",
      "Unknown ordinance references (manual review): Ordinance 37.3",
      "Split long merged paragraph into 3 parts"
    ]),
    metadataJson: JSON.stringify({
      referenceValidation: {
        unknownRules: ["Rule 37.2", "Rule 37.8"],
        unknownOrdinance: ["Ordinance 37.3"]
      }
    }),
    indexCodesJson: JSON.stringify(["10"]),
    rulesSectionsJson: JSON.stringify(["Rule 6.10", "Rule 37.2", "Rule 37.8"]),
    ordinanceSectionsJson: JSON.stringify(["Ordinance 37.2", "Ordinance 37.3"])
  });

  assert.equal(result.eligible, true);
  assert.equal(result.reason, "stale_qc_safe_autoconfirm");
  assert.equal(result.payload.confirm_required_metadata, true);
  assert.deepEqual(result.payload.index_codes, ["10"]);
});

test("evaluateStaleQcCandidate blocks critical-reference rows", () => {
  const result = evaluateStaleQcCandidate({
    qcPassed: 0,
    qcRequiredConfirmed: 0,
    extractionConfidence: 0.84,
    extractionWarningsJson: JSON.stringify([
      "Index Codes not detected",
      "Critical reference exception: 37.15 may be cross-context ambiguous (ordinance vs rules); manual QC required"
    ]),
    metadataJson: JSON.stringify({
      referenceValidation: {
        unknownRules: ["Rule 37.2"],
        unknownOrdinance: ["Ordinance 37.15"]
      }
    }),
    indexCodesJson: JSON.stringify(["10"]),
    rulesSectionsJson: JSON.stringify(["Rule 37.2"]),
    ordinanceSectionsJson: JSON.stringify(["Ordinance 37.15"])
  });

  assert.equal(result.eligible, false);
  assert.equal(result.reason, "critical_reference_exception_present");
});

test("evaluateStaleQcCandidate allows dual-context 37.15 rows", () => {
  const result = evaluateStaleQcCandidate({
    qcPassed: 0,
    qcRequiredConfirmed: 0,
    extractionConfidence: 0.84,
    extractionWarningsJson: JSON.stringify([
      "Index Codes not detected",
      "Unknown rules references (manual review): Rule 37.2",
      "Unknown ordinance references (manual review): Ordinance 37.3, Ordinance 37.15",
      "Critical reference exception: 37.15 may be cross-context ambiguous (ordinance vs rules); manual QC required"
    ]),
    metadataJson: JSON.stringify({
      referenceValidation: {
        unknownRules: ["Rule 37.2"],
        unknownOrdinance: ["Ordinance 37.3", "Ordinance 37.15"]
      }
    }),
    indexCodesJson: JSON.stringify(["10"]),
    rulesSectionsJson: JSON.stringify(["Rule 37.2", "Rule 37.15"]),
    ordinanceSectionsJson: JSON.stringify(["Ordinance 37.3", "Ordinance 37.15"])
  });

  assert.equal(result.eligible, true);
  assert.equal(result.reason, "stale_qc_safe_autoconfirm");
});

test("selectStaleQcCandidates sorts higher-confidence rows first", () => {
  const result = selectStaleQcCandidates([
    {
      citation: "DOC-2",
      decisionDate: "2024-01-01",
      qcPassed: 0,
      qcRequiredConfirmed: 0,
      extractionConfidence: 0.74,
      extractionWarningsJson: JSON.stringify(["Index Codes not detected"]),
      metadataJson: JSON.stringify({ referenceValidation: { unknownRules: [], unknownOrdinance: [] } }),
      indexCodesJson: JSON.stringify(["10"]),
      rulesSectionsJson: JSON.stringify(["Rule 6.10"]),
      ordinanceSectionsJson: JSON.stringify(["Ordinance 37.2"])
    },
    {
      citation: "DOC-1",
      decisionDate: "2024-02-01",
      qcPassed: 0,
      qcRequiredConfirmed: 0,
      extractionConfidence: 0.84,
      extractionWarningsJson: JSON.stringify(["Index Codes not detected"]),
      metadataJson: JSON.stringify({ referenceValidation: { unknownRules: [], unknownOrdinance: [] } }),
      indexCodesJson: JSON.stringify(["10"]),
      rulesSectionsJson: JSON.stringify(["Rule 6.10"]),
      ordinanceSectionsJson: JSON.stringify(["Ordinance 37.2"])
    }
  ]);

  assert.equal(result.selected.length, 2);
  assert.equal(result.selected[0].row.citation, "DOC-1");
  assert.equal(result.selected[1].row.citation, "DOC-2");
});
