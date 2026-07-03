import test from "node:test";
import assert from "node:assert/strict";

import { buildFacetStorageReport } from "../scripts/facet-storage-audit.mjs";

test("facet storage audit counts normalized per-document facet rows", () => {
  const report = buildFacetStorageReport([
    {
      indexCodesJson: JSON.stringify(["IC-104", "104", "G-28"]),
      rulesSectionsJson: JSON.stringify(["Rule 1.11", "1.11"]),
      ordinanceSectionsJson: JSON.stringify(["Ordinance 37.3(a)(1)"])
    },
    {
      indexCodesJson: JSON.stringify(["G-28"]),
      rulesSectionsJson: "not json",
      ordinanceSectionsJson: JSON.stringify(["37.3(a)(1)", "37.15"])
    }
  ]);

  assert.equal(report.summary.decisionRows, 2);
  assert.equal(report.summary.docsWithIndexCodes, 2);
  assert.equal(report.summary.docsWithRulesSections, 1);
  assert.equal(report.summary.docsWithOrdinanceSections, 2);
  assert.equal(report.summary.estimatedDocumentIndexCodeRows, 3);
  assert.equal(report.summary.estimatedDocumentRulesRows, 1);
  assert.equal(report.summary.estimatedDocumentOrdinanceRows, 3);
  assert.equal(report.summary.distinctIndexCodes, 2);
  assert.equal(report.summary.distinctRulesSections, 1);
  assert.equal(report.summary.distinctOrdinanceSections, 2);
  assert.equal(report.summary.malformedRulesJsonRows, 1);
  assert.deepEqual(report.recommendedTables.map((table) => table.name), [
    "document_index_codes",
    "document_rules_sections",
    "document_ordinance_sections"
  ]);
  assert.deepEqual(report.topIndexCodes[0], { value: "g-28", count: 2 });
});
