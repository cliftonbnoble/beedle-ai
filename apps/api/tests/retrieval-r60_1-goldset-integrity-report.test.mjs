import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_1GoldsetIntegrityReport } from "../scripts/retrieval-r60_1-goldset-integrity-report.mjs";

test("R60.1 classifies empty runtime results and expected-id mismatch deterministically", () => {
  const tasks = [
    {
      queryId: "q1",
      query: "Rule 37.8",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["authority_discussion"]
    },
    {
      queryId: "q2",
      query: "Findings",
      expectedDecisionIds: ["doc_b"],
      expectedSectionTypes: ["findings"]
    }
  ];

  const evalReport = {
    trustedCorpus: {
      trustedDocumentIds: ["doc_live_1"]
    },
    queryResults: [
      { queryId: "q1", topResults: [] },
      { queryId: "q2", topResults: [] }
    ]
  };

  const out = buildR60_1GoldsetIntegrityReport({ evalReport, tasks });
  assert.equal(out.phase, "R60.1");
  assert.equal(out.tasksWithMissingExpectedIds.length, 2);
  assert.equal(out.tasksWithNoReturnedResults.length, 2);
  assert.equal(out.tasksWithReturnedResultsButNoExpectedIdMatch.length, 0);
  assert.equal(out.rootCauseClassification, "empty_runtime_results");
});

test("R60.1 identifies section-type mapping bug when IDs match but section types do not", () => {
  const tasks = [
    {
      queryId: "q1",
      query: "Issue",
      expectedDecisionIds: ["doc_a"],
      expectedSectionTypes: ["holding_disposition"]
    }
  ];
  const evalReport = {
    trustedCorpus: { trustedDocumentIds: ["doc_a"] },
    queryResults: [
      {
        queryId: "q1",
        topResults: [{ documentId: "doc_a", chunkType: "analysis_reasoning" }]
      }
    ]
  };

  const out = buildR60_1GoldsetIntegrityReport({ evalReport, tasks });
  assert.equal(out.tasksWithExpectedIdMatchButNoSectionTypeMatch.length, 1);
  assert.equal(out.rootCauseClassification, "section_type_mapping_bug");
});
