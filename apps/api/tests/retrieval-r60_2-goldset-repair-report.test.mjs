import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_2GoldsetRepair } from "../scripts/retrieval-r60_2-goldset-repair-report.mjs";

test("R60.2 repairs tasks to trusted decision IDs and normalized section taxonomy", () => {
  const originalTasks = [
    {
      queryId: "q1",
      query: "Rule 37.8",
      intent: "citation_direct",
      expectedDecisionIds: ["doc_old_1"],
      expectedSectionTypes: ["authority_discussion"],
      minimumAcceptableRank: 5,
      notes: "old"
    },
    {
      queryId: "q2",
      query: "findings",
      intent: "findings",
      expectedDecisionIds: ["doc_old_2"],
      expectedSectionTypes: ["findings"],
      minimumAcceptableRank: 5,
      notes: "old"
    }
  ];

  const r60Eval = {
    trustedCorpus: {
      trustedDocumentIds: ["doc_a", "doc_b"]
    }
  };

  const liveQa = {
    queryResults: [
      {
        queryId: "citation_rule_direct",
        intent: "citation",
        topResults: [{ documentId: "doc_a", chunkType: "ANALYSIS" }]
      },
      {
        queryId: "findings_credibility",
        intent: "findings",
        topResults: [{ documentId: "doc_b", chunkType: "FINDINGS" }]
      }
    ]
  };

  const { repairedTasks, report } = buildR60_2GoldsetRepair({ originalTasks, r60Eval, liveQa });
  assert.equal(repairedTasks.length, 2);
  assert.deepEqual(repairedTasks[0].expectedDecisionIds, ["doc_a"]);
  assert.ok(repairedTasks[0].expectedSectionTypes.includes("analysis_reasoning"));
  assert.deepEqual(repairedTasks[1].expectedDecisionIds, ["doc_b"]);
  assert.ok(repairedTasks[1].expectedSectionTypes.includes("findings"));
  assert.equal(report.originalTaskCount, 2);
  assert.equal(report.repairedTaskCount, 2);
  assert.equal(report.tasksDropped.length, 0);
  assert.deepEqual(report.trustedDecisionIdsUsed, ["doc_a", "doc_b"]);
});
