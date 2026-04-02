import test from "node:test";
import assert from "node:assert/strict";
import { aggregateRunSummaries } from "../scripts/lib/overnight-corpus-lift-summary-utils.mjs";

test("aggregateRunSummaries rolls up enabled and retrieval-activated counts across runs", () => {
  const summary = aggregateRunSummaries(
    [
      {
        generatedAt: "2026-03-28T06:00:00.000Z",
        runStatus: "completed",
        startSnapshot: { searchableDecisionDocs: 3611, activeRetrievalDecisionCount: 2450 },
        endSnapshot: { searchableDecisionDocs: 3636, activeRetrievalDecisionCount: 2550 },
        stageResults: {
          missingIndexReprocess: { recoveredIndexCodeCount: 60 },
          searchabilityEnable: { enabledCount: 25 },
          retrievalActivationGap: { activatedDocumentCount: 100 },
          missingIndexAudit: { candidateDocCount: 412, summaryBreakdowns: { byIssueFamily: [{ key: "heat", count: 80 }] } }
        }
      },
      {
        generatedAt: "2026-03-28T07:00:00.000Z",
        runStatus: "completed",
        startSnapshot: { searchableDecisionDocs: 3636, activeRetrievalDecisionCount: 2550 },
        endSnapshot: { searchableDecisionDocs: 3660, activeRetrievalDecisionCount: 2645 },
        stageResults: {
          missingIndexReprocess: { recoveredIndexCodeCount: 55 },
          searchabilityEnable: { enabledCount: 24 },
          retrievalActivationGap: { activatedDocumentCount: 95 },
          missingIndexAudit: { candidateDocCount: 405, summaryBreakdowns: { byIssueFamily: [{ key: "heat", count: 78 }, { key: "mold", count: 32 }] } }
        }
      }
    ],
    { targetSearchable: 7000 }
  );

  assert.equal(summary.runCount, 2);
  assert.equal(summary.totalMissingIndexRecoveredOvernight, 115);
  assert.equal(summary.totalEnabledOvernight, 49);
  assert.equal(summary.totalRetrievalActivatedOvernight, 195);
  assert.equal(summary.startSnapshot.searchableDecisionDocs, 3611);
  assert.equal(summary.endSnapshot.searchableDecisionDocs, 3660);
  assert.equal(summary.progress.searchableDelta, 49);
  assert.equal(summary.topUnresolvedMissingIndexBuckets[0].key, "heat");
});

test("aggregateRunSummaries returns empty-safe defaults when no runs exist", () => {
  const summary = aggregateRunSummaries([], { targetSearchable: 7000 });
  assert.equal(summary.runCount, 0);
  assert.equal(summary.totalMissingIndexRecoveredOvernight, 0);
  assert.equal(summary.totalEnabledOvernight, 0);
  assert.equal(summary.totalRetrievalActivatedOvernight, 0);
  assert.deepEqual(summary.runs, []);
});
