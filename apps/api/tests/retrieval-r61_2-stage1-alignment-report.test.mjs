import test from "node:test";
import assert from "node:assert/strict";
import { buildR61_2Stage1AlignmentReport } from "../scripts/retrieval-r61_2-stage1-alignment-report.mjs";

test("R61.2 alignment report computes before/after deltas and resolution flag", () => {
  const beforeStage1 = {
    tasksWhereR61Stage1HadCandidates: 0,
    tasksWhereR60_8WorkedButR61Stage1Failed: 3,
    comparisonSingleStageVsStage1: {
      stage1OnlyPath: { candidateRecall: 0 }
    }
  };
  const afterStage1 = {
    tasksWhereR61Stage1HadCandidates: 4,
    tasksWhereR60_8WorkedButR61Stage1Failed: 1,
    rootCauseClassification: "mixed_stage1_failure",
    comparisonSingleStageVsStage1: {
      stage1OnlyPath: { candidateRecall: 0.1667 }
    }
  };
  const beforeTwoStage = {
    top1DecisionHitRate: 0,
    top3DecisionHitRate: 0,
    top5DecisionHitRate: 0,
    sectionTypeHitRate: 0,
    emptyTasksAfter: 24
  };
  const afterTwoStage = {
    top1DecisionHitRate: 0.0417,
    top3DecisionHitRate: 0.0833,
    top5DecisionHitRate: 0.125,
    sectionTypeHitRate: 0.0417,
    emptyTasksAfter: 20
  };

  const report = buildR61_2Stage1AlignmentReport({
    beforeStage1,
    beforeTwoStage,
    afterStage1,
    afterTwoStage
  });

  assert.equal(report.phase, "R61.2");
  assert.equal(report.stage1AlignmentResolved, true);
  assert.equal(report.tasksWhereR61Stage1HadCandidatesBefore, 0);
  assert.equal(report.tasksWhereR61Stage1HadCandidatesAfter, 4);
  assert.equal(report.candidateRecallBefore, 0);
  assert.equal(report.candidateRecallAfter, 0.1667);
  assert.equal(report.top5DecisionHitRateBefore, 0);
  assert.equal(report.top5DecisionHitRateAfter, 0.125);
  assert.equal(report.emptyTasksAfterBefore, 24);
  assert.equal(report.emptyTasksAfterAfter, 20);
  assert.equal(Array.isArray(report.mappingFixesApplied), true);
  assert.equal(report.mappingFixesApplied.length > 0, true);
});
