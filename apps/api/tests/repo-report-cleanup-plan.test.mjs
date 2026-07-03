import test from "node:test";
import assert from "node:assert/strict";

import { buildReportCleanupPlan } from "../scripts/repo-report-cleanup-plan.mjs";

test("repo report cleanup plan selects old generated report files only", () => {
  const nowMs = Date.parse("2026-06-26T12:00:00.000Z");
  const oldMs = Date.parse("2026-06-01T12:00:00.000Z");
  const freshMs = Date.parse("2026-06-25T12:00:00.000Z");
  const plan = buildReportCleanupPlan(
    [
      { relativePath: "old/report.json", isFile: true, size: 1000, mtimeMs: oldMs },
      { relativePath: "old/report.md", isFile: true, size: 500, mtimeMs: oldMs },
      { relativePath: "fresh/report.json", isFile: true, size: 250, mtimeMs: freshMs },
      { relativePath: "old/blob.sqlite", isFile: true, size: 4000, mtimeMs: oldMs }
    ],
    { nowMs, retentionDays: 14 }
  );

  assert.equal(plan.summary.totalFileCount, 4);
  assert.equal(plan.summary.totalBytes, 5750);
  assert.equal(plan.summary.candidateFileCount, 2);
  assert.equal(plan.summary.candidateBytes, 1500);
  assert.deepEqual(
    plan.candidates.map((row) => row.relativePath),
    ["old/report.json", "old/report.md"]
  );
});
