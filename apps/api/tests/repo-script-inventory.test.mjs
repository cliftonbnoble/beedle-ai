import test from "node:test";
import assert from "node:assert/strict";

import { buildScriptInventory } from "../scripts/repo-script-inventory.mjs";

test("repo script inventory flags missing, duplicate, and unaliased scripts", () => {
  const report = buildScriptInventory({
    packageJson: {
      scripts: {
        "report:alpha": "node ./scripts/alpha-report.mjs",
        "report:alpha-copy": "node ./scripts/alpha-report.mjs",
        "write:alpha": "ALPHA_APPLY=1 node ./scripts/alpha-report.mjs",
        "report:beta": "node ./scripts/beta-report.mjs",
        "write:beta": "BETA_APPLY=1 node ./scripts/beta-report.mjs",
        "report:missing": "node ./scripts/missing-report.mjs",
        "test:unit": "node --test ./tests/unit.test.mjs",
        dev: "wrangler dev"
      }
    },
    scriptFiles: ["alpha-report.mjs", "beta-report.mjs", "orphan-audit.mjs", "tasks.sample.json"],
    reportStats: { fileCount: 3, totalBytes: 1536 }
  });

  assert.equal(report.summary.packageAliasCount, 8);
  assert.equal(report.summary.topLevelScriptFileCount, 4);
  assert.equal(report.summary.aliasedScriptFileCount, 3);
  assert.equal(report.summary.unaliasedScriptFileCount, 2);
  assert.equal(report.summary.duplicateTargetCount, 1);
  assert.equal(report.summary.commandVariantTargetCount, 1);
  assert.equal(report.summary.expectedCommandVariantTargetCount, 1);
  assert.equal(report.summary.missingTargetCount, 1);
  assert.equal(report.summary.reportTotalSize, "1.5 KB");
  assert.deepEqual(report.aliasesByCategory, {
    report: 4,
    write: 2,
    test: 1,
    uncategorized: 1
  });
  assert.deepEqual(report.duplicateTargets, [
    {
      script: "alpha-report.mjs",
      command: "node ./scripts/alpha-report.mjs",
      aliases: ["report:alpha", "report:alpha-copy"]
    }
  ]);
  assert.deepEqual(report.commandVariantTargets, [
    {
      script: "alpha-report.mjs",
      aliases: ["report:alpha", "report:alpha-copy", "write:alpha"],
      commandCount: 2
    }
  ]);
  assert.deepEqual(report.expectedCommandVariantTargets, [
    {
      script: "beta-report.mjs",
      aliases: ["report:beta", "write:beta"],
      commandCount: 2
    }
  ]);
  assert.deepEqual(report.missingTargets, [{ alias: "report:missing", target: "./scripts/missing-report.mjs" }]);
  assert.deepEqual(report.unaliasedScriptFiles, ["orphan-audit.mjs", "tasks.sample.json"]);
  assert.deepEqual(report.mutatingAliases, [
    { alias: "write:alpha", command: "ALPHA_APPLY=1 node ./scripts/alpha-report.mjs" },
    { alias: "write:beta", command: "BETA_APPLY=1 node ./scripts/beta-report.mjs" }
  ]);
});
