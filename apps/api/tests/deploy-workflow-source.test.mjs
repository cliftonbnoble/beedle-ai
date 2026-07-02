import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const repoRoot = path.resolve(process.cwd(), "../..");
const deployWorkflowPath = path.join(repoRoot, ".github/workflows/deploy-api.yml");
const migrationWorkflowPath = path.join(repoRoot, ".github/workflows/apply-d1-migrations.yml");

test("production D1 migrations are manual and environment-gated", async () => {
  const deployWorkflow = await fs.readFile(deployWorkflowPath, "utf8");
  const migrationWorkflow = await fs.readFile(migrationWorkflowPath, "utf8");

  assert.doesNotMatch(deployWorkflow, /d1 migrations apply beedle --remote/);
  assert.match(migrationWorkflow, /on:\s*\n\s*workflow_dispatch:/);
  assert.match(migrationWorkflow, /environment:\s*production-d1-migrations/);
  assert.match(migrationWorkflow, /pnpm wrangler d1 migrations list beedle --remote/);
  assert.match(migrationWorkflow, /pnpm wrangler d1 migrations apply beedle --remote/);
});

test("deploy workflow runs typecheck + tests before deploying (pre-deploy gate)", async () => {
  const deployWorkflow = await fs.readFile(deployWorkflowPath, "utf8");

  // The gate must typecheck both packages and run every deterministic suite family before deploy:
  // the source guards, the util suites, the relevance + queryability-gate behavioral pins, and the
  // full web suite set (TEST-02 wired these — previously 17 suites ran nowhere).
  assert.match(deployWorkflow, /pnpm --filter @beedle\/api typecheck/);
  assert.match(deployWorkflow, /pnpm --filter @beedle\/web typecheck/);
  assert.match(deployWorkflow, /pnpm --filter @beedle\/api test:source/);
  assert.match(deployWorkflow, /pnpm --filter @beedle\/api test:utils/);
  assert.match(deployWorkflow, /pnpm --filter @beedle\/web test:web/);
  assert.match(deployWorkflow, /search-phrase-relevance\.test\.mjs/);
  assert.match(deployWorkflow, /retrieval-search-queryability-gate\.test\.mjs/);

  // Every gate suite must run before the deploy step.
  const deployIndex = deployWorkflow.indexOf("wrangler deploy");
  assert.ok(deployIndex > -1);
  for (const marker of ["test:source", "test:utils", "test:web", "retrieval-search-queryability-gate.test.mjs"]) {
    const gateIndex = deployWorkflow.indexOf(marker);
    assert.ok(gateIndex > -1 && gateIndex < deployIndex, `${marker} must run before deploy`);
  }
});
