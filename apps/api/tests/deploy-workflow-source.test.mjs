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

  // The gate must typecheck both packages and run the deterministic source-guard suite and
  // the relevance/highlight tests before the deploy step.
  assert.match(deployWorkflow, /pnpm --filter @beedle\/api typecheck/);
  assert.match(deployWorkflow, /pnpm --filter @beedle\/web typecheck/);
  assert.match(deployWorkflow, /pnpm --filter @beedle\/api test:source/);
  assert.match(deployWorkflow, /search-phrase-relevance\.test\.mjs/);

  // The source-guard suite must run before the deploy step.
  const gateIndex = deployWorkflow.indexOf("test:source");
  const deployIndex = deployWorkflow.indexOf("wrangler deploy");
  assert.ok(gateIndex > -1 && deployIndex > -1 && gateIndex < deployIndex, "test:source must run before deploy");
});
