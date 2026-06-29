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
