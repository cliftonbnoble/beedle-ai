import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const repoRoot = path.resolve(process.cwd(), "../..");

test("repo hygiene policy documents report and script cleanup expectations", async () => {
  const policy = await fs.readFile(path.resolve(repoRoot, "docs/repo-hygiene.md"), "utf8");
  const scriptsReadme = await fs.readFile(path.resolve(process.cwd(), "scripts/README.md"), "utf8");
  const gitignore = await fs.readFile(path.resolve(repoRoot, ".gitignore"), "utf8");

  assert.match(gitignore, /apps\/api\/reports\//);
  assert.match(policy, /apps\/api\/reports\/` is generated output/);
  assert.match(policy, /report:repo-cleanup-plan/);
  assert.match(policy, /REPO_REPORT_CLEANUP_APPLY=1/);
  assert.match(policy, /Keep package scripts for durable commands/);
  assert.match(policy, /review `apps\/api\/package\.json` for stale aliases/);
  assert.match(policy, /report:repo-scripts/);
  assert.match(scriptsReadme, /Generated output belongs in `apps\/api\/reports\/`/);
  assert.match(scriptsReadme, /Mutating scripts should be easy to identify/);
  assert.match(scriptsReadme, /report:repo-cleanup-plan/);
  assert.match(scriptsReadme, /deletes nothing unless `REPO_REPORT_CLEANUP_APPLY=1`/);
});
