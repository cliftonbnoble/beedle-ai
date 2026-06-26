# API Scripts

This folder contains operational utilities, reports, backfills, and investigation scripts for the API.

Use these conventions when adding or promoting scripts:

- Durable commands should have clear names and, when useful, package aliases in `apps/api/package.json`.
- One-off investigations can live here temporarily, but should not automatically receive package aliases.
- Mutating scripts should be easy to identify from the filename or package alias, and should default to dry-run when practical.
- Generated output belongs in `apps/api/reports/`, which is intentionally ignored by git.
- If a script replaces an older workflow, remove or document the older path in the same focused cleanup commit.

Useful hygiene command:

- `pnpm --filter @beedle/api report:repo-scripts` writes a script/alias inventory to `apps/api/reports/` and flags missing targets, duplicate target mappings, unaliased top-level scripts, and local report volume.
- `pnpm --filter @beedle/api report:repo-cleanup-plan` writes a dry-run cleanup plan for old generated reports. It deletes nothing unless `REPO_REPORT_CLEANUP_APPLY=1` is set explicitly.

See `docs/repo-hygiene.md` for the repository-level cleanup policy.
