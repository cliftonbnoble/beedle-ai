# Repo Hygiene Policy

This repository keeps operational commands available, but generated artifacts and one-off experiments should not become permanent surface area by accident.

## API Reports

- `apps/api/reports/` is generated output and must stay ignored by git.
- Treat reports as disposable unless they are linked from an issue, release note, or migration audit.
- Before a release pass, remove stale local reports that are not needed for comparison or audit.
- Use `pnpm --filter @beedle/api report:repo-cleanup-plan` to preview stale report cleanup candidates.
- Use `pnpm --filter @beedle/api write:repo-report-cleanup` to apply the guarded cleanup path; adjust `REPO_REPORT_CLEANUP_MAX_APPLY` when a larger local cleanup pass is intentional.
- If a report becomes durable evidence, summarize the finding in `ISSUES.md`, a runbook, or a checked-in fixture instead of committing the generated report.

## API Scripts

- Keep package scripts for durable commands that are expected to be reused.
- Prefer direct script invocation for one-off investigations instead of adding new package script aliases.
- Treat shared `*-utils.mjs`, task JSON, sample JSON, and allowlist JSON files as expected support/config files unless they graduate into direct workflows.
- Prefix mutating scripts with `write:`, `run:`, or another clearly unsafe verb, and keep dry-run defaults where practical.
- When an experimental script graduates into a workflow, add a short note in `apps/api/scripts/README.md` describing its purpose and safety posture.

## Cleanup Cadence

- During release hardening, review `apps/api/package.json` for stale aliases.
- Run `pnpm --filter @beedle/api report:repo-scripts` before pruning aliases or archiving scripts so the cleanup starts from a reproducible inventory.
- Start pruning from actionable unaliased scripts; expected unaliased support/config files generally need no package alias.
- Archive or remove superseded experiment scripts only in focused commits with tests or documentation explaining the replacement path.
- Avoid mixing cleanup with search ranking, ingestion, or deployment behavior changes.
