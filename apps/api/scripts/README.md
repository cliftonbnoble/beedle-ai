# API scripts

Start at [current operations](../../../docs/operations.md) and [corpus operations](../../../docs/corpus-operations.md). Run package filters from the checkout root. Generated output belongs in `apps/api/reports/`, which is ignored by Git; reports can contain private corpus facts.

| Command family | Status and safety posture |
| --- | --- |
| `types:generate`, `types:check`, `lint`, `test:*` | Maintained local gates. Native/browser tests use isolated resources; types/native tests require loopback sockets. [Contracts](../../../docs/runtime-contracts-and-types.md). |
| `types:migration:check` | Checks the private scheduled migration runner's generated DB-only bindings using pinned Wrangler and a credential-free temporary config. Regenerate with `node apps/api/scripts/generate-migration-worker-types.mjs`; no deployment or database write. |
| `deploy:preview`, `deploy:production`, `release:preflight` | Explicit environments and private release evidence. `--dry-run` is an offline bundle check. Read the [release guide](../../../docs/release-readiness-and-rollout.md) before real operations. |
| `recovery` | Maintained capture/plan/apply/verify with explicit descriptors and reviewed checksum. [Recovery runbook](../../../docs/recovery-runbook.md). `run:d1-remote-data-migrate` is a compatibility alias with the same arguments; heuristic postmigration repair is retired. |
| `python3 apps/api/scripts/capture-frozen-baseline.py plan\|capture ...` | Explicit, owner-private legacy-0012 backup after separately reviewed source freeze. Python 3.11+, absolute source/output inputs, owned fences and valid Cloudflare credentials required. Reads source D1/R2; writes local typed checkpoints/originals. Resume the same plan only; [capture and maintenance rollback](../../../docs/recovery-runbook.md#frozen-legacy-0012-baseline). |
| Reference/vector maintenance and activation | Use current bounded job/generation contracts and save the job/plan identity. [Maintenance](../../../docs/maintenance-jobs.md), [activation](../../../docs/activation-generations.md). They can spend budget and mutate explicit targets. |
| `report:*`, evaluation/workbench scripts | Inspect individually. Some query remote APIs/providers, materialize sessions or scan whole local snapshots. Explicit target/data selection is required; names do not establish read-only isolation. |
| Legacy raw SQL repair / catch-up / loops | Investigation compatibility only. Not supported live corpus writers; can bypass modern invariants. Use offline snapshots and the current versioned API for actual corrections. |
| `report:repo-scripts`, `report:repo-cleanup-plan` | Local inventory and cleanup plan, no deletion. `write:repo-report-cleanup` explicitly deletes eligible generated reports under the cap; retain important evidence first. |

All affected SQLite entrypoints require `D1_DB_PATH` to explicitly name an absolute existing readable SQLite file. No machine-specific file or automatic Wrangler-state discovery is used. Pure utility imports remain usable without a database; validation occurs before actual I/O. A snapshot must be captured consistently before analysis; path validation alone does not guarantee that.

When adding a command, document its environment, required inputs, side effects, expected postcondition, acknowledgement/retry identity and rollback. Prefer current versioned APIs over raw SQL. Keep expected support/config files unaliased and retired investigations in `archive/`; use the [repository hygiene policy](../../../docs/repo-hygiene.md). Preserve human approval/provenance and never broaden a repair merely because a report produced candidate rows.

Mutating scripts should be easy to identify from their command names and documented side effects. Retired one-off scripts belong in `apps/api/scripts/archive/`. The cleanup plan deletes nothing unless `REPO_REPORT_CLEANUP_APPLY=1` is explicitly set; `write:repo-report-cleanup` selects that guarded mode with the `REPO_REPORT_CLEANUP_MAX_APPLY` cap. Save required evidence before deleting generated reports.
