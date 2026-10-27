# Current operations index

Updated 2026-10-27. Start with the repository README. Run package commands from the checkout root; package filters select the API/web working directory. Paths in `docs/archive/` and original audit evidence are historical references, not machine setup requirements.

## Command posture

| Operation | Target / prerequisites | Side effect and expected postcondition | Rollback / recovery |
| --- | --- | --- | --- |
| Frozen install / types / lint / deterministic tests | This checkout, Node 22.16+, pnpm 9.12.0 | Local dependencies/generated types/test output; all checks pass | Restore the lockfile/declarations only through reviewed source changes; remove disposable outputs |
| `pnpm --filter @beedle/api migrate:local` | Default **local** D1, no remote flag | Writes local migrations; full ledger succeeds, no corpus imported | Use a new disposable persistence directory; preserve existing operator state |
| `pnpm dev:api:local`, `pnpm dev:web:local` | Loopback 8787/5555 and local manifest | Local servers; health responds; empty corpus can have no results | Stop those processes; no cloud rollback |
| `pnpm --filter @beedle/api test:integration` | Fresh temporary local D1/R2/Queue, free loopback ports | Native failure/replay/recovery assertions pass; no cloud/provider calls | Fixture cleanup removes its own temporary state |
| Local Pages build + browser suite | Explicit `NEXT_PUBLIC_APP_ENV=local`, API 8819; test-owned ports 5819/8819 | Actual adapter output and fresh fixture; browser suite passes | Stop test-owned processes; rebuild/discard generated output |
| `pnpm test:search-stack` | Already-running **loopback only** servers, synthetic fixture data | Health/search probes and local reports, no corpus mutation; requires appropriate test corpus | Discard reports; bounded search sessions expire |
| `D1_DB_PATH=/absolute/snapshot.sqlite pnpm --filter @beedle/api report:document-metadata-anomalies` | Explicit existing SQLite snapshot; sqlite3 CLI | Local metadata audit/report; no source writes | Discard report; keep source snapshot |
| Real migration / release / recovery | Explicit isolated preview or reviewed production descriptor, credentials, private artifacts and release evidence | Governed by linked runbooks below; never inferred from a successful local check | Exact compatible version rollback or verified recovery; preserve manifests/receipts/history |

`report:` is a naming convention, not a sandbox: some historical report scripts make API searches/provider calls, create remote paging sessions or scan a full snapshot. Inspect the script and set its target before use. `write:`, `run:`, `backfill:`, `import:`, `loop:` and apply flags can mutate data or spend provider budget. They are not default test commands.

## Maintained guides

- [Architecture](architecture.md), [corpus operations](corpus-operations.md), [API scripts catalog](../apps/api/scripts/README.md).
- [Environment isolation](environment-isolation.md), [deployment entry point](cloudflare-deployment-runbook.md), [release/rollback](release-readiness-and-rollout.md), [recovery](recovery-runbook.md).
- [Integration quality gates](integration-and-quality-gates.md), [runtime types/contracts](runtime-contracts-and-types.md), [dependency posture](dependency-security.md).
- [Telemetry/SLO evidence](operational-telemetry.md), [corpus relevance evaluation](evaluation/independent-holdout.template.json), [engineering audit and repair records](application-engineering-audit-2026-09-29.md).

Authentication/H01 is unfinished; admin endpoints, CORS and local environment guards do not establish access control. Local verification does not certify a deployed perimeter or authorize production operations.

## Historical archive

- [Phase 6A README](archive/readme-through-phase-6a.md).
- [April 2026 deployment instructions](archive/cloudflare-deployment-2026-04-01.md).

The originals are retained with a prominent historical banner. New operational guidance belongs in the maintained guides above; do not append another chronological phase to the README.
