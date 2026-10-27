# Cloudflare deployment entry point

Updated 2026-10-27. This entry point replaces the April 2026 procedure. That procedure is preserved in [the historical archive](archive/cloudflare-deployment-2026-04-01.md); do not execute its default-environment commands or rely on its authentication claims.

Use the following maintained guides in order:

1. [Environment isolation](environment-isolation.md): `apps/api/wrangler.json`, local/preview/production identities, explicit public build variables and unconfigured preview prerequisites. The default config is local; preview must have independent resources. Readiness declarations are not authorization.
2. [Release readiness and rollout](release-readiness-and-rollout.md): supported schema from `apps/api/release/contract.json`, bounded backfills, private smoke plan, exact-commit preview evidence, manual protected release and rollback. `/health` is not a release gate. Apply all required migrations before the matching new reader; inspect actual bindings separately from declarations.
3. [Integration and quality gates](integration-and-quality-gates.md): frozen install, native Worker/D1/R2/Queue tests and browser tests through the actual Pages adapter. `next build` alone is insufficient.
4. [Recovery runbook](recovery-runbook.md): consistent SQLite/R2 capture, explicit source/target descriptors, reviewed plan checksum, isolated empty target, resumable restore and content verification. Never substitute a raw SQLite import plus heuristic repair.
5. [Operational telemetry](operational-telemetry.md): readiness, latency/cost/error budgets and incident evidence. Synthetic local results are not populated-corpus or production SLO evidence.

## Offline checks

From the checkout root, with installed locked dependencies:

```sh
pnpm --filter @beedle/api deploy:production --dry-run
NEXT_PUBLIC_APP_ENV=local NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8819 \
  NEXT_TELEMETRY_DISABLED=1 VERCEL_TELEMETRY_DISABLED=1 pnpm --filter @beedle/web pages:build
```

The first command selects the production **configuration for a bundle-only check**; it performs no deployment or remote migration. Expect a successful bundle/binding summary. The second writes generated local Pages output and captures a loopback API in that bundle; it must never be published as a production build. Discard/rebuild generated output to undo either check.

## Real release boundary

Real preview/production operations require the explicit environment-specific resources, credentials, private plan/report paths and evidence in the release guide. The GitHub workflows are manual, use protected environments and coordinate migration/deployment locks. Verify hosted branch/environment protections; repository code cannot prove those settings. A failed or stale preview is not permission to bypass the gate.

The current web path remains Next.js on the gated Pages adapter. Its support gap, replacement acceptance and browser-origin/storage continuity are tracked in [ADR 0001](decisions/0001-cloudflare-web-hosting.md). Authentication/H01 and deployed Access/perimeter verification remain separate unfinished work. No command in this documentation update authorizes a cloud change.
