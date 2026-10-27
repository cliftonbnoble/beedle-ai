# Beedle AI

Cloudflare Worker API, Next.js web UI and shared TypeScript contracts for decision ingestion/review, published-corpus search, research chat and drafting. Model output is unverified research assistance; review its evidence and original sources.

## Local setup

Run commands from this checkout's root; no fixed checkout path is required. Use Node **22.16+** (CI uses the latest Node 22), pnpm **9.12.0**, Git and the `sqlite3` CLI (snapshot/operator tests). The recovery tool requires `node:sqlite` backup support. Install with the frozen lockfile:

```sh
corepack enable
corepack prepare pnpm@9.12.0 --activate
pnpm install --frozen-lockfile
pnpm --filter @beedle/api migrate:local
```

The migration command targets **local** D1 only and writes disposable state under `apps/api/.wrangler`. It must finish all migrations. It does not import a corpus. To reset a rehearsal, create a new isolated local persistence directory; do not delete an operator's existing state or reuse a production snapshot as test data.

In two terminals at the checkout root:

```sh
pnpm dev:api:local
```

```sh
pnpm dev:web:local
```

API: `http://127.0.0.1:8787`; web: `http://localhost:5555`. `/health` proves liveness only. Empty local state has no corpus results; populated upgrades can remain held until bounded index maintenance completes. Read [search readiness](docs/search-readiness.md) before importing fixtures. Local mode strips paid provider bindings/credentials from runtime services. The isolated test fixture supplies synthetic data and fake providers without touching this local development state.

## Verify

```sh
pnpm typecheck
pnpm --filter @beedle/api types:check
pnpm --filter @beedle/api lint
pnpm --filter @beedle/api test:contracts
pnpm --filter @beedle/api test:behavior
pnpm --filter @beedle/api test:source
pnpm --filter @beedle/api test:utils
pnpm --filter @beedle/web test:web
```

Types generation/check and native/browser tests open local loopback sockets. For actual Worker/Pages acceptance, follow [integration and quality gates](docs/integration-and-quality-gates.md) and [the current release guide](docs/release-readiness-and-rollout.md). Native tests create fresh temporary D1/R2/Queue resources; the recovery drill can take several minutes. Run it before the adapter build to avoid resource contention. No cloud migration or deployment occurs in these checks.

## Documentation

- [Operations index](docs/operations.md): current guides, side effects, targets and rollback.
- [Architecture](docs/architecture.md): boundaries and current invariants.
- [Corpus operations](docs/corpus-operations.md): source, reviewer, search/vector maintenance and legacy script limits.
- [Deployment entry point](docs/cloudflare-deployment-runbook.md) and [recovery](docs/recovery-runbook.md).
- [Engineering findings and repair records](docs/application-engineering-audit-2026-09-29.md); [Medium verification](docs/medium-fix-verification-2026-10-27.md).
- [Historical Phase 6A README](docs/archive/readme-through-phase-6a.md), preserved as history rather than current instructions.

Authentication (H01) is deferred. CORS, admin URL names, local success and the release gate do not establish authorization or deployed perimeter protection. Browser intake remains planned. The current Pages adapter has an upstream support gap recorded in [ADR 0001](docs/decisions/0001-cloudflare-web-hosting.md); no hosting migration or production acceptance is implied.
