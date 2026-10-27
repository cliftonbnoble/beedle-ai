> M20 update: real releases now require schema/backfill checks, private smoke plans and matching preview evidence. Follow [Release readiness and rollout](release-readiness-and-rollout.md); the dry-run commands below remain offline validation.

# Explicit deployment environments (H17)

The API now uses `apps/api/wrangler.json`. Its default environment is local: a dummy D1 ID, separate local R2/queue/rate-limit identities, no scheduled trigger, no AI or Vectorize binding, and no public workers.dev or version-preview route. `pnpm dev:api` uses `wrangler dev --local`. Migrate the disposable local database with `pnpm --filter @beedle/api migrate:local`. Old local database state used a production binding ID and is deliberately not selected automatically. Import only reviewed test fixtures into the new local state.

Every fetch, scheduled event and queue delivery validates `APP_ENV`, readiness and namespace before work. Local mode also removes AI, Vectorize and external LLM credentials from the environment passed to services, including secrets left in an old `.dev.vars`. Local search uses its existing text fallback; local vector work cannot produce successful embeddings. Use the behavioral fake-provider tests for offline vector state-machine coverage. Real-provider testing requires a separately provisioned preview environment.

## Preview preparation

`env.preview` declares independent D1, R2, Vectorize, queue and rate-limit identities and `beedle-docs-preview`. It is intentionally unconfigured (`DEPLOYMENT_READY=false`, placeholder D1 ID and URLs). This change creates no Cloudflare resources. Before enabling preview:

1. Provision a separate preview database, bucket, queue and vector index with the production model's dimensions/metric. Record actual resource IDs and the preview API/web origins in `env.preview`; never copy production identifiers or corpus credentials.
2. Apply the migrations to the preview database explicitly. Configure environment-specific provider secrets with preview budgets and access policies. Do not reuse production secrets. Authentication/perimeter implementation remains H01 and is not supplied by CORS.
3. Set readiness to `true` only after verifying resource identities, route, allowed origin and namespace. `pnpm --filter @beedle/api deploy:preview --dry-run` validates the manifest. The deployment wrapper rejects IDs/names shared with production, placeholders and mismatched environment selection. Then review an explicitly authorized preview deployment separately.
4. Keep preview test corpus and vector IDs disposable. If testing remote bindings from a local runtime, use only a separately reviewed preview config; do not enable remote bindings in the default config.

## Production compatibility

`env.production` retains the existing Worker name `beedle-api`, D1 ID `f7e4fa53-4d05-474f-bf2c-d08a9f6139d5`, bucket `beedle-sources`, queue `beedle-vector-jobs`, index `beedle-docs`, namespace `beedle-docs`, model and rate-limit IDs. Explicitly retaining the Worker name avoids creating an empty replacement under Wrangler's normal environment suffix. Production CORS now lists only `https://beedle-ai.pages.dev`; local and old feature-preview origins are removed. Public version preview URLs are disabled because they would share production bindings.

`pnpm --filter @beedle/api deploy` fails without an environment. Use `deploy:production --dry-run` to inspect the exact bindings; the main-branch workflow now selects production explicitly. Any direct Wrangler maintenance command must select `--env production` and `--remote` deliberately when actually intended. Existing operator scripts with explicit remote flags are still administrative tools, not safe test harnesses. Never use them for local fixtures. Review stored credentials and restrict preview service identities independently of source-code configuration.

Before deploying, export the existing binding inventory and database, and compare document/chunk/vector counts and namespace coverage. Investigate suspected pre-fix contamination from logs and vector metadata; do not delete vectors merely because their IDs look unfamiliar. No production inventory, cleanup, migration or deployment was performed here. Rollback must keep the isolated default and explicit production selection; reverting the old remote-default config reintroduces this issue.

## Web builds

`pnpm dev:web` explicitly selects local mode and accepts only a loopback API. All builds require `NEXT_PUBLIC_APP_ENV=local|preview|production`. Preview and production also require an explicit `NEXT_PUBLIC_API_BASE_URL` HTTPS origin. Preview rejects the reviewed production API; production requires that exact reviewed origin. Custom-domain changes require updating the reviewed origin policy and tests together. There is no NODE_ENV-based production fallback.

For example, after a preview API is provisioned:

```sh
NEXT_PUBLIC_APP_ENV=preview NEXT_PUBLIC_API_BASE_URL=https://YOUR_PREVIEW_API pnpm --filter @beedle/web pages:build
```

Configure variables separately in Pages production and preview build settings. In Pages builds also set `BEEDLE_PRODUCTION_BRANCH` (currently `main` for the API workflow). The build checks `CF_PAGES_BRANCH` against that explicit branch and rejects inconsistent selections. Browser bundles capture public variables at build time; rebuild rather than changing runtime values after publishing. Local production-mode rehearsals can explicitly use `NEXT_PUBLIC_APP_ENV=local` and a loopback API.

Cloudflare documents that bindings/vars must be declared per [Wrangler environment](https://developers.cloudflare.com/workers/wrangler/environments/) and that AI/Vectorize lack [local simulations](https://developers.cloudflare.com/workers/local-development/bindings-per-env/). Pages supplies the [build branch variable](https://developers.cloudflare.com/pages/configuration/build-configuration/); that variable alone is not an environment or authorization policy.
