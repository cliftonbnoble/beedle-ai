# Cloudflare Deployment Runbook

This runbook is for deploying the current Beedle app from GitHub to Cloudflare in a way that is:

- repeatable
- easy to maintain
- safe enough for judge testing behind Cloudflare Access
- friendly to continued search and retrieval improvement

It is written for this repo as it exists on April 1, 2026.

## 1. Operating model

We are using only two environments:

- local development on your computer
- one Cloudflare production environment

That means:

- `localhost` is where we tune search, retrieval, imports, and UI changes
- Cloudflare is where judges test the live app

We are not creating a separate Cloudflare staging environment.

## 2. What we are deploying

This repo is a monorepo with two deployable apps:

- Web app: `/Users/cliftonnoble/Documents/Beedle AI App/apps/web`
- API Worker: `/Users/cliftonnoble/Documents/Beedle AI App/apps/api`

Important repo facts:

- The web app calls the API using `NEXT_PUBLIC_API_BASE_URL`.
  - file: [/Users/cliftonnoble/Documents/Beedle AI App/apps/web/src/lib/api.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/web/src/lib/api.ts)
- The web app already has a Cloudflare Pages build command:
  - `pnpm --filter @beedle/web pages:build`
- The API is already a Cloudflare Worker with bindings for:
  - `DB` (D1)
  - `SOURCE_BUCKET` (R2)
  - `VECTOR_INDEX` (Vectorize)
  - `AI` (Workers AI)
  - file: [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/wrangler.toml](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/wrangler.toml)
- The API already supports serving source decisions through `/source/:documentId` when stored source links use the placeholder `example.invalid` domain.
  - file: [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/src/services/storage.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/src/services/storage.ts)

That last point is useful because it lets us keep source decisions protected behind the API instead of making R2 public immediately.

## 3. Naming and hostnames

We will use `beedle-ai` as the production app name.

Recommended Cloudflare naming:

- Pages project name: `beedle-ai`
- Worker name: `beedle-api`
- D1 database: `beedle`
- R2 bucket: `beedle-sources`
- Vectorize index: `beedle-docs`

Recommended first live hostnames:

- Web: `beedle-ai.pages.dev`
- API: `beedle-ai.clifton23.workers.dev`

If you want a custom domain later, the clean production pattern is:

- Web: `beedle-ai.<your-domain>`
- API: `api.beedle-ai.<your-domain>`

If you do not yet have the final domain chosen, we can still complete almost all of the deployment using the default Cloudflare hostnames.

## 4. Recommended deployment model

Use:

- Web: Cloudflare Pages with GitHub integration
- API: GitHub Actions with Wrangler deploy

Why:

- Pages GitHub integration is the easiest way to get automatic web deploys on every push.
- The API needs a slightly more controlled path because D1 migrations must be applied deliberately.

## 5. The safest sequence

Follow this order:

1. Push the repo to GitHub.
2. Create one set of Cloudflare production resources.
3. Update `wrangler.toml` with the real production resource IDs.
4. Add Cloudflare secrets.
5. Apply D1 migrations.
6. Deploy the API.
7. Create the Pages project and connect GitHub.
8. Set the Pages environment variable that points the web app at the production API.
9. Add Cloudflare Access to the web app immediately.
10. Add Access to the API after the small credentials/CORS patch described below.
11. Load your corpus.
12. Test live search and assistant behavior.

## 6. One important Cloudflare Access caveat

Right now the browser calls the API cross-origin and the API responds with wildcard CORS:

- web fetch base: [/Users/cliftonnoble/Documents/Beedle AI App/apps/web/src/lib/api.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/web/src/lib/api.ts)
- API CORS headers: [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/src/index.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/src/index.ts)

That is fine with no Access in front of the API.

If you protect the API hostname with Cloudflare Access, browser requests to that separate hostname will need credential-aware CORS.

Cloudflare docs:

- [Cloudflare Access CORS docs](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/cors/)

Practical meaning for this repo:

- You can protect the web app with Access right away.
- Before you protect the API hostname with Access, make this small patch:
  - set browser fetches to `credentials: "include"` in [/Users/cliftonnoble/Documents/Beedle AI App/apps/web/src/lib/api.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/web/src/lib/api.ts)
  - replace wildcard CORS with an explicit allowed-origin list in [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/src/index.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/src/index.ts)
  - add `access-control-allow-credentials: true`

Recommendation:

- First deploy: protect the web app with Access immediately.
- Next small infrastructure pass: patch API CORS/credentials, then protect the API too.

## 7. Push the repo to GitHub first

From the repo root:

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App"
git remote add origin git@github.com:<your-org-or-user>/<your-repo>.git
git push -u origin main
```

Because you want every push to trigger deployment, `main` can be the live branch.

If later you want a buffer, we can always introduce a `release` branch. You do not need that yet.

## 8. Create Cloudflare resources

We only need one production set.

### 8.1 Create D1

Cloudflare docs:

- [D1 Wrangler commands](https://developers.cloudflare.com/d1/wrangler-commands/)
- [D1 migrations](https://developers.cloudflare.com/d1/reference/migrations/)

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
pnpm wrangler d1 create beedle
```

Copy the returned `database_id`.

### 8.2 Create R2

Cloudflare docs:

- [Create R2 buckets](https://developers.cloudflare.com/r2/buckets/create-buckets/)

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
pnpm wrangler r2 bucket create beedle-sources
```

### 8.3 Create Vectorize

Cloudflare docs:

- [Vectorize create indexes](https://developers.cloudflare.com/vectorize/best-practices/create-indexes/)
- [Workers AI bge-base-en-v1.5 model page](https://developers.cloudflare.com/workers-ai/models/bge-base-en-v1.5/)

This repo uses `@cf/baai/bge-base-en-v1.5`, which Cloudflare documents as a 768-dimensional embedding model.

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
pnpm wrangler vectorize create beedle-docs --dimensions=768 --metric=cosine
```

### 8.4 Workers AI

The API already expects a Workers AI binding named `AI`.

You do not need to create a separate model object, but Workers AI must be enabled on the Cloudflare account.

## 9. Update API Wrangler config

Current file:

- [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/wrangler.toml](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/wrangler.toml)

Right now it still contains placeholder local values like:

- `database_id = "replace-me"`
- `bucket_name = "beedle-sources"`
- placeholder proxy URLs

For your one-environment setup, the simplest production configuration is:

```toml
name = "beedle-api"
main = "src/index.ts"
compatibility_date = "2025-02-15"
compatibility_flags = ["nodejs_compat"]

[vars]
AI_EMBEDDING_MODEL = "@cf/baai/bge-base-en-v1.5"
VECTOR_NAMESPACE = "beedle-docs"
CORS_ALLOWED_ORIGINS = "http://localhost:5555,http://127.0.0.1:5555,https://beedle-ai.pages.dev"
R2_PUBLIC_BASE_URL = "https://example.invalid/r2"
SOURCE_PROXY_BASE_URL = "https://beedle-ai.clifton23.workers.dev"
LLM_BASE_URL = "https://api.openai.com/v1"
LLM_MODEL = "gpt-4.1-mini"

[[d1_databases]]
binding = "DB"
database_name = "beedle"
database_id = "f7e4fa53-4d05-474f-bf2c-d08a9f6139d5"

[[r2_buckets]]
binding = "SOURCE_BUCKET"
bucket_name = "beedle-sources"

[[vectorize]]
binding = "VECTOR_INDEX"
index_name = "beedle-docs"
remote = true

[ai]
binding = "AI"
remote = true
```

Two important choices here:

- keep `R2_PUBLIC_BASE_URL` as `https://example.invalid/r2`
- set `SOURCE_PROXY_BASE_URL` to the real API hostname

Why:

- this causes source decision links to flow through the Worker’s `/source/:documentId` route
- that keeps decision files protected while the app is private behind Access

## 10. Add required secrets

The API currently expects at least:

- `LLM_API_KEY`

Set it:

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
pnpm wrangler secret put LLM_API_KEY
```

If you add more secrets later, manage them with `wrangler secret put`, not inside `wrangler.toml`.

## 11. Apply D1 migrations

This repo already has migrations:

- [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/migrations/0001_init.sql](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/migrations/0001_init.sql)
- through
- [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/migrations/0007_search_runtime_indexes.sql](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/migrations/0007_search_runtime_indexes.sql)

Apply them to production D1:

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
pnpm wrangler d1 migrations apply beedle
```

## 12. Corpus strategy

You have two valid choices.

### Option A: Recommended

Rebuild remote production from source decision files.

Use this if you want the cleanest long-term setup.

Flow:

1. deploy the production API
2. point your import scripts at the live API using `API_BASE_URL=https://<your-api-hostname>`
3. import source decisions into remote R2 and D1
4. run retrieval/searchability/vector backfills deliberately

### Option B: Faster carry-over

Export the local SQLite database and import it into remote D1.

Cloudflare docs:

- [D1 import/export](https://developers.cloudflare.com/d1/best-practices/import-export-data/)

If you do this:

- export SQL from local sqlite
- import SQL into remote D1
- separately move the R2 objects into remote R2

This is acceptable if speed matters more than cleanliness.

My recommendation for your app:

- if you can tolerate a little setup time, use Option A
- it is the least confusing long-term, especially as search keeps improving

## 13. Deploy the API from GitHub Actions

Use GitHub Actions so every push to GitHub can deploy the Worker.

Create:

- `.github/workflows/deploy-api.yml`

Recommended GitHub secrets:

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

Recommended workflow:

```yaml
name: Deploy API

on:
  push:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: apps/api
    steps:
      - uses: actions/checkout@v4

      - uses: pnpm/action-setup@v4
        with:
          version: 9

      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: pnpm

      - run: pnpm install --frozen-lockfile

      - name: Apply D1 migrations
        run: pnpm wrangler d1 migrations apply beedle
        env:
          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}

      - name: Deploy worker
        run: pnpm wrangler deploy
        env:
          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
```

This workflow now exists in the repo here:

- [/Users/cliftonnoble/Documents/Beedle AI App/.github/workflows/deploy-api.yml](/Users/cliftonnoble/Documents/Beedle%20AI%20App/.github/workflows/deploy-api.yml)

Cloudflare docs:

- [Workers CI/CD](https://developers.cloudflare.com/workers/ci-cd/)
- [Workers Builds](https://developers.cloudflare.com/workers/ci-cd/builds/)

## 14. Deploy the web app from Cloudflare Pages

Use Cloudflare Pages GitHub integration.

Cloudflare docs:

- [Pages Git integration](https://developers.cloudflare.com/pages/get-started/git-integration/)

In Cloudflare:

1. Go to Workers & Pages.
2. Create a Pages project.
3. Connect the GitHub repo.
4. Name the Pages project `beedle-ai`.

Recommended Pages settings for this monorepo:

- Framework preset: `None`
- Root directory: repo root
- Build command:

```bash
pnpm install --frozen-lockfile && pnpm --filter @beedle/web pages:build
```

- Build output directory:

```bash
apps/web/.vercel/output/static
```

Why repo root:

- the web app depends on workspace packages like `@beedle/shared`
- repo-root builds are the least fragile way to build this monorepo on Pages

### Required Pages environment variable

Set:

- `NEXT_PUBLIC_API_BASE_URL=https://beedle-ai.clifton23.workers.dev`

If you are using the default Worker hostname first, use that exact URL.

## 15. Add Cloudflare Access

Cloudflare docs:

- [Self-hosted applications](https://developers.cloudflare.com/cloudflare-one/applications/configure-apps/self-hosted-apps/)
- [Access policies](https://developers.cloudflare.com/cloudflare-one/policies/access/)

### First Access step: web app

Protect the web app immediately.

Because you are using the production `beedle-ai.pages.dev` domain, do not stop at the preview-deployments toggle alone. Cloudflare documents that the first toggle protects preview URLs only, and then you must adjust the resulting Access application to secure the actual `beedle-ai.pages.dev` hostname.

Cloudflare docs:

- [Pages preview deployment Access note](https://developers.cloudflare.com/pages/configuration/preview-deployments/)
- [Pages known issues: securing your `*.pages.dev` domain behind Access](https://developers.cloudflare.com/pages/platform/known-issues/)

In Zero Trust:

1. Go to Access controls > Applications.
2. In the Pages project, first enable the Pages Access policy.
3. Then open the Access application that Cloudflare created for the project.
4. Change the public hostname from the preview wildcard pattern to the actual production hostname `beedle-ai.pages.dev`.
5. Add an Allow policy.

Recommended pilot policy:

- Include -> Emails -> exact judge email addresses

Or if you want a slightly broader pilot:

- Include -> Emails ending in -> your firm domain

### Second Access step: API

Do this after the small browser credentials / API CORS patch from Section 6.

Then create a second Access application for the API hostname and reuse the same allowlist.

Because the API is on `beedle-ai.clifton23.workers.dev`, you can enable Cloudflare Access on the `workers.dev` route itself.

Cloudflare docs:

- [Workers `workers.dev` access management](https://developers.cloudflare.com/workers/configuration/routing/workers-dev/)

## 16. Keep source decisions protected

Do not make R2 public yet unless you truly need it.

Cloudflare docs:

- [R2 public buckets](https://developers.cloudflare.com/r2/buckets/public-buckets/)

For this app, the cleanest private setup is:

- keep `R2_PUBLIC_BASE_URL` as `https://example.invalid/r2`
- set `SOURCE_PROXY_BASE_URL` to the real API URL

That makes source links resolve through:

- `/source/:documentId`

This is already supported by:

- [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/src/services/storage.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/src/services/storage.ts)
- [/Users/cliftonnoble/Documents/Beedle AI App/apps/api/src/routes/source.ts](/Users/cliftonnoble/Documents/Beedle%20AI%20App/apps/api/src/routes/source.ts)

## 17. Keep search easy to improve after deploy

This matters a lot for your app.

Do not treat Cloudflare as the place where search gets tuned.

Recommended split:

### Localhost remains the tuning lab

Use localhost for:

- retrieval experiments
- ranking changes
- corpus repair
- import dry-runs
- index-code repair
- regression harness work
- overnight tuning or backfill jobs

Why:

- many repo scripts already assume the local API is `http://127.0.0.1:8787`
- many audit scripts inspect the local `.wrangler/state` sqlite directly
- localhost is still the fastest place to iterate safely

### Cloudflare becomes the judge-validation environment

Use the Cloudflare deployment for:

- real judge workflow testing
- UI validation
- live search spot checks
- assistant behavior validation
- confirmation that local improvements behave correctly in production

### Recommended habit

Before pushing search changes to GitHub, run locally:

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
pnpm report:keyword-regression-medium
pnpm report:issue-query-medium
```

After deploy, do live smoke checks against production.

For scripts that support `API_BASE_URL`, point them at the production API when you want a remote check.

Example:

```bash
cd "/Users/cliftonnoble/Documents/Beedle AI App/apps/api"
API_BASE_URL="https://<your-api-hostname>" pnpm report:issue-query-medium
```

Be conservative with any data-changing scripts against the remote environment.

Promotion flow should be:

1. tune locally
2. validate locally
3. push to GitHub
4. let Cloudflare deploy
5. validate production behavior
6. only then run remote data-changing steps if needed

## 18. Minimum first-deploy checklist

### GitHub

- push repo to GitHub
- add Cloudflare and GitHub secrets

### Cloudflare resources

- create D1 `beedle`
- create R2 `beedle-sources`
- create Vectorize `beedle-docs`
- enable Workers AI

### API

- update `apps/api/wrangler.toml`
- add `LLM_API_KEY`
- apply D1 migrations
- deploy Worker

### Web

- create Pages project named `beedle-ai`
- connect GitHub
- set build command and output directory
- set `NEXT_PUBLIC_API_BASE_URL`
- deploy web app

### Access

- protect web app with Access
- patch API credentials/CORS
- then protect API with Access

### Data

- load corpus
- validate search
- validate assistant

## 19. My recommendation for your exact setup

Because you want to keep this simple:

1. Use `localhost` as your only development environment.
2. Use one Cloudflare production environment.
3. Name the Pages app `beedle-ai`.
4. Keep source decisions behind the API proxy route.
5. Protect the web app with Cloudflare Access immediately.
6. Add API Access right after the small credentials/CORS patch.
7. Keep all serious search tuning local first, then push to GitHub.

That gives you the simplest system that still supports:

- continuous deploys from GitHub
- private judge testing
- and ongoing search improvement without turning production into the tuning lab

## 20. Official references

- [Cloudflare Pages Git integration](https://developers.cloudflare.com/pages/get-started/git-integration/)
- [Cloudflare Pages CI direct upload](https://developers.cloudflare.com/pages/how-to/use-direct-upload-with-continuous-integration/)
- [Cloudflare Workers CI/CD](https://developers.cloudflare.com/workers/ci-cd/)
- [Cloudflare Workers Builds](https://developers.cloudflare.com/workers/ci-cd/builds/)
- [Cloudflare D1 Wrangler commands](https://developers.cloudflare.com/d1/wrangler-commands/)
- [Cloudflare D1 migrations](https://developers.cloudflare.com/d1/reference/migrations/)
- [Cloudflare D1 import/export](https://developers.cloudflare.com/d1/best-practices/import-export-data/)
- [Cloudflare Vectorize create indexes](https://developers.cloudflare.com/vectorize/best-practices/create-indexes/)
- [Cloudflare Workers AI bge-base-en-v1.5](https://developers.cloudflare.com/workers-ai/models/bge-base-en-v1.5/)
- [Cloudflare Access self-hosted applications](https://developers.cloudflare.com/cloudflare-one/applications/configure-apps/self-hosted-apps/)
- [Cloudflare Access policies](https://developers.cloudflare.com/cloudflare-one/policies/access/)
- [Cloudflare Access CORS](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/cors/)
- [Cloudflare R2 public buckets](https://developers.cloudflare.com/r2/buckets/public-buckets/)
