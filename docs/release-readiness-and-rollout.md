# Release readiness, canary evidence and rollback (M20)

M20 changes delivery controls; it does not deploy, apply remote migrations, provision preview, change authentication or migrate hosting. `deploy:production --dry-run` remains an offline bundle check. Real releases now require explicit inputs, a clean committed checkout, compatible D1 schema/backfills and successful semantic smoke.

## One contract, two independent probes

`apps/api/release/contract.json` records the supported migration range (currently 31–31), exact migration names, required schema objects/columns, search schema, reviewer policy and immutable vector profile. The candidate's `GET /admin/release-readiness` and the CLI preflight use the same pure policy and read-only SQL. `/health` is liveness only. The readiness endpoint returns 503 for missing/partial schema, missing migration ledger, unsupported future schema, unfinished FTS/facet/recovery state, stale/dirty reviewer materialization, missing catalog state or inconsistent vector configuration. It performs no DDL, catalog bootstrap, job advancement or provider requests. Administrative request quotas apply; authentication is still H01.

The endpoint returns a contract digest, environment, code revision, deployment-config digest, corpus revision and capability. The code/config values are deployment declarations injected by the wrapper, not authenticated attestations of cloud infrastructure. The wrapper selects the same explicit Wrangler environment for database inspection and deployment; H17 separately checks preview resource isolation. Verify the physical binding inventory before release. A restored/copy database's metadata alone cannot prove cloud resource identity.

Both vector flags must be false for an explicitly planned `lexical_only` release, or both true for configured `semantic` operation. Semantic readiness requires the current profile's D1 sync/filter checkpoints and verified published chunk coverage. It does not prove Vectorize's actual remote contents, metadata indexes, provider availability or legal relevance; run the M04/M05 provider reconciliation/probe and M19 domain evaluation separately. The checked-in production flags remain false. No gate silently enables semantic search.

The full contract includes schema retained for source/reviewer receipts and immutable legal editions. Legacy/unverified documents retain their holds; readiness never reapproves them or certifies governing law. The migration ledger must be complete; a hand-imported database without it requires a reviewed schema reconciliation, not fabricated applied-migration rows.

When adding a migration, reader policy or embedding profile, update this contract **in that finding's commit**. Behavioral tests fail stale migration/profile/policy declarations. Generate proposed object/column requirements from a freshly migrated disposable database after the phrase-index cutover, then review the change. Do not widen the supported range based only on a migration being additive: assess old readers, writers, Queue deliveries and retained generation manifests. Compare required columns with `PRAGMA table_xinfo` so generated edition columns are included. D1 schema inspection excludes provider-owned internal tables.

## Local gates and actual Pages output

`CI` is reusable by the manually invoked API release workflow. It runs the deterministic gates plus native Worker integration, builds the **actual** `next-on-pages` output, then runs Playwright through `wrangler pages dev`. Browser flows cover search and dynamic decision-reader navigation/direct reload, reviewer selection races and chat pending/quota/two-tab recovery. The adapter cannot pass CI on `next build` alone. `apps/web/vercel.json` makes Vercel's install explicitly use the repository root, frozen lockfile and development build dependencies; no implicit re-resolution is accepted.

To reproduce without a real provider:

```sh
pnpm install --frozen-lockfile
NEXT_PUBLIC_APP_ENV=local NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8819 \
  NEXT_TELEMETRY_DISABLED=1 VERCEL_TELEMETRY_DISABLED=1 pnpm --filter @beedle/web pages:build
BEEDLE_E2E_ADAPTER=pages pnpm --filter @beedle/web test:e2e
```

Install the locked Playwright browser first, or use `BEEDLE_TEST_CHROME_CHANNEL=chrome` locally. The test fixture uses fresh local resources and no paid inference. These checks do not establish a real deployed preview's routing, perimeter, provider behavior or resource isolation.

## Explicit release sequence

Configure protected GitHub environments `api-preview`, `api-production`, and the existing `production-d1-migrations`, with environment-specific Cloudflare credentials and reviewed human controls. Require both CI jobs in branch protection. These hosted settings were not changed here. API release runs only by `workflow_dispatch` from `main`; migrations remain manual. Deployment and production migration workflows share a non-cancelling `api-release-production` lock. This prevents overlap within those workflows, not uncoordinated CLI/console operations; use the broader M26 migration/recovery procedure for those.

1. Back up/rehearse against a consistent populated isolated corpus. Review legacy holds, legal edition uncertainty, v3 generation/writer compatibility, data volume and quotas. Apply additive migrations deliberately before the new API reader. Finish the bounded search/reviewer/catalog/vector maintenance described in the associated runbooks. Stop incompatible old writers, scheduled deliveries and ingestion as required; applying a migration does not authorize release.
2. Provision and verify H17's independent preview resources. The checked-in preview is intentionally unconfigured and normal deployment refuses it. For initial bootstrap only, the workflow's `prepare_preview` option checks the exact migration ledger, deploys **preview only** under a maintenance hold and produces `passed:false` evidence. Customer search, chat, draft and source routes stay held; administrative/ingestion routes remain available to complete fixtures and bounded backfills. This is operational staging, not an authentication boundary. A production preparation request is rejected before deployment. Preparation evidence cannot authorize production.
3. Run a **normal preview release** at the final committed revision. It performs the full schema/backfill preflight before removing the hold and then checks the candidate. Supply `RELEASE_SMOKE_PLAN_JSON` as an environment-specific private GitHub secret. Each plan must include a positive query, an empty negative query, an explicitly filtered query, exact expected document identities, expected capability and SHA-256 of at least one original source. A small bounded synthetic canary is appropriate for delivery; independent real relevance/provider validation remains M19.
4. Review that isolated canary, actual provider probes, legal/domain evidence, hosting acceptance and rollback plan. For production, enter the successful preview workflow **run ID at the exact same commit**. The workflow verifies its GitHub repository, workflow path, branch, revision and success before downloading its private artifact. The wrapper additionally requires a matching contract/config digest, successful readiness/search/source checks and evidence no older than 24 hours. A preparation run, another commit, changed preview config or incomplete report is refused.
5. A protected production release repeats the read-only target preflight, records the previous Worker deployment, deploys that committed candidate with code/config identifiers and executes the production-specific smoke plan. Positive/negative/filtered document sets must match; original source bytes must match their SHA, with no reconstructed-source fallback. Candidate readiness and corpus revision must remain stable across checks. HTTP 200 alone is insufficient. Smoke calls are bounded and perform no administrative corpus writes; normal search may create its bounded paging session.

The preview is the automated canary. This change does not silently start a mixed-version production rollout. With M18's new writer/generation semantics, an old production version may be unsafe alongside the candidate. Rehearse a separate gradual rollout only after proving both versions' readers **and writers** support the same data contract; otherwise use a reviewed maintenance cutover. First production cutover/backfill access and old-writer shutdown need an operator-controlled plan; no automatic production bootstrap bypass is provided.

For an explicitly authorized operator run, the same wrapper requires private plan/evidence paths and a new report path:

```sh
pnpm --filter @beedle/api release:preflight --environment preview
BEEDLE_RELEASE_SMOKE_PLAN=/private/release/preview-plan.json \
  BEEDLE_RELEASE_REPORT=/private/release/preview-report.json \
  pnpm --filter @beedle/api deploy:preview
```

Production additionally requires `BEEDLE_PREVIEW_EVIDENCE` pointing to that matching successful preview report. Direct CLI artifacts are declarations; the GitHub workflow adds trusted-run provenance checks. `BEEDLE_PREPARE_PREVIEW=1` is limited to a separately reviewed isolated-preview bootstrap. Never point these examples at production as a test.

A plan has this shape; replace every placeholder and supply all three classes before use:

```json
{"version":1,"capability":"lexical_only","cases":[{"id":"positive","category":"positive","query":"REVIEWED_QUERY","filters":{"approvedOnly":true},"expectedDocumentIds":["REVIEWED_ID"]},{"id":"negative","category":"negative","query":"REVIEWED_EMPTY_QUERY","filters":{"approvedOnly":true},"expectedDocumentIds":[]},{"id":"filtered","category":"filtered","query":"REVIEWED_QUERY","filters":{"documentIds":["REVIEWED_ID"],"approvedOnly":true},"expectedDocumentIds":["REVIEWED_ID"]}],"sources":[{"documentId":"REVIEWED_ID","sha256":"REVIEWED_ORIGINAL_SOURCE_SHA256"}]}
```

Plans and source facts stay private. Reports contain identities/digests, corpus revision and check counts, not queries, documents, provider payloads or secrets. Reports reserve a new mode-0600 file before deployment; pending/failed receipts cannot authorize release. Interrupted or malformed evidence fails closed. After a post-deploy failure, rerun the explicit `release:smoke` command into a new report only after investigating the candidate; do not blindly redeploy. CI retains release/previous-deployment artifacts for two days; archive approved recovery evidence in the controlled release record if longer retention is needed.

## Rollback criteria and limits

Stop promotion on missing readiness, schema drift, unexpected empty/nonempty identities, wrong filter scope, source mismatch/fallback, capability mismatch, corpus drift or provider/operational regressions. A post-deploy smoke failure explicitly reports that deployment already happened and fails the workflow; it **does not automatically restore an old writer**. Preserve the prior deployment artifact and incident evidence, hold incompatible writes/deliveries, and choose a reviewed compatible version or a forward repair.

For a compatible code rollback after additive migrations, keep the database at its migrated version and preserve all receipts, source objects, edition snapshots and v3 generation artifacts. Do not reverse migrations or restore just D1 over newer R2/vector state. The populated rehearsal proves source identity and legacy holds survive additive migrations and the unchanged prior search reader still operates; it does not validate every historical production binary. An older writer that does not understand v3 bindings must not be re-enabled. M26 owns consistent restore/reconciliation.

Real isolated-preview provisioning, deployed adapter/provider verification, hosted branch/environment settings, independent human/domain judgments and a first production cutover remain external release prerequisites. No cloud write, production inspection, deployment, push or hosting migration was performed in this remediation.
