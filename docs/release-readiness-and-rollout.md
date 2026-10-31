# Release readiness, canary evidence and rollback (M20)

M20 changes delivery controls; it does not deploy, apply remote migrations, provision preview, change authentication or migrate hosting. `deploy:production --dry-run` remains an offline bundle check. Real releases now require explicit inputs, a clean committed checkout, compatible D1 schema/backfills and successful semantic smoke.

## One contract, two independent probes

`apps/api/release/contract.json` records the supported migration range (currently 33–33), exact migration names, required schema objects/columns, search schema, reviewer policy and immutable vector profile. The candidate's `GET /admin/release-readiness` and the CLI preflight use the same pure policy and read-only SQL. `/health` is liveness only. The readiness endpoint returns 503 for missing/partial schema, missing migration ledger, unsupported future schema, unfinished FTS/facet/recovery state, stale/dirty reviewer materialization, missing catalog state or inconsistent vector configuration. It performs no DDL, catalog bootstrap, job advancement or provider requests. Administrative request quotas apply; authentication is still H01.

The endpoint returns a contract digest, environment, code revision, deployment-config digest, corpus revision and capability. The code/config values are deployment declarations injected by the wrapper, not authenticated attestations of cloud infrastructure. The wrapper selects the same explicit Wrangler environment for database inspection and deployment; H17 separately checks preview resource isolation. Verify the physical binding inventory before release. A restored/copy database's metadata alone cannot prove cloud resource identity.

Both vector flags must be false for an explicitly planned `lexical_only` release, or both true for configured `semantic` operation. Semantic readiness requires the current profile's D1 sync/filter checkpoints and verified published chunk coverage. It does not prove Vectorize's actual remote contents, metadata indexes, provider availability or legal relevance; run the M04/M05 provider reconciliation/probe and M19 domain evaluation separately. The checked-in production flags remain false. No gate silently enables semantic search.

The full contract includes schema retained for source/reviewer receipts and immutable legal editions. Legacy/unverified documents retain their holds; readiness never reapproves them or certifies governing law. The migration ledger must be complete; a hand-imported database without it requires a reviewed schema reconciliation, not fabricated applied-migration rows.

The contract requires the current metadata, document and retrieval posting
triggers. It does not require the six old `*_search_fts` triggers: phrase cutover
freezes their legacy target, and generation recovery deliberately removes them.
Recreating those old triggers would not repair current postings. A populated
recovery regression checks both runtime and CLI readiness, verifies live posting
updates, and rejects removal of each of the nine current posting triggers.

When adding a migration, reader policy or embedding profile, update this contract **in that finding's commit**. Behavioral tests fail stale migration/profile/policy declarations. Generate proposed object/column requirements from a freshly migrated disposable database after the phrase-index cutover, then review the change. Do not widen the supported range based only on a migration being additive: assess old readers, writers, Queue deliveries and retained generation manifests. Compare required columns with `PRAGMA table_xinfo` so generated edition columns are included. D1 schema inspection excludes provider-owned internal tables.

## Local gates and actual Pages output

`CI` is reusable by the manually invoked API release workflow. It runs the deterministic gates plus native Worker integration, builds the **actual** `next-on-pages` output, then runs Playwright through `wrangler pages dev`. Browser flows cover search and dynamic decision-reader navigation/direct reload, reviewer selection races and chat pending/quota/two-tab recovery. The adapter cannot pass CI on `next build` alone. `apps/web/vercel.json` makes Vercel's install explicitly use the repository root, frozen lockfile and development build dependencies; no implicit re-resolution is accepted.

Run `CI` manually at the committed revision to collect hosted quality evidence
while deployment stays held. This workflow uses read-only repository permissions
and runs without deployment credentials. A production release still requires the
successful protected preview and release evidence at the final commit.

Native integration files run in separate matrix jobs, discovered from the test
directory so new rehearsals are included automatically. Each job has a 20-minute
deadline, covering the existing 15-minute recovery test bounds and setup. Browser
validation starts only after every native result succeeds; its required check
fails if any native job fails, is cancelled or is skipped.

Release smoke requests identify the application with
`User-Agent: Beedle-Release-Smoke/1.0`. Cloudflare recommends a descriptive
application identifier for clients calling `workers.dev` and `pages.dev`;
default library identifiers can receive error1010. This requires no access-policy
or credential change. See [Cloudflare's error1010 guidance](https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-1xxx-errors/error-1010/).

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
2. Provision and verify H17's independent preview resources. The [2026-10-27 rollout](deployments/2026-10-27-preview.md) records the provisioned preview identities; missing or placeholder configuration is refused. For initial bootstrap only, the workflow's `prepare_preview` option checks the exact migration ledger, deploys **preview only** under a maintenance hold and produces `passed:false` evidence. Customer search, chat, draft and source routes stay held; administrative/ingestion routes remain available to complete fixtures and bounded backfills. This is operational staging, not an authentication boundary. A production preparation request is rejected before deployment. Preparation evidence cannot authorize production.
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

## Restored corpus across independent databases

The optional `restored-corpus-32-v1` deployment is an initial, read-only lexical release. Each document and all its generations belong to one of 32 independent databases, selected by the first unsigned big-endian 32 bits of its UTF-8 ID SHA-256, modulo 32. The old recovered database and its verification failures remain preserved; this layout is a new target built from verified originals. Full rank=1 external-content FTS, whole-database physical, foreign-key and original-source checks are required for every new target. Native timings do not satisfy the remote gate.

`CORPUS_PARTITIONS_JSON` pins the restore owner, plan/source/catalog hashes, counts, bindings and all 32 corpus revisions. The CLI independently selects every physical binding; the runtime checks all revisions before and after search, cursor reads, readers and global projections. One failed partition fails the operation. Discovery gathers bounded identities across every partition, chooses at most 200 matching documents ordered by decision date and ID, then runs existing passage ranking within those candidates. The final score/ID merge has a distinct ranking version. Counts describe bounded candidates rather than an exhaustive corpus result. Four pipelines at a time share one deadline and a finite aggregate SQL/text/scoring budget below the provider invocation ceiling.

Canonical reviewer state stays beside its documents. The coordinator contains a separate, owned global read cache for exact queue counts, peer groups and pagination. It must match the sealed partition manifest. Application reads cannot build or repair this cache. Runtime and CLI readiness require all base schema/backfill checks, exact restoration ownership, six acknowledged verification receipts at each pinned revision, nonempty provisional publication and the complete cache. `medium-release-5` adds this conditional policy without widening the 33-entry application migration range or reapplying old migrations.

A version-2 smoke plan explicitly declares `publicationPolicy: "unreviewed-provisional-v1"`, `corpusMode: "trusted_plus_provisional"`, lexical capability and `approvedOnly: false` in every case. It additionally verifies provisional result labels and the read-only partitioned readiness contract. Version-1 plans retain their trusted-only meaning. A provisional release cannot use a trusted-only preview report. No synthetic plan belongs in production. The protected exact-run review, exact-final-head CI/preview freshness, original-source, real-data performance, capacity and compatible rollback gates still apply.

For the matching frontend, explicitly build with `NEXT_PUBLIC_CORPUS_POLICY=unreviewed-provisional-v1`. Search then initially includes clearly labelled provisional records, while an explicit trusted-only URL remains respected. The interface labels deferred editing/model actions; the API independently blocks them and has no active Queue consumer or scheduled writer. Vector outboxes remain durable and explicitly deferred, never falsely completed. Global operational health retains their actual counts/ages and distinguishes intentional lexical deferral from active/failed/expired work.

This section describes the supported contract and required rollout evidence; it does not assert a completed production deployment.

Before case import, new empty restoration targets can initialize their current search, facet and reviewer generations with the private, single-database initializer. Its native-approved schema transitions, copied catalog, exact 33-entry ledger, physical resource owner and empty corpus are pinned in the operational plan. The bootstrap has one durable attempt; a lost response requires independent readback, and partial or differing state remains held. The scheduled initializer has no source, model, queue, public or preview access, respects existing leases and keeps immutable tick/error history. This operational setup neither publishes records nor supplies final populated-corpus integrity evidence.

The separate private source Worker accepts one hash-bound partition plan and the matching physical resource owner. It verifies each seeded metadata row and original object version, copies bytes without overwriting, runs existing generation staging, then independently verifies the current original and durable source proof. Its initial build performs at most four ordinary records or one exceptional record per tick, with a 20-second loop budget and a bounded D1 binding-call budget. Unknown outcomes retain their claims, generations and proofs; guarded failures hold the owned controller. The source Worker stops at the next indexing phase. It grants no human approval, publication or final integrity pass. An unreadable original is copied and held without fabricated text. Its original binding is passed only to a get-only capability; models and Queue delivery remain absent.

Future source Worker builds also attempt one immutable failure proof after a
preflight, claim or source failure. The private proof records the stage, binding
method, statement count, SQL fingerprints and recognized error codes/markers.
Messages, bound values, object keys and arbitrary error names are excluded;
message and cause hashes preserve diagnostic correlation. Markers describe
reported errors and do not establish their cause. Conditional R2 creation and
exact byte/version readback prevent an existing proof from being overwritten.
A missing or unacknowledged diagnostic never clears the hold, retries a source
job or supplies an integrity pass. Updating this source does not authorize
redeploying a held execution, resetting its claims or replaying staging writes;
existing deployed bundles and their failed evidence remain preserved.

A selected reprocess after a fully reconciled staging failure uses a distinct
finite operation and immutable conditional claim. Its reviewed plan pins the
physical owner, current schema, held owner/tick/history, original seeded job,
failed generation, current metadata, original source version/bytes and copied
source ETag. The R2 binding's actual copy version is acknowledged and saved
before staging, then checked again afterward. The existing generation workflow
creates a new generation. The expected resulting revision is rehearsed for the
exact record because metadata and edition triggers can increment it more than
once. The old job, failed generation, owner, tick and history remain unchanged
and held. This operation neither finalizes the old source receipt nor permits
bulk continuation. A lost claim or outcome acknowledgment requires observation;
later deliveries cannot reissue the SQL. No human approval, search publication,
full integrity or peak-capacity pass follows from this individual check.

A separately owned storage-only setup may initialize the two new private buckets
and their content-addressed partition plans. It has no database or original-source
binding. A finite admission window, one immutable conditional claim, per-object
intents, conditional puts and full byte/version readback prevent duplicate
scheduled deliveries from replaying writes. Lost acknowledgments remain held for
independent observation; no claim or object is overwritten. Its positive result
and private bindings must be reconciled before retiring the schedule and deleting
the matching helper. This setup supplies no case-processing or integrity credit.

Source metadata seeding is a separate one-attempt operation on an untouched,
owned empty target. It preserves the 33-entry ledger and copied catalog, pins the
native operational schema and compares every stored metadata/job value. It seeds
legacy, unpublished, unconfirmed records and idle controls. An uncertain import
can only be independently observed; a partial import remains held and cannot be
reissued. Case text still comes through the bounded original-source Worker.

Source Workers are uploaded cold with exact source/version and six binding
readbacks. Activation requires every metadata seed and every cold Worker to be
verified. Changing the owned database from seeded to building and enabling its
schedule have separate durable intents. Unknown acknowledgments are observed
without reissuing SQL, uploads or schedule changes. Completed source schedules
can retire only after all source proofs are present and the tick is quiescent.

A directly started private Workflow can run that same selected operation without
Cron propagation delay. Its instance ID and sole plan-hash payload must match the
fixed finite plan. Its single step configures zero retries, while the underlying
immutable R2 claim still prevents SQL reissue after a lost acknowledgment. A
Workflow marked complete with a held domain result remains a failed operation.
The exact source, private bindings, Workflow definition, empty initial instance
list and unchanged predecessor must be verified before one separately recorded
create request. An uncertain create is observed by its exact instance ID; it is
never triggered, restarted or resumed again. This changes neither D1 statement
limits nor the full integrity, publication, capacity and release requirements.

Restoration copy plans take the upload ETag from R2's structured object metadata,
not the management API's HTTP response ETag, which can be weak and describe an
encoded response. For these bounded single-part copies, the metadata's exact key,
size, owner/plan/source metadata and raw ETag must match the full downloaded bytes.
SHA-256 remains the source identity check; MD5 correlates the upload ETag. A
differing binding ETag still holds before staging. Correcting a mistaken operator
plan requires preserving its claim and failure, terminal reconciliation, and a
distinct tested operation; no claim, guard or receipt is cleared.
