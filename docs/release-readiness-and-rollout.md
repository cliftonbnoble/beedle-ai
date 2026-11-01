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

The restoration capacity observer records only acknowledged D1 result metadata
(`meta.size_after`) and bounded phase maxima. Missing or invalid samples supply
no capacity evidence. Reaching the 9 GB guard preserves the acknowledged
checkpoint and requires the owning writer to hold; it never retries SQL. These
committed-size samples cannot establish an unobserved transient peak. Controlled
retained, failed, staged, second-reprocess and restore measurements, with physical
resource bookends, remain required. This observer alone enables no writer.

A new private source execution uses separate run, tick and history rows and a
conditional current pointer. Terminal rows remain immutable. Initialization pins
the original seeded or held root, every known V2 hold, preserved case artifacts
and the complete prior execution journal. It adds the reviewed operational schema
once, or appends a distinct run after a proven terminal predecessor. It never
resets the earlier singleton, source claims, failed generations or staged rows.

Each zero-retry Workflow step can claim exactly one untouched source. Its D1
budget is 700 binding calls, with a separate bounded 30-call reserve for holding
the owned execution after failure. The 20-second loop budget controls admission
to that single case; it does not cancel an outstanding SQL request. CPU remains
30 seconds, individual SQL limits remain enforced, and the whole instance's
explicit request budget must cover initialization, every source and completion.
The supported larger instance budget applies only to a new reviewed helper; it
does not modify or restart a failed older version.

An immutable conditional proof claim precedes each step. Lost claim, SQL or
outcome acknowledgments require independent observation. A different completion
execution may acknowledge already reconciled full coverage without processing
another source, while retaining the prior hold. Unreadable originals receive a
truthful held receipt and no generated text. These operational receipts supply
neither human review nor publication, full integrity or remote peak-capacity
approval. Exact frozen native checks, current binding types, source readback and
fresh ownership, schema, source, lease, hold and access guards precede live use.

Private restoration controls also validate configuration capacity before upload.
Each text variable is bounded to 5,000 UTF-8 bytes, below the provider's 5 KiB
limit. A multi-target terminal operation keeps a small header containing each
target plan's digest and stores the plans in separate bounded variables. The
Workflow checks every digest and target identity before creating a durable claim
or issuing SQL. An upload rejected for configuration size is preserved and
independently reconciled before preparing a distinct operation; its intent is
never replayed. Terminal disposition records an abandoned executor's actual
state and preserves source claims, staged data and published generations. It
does not establish source success, integrity or publication readiness.

A later source recovery pins every known held executor in addition to the
original held root. Version-2 selected-reprocess, forward-completion and
published-receipt plans require the exact successor control, tick and history
digests, or explicit proof that no successor tables exist. Unknown journals fail
closed. A selected reprocess can preserve an abandoned generation's nonempty
staged rows by their exact digest; forward completion verifies that same digest
again and appends one receipt at the planned coverage count. Published text is
reconciled without parsing or staging it again. None of these operations clears
an earlier hold or grants content approval.

For the matching frontend, explicitly build with `NEXT_PUBLIC_CORPUS_POLICY=unreviewed-provisional-v1`. Search then initially includes clearly labelled provisional records, while an explicit trusted-only URL remains respected. The interface labels deferred editing/model actions; the API independently blocks them and has no active Queue consumer or scheduled writer. Vector outboxes remain durable and explicitly deferred, never falsely completed. Global operational health retains their actual counts/ages and distinguishes intentional lexical deferral from active/failed/expired work.

This section describes the supported contract and required rollout evidence; it does not assert a completed production deployment.

The private provisional-publication Workflow accepts a finite, hash-bound plan
through private instance parameters and keeps only its small identity header in
configuration. Its first conditional proof claim and exact stored-plan readback
precede database writes. Each zero-retry step publishes at most 25 technically
verified records with the existing one-statement cohort guard. An unknown SQL or
outcome acknowledgment retains its claim and permits observation only.

Before and after every publication batch, the Workflow pins the physical owner,
schema and exact migration ledger, every source journal, source jobs and receipts,
catalog, document values and the expected corpus-revision increment. Only
`searchable_at`, `published_revision` and the corresponding `updated_at` may
change. The exact eligible set excludes unreadable sources. Existing approvals,
metadata confirmations, generations and historical holds remain unchanged; the
result explicitly declares unreviewed provisional publication. This operational
result does not establish facet/reviewer/global-cache maintenance, full integrity,
peak capacity, human review or release readiness. Complete those later gates at
the final frozen revision before application promotion.

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

A separately reviewed source successor can process untouched jobs after an
individual recovery and receipt completion have been independently reconciled.
Its version-2 declaration explicitly identifies a preserved failed or published
generation and the completed recovery operation. A published generation remains
published; it cannot be relabelled as failed. The complete predecessor document
and all its generations are hash-pinned before initialization and every tick.
The contract applies to the exact owned partition, rather than assuming partition
zero. It adds its own immutable identity, tick and history tables; the predecessor's
held tick, terminal history and document generations remain unchanged. Initialization
pins both the original and prospective exact schemas and runs one D1 batch
transaction behind a conditional proof claim. There is no migration replay or
`IF NOT EXISTS` fallback. An uncertain initialization is observed without retry.

The successor Workflow has a fixed instance, finite admission and completion
windows, zero step retries, and an immutable claim for each bounded source tick.
Four ordinary records or one exception, the 20-second loop budget and the D1
binding-call limits remain in force. A duplicate callback cannot acquire another
tick after losing an acknowledgment. Each new source receipt records successor
lineage. Only complete logical source coverage can mark that execution complete
and advance the root lifecycle to indexing. This neither clears the historical
failure nor supplies publication, full integrity, capacity or release approval.

Review the whole Workflow request budget before uploading an execution. The
per-tick D1 meter does not cover an entire Workflow or its R2 proof and source
calls. The default whole-instance subrequest limit is 10,000; use an explicitly
reviewed supported configuration and independently read it back. The capacity
guard reserves 700 database calls and 200 storage/control calls per step, plus
initialization headroom. This changes no individual D1 statement limit or CPU
limit. Preserve failed instances and claims; a new capacity plan does not
authorize restarting or overwriting an existing execution. See [Cloudflare's
Workflow limits](https://developers.cloudflare.com/workflows/reference/limits/).

If a provably errored, quiescent Workflow leaves a running executor checkpoint,
a distinct conditional terminal-disposition attempt can record that executor as
held while preserving its claim tokens and staged rows. Only a genuinely
abandoned, nonpublished staging generation can receive a technical failure
disposition. A published generation stays unchanged and needs its own receipt
reconciliation. These dispositions do not reprocess source, approve a document,
clear a hold or establish integrity. Lost acknowledgments require exact outcome
observation and cannot authorize another mutation attempt.

An interruption after internal text publication requires a different receipt
reconciliation. It must verify the unchanged held executor and job, the current
document and both existing generations, full current/staged artifact manifests,
and original/copied/current source bytes and binding versions. If the original
source proof is absent, a distinct conditional operation can append a truthful
version-3 receipt and complete that one seeded job. It cannot reprocess text,
invent a failed generation, overwrite the absent original proof's key or clear
the old hold. Its fixed private Workflow has zero retries; an uncertain result
requires independent observation.

Later provisional publication uses a separately claimed, explicit plan with at
most 25 records. It pins current metadata, revisions, generations and original
source evidence and publishes the whole batch with one guarded statement. Any
changed record blocks the batch. Publication leaves human approval, metadata
confirmation and legal provenance unconfirmed; unreadable sources remain held.
This capability does not establish capacity, integrity or release readiness.

Restoration copy plans take the upload ETag from R2's structured object metadata,
not the management API's HTTP response ETag, which can be weak and describe an
encoded response. For these bounded single-part copies, the metadata's exact key,
size, owner/plan/source metadata and raw ETag must match the full downloaded bytes.
SHA-256 remains the source identity check; MD5 correlates the upload ETag. A
differing binding ETag still holds before staging. Correcting a mistaken operator
plan requires preserving its claim and failure, terminal reconciliation, and a
distinct tested operation; no claim, guard or receipt is cleared.

A verified selected reprocess can be followed by a separate, one-attempt source
completion operation. It checks the old held owner, tick, immutable history,
seeded claim and failed generation, the acknowledged selected outcome, current
source/version and the retained initial snapshot. A conditional version-2 source
proof links both attempts. One guarded D1 batch appends its source receipt and
advances only the still-seeded job to processed, keeping the original claim. It
changes no text, source, review state, predecessor tick or failed generation. A
lost acknowledgment retains the claim and committed evidence for independent
observation; it cannot reissue the batch. A separate successor protocol is still
required for untouched jobs. The old held execution is never resumed or made
to appear successful.

After that forward disposition is independently reconciled, a new technical
admission may address untouched non-pilot partitions. It references both actual
positive operations and a fresh read of the still-held pilot, its five completed
source receipts, original failed evidence and disabled schedule. It never creates
a healthy-pilot receipt or restarts the predecessor. The first cohort is limited
to two untouched partitions. Expansion to at most eight requires independently
verified successful source results and no held failures in an earlier cohort.
Each activation and schedule change still has its own immutable intent, exact
private Worker/source/binding checks, seeded-state check and uncertain-outcome
observation. These cohort declarations supply neither publication approval nor
final integrity evidence, and do not resolve the pilot's remaining planned jobs.

A version-3 selected recovery and its separate forward receipt also pin the
new execution journal: every run, tick, history row, current pointer and retained
peer case must match the approved terminal snapshot. Older plans require those
tables to be absent. The recovery preserves the failed staged rows and all three
held histories; it cannot reset or silently ignore a newer executor.

A version-2 source Workflow can process at most 32 untouched records before a
planned handoff. It verifies the exact receipt delta, untouched remainder and
quiescent tick, then terminally holds its own run with an acknowledged journal
digest. It reports `source_checkpointed`, leaving the original lifecycle held or
seeded. A distinct execution can append after that terminal predecessor without
replaying schema creation or changing historical rows. Only full logical source
coverage reports completion and advances to indexing. This smaller batch design
requires native and frozen-runtime checks plus an actual cloud CPU canary; local
success does not establish cloud CPU capacity or release readiness.


A private source-batch Workflow can retain one static definition while the owned
local controller creates distinct, fixed child instances. Each instance processes
at most 32 sources. Its immutable root pins the source plan, original schema and
execution journal, preserved cases, batch bound and a finite window of at most
eight hours. A later child requires the preceding actual engine to be complete,
its durable result to match, and the full journal to match that acknowledged
checkpoint. The child has no schedule and cannot create another instance.

The controller may deliver bounded canonical root and child plans through the
private Workflow parameters, below the documented event-size limit. Before SQL,
a zero-retry step claims that delivery and conditionally creates the hash-pinned
R2 plans with exact metadata and byte/version readback. Cached callbacks do not
write them again. Lost plan, claim, create or outcome acknowledgments require
independent observation; neither the plans nor their claims may be overwritten.
The helper remains HTTP 503 with only its owned database, original/copy/proof
buckets, non-secret controls and its Workflow binding. CPU and D1 query limits
remain unchanged. Native and frozen-runtime proofs do not replace an actual
bounded canary or establish publication, integrity, peak capacity or release
readiness. See [Workflow parameters](https://developers.cloudflare.com/workflows/build/events-and-parameters/)
and [limits](https://developers.cloudflare.com/workflows/reference/limits/).


Reviewer-queue maintenance materializes technical read rows; it grants no content
review or publication approval. Its finite private Workflow uses zero-retry,
conditionally claimed steps of at most 25 documents, exact source/job/receipt and
held-journal snapshots, measured capacity and frozen index/catalog/revision
checks. Older plans require zero published records. An explicit version-2 plan
instead pins the complete already-published provisional cohort, requires every
published record to match its current revision and verified eligible source, and
still refuses human approvals or metadata confirmations. All original source and
terminal execution rows stay unchanged, including legitimate historical holds.
Run this maintenance after provisional publication, then build the global read
cache before collecting final integrity receipts at the frozen corpus revision.
Lost acknowledgments preserve their claims and materialized rows for independent
observation; they never permit an automatic replay or a hold reset.

The independent source-byte audit helper accepts only R2 GET capabilities and
has no database capability. It checks the exact owned source receipt, sealed
original version and bytes, owned copy version and bytes, and current source
version and bytes. Its result contains hashes and versions rather than case text
or object keys. It neither repairs a source nor retries an uncertain read.
Controllers must independently pin and reconcile database ownership, schema,
journals, jobs, receipts, document values and corpus revision around this byte
evidence. The helper explicitly supplies no database-bookend, full-integrity or
publication approval; a tested helper alone is not a deployed audit service.

The byte-audit Workflow component groups at most eight records per zero-retry
step. A finite root pins all 32 physical source identities and sealed partition
plans. Each operation checks record membership against its sealed plan before
reading source bytes, and conditionally claims and reads back each proof before
proceeding. Lost claim or outcome acknowledgments remain held for independent
reconciliation. Only the owned operational-proof prefix receives writes; original
and target source capabilities expose GET only. Its acknowledged results still
require matching external database bookends and complete coverage, including a
separate preserved-source check for unreadable records without a current parsed
source. Component tests supply no deployed binding, full integrity or release
approval.

The auditor's finite root plan can be uploaded by a separate proof-only Workflow
instance whose ID is the declared root ID. That single zero-retry callback
conditionally claims the upload, stores and reads back the exact hash-bound root,
then conditionally stores its outcome. It reads no source bytes and has no database
capability. Ordinary audits require the matching root claim, outcome and current
R2 version. Operators must additionally reconcile the exact successful bootstrap
engine result before admitting an audit; an unacknowledged bootstrap remains held
and cannot be replayed or overwritten.
