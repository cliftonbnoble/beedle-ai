# Sealed recovery and migration

This workflow replaces `run:d1-remote-data-migrate`'s implicit production target and retires the heuristic post-migration repair. It never deploys, promotes a database, repairs judges, changes reference validity, grants approval or recreates an authenticated actor. Authentication remains separate.

Recovery is **high risk**. Use a newly created, isolated D1 database **without applying migrations first**, a new R2 bucket and separate local persistence or remote resources. Keep the application, cron, Queue consumers and other writers stopped. The workflow creates schema from the captured release, rather than merging into an existing database. An existing schema, a conflicting key or a different receipt is an error. Never delete a receipt to force a retry.

## Prerequisites and explicit inputs

Use the repository's pinned Wrangler and Node with `node:sqlite`'s online backup API (Node 22.16+; CI uses the latest Node 22). Run from a trusted checkout matching the captured schema contract. Treat the source database and its SQL as trusted operator input. Store bundles in a private directory outside tracked source; `recovery-bundles/` is ignored. They contain the entire corpus, reviewer receipts and source bytes. They are not encrypted by this tool. Back up the sealed bundle using the organization's storage/access/retention policy.

Create a dedicated **flat JSON** Wrangler config for each resource set. No inherited environments or implicit account selection are supported. Each must name exactly one `DB` D1 binding and a `SOURCE_BUCKET` R2 binding. A remote config must declare its `account_id`. Local mode strips cloud credentials and always supplies `--local` plus the explicit persistence directory. No resource is created automatically.

Example source descriptor (all paths are explicit absolute paths on the operator's machine):

```json
{
  "mode": "local",
  "config": "/absolute/operator/source-wrangler.json",
  "persistTo": "/absolute/operator/source-state",
  "sqlite": "/absolute/operator/source-state/v3/d1/explicit-source.sqlite"
}
```

Select the SQLite file belonging to that exact D1 resource. There is no machine-specific file discovery or default database name. Local capture uses SQLite's online backup API, including committed WAL content; it does not open the live file with `immutable=1` or copy the main file without its WAL. Integrity and foreign-key checks must pass. [SQLite online backup](https://www.sqlite.org/backup.html).

For a **remote-origin** source, supply a separately obtained, consistent SQLite snapshot and `"mode":"remote","consistentExport":true`, with the source's explicit account/D1/R2 config. This assertion records an operator prerequisite, not proof that an arbitrary export is consistent. An unfenced live multi-query scan is not a backup. D1's documented SQL export has virtual-table limitations; do not drop live FTS tables just to obtain an export. Use the provider's supported backup/Time Travel process or an independently rehearsed consistent export procedure. [D1 import/export limitations](https://developers.cloudflare.com/d1/best-practices/import-export-data/).

### Frozen legacy 0012 baseline

`apps/api/scripts/capture-frozen-baseline.py` provides an explicit maintenance procedure for the known 0001–0012 baseline. It requires Python 3.11+ with SQLite and standard-library `tomllib`. This is an operator workflow, not an application endpoint. Obtain approval for the full private corpus/original-file copy and downtime first. Keep the destination owner-private and Git-ignored.

The source descriptor names `accountId`, `databaseId`, `bucket`, the absolute Wrangler OAuth `oauthFile`, and a trusted TLS `caFile`. A preconfigured `CLOUDFLARE_API_TOKEN` can be used instead. Never print either credential. The tool has no implicit production identity. Generate a plan in a new directory:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 apps/api/scripts/capture-frozen-baseline.py plan /absolute/operator/capture-source.json /absolute/private/baseline
```

Planning reads metadata and writes owned `freeze.sql`, `unfreeze.sql` and `freeze-plan.json`; it does not execute DDL. Review and preserve all three. Separately stop application writers, schedules and Queue consumers, establish an empty/drained queue disposition, save their previous settings, and apply the exact freeze SQL to the explicitly named source. It installs reject-write triggers on every ordinary table, including the migration ledger. No case values are updated. Keep R2 writers stopped throughout capture. Do not apply release migrations to this source.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 apps/api/scripts/capture-frozen-baseline.py capture /absolute/operator/capture-source.json /absolute/private/baseline
```

Capture checks owned fences/schema repeatedly, preserves integer/REAL values and rowids through typed ASCII transfer, and commits each 128-row page together with its cursor/hash checkpoint. Resuming the same command rechecks completed local content chains and source counts. A file lock rejects simultaneous owners. It reconstructs the disposable legacy FTS cache from captured source rows, checks SQLite/foreign-key integrity, and downloads all referenced originals with four bounded transfers. Recorded object hashes must match; absent legacy hashes are labeled observed bytes. `capture-manifest.json` is written only after all stages pass. `capture-status.json` is progress, never completion evidence. Missing originals, authorization failures, changed fences/schema, disk reserve or content failures hold the capture; investigate before resuming. Keep the machine awake and credentials valid under the operator's session policy.

Only after the completed manifest and all checks pass may the recovery source descriptor assert `consistentExport:true`. For this baseline also set `legacyBaseline:true`, `deferFtsRebuild:true`, and `objectArchive` to the completed capture directory. Archive reuse validates source identity and every local object hash. It never grants new provenance or approval. The exact 0001–0012 ledger must match; later release migrations are required on the isolated restored target before any application use. Recovery excludes its own capture metadata/fences from the restored schema.

To abandon maintenance, stop capture/supervision, inspect the saved source identity, and apply only the exact owned `unfreeze.sql`. A different freeze owner fails its guard. Restore saved writer/schedule/Queue settings deliberately. Never unfreeze while a backup is in progress or deploy an older writer against migrated schema.

Example target descriptor:

```json
{
  "mode": "local",
  "config": "/absolute/operator/recovery-wrangler.json",
  "persistTo": "/absolute/operator/new-recovery-state",
  "purpose": "isolated-recovery",
  "writersQuiesced": true,
  "freshBucket": true
}
```

Create the local persistence directory first. A remote descriptor uses `"mode":"remote"` and omits `persistTo`. Its dedicated config must explicitly declare the target account, database UUID/name and bucket. Target config vars must contain `VECTOR_LIFECYCLE_READY="false"`, `VECTOR_FILTERS_READY="false"` and `RELEASE_MAINTENANCE_ONLY="true"`, with no cron or Queue consumers. These checks and the explicit quiescence/fresh-bucket assertions do not discover unrelated deployed writers; the operator must establish isolation. Separate vector indexes/namespaces and Queues are required before later deployment. Old provider data is not portable proof of synchronization.

Large-corpus plans may explicitly set `maxSqlBytes` (up to a conservative 5,000,000,000 bytes) and `importTimeoutMs` (120,000–7,200,000 milliseconds). The default remains 512 MiB/two minutes. A budget above 512 MiB requires `deferFtsRebuild:true` on capture. The sealed plan records the budget and timeout. Measure the actual SQL file, remote import behavior, target allocation and later FTS/generation peak before relying on a larger ceiling; do not infer capacity from an empty migration.

## Capture, review, apply, verify

From the repository root, substitute the exact descriptors and a **new** bundle directory. These commands have no hidden production target:

```sh
pnpm --filter @beedle/api recovery capture /absolute/operator/source.json /absolute/operator/recovery-bundles/run-001
pnpm --filter @beedle/api recovery plan /absolute/operator/recovery-bundles/run-001 /absolute/operator/target.json
```

Capture reads the selected source D1 snapshot and referenced R2 originals/generations/outboxes. It fails if a required object is missing or differs from its recorded hash. An unfinished ingestion reservation may have no object; that absence is recorded, and its request remains retryable. Legacy objects without recorded hashes are labeled observed bytes, never retrospectively verified provenance. Source and target D1/R2 identity, config hashes, schema/contract/tool hashes, source and normalized snapshot hashes and run UUID seal the plan. Changes require a new capture/plan rather than resetting a shared state file.

Inspect private `bundle.json`, `plan.json` and `restore.sql`. The plan prints its SHA-256 and explicit target. Copy that exact hash into the apply command after reviewing the plan:

```sh
pnpm --filter @beedle/api recovery apply /absolute/operator/recovery-bundles/run-001 /absolute/operator/target.json EXACT_PLAN_SHA256
pnpm --filter @beedle/api recovery verify /absolute/operator/recovery-bundles/run-001 /absolute/operator/target.json
```

Apply first claims the empty D1 target for this exact run/bundle/resource identity, then copies objects and reads them back **before** importing the application data. Another run cannot acquire the same target. An interrupted claim is resumed only by its original sealed run. Existing identical bytes are accepted; different bytes fail. Only a positively identified missing-key response permits a new object write; denied/transient reads fail closed. No object is deleted automatically. Wrangler object transfer preserves the source bytes; custom R2 validation metadata is deliberately not invented. Restored originals download as opaque attachments until revalidated through the source ingestion/reprocessing workflow. Compare bytes and attachment safety in the canary, and include this MIME downgrade in rollout review. A failed run can leave isolated objects without a committed database; keep the resources private and resume the same plan.

The SQL file contains the whole restore. D1 file import supplies its transaction, so no explicit `BEGIN/COMMIT` appears in the file. It creates a unique receipt table first and checks the target has no application schema before writing data. Concurrent or delayed imports cannot overwrite a committed restore. Failure rolls back DDL and data; native Wrangler tests enforce this assumption locally. Remote provider behavior, corpus-size timing and limits must be rehearsed on disposable remote resources **before** relying on this for production. The default SQL budget is 512 MiB, with the explicit reviewed larger budget described above; statements remain capped at 90 KB; large text/blob values use bounded value staging. It never silently splits an atomic restore. Verification is a finite offline full-content scan with small response pages, rather than an online request path; measure its time/SQL cost on the representative dataset. Large restores require a reviewed capacity plan. [D1 transaction/import requirements](https://developers.cloudflare.com/d1/best-practices/import-export-data/).

A timeout or lost acknowledgment is uncertain. Resume `apply` with the **same** bundle/target/hash: it first checks the durable receipt and verifies committed content, instead of trusting attempted-row counters. A repeated complete import aborts at its first receipt-table creation. `checkpoint.json` is atomically replaced and bound to the exact run/plan; `operation.lock` prevents concurrent local writers. Inspect its PID before manually removing an abandoned filesystem lock. A different host/run is fenced by the durable target claim before object copying and by the unique transactional import receipt. The new bucket must be dedicated to this target/run; do not share it with another database's recovery. Never remove target schema to force a retry on a non-disposable resource.

Verification hashes actual typed table content in stable rowid or non-null composite-primary-key order and checks schema, row counts, foreign keys and R2 readback. Byte-bounded keyset pages avoid repeated full-table sorts/OFFSET scans. Integer values travel as decimal strings; inspection text/blob bytes travel as hex. Finite REAL values are normalized across SQLite decimal renderings without rounding integer strings. Restore preserves implicit rowids. SQL uses escaped compact UTF-8 text where exact, hex for NUL/invalid UTF-8/blob bytes, and bounded multi-row inserts. Parent-first loading and temporary foreign-key indexes avoid deferred-constraint scans; no source schema index is removed. A matching count alone is insufficient. Private logs and mismatch reports contain operational/corpus information and must not be committed. Errors on stdout contain no SQL rows or source contents.

## Bounded parallel file execution for an existing plan

`recovery:parallel` is an additive executor for an already sealed plan. It validates the **unchanged original recovery tool/contract hashes**, both snapshot hashes, every local original, SQL bytes/hash, exact target config/identity, original plan checksum and durable claim/receipt. It does not edit or reseal the original bundle, SQL or plan. Each invocation records its own controller/Python-helper hashes and options in a separate private execution receipt under `parallel-executions/`.

Use an explicit private options JSON file, for example `{"concurrency":4,"requestsPerMinute":180,"oauthFile":"/absolute/path/to/the/existing/Wrangler/session.toml","caFile":"/etc/ssl/cert.pem"}`. Remote mode reuses the current Wrangler session (or an existing `CLOUDFLARE_API_TOKEN`); it does not create application authentication or credentials. Local mode strips cloud credentials. Python 3.11+ is required. The session file must be the one the pinned Wrangler actually refreshes. Refresh is serialized; upload children receive a temporary access-token snapshot through their environment and cannot race on the session file. Never print tokens or raw execution logs.

```sh
pnpm --filter @beedle/api recovery:parallel apply /absolute/private/bundle /absolute/operator/target.json EXACT_ORIGINAL_PLAN_SHA /absolute/operator/parallel-options.json
pnpm --filter @beedle/api recovery:parallel verify /absolute/private/bundle /absolute/operator/target.json EXACT_ORIGINAL_PLAN_SHA /absolute/operator/parallel-options.json
```

Stop the previous owner and establish that its lock is released before switching executors. The parallel executor takes the **same** bundle lock as the serial tool. Never run both. Remote execution rechecks the matching candidate claim and disabled managed/custom bucket access before file work. At most four file tasks run; the shared pacer allows no initial burst and defaults to 180 object operations/minute, leaving room under Cloudflare's cumulative API limit. HTTP 429 pauses all workers, with a bounded retry budget. [Cloudflare API limits](https://developers.cloudflare.com/fundamentals/api/reference/limits/).

Every file is hashed locally, read from the target and compared against its expected bytes/hash. A matching existing file is reused without writing; that full fresh GET is its copy-stage verification. Missing files are uploaded through the pinned Wrangler and read back in full. An uncertain PUT acknowledgment is resolved by a readback before considering another PUT. Only a positive not-found response permits upload; authentication/permission errors, conflicting bytes and other integrity failures hold the job. Transient reads/writes have finite retries; content mismatches are never retried into success or overwritten. Remote GETs use a bounded direct API read rather than trusting a CLI download file. Each response is capped at 32 MiB and only four tasks can retain responses. A failed wave drains all active tasks before its parent can release ownership; interruption stops scheduling and drains current work.

SQL import remains a single transaction behind the exact original receipt guard and is never blindly retried. Full typed table/cursor hashes, schema, counts and foreign keys are checked serially, followed by a separate parallel full readback of **every** original. A new execution invalidates an old positive verification report while preserving it privately; a failed revalidation cannot leave stale positive readiness. The independent serial verifier accepts successful native parallel recoveries in integration tests. `dataVerified` still does not grant publication or promotion.

Use `parallel-status.json` plus its linked copy/verify status for actual completed/total/retry counts, active workers and heartbeat time. Raw child logs contain private keys and must stay private. A completed process, partial counter, ownership claim or prior report is insufficient: require current `data_verified_not_promoted`, a matching full verification report and all later release gates.

## What is preserved, held and rebuilt

`source.sqlite` is an untouched consistent source snapshot. `restore.sqlite` and its manifest describe explicit recovery transformations:

- Preserve documents, revision/approval evidence, reviewer mutation receipts, immutable reference editions/supersessions/validation history, retained generations and their v1/v2/v3 manifests, ingestion receipts and logical source revision heads. Do not manufacture hashes, actors, governing-law applicability or new approvals.
- Clear expiring search sessions, reconcile reserved capacity and advance the corpus epoch. Clear transient cost counters and provider cache/profile/sync state. Retain historical job evidence in the original snapshot.
- Hold pending vector jobs as failed and pending maintenance plans as cancelled with an explicit recovery reason. Completed jobs remain historical evidence, not proof that the new target has vectors. Release processing ingestion leases into retryable state; retain fingerprints and completed receipts.
- Reset reviewer materialization to backfill and release captured search-builder leases. FTS shadow tables are not copied; external-content postings are rebuilt from source-backed content, while ordinary FTS content is portably inserted. Source readiness checkpoints are retained, so incomplete source builds remain incomplete. Frozen FTS archives are disposable and are not independent source backups.

With explicit deferred rebuilding, the normalized current-release copy drops disposable previous/frozen FTS caches and resets readiness to pending. The SQL import creates empty source-backed FTS indexes; it does not run an unbounded rebuild. The legacy mode accepts only the known source-backed legacy cache. Run the actual bounded M10 recovery after release migrations. Keep the untouched source snapshot and completed original-file archive.

`dataVerified:true` is **not** a promotion decision. Keep the target private. With the matching API and explicitly targeted lexical-only bindings, run a fresh bounded [M10 full FTS recovery and coverage check](search-index-recovery.md), finish search/facet and [reviewer builds](reviewer-queue-pagination.md), then run search/source/approval and receipt canaries against the manifest. Record IDs/hashes privately. Test a direct source download, a previously approved case, a rejected/unpublished case, mutation/ingestion replay and governing-reference edition resolution. Never replay a real reviewer mutation with a new key merely to test recovery.

Semantic recovery requires a separately reviewed rebuild into the **new** vector index/namespace, verified visibility, filter version 3 and lifecycle readiness. Use bounded current maintenance workflows, not captured Queue deliveries. Keep semantic flags false until the [release gate](release-readiness-and-rollout.md) accepts that target. Real corpus relevance/independent labels, legal review, provider acceptance and remote restore time remain external gates; the local fixture cannot certify them.

## Rollback

Before promotion, rollback means abandoning the isolated resources and retaining their private manifest/logs for investigation. Recovery apply never writes to the source; separately managed source maintenance fences/writers require the owned rollback described above. After a real promotion, use the separately reviewed release rollback and backup plan; do not merge snapshots, roll an older writer onto newer schema, delete receipts or restore D1 without matching R2/reference provenance. Stop all writers before any data recovery. Implementation tests use disposable synthetic resources.
