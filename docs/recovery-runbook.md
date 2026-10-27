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

For a **remote-origin** source, supply a separately obtained, consistent SQLite snapshot and `"mode":"remote","consistentExport":true`, with the source's explicit account/D1/R2 config. This assertion records an operator prerequisite, not proof that an arbitrary export is consistent. Remote multi-query scans are not a backup. D1's documented SQL export has virtual-table limitations; do not drop live FTS tables just to obtain an export. Use the provider's supported backup/Time Travel process or an independently rehearsed consistent export procedure. [D1 import/export limitations](https://developers.cloudflare.com/d1/best-practices/import-export-data/).

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

The SQL file contains the whole restore. D1 file import supplies its transaction, so no explicit `BEGIN/COMMIT` appears in the file. It creates a unique receipt table first and checks the target has no application schema before writing data. Concurrent or delayed imports cannot overwrite a committed restore. Failure rolls back DDL and data; native Wrangler tests enforce this assumption locally. Remote provider behavior, corpus-size timing and limits must be rehearsed on disposable remote resources **before** relying on this for production. The tool caps SQL at 512 MiB and statements at 90 KB; large text/blob values use bounded value staging. It never silently splits an atomic restore. Verification is a finite offline full-content scan with small response pages, rather than an online request path; measure its time/SQL cost on the representative dataset. Large restores require a reviewed capacity plan. [D1 transaction/import requirements](https://developers.cloudflare.com/d1/best-practices/import-export-data/).

A timeout or lost acknowledgment is uncertain. Resume `apply` with the **same** bundle/target/hash: it first checks the durable receipt and verifies committed content, instead of trusting attempted-row counters. A repeated complete import aborts at its first receipt-table creation. `checkpoint.json` is atomically replaced and bound to the exact run/plan; `operation.lock` prevents concurrent local writers. Inspect its PID before manually removing an abandoned filesystem lock. A different host/run is fenced by the durable target claim before object copying and by the unique transactional import receipt. The new bucket must be dedicated to this target/run; do not share it with another database's recovery. Never remove target schema to force a retry on a non-disposable resource.

Verification hashes actual typed, ordered table content and checks schema, row counts, foreign keys and R2 readback. Integer values travel as decimal strings; text/blob bytes travel as hex. A matching count alone is insufficient. Private logs and mismatch reports contain operational/corpus information and must not be committed. Errors on stdout contain no SQL rows or source contents.

## What is preserved, held and rebuilt

`source.sqlite` is an untouched consistent source snapshot. `restore.sqlite` and its manifest describe explicit recovery transformations:

- Preserve documents, revision/approval evidence, reviewer mutation receipts, immutable reference editions/supersessions/validation history, retained generations and their v1/v2/v3 manifests, ingestion receipts and logical source revision heads. Do not manufacture hashes, actors, governing-law applicability or new approvals.
- Clear expiring search sessions, reconcile reserved capacity and advance the corpus epoch. Clear transient cost counters and provider cache/profile/sync state. Retain historical job evidence in the original snapshot.
- Hold pending vector jobs as failed and pending maintenance plans as cancelled with an explicit recovery reason. Completed jobs remain historical evidence, not proof that the new target has vectors. Release processing ingestion leases into retryable state; retain fingerprints and completed receipts.
- Reset reviewer materialization to backfill and release captured search-builder leases. FTS shadow tables are not copied; external-content postings are rebuilt from source-backed content, while ordinary FTS content is portably inserted. Source readiness checkpoints are retained, so incomplete source builds remain incomplete. Frozen FTS archives are disposable and are not independent source backups.

`dataVerified:true` is **not** a promotion decision. Keep the target private. With the matching API and explicitly targeted lexical-only bindings, run a fresh bounded [M10 full FTS recovery and coverage check](search-index-recovery.md), finish search/facet and [reviewer builds](reviewer-queue-pagination.md), then run search/source/approval and receipt canaries against the manifest. Record IDs/hashes privately. Test a direct source download, a previously approved case, a rejected/unpublished case, mutation/ingestion replay and governing-reference edition resolution. Never replay a real reviewer mutation with a new key merely to test recovery.

Semantic recovery requires a separately reviewed rebuild into the **new** vector index/namespace, verified visibility, filter version 3 and lifecycle readiness. Use bounded current maintenance workflows, not captured Queue deliveries. Keep semantic flags false until the [release gate](release-readiness-and-rollout.md) accepts that target. Real corpus relevance/independent labels, legal review, provider acceptance and remote restore time remain external gates; the local fixture cannot certify them.

## Rollback

Before promotion, rollback means abandoning the isolated resources and retaining their private manifest/logs for investigation. The source was read-only. After a real promotion, use the separately reviewed release rollback and backup plan; do not merge snapshots, roll an older writer onto newer schema, delete receipts or restore D1 without matching R2/reference provenance. Stop all writers before any data recovery. No production resources were exercised by the implementation tests.
