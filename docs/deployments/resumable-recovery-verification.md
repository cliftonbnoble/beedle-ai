# Resumable full recovery verification

The original full verifier retained its table hash only in memory. A terminated
process or exhausted Cloudflare read retry therefore restarted the entire pass.
`apps/api/scripts/recovery-resumable-verify.mjs` adds durable verification without
changing the original recovery tool, backup, SQL, approved import plans, or target.
Its `verify` action is read-only. Separate reviewed `fence-plan`/`fence-apply`
actions install candidate maintenance controls; they do not rewrite original rows
or original schema objects. It does not run release migrations, backfill, approve
documents, or deploy the application.

The production migration's isolated candidate completed this full pass on
October 29, 2026. Its positive report, complete D1/R2 checkpoint and exact fence
evidence were independently audited and archived immutably before the owned
fence was deliberately released for candidate release migrations. **That specific
epoch and checkpoint are permanently retired.** Do not resume them or recreate
their fences against the migrated schema. The sequence below describes the
general verification protocol; consult the current private operational handoff
before running any action on an existing migration target.

## What is verified

- Every original snapshot/SQL/object seal, target binding and distinct source;
  the original recovery claim and completion receipt; the exact schema; and all
  committed SQL-range/final-step receipts when those approved plans are supplied.
- Every ordinary row in its original stable key order, with the original typed
  encodings for integers, REALs, text, NULs, blobs and composite keys. Page bounds
  and terminal pages match the original verifier, including empty tables.
- The complete foreign-key check, before file verification and again at completion.
- A full fresh SHA-256 and byte-count readback of every required original file
  at least once within the same unchanged upload-version epoch.
- Fresh ownership, schema, receipt, database-state, bucket-privacy and file-version
  checks before a positive final report. Authentication remains unchanged.

## How a completed page can be reused

Derive a separate private page plan from the **unchanged** `restore.sqlite`.
Planning streams every typed row, recomputes each original whole-table SHA-256
and count, and requires both to match the original manifest. It then seals the
expected canonical bytes/hash, row count and exact start/end cursor of every page.
The live verifier compares every returned page with its sealed descriptor.

This retains the whole-table content proof: all ordered page byte sequences must
match the snapshot whose concatenation already matches the original table hash.
It does **not** concatenate page hashes and claim they are the original table hash.

Successful page and object comparisons commit to a private SQLite checkpoint with
`synchronous=FULL`. A crash before commit re-reads only that unfinished unit; a
crash after commit can reuse the exact receipt after fresh guards pass. Page
receipts must form a complete contiguous prefix. Parallel object receipts may
have gaps because an entire active wave drains on failure. Wrong identities,
damaged databases, altered receipts, unexpected rows and gaps in page receipts hold.

The checkpoint pins the original bundle/plan/target/tool, derived page plan, new
controller source hash, committed-import plans and an owned persistent candidate
fence epoch. Before reuse and at final guards, it requires the exact complete
schema (including all fence definitions) and immutable epoch receipt. The fence
uses [SQLite reject-write triggers](https://www.sqlite.org/lang_createtrigger.html#the_raise_function)
for INSERT, UPDATE and DELETE on every verified ordinary table, every existing
recovery-control table and the epoch receipt itself. All application writers and
migration/backfill executors must remain quiesced while that epoch is active.

This is the same maintenance ownership model as the source capture. Privileged
operator DDL or any fence change invalidates the epoch, even if later undone;
never recreate a missing fence to reuse old receipts. Missing/altered/partial
fences, a different owner or schema drift hold. Installation has one write attempt
and a durable pending acknowledgment. A lost acknowledgment can resolve only by
inspecting the complete matching installed schema and receipt, without replay.

[D1 bookmarks](https://developers.cloudflare.com/api/resources/d1/subresources/database/subresources/time_travel/methods/get_bookmark/)
remain opaque diagnostic observations. The first real interruption rehearsal
correctly held when a bookmark changed across a read-only pause, despite unchanged
other sealed identities. The earlier 190-page unfenced checkpoint is preserved
without credit. The fenced epoch starts a separate checkpoint from zero. No
bookmark fields are parsed or assumed to be a persistent content revision;
unsupported schema-version PRAGMAs are not used. Counts, mtimes and progress
messages never establish continuity.

## File versions and private metadata receipts

[R2 upload versions](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/#r2object-definition)
identify a specific upload. Before using any file receipt, capture the current
versions of all required originals and bind them to a separate durable file epoch.
After all full SHA-256 readbacks, capture them again and require the identical
versions. Missing/replaced uploads hold, even when size or ETag remains the same.
File inspection failures retain verified database pages.

The local inspector is loopback-only and exposes version metadata through a
single read-only bucket binding. Cloud accounts can instead select the explicit
`scheduled-private` mode when remote preview sessions are unavailable. That mode
uses a uniquely named temporary Worker with no public/preview access, no D1 or
queue binding, and only two buckets: the candidate originals (list only) and a
distinct private operational-proof bucket (new conditional receipt only). It
records ownership before deployment, checks the deployed bindings/owner, waits for
the exact fresh capture ID, and deletes only its matching owned Worker afterward.
Original files are never written. Failed or uncertain ownership/cleanup holds.
Inspect the private ownership receipt after any interrupted auxiliary deployment;
do not duplicate its job or delete an unverified owner.

Cron changes can take [up to 15 minutes to propagate](https://developers.cloudflare.com/workers/configuration/cron-triggers/).
Each capture has a 20-minute deadline and bounded metadata/page sizes. Metadata
receipts remain private, just like case keys, cursors, logs, snapshots and credentials.
They must not be committed or sent to an LLM. An ETag or a size check alone never
replaces SHA-256 readback. Unknown, permission, content, schema and integrity errors
are holds; only recognized transient reads receive finite retries.

## Operating sequence

1. Read the private working state, verify actual PID/start identity and every
   ownership lock, and confirm no active owner before launching verification.
   Keep the source maintenance fences and original recovery seals intact.
2. Derive a **new** private plan with `plan`, using the existing original plan SHA
   and target specification. Plan derivation is local/read-only and does not own or
   change the remote candidate. Never replace an approved derived plan.
3. Review its coverage and record its canonical JSON SHA in the private options.
   Include the exact original shard/final-step plans, existing CLI session/CA,
   explicit version inspection mode, and the distinct private proof bucket when
   scheduled mode is selected. Keep the original target config unchanged.
4. With no live owner, derive a separate `fence-plan` from the actual candidate
   schema, review its SHA/coverage, and run `fence-apply` once. The original claim,
   completion/import receipts and ordinary schema must match before installation.
   Keep all writers quiesced. Launch `verify` under the existing shared bundle lock.
   All subsequent resumes must use the same reviewed options, page/fence plans,
   controller, fence epoch and checkpoint. Never
   delete/reset a checkpoint to bypass a failed guard. Preserve an invalid epoch
   for diagnosis; an explicitly reviewed fresh pass requires separate artifacts.
5. Follow `resumable-verification-status.json` and its matching execution report.
   Old final-step/parallel statuses are historical after a controlled handoff.
   A positive `verification.json`, complete checkpoint coverage, positive matching
   status and the exact original receipts are all required before migrations.
6. Archive positive legacy verification, checkpoint and exact fence proof before
   upgrading the candidate schema. Only after that matching positive proof may a
   deliberate ownership-checked release drop the exact epoch's fence triggers and
   control table, before candidate-only migrations. Releasing the fence permanently
   invalidates its verification checkpoint; never resume that epoch afterward.
   Full recovery verification is only one release gate: capacity, source and
   publication evidence, exact-commit tests, application smokes and a compatible
   rollback still must pass before the owner's conditional cutover authorization.

Example invocation (all uppercase values are explicit private artifact paths or
reviewed checksums; do not paste case-bearing options into logs):

```sh
node apps/api/scripts/recovery-resumable-verify.mjs plan BUNDLE TARGET ORIGINAL_PLAN_SHA OPTIONS
node apps/api/scripts/recovery-resumable-verify.mjs fence-plan BUNDLE TARGET ORIGINAL_PLAN_SHA OPTIONS
node apps/api/scripts/recovery-resumable-verify.mjs fence-apply BUNDLE TARGET ORIGINAL_PLAN_SHA OPTIONS
node apps/api/scripts/recovery-resumable-verify.mjs verify BUNDLE TARGET ORIGINAL_PLAN_SHA OPTIONS
```

The scheduled capture mode deliberately changes only separately owned temporary
operational resources and private metadata receipts. The verifier never retries
an import, changes source fences, enables production publishing, alters application
authentication, or treats index readiness as publication readiness.
