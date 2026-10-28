# Production migration preparation — started 2026-10-27

## Current operating state

**Production maintenance is active. The final application cutover has not happened.** The owner authorized downtime, starting recovery/backfill preparation, and a full private copy of production D1 and all referenced original R2 files. Authentication/H01 remains out of scope. Work stays on `main`.

Maintenance began **2026-10-27 21:56 UTC / 2:56 PM PDT**. The full typed backup began approximately **21:58 UTC / 2:58 PM PDT**. Its source is the existing legacy 0001–0012 database: 14,071 documents, approximately 5.39 GB allocated, with about 257 MB of original files. The source case records and migration ledger have not been rewritten; maintenance controls have changed deliberately.

- The old API's workers.dev and preview access is disabled; its Worker version is retained.
- Its scheduled task is removed, its vector queue is paused, and its observed queue backlog was zero.
- An owned freeze receipt and 66 reject-write triggers fence all 22 ordinary source tables, including the migration ledger.
- Automatic production Pages publishing remains paused. The old frontend deployment remains selected; the API is unavailable during maintenance.
- A distinct candidate D1 and private R2 bucket have been created and rechecked empty/private before recovery. Recovery staging is now underway. No release migration has been applied to the source, and no new application has been promoted.

**The full backup completed at 2026-10-28 01:56:57 UTC / October 27, 6:56:57 PM PDT.** All 22 ordinary tables (5,131,185 rows) and all 14,071 referenced original files were captured. The originals total 256,240,261 bytes; the completed source SQLite file is 5,300,330,496 bytes. The final manifest, local integrity/foreign-key checks, typed content chains and original-file checks passed. An additional verification rechecked every local original hash, source freeze/schema ownership and completion receipts, and recorded the source-file SHA-256 before authorizing recovery capture.

Cloudflare OAuth expiries were handled through finite, validated resumes; no content/schema/fence failure was bypassed. The old capture/supervisor locks are released. A separately owned process is preparing the sealed candidate recovery bundle and plan. Candidate restore/readback, release migrations and index backfill remain separate gates; **backfill has not started**.

Private operating state is under the ignored `recovery-bundles/migration-execution-2026-10-27/` directory. Start with `WORKING_STATE.md`, `capture-supervisor-status.json` and `current-baseline/capture-status.json`. These artifacts contain operational identities or case material and must not be committed or sent to an LLM. The local process keeps the machine awake while capturing; the scheduled follow-up still requires this computer and Codex to remain available.

## What has been resolved and tested

| Assessment finding | Implemented preparation / evidence | Remaining limit |
| --- | --- | --- |
| B01: reliable full capture | Full current capture completed: 22 tables and all 14,071 originals; final manifest, integrity, typed chains, archive hashes and owned fences checked. Six checkpoint/ownership tests pass. | Actual candidate restore/readback must still finish. The backup does not invent missing historical source hashes. |
| B02: recovery size and correctness | Explicit sealed SQL budget/timeout; compact exact text and bounded multi-row statements; stable rowids/composite keys; portable REAL verification; keyset verification; parent-first loading and temporary indexes for cyclic foreign keys; deferred bounded FTS rebuild; explicit legacy-0012 recovery. | The actual full current-production remote restore, R2 readback and allocation checks remain required. |
| B04/B07: resumable bootstrap | A separate scheduled-only runner binds only the candidate DB. It checks the sealed recovery receipt and all 33 release migration names before any write, advances at most 20 checkpoints per tick, respects builder leases and operator holds, and holds after five consecutive checkpoint failures. Five behavior tests exercise guards, real indexes, publication preservation and failure/hold replay. | Checked-in config is disabled, has no schedule/public route and names no real database. Deploy only after verified isolated recovery and candidate migrations. It has not started yet. |
| B09: unchanged large reference arrays | Reviewer saves submit only changed arrays. Unchanged legacy arrays survive scalar edits and confirmation without being resubmitted into the 12-item request bound. Eleven targeted tests and web types pass. | Editing an array above the limit still needs a bounded workflow; no citations are truncated. Such cases can remain held from the release cohort. |
| B11: empty held-corpus search | Indexed global publication precheck avoids an expensive FTS scan when no documents are publishable, while preserving readiness/error/revision behavior. Historical local broad-query time fell from 38.975 seconds to approximately 4.3 ms. | Positive populated-corpus relevance, filters and native/cloud latency still need release-cohort benchmarks. |

The recovery implementation has 11 behavior tests, including int64 extremes, NUL/invalid UTF-8, SQL-looking text, wide rows, sparse rowids, composite keys, REAL precision, budget failures, deferred FTS and cyclic dependencies. Native local D1/R2 recovery exercises transaction rollback, conflicting ownership, replay, readback and content mismatch. These tests do not certify an unperformed full remote restore.

## Full historical SQL restore rehearsal

The separate historical corpus was used for scale, not relabeled as a current production backup. Its ledger has known historical gaps. No missing migration history was invented.

- Normalizing the already migrated historical copy took **591.876 seconds**, retaining the untouched source and deferring disposable FTS caches.
- The initial unbatched/hex-heavy SQL exceeded the explicit 4 GiB budget. Compact exact UTF-8 and bounded inserts reduced the successful file to **3,161,459,139 bytes**. Statement size remains below 90 KB.
- A first load order caused expensive deferred foreign-key scans. Parent-first ordering fixed that path; cyclic/self-references receive temporary support indexes.
- The corrected file streamed through **36,626 statements** in one local transaction in **547.695 seconds**; import plus initial integrity checks took **593.512 seconds**.
- The restored file was **4,251,025,408 bytes before FTS rebuilding**. All **79 ordinary table counts/content hashes and the schema hash matched** the normalized input; the final canonical comparison took **41.682 seconds**. SQLite quick/foreign-key checks passed.
- A separate disposable remote D1 test intentionally failed after 2 MB of SQL and 2,000 inserts. No partial test table survived. This proves that synthetic failure case only.
- A synthetic remote 99-column table passed exact int64, grouped typed-row encoding, Unicode/NUL, neighboring REAL and blob readback. An initial 103-column CREATE was rejected by D1's 100-column limit; that test was corrected rather than interpreted as a recovery failure.
- A synthetic native export rounded the maximum int64 and emitted a literal NUL. It was rejected as an exact production backup method, even though a filtered export succeeded with FTS present.

Historical file size, local throughput and synthetic remote rollback are separate evidence. Actual current-corpus remote import, original-file readback, final derived-index size and reprocessing peak remain gates. The earlier historical post-build size was 8.66 GB; its limited headroom under D1's 10 GB cap remains B10. [Cloudflare D1 limits](https://developers.cloudflare.com/d1/platform/limits/).

Local verification also passed 333 API behavior, 84 source, 38 utility, four contract, six Python capture and 53 web tests, plus lint, generated binding checks and API/web/integration types. The 16 existing native integration tests passed, followed by the added native migration-runner test. The latter checks wrong-receipt rejection, real D1 index completion/replay and unchanged held-document values. CI includes these gates; none deploys the application.

## Remaining execution sequence

1. **Completed:** fenced full capture, local reconstruction of disposable legacy FTS, integrity/original-file checks and sealed private capture manifest. Preserve the exact owned freeze and rollback files. A partial file or selected cohort is insufficient.
2. Capture a sealed legacy recovery bundle from that completed snapshot/archive. Review source/target identities, the 4 GiB SQL budget, hashes and candidate privacy. Apply only to the distinct candidate D1/R2 and verify full typed content plus every original-file readback. Uncertain acknowledgment must replay the same receipt/plan, never a new identity.
3. Apply 0013–0033 to the verified candidate only. Confirm the exact 33-entry ledger, publication holds, source access and measured capacity. Keep the source at 0012 and fenced. Never run incompatible old writers on the candidate.
4. Configure/deploy the private DB-only runner with the exact recovery receipt hashes and a fresh owned generation. Start its bounded schedule; record the actual start, checkpoint rate, failures and D1 growth. Finish substrings, phrases, facets and reviewer materialization. Verify posting coverage and integrity. Index completion grants no document approval.
5. Resolve source reprocessing, oversized-document disposition, reference-catalog provenance and an explicitly reviewed initial publication cohort (B03/B05/B06). Measure retained-generation peak space (B10). Preserve exclusions and unknown applicability. Do not bulk-convert historical searchable/QC flags into approvals.
6. Produce final-main preview/release evidence and real positive, negative, filtered and source-download smoke tests; rehearse compatible rollback (B08). Present the verified result for the owner's final application cutover. Keep authentication separate.

## Timing and return point

**Full backup is complete and candidate recovery staging has started; the index-backfill clock has not started.** The earlier 65.4-hour figure is the estimated minimum for roughly 23,546 deliveries through the public six-requests-per-minute maintenance path. It excludes recovery, original-file transfer/readback, reprocessing and review. It is not a timer that expires automatically.

The private runner can attempt up to 20 deliveries per minute without weakening public quotas. At the earlier checkpoint estimate, that is approximately 19.6 hours for indexes if all deliveries fit and succeed, before measured Cloudflare overhead and any extra recovery-map work. This is a planning scenario, not a completion promise. Do not schedule cutover from it until actual remote throughput, capacity and content checks support an ETA.

A quiet continuation is scheduled every 30 minutes in this chat. It advances the reviewed preparation, records meaningful changes, and reports actual backfill start, blockers, required review or readiness. It may not bypass failed checks, approve cases, change authentication or perform final cutover. **Return when notified that preparation needs review or is ready; waiting a fixed 66 hours is insufficient.**

## Maintenance rollback and evidence

The old Worker version, cron, public-access flags, queue state and Pages settings are saved privately. If preparation is abandoned, stop capture/supervision first, apply only the matching owned `unfreeze.sql`, verify the legacy source, and restore saved old-writer settings deliberately. No source data restore is needed merely to remove these maintenance controls. Candidate resources remain disposable and private. Never delete receipts, merge partial snapshots or point old code at the migrated schema.

See the [original assessment](production-migration-assessment-2026-10-27.md), [sealed recovery runbook](../recovery-runbook.md), [index recovery](../search-index-recovery.md) and [release/rollback gates](../release-readiness-and-rollout.md). A credential-looking legacy binding name was observed during settings inspection; rotate that provider credential separately without copying it into reports or Git.
