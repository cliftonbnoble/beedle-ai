# Production migration assessment and rehearsal — 2026-10-27

## Scope and current decision

The assessment inventoried production read-only, rehearsed on isolated copies, and documented blockers and a cutover estimate. Production was not migrated, restored, deployed over or otherwise changed. **Authentication/H01 remains out of scope.** Application source starts at merged `main` commit `0df9ed9ab1c5ef8d21dec87bde82b2de66097586`.

**Maintenance-window availability is cleared; technical readiness remains blocked.** The owner has notified users and accepts downtime whenever needed, including approximately 66 hours for maintenance. A short outage or uninterrupted availability is no longer a rollout requirement. This notes update does not start a migration or backfill. Passing schema/index tests does not establish source provenance, document approval, legal applicability or production performance. The earlier synthetic preview remains separate from this real-corpus rehearsal.

## Owner decision and return point — 2026-10-27

- Users have been notified, according to the owner. There is no need to wait for a calendar maintenance slot.
- Approximately 66 hours of backfill is an acceptable operating tradeoff. A faster runner is an optimization, rather than a prerequisite solely to shorten downtime.
- **No production migration/backfill has been started by this work. The 66-hour clock is not running.** Waiting 66 hours from this conversation will not make the application ready.
- The next working session should begin the technical preparation below. There is no reason to wait before that session. Recovery, storage and application defects still need resolution before production migration begins.
- Once an actual bounded, resumable run starts, plan at least approximately three days for the existing API backfill path, plus capture/restore checks, source reprocessing, review and final validation. This is not a promise that cutover is ready after three days.
- Set the return appointment for final cutover after the run reports completed checkpoints, successful recovery/space checks, an agreed publication cohort and passing release smoke tests. Record the actual start, progress and revised ETA then; there is no supported calendar completion date yet.

## Read-only production inventory

Production D1 `beedle` was inspected through metadata, schema, migration-ledger, bounded SELECT and read-only PRAGMA calls. No export, import, DDL, domain mutation, queue delivery, provider inference or restore was issued against production. The initial inventory bookends had the same Time Travel bookmark. The final bookmark advanced during the longer local rehearsal; its cause is not established, so these separate reads are **not** a transactionally consistent production snapshot. Final explicit schema objects, migration names, document publication counts, selected table counts and allocated bytes match the initial inventory. No claim is made that every production row was byte-identical throughout the session. A combined-count query was rejected; counts were collected with individually checked queries instead.

| Measurement | Observed value |
| --- | ---: |
| D1 allocated bytes | 5,388,255,232 |
| Applied migrations | 0001–0012 |
| Pending release migrations | 0013–0033: 21 |
| Documents | 14,071, all `decision_docx` |
| Historically searchable | 12,667 |
| Not historically searchable | 1,404 |
| Approval timestamps / metadata confirmations | 0 / 0 |
| Primary chunks | 467,427 |
| Activated retrieval chunks | 667,180 |
| Total primary/retrieval chunk identities | 1,134,607 |
| Sections / paragraphs | 163,989 / 1,228,567 |
| Retrieval embedding rows / search rows | 667,180 / 667,191 |
| Activation batches | 4,511 |
| Reference links / unresolved issues | 245,389 / 141,233 |
| Index codes / Rules provisions / Ordinance provisions | 652 / 714 / 598 |
| Existing vector jobs | 0 |
| Distinct nonempty source keys | 14,071 |
| Recorded source SHA-256 values | 0 |
| Documents with any reference array above the 12-item edit limit | 2,238 |
| Index-code / Rules arrays above that limit | 171 / 2,136; maximum lengths 20 / 30 |
| Invalid metadata JSON | 0 |
| QC-passed flags | 12,664 |
| Extraction confidence below 0.55 | 190 |
| Missing index / Rules / Ordinance detection | 1,386 / 436 / 436 |
| Missing decision dates | 30 |
| Largest chunks per document | 780; none exceeds the 1,000-chunk inline limit |
| Largest paragraphs per document | 5,906; one exceeds the 5,000-paragraph inline limit |
| Primary chunk text / paragraph text bytes | 247,993,277 / 246,470,962 |
| Largest individual chunk/paragraph | 17,042 bytes |
| Source-bucket metadata | 14,078 objects, approximately 257 MB reported by Wrangler |

Bucket object count does not prove every referenced key exists, and an existing key does not prove its bytes match the intended original. No source content or document identifiers are included in this public report. Raw artifacts are private and ignored by Git under `recovery-bundles/production-assessment-2026-10-27/`.

The reference-issue inventory contains 45,857 index-code issues across 12,685 documents, 64,590 Rules issues across 13,585 documents and 30,786 Ordinance issues across 12,009 documents. These are existing records, not newly introduced migration failures. They need triage against the current normalization/catalog policy; they cannot be interpreted as automatic approval or automatic rejection of every affected case.

## Copy strategy and evidence boundaries

Cloudflare documents that normal D1 export blocks other database requests and does not support databases containing virtual tables. Production contains FTS virtual tables. Its documented workaround deletes/recreates those tables, which was outside the read-only assessment scope. Time Travel currently supports in-place restore, not clone/fork. Therefore no blocking export, virtual-table deletion or Time Travel restore was attempted. Accepted downtime makes a quiesced capture strategy an option for subsequent work; it does not establish that a particular capture/restore procedure is correct. Sources: [D1 import/export limitations](https://developers.cloudflare.com/d1/best-practices/import-export-data/), [Time Travel](https://developers.cloudflare.com/d1/reference/time-travel/).

Two distinct kinds of evidence are used:

1. **Historical full-scale local copy.** An existing 5,412,462,592-byte local corpus database contains 14,177 documents (14,157 decisions and 20 law PDFs), 467,670 primary chunks, 1,229,007 paragraphs and 667,180 retrieval chunks. A WAL-aware SQLite online backup into a new private assessment file took about 8.3 seconds; snapshot plus quick/foreign-key checks completed in about 144.8 seconds. `quick_check` passed and foreign-key violations were zero. It differs from production by 106 documents and other rows; it is explicitly **not a current production backup**.
2. **Current production selected cohort.** A finite 20-document selection covers large, low-confidence, reference-heavy, ordinary searchable and unpublished cases. After automatic approval review initially stopped the transfer, the owner explicitly approved up to 20 documents and the shared reference catalog. The read-only transfer completed into private local storage; its measured results are recorded separately below. Historical-copy results are not relabeled as current-production results.

The historical table column definitions agree with the captured production baseline except for an absent empty vector-job table. Its original migration ledger records only 0001–0007 despite later physical schema objects. The isolated rehearsal executes the actual 0011/0012 prerequisite SQL, then the 21 release migrations. It deliberately does **not** invent missing historical 0008–0010 ledger entries. That limits its use to scale/schema/data-behavior evidence rather than exact production release-preflight evidence. Its untouched assessment snapshot is retained separately.

## Historical scale rehearsal

The 21 release migrations applied successfully on a second isolated historical copy. Including the two prerequisite SQL files, total local SQLite migration execution was **3.224 seconds**; creating that working copy took **7.041 seconds**. The migrated file was 5,421,957,120 bytes before backfills.

All 14,177 documents became `legacy`, with no current published or approved revision, as intended by migration 0013. One historical invalid date was retained as source evidence while its valid-date projection became unknown. No readiness, approval, source hash or actor was manufactured.

The actual production substring, phrase, facet and reviewer builder functions completed against this copy through a local SQLite D1-compatible adapter. No cloud binding or provider was attached. These measurements exclude Cloudflare network/durable-write overhead and public endpoint quotas; they are not remote D1 timing guarantees.

| Local stage | Checkpoint calls | Elapsed seconds | SQL statements |
| --- | ---: | ---: | ---: |
| Substrings | 11,494 | 311.002 | 114,933 |
| Phrases | 11,351 | 86.857 | 113,524 |
| Facets | 143 | 5.106 | 1,854 |
| Reviewer | 568 | 8.783 | 20,428 |
| Total | 23,556 | 411.748, approximately 6m 52s | 250,739 inside stages; 250,742 including final inspections |

Search schema readiness passed at version 28, all three search builders were ready, recovery was idle, and reviewer indexed/source counts were both 14,177 with no pending rows. **Readiness does not publish legacy documents.** The actual publication predicates select zero published and zero trusted documents after migration. The reviewer projection reports ingestion holds across all 14,177 historical records and stale reference-catalog evidence across the same population; these are expected revalidation requirements, not authority to mark the corpus approved.

The final working file was **8,663,502,848 bytes**, approximately **86.6% of a conservative decimal 10 GB planning ceiling**, compared with 5,412,462,592 bytes in the historical source. The approximately 3.25 GB growth leaves only **1.34 GB** before retained reprocessing generations and ordinary growth. Local file allocation is not an exact remote D1 allocation prediction. An object-level `dbstat` breakdown was unavailable in this runtime.

Behavior checks called the actual public search service with sessions disabled and network/provider access prohibited. A quoted phrase returned zero results as expected for held documents. Both trusted-only and provisional broad `rent` queries hit the `deadline` work-budget error; no publication was granted. The repeated queries ran while the separate integrity scan was active, so their 40–56-second local wall times are not standalone performance benchmarks. An earlier broad query also hit the deadline before that scan started. A separate run after the integrity scan completed reproduced the deadline at **38.975 seconds**; its slow FTS SQL consumed **38.959 seconds**. A read-only plan for that captured SQL shows an FTS candidate scan, per-document primary-key lookups and a temporary sort before the result limit. This identifies the expensive path but does not prove which change will meet the latency target. Native/cloud query plans and latency remain unproven. Node's synchronous adapter cannot interrupt an individual SQL call at the service deadline.

The actual shared mutation schema rejected a real historical form-equivalent payload at `rules_sections` with `too_big`. Omitting unchanged arrays allowed the confirmation DTO to parse. This tests request compatibility only; no confirmation/approval mutation was performed.

A direct exercise of the checked-in recovery writer against the historical source stopped at its documented ceiling after about 9.1 seconds, with 536,869,827 bytes of partial SQL written. The disposable partial file was removed. No restore/import was attempted; the result confirms a capacity blocker rather than a successful backup rehearsal.


Post-build `quick_check`, foreign-key checking and external-content integrity checks for body substrings, metadata substrings and phrase FTS all passed. Local times were approximately 227.8, 43.6, 114.3, 1.1 and 51.7 seconds respectively. These are correctness checks, separate from the builder timings above.

## Current-production cohort rehearsal

The owner-approved transfer selected **20 current production decisions** and related rows plus the shared reference catalog. It made **273 bounded read-only calls**, received **30,047,582 response bytes**, and completed its copy, FTS reconstruction and initial-check phase in **56.886 seconds**. The resulting baseline SQLite file was **26,771,456 bytes**. Its before/after Time Travel bookmarks matched; the same bookmark still matched during the subsequent document-value verification. A later final bookmark advanced again while rehearsal work remained local; the allocated bytes and live Pages deployment still matched. This supports a stable bounded projection during capture, not a general full-production backup guarantee or a claim about the cause of later bookmark movement.

The projection contains 3,064 primary chunks, 1,279 sections, 13,618 paragraphs and 1,582 retrieval chunks. It retains nine related activation batches and the full shared catalog. Batch summary fields can describe rows outside this selected cohort; no claim is made that a projected batch can be replayed as a full original batch. Derived FTS was rebuilt locally from copied source rows. Baseline `quick_check` passed and foreign-key violations were zero.

All 21 non-document tables matched their source typed-value digests exactly. The initial document-table textual digest differed because remote and local SQLite rendered REAL decimals differently in 17 rows. A bounded re-read of those same 20 rows verified all document values, including identical IEEE-754 binary64 bits for REALs, with canonical source/local hashes equal. This was a verification-format difference, not lost or rounded case data. No document identifier or copied text is published here.

| Current cohort check | Result |
| --- | --- |
| Migrations 0013–0033 | All 21 passed; 0.083 seconds locally |
| Complete ledger after migration | All 33 entries present, no fabricated prerequisite entries |
| Search/facet/reviewer builders | All passed; 103 checkpoints, approximately 0.575 seconds inside stages |
| Search/reviewer readiness | Ready; 20 source and 20 indexed reviewer records, none pending |
| Final working file | 44,314,624 bytes |
| Publication | All 20 held as legacy, zero published/trusted |
| Baseline preservation | Every original document, chunk, section and paragraph column unchanged after migrations/builds |
| Post-build integrity | SQLite quick check, foreign keys and all three external-content FTS checks passed |
| Actual public service search | Three held-corpus searches returned zero results in approximately 8–56 ms locally |
| Reviewer compatibility | Existing form-equivalent payload reproduced `index_codes` array-limit rejection; omission of unchanged arrays passed DTO validation |

The small current cohort does **not** reproduce the full-scale search deadline; that failure appears on the historical full corpus. Neither successful small tests nor correct publication holds validate full-corpus latency. No original R2 bytes, full-corpus restore, ingestion reprocessing, human approval, positive real-corpus public result, Cloudflare-native throughput or provider behavior was exercised. These remain explicit cutover prerequisites.

## Confirmed blockers and remedies

| ID | Blocker / evidence | Required remedy and acceptance |
| --- | --- | --- |
| B01 | No full snapshot/restore procedure has been proven for this FTS database. Normal export can block requests; Time Travel cannot create a new copy. Downtime is now accepted, so uninterrupted capture is not required. | Select and rehearse a consistent large-corpus capture/restore procedure using the accepted maintenance period if necessary. Quiesce all writers and coordinate capture, restore and rollback. A bounded cohort is useful testing but is not a full backup. |
| B02 | The current portable recovery writer stops at 512 MiB of SQL. Primary chunk and paragraph text alone require at least **988,928,478 bytes** as its hex-encoded SQL values, already above the **536,870,912-byte** limit, before any other columns/tables/SQL overhead. | Implement and verify a larger-dataset recovery design with bounded statements, content checks and resumable ownership. Compression does not remove the decompressed writer/import constraint. Do not simply remove the cap without a remote limits/atomicity rehearsal. |
| B03 | Current approval/confirmation timestamps and source hashes are all absent. Existing searchable/QC flags are not the evidence required by the new publication model. | Verify original-source availability/bytes, reprocess using guarded generations, review metadata/extraction/reference changes, and explicitly confirm/approve eligible revisions. Choose a release cohort and record held exclusions. Never mass-copy old searchable flags into new readiness/approval fields. |
| B04 | Full backfill needs approximately **23,546** HTTP maintenance deliveries at current page sizes. The checked-in release admin-write policy allows six requests/minute per connecting-IP bucket. Its approximately 65.4-hour minimum is accepted; reliable execution remains unproven. | Verify a bounded, resumable executor with leases, durable progress, retries and provider limits. Using the existing API at its permitted rate is acceptable. A private faster runner is optional. Do not rotate IPs or relax public quotas. Measure actual throughput and update the ETA once the run starts. |
| B05 | One current document exceeds the inline 5,000-paragraph reprocessing limit; 190 have low extraction confidence and many have unresolved references. | Design a bounded large-document path or explicitly hold the oversized case. Triage failures and measure review throughput. Do not silently raise memory/work limits or drop paragraphs to pass. |
| B06 | Existing legal catalogs have no established immutable edition/source-hash evidence under the new policy. Migration 0031 correctly records unknown applicability. | Establish catalog provenance/normalization and effective context where supported by original artifacts; retain explicit unknown/ambiguous holds elsewhere. Do not label legacy catalog presence as verified governing law. |
| B07 | First production bootstrap needs a compatible execution path for backfills before the normal release gate passes. The preparation bypass is intentionally preview-only; old/new writers are not assumed compatible. | Rehearse an isolated replacement-database strategy or a controlled maintenance runner/cutover. Keep old writers, Queue deliveries and cron behavior coordinated. Do not bypass production preflight or run incompatible old writers against migrated data. |
| B08 | Existing preview proof predates the merge SHA. Hosted release environments/private smoke plans and exact-commit preview provenance must be completed for production promotion. | Produce a successful normal preview release from the final `main` revision with the required protected-workflow artifact and fresh production-specific source/positive/negative/filtered smoke plan; retain compatible rollback evidence. |
| B09 | The reviewer form always submits all three reference arrays. The new mutation schema caps each at 12, while 2,238 current documents have at least one larger array. Historical data reproduces request-schema rejection even when the reviewer intends only to confirm metadata. | Submit only changed fields, preserve unchanged legacy references, and provide an explicit bounded edit/review path for larger valid arrays. Test unchanged confirmation, scalar edits, additions/removals and reference-validation limits together. Never truncate existing citations to make a request pass. |
| B10 | Full historical index builds grow the database to 8.66 GB, leaving approximately 1.34 GB under the 10 GB cap before new generations. | Measure remote allocation and peak reprocessing/generation retention, establish a reserve and cleanup policy with recovery coverage, and evaluate splitting or reducing derived storage if the measured peak does not fit. An empty-database migration is not a capacity test. |
| B11 | A broad keyword search against the migrated, entirely held historical corpus exceeded the 25-second search budget rather than returning a timely empty result. | Profile actual query plans and representative public scopes on the full target; use a cheap indexed empty-publication check and/or repair the expensive FTS/publication join path. Keep work budgets enabled. Re-test populated, empty, filtered and quoted search, then benchmark native/cloud D1. |

Storage must be measured after derived-index builds **and** retained reprocessing generations, not only after additive DDL. Cloudflare's documented D1 per-database limit is 10 GB and cannot be increased; a local SQLite run does not enforce that cap. [D1 limits](https://developers.cloudflare.com/d1/platform/limits/).

## Cutover estimate and planning assumptions

The SQL migration statements themselves are not the dominant measured work: the historical local rehearsal measured about 3.2 seconds of DDL/data migration and 6m 52s of index building. Those figures must not be used as a cloud deployment or downtime promise. For the observed production counts and checked-in 100-identity search / 25-document reviewer batches:

| Work | Estimated checkpoint calls |
| --- | ---: |
| Substring metadata/body build | 11,491 |
| Phrase build | 11,349 |
| Facet build | 142 |
| Reviewer build | approximately 564 |
| Total | approximately 23,546 |

At the checked-in release limit of six public admin requests/minute, that is approximately **65.4 hours** of serial endpoint maintenance before reprocessing/reapproval and other administrative writes. This is a configuration-derived lower-bound planning figure for that path, not a provider outage estimate or a measured cloud runtime. Reviewer cron alone, one 25-document delivery every five minutes, would take approximately **47 hours**, before other work. Search bootstrap is an explicit operator action; waiting for cron is not a complete migration strategy.

**Downtime and calendar availability are not blockers.** The owner accepts the slower path, so an accelerated runner and a rollout preserving continuous availability are optional. A full-corpus recovery path, reliable bounded executor, storage headroom, working search/reviewer behavior and an agreed review/initial-publication cohort remain prerequisites. A replacement database and a controlled in-place maintenance rollout are options to compare; neither has been validated for final cutover. Rehearse the selected sequence, including writer coordination, capture/delta reconciliation, exact API/web release and rollback checks.

**Return timing:** come back for the next preparation session whenever convenient; waiting does not advance an unstarted run. Once execution begins, the existing API path needs at least approximately **66 hours for backfill alone**, followed by any remaining reprocessing/review and validation. Record its real start time and observed throughput before assigning a final cutover appointment.

LLM credential selection and authentication do not need to be implemented to measure these database tasks. First rollout can remain explicitly lexical-only; semantic activation requires separate current-profile/filter coverage and provider visibility evidence.


## Proposed preparation order

1. **Recovery and capture first (B01/B02).** Agree the authorized data boundary, choose a consistent full-corpus capture mechanism, and demonstrate a content-verified restore into a separate target. Preserve the live database and the untouched local historical snapshot. The approved selected current cohort has been copied and tested, but cannot replace this full-backup proof.
2. **Resolve capacity and orchestration (B04/B07/B10).** Verify a bounded resumable executor and measure peak remote space while rebuilding and retaining/replacing generations. The existing rate-limited API path is acceptable even if it takes approximately 66 hours. Choose a replacement database or an in-place maintenance rollout based on verified recovery/capacity and compatibility; continuous live availability is not required.
3. **Repair corpus compatibility and profile searches (B05/B09/B11).** Address unchanged large reference arrays, establish an oversized-document disposition, and remove the full-corpus query bottleneck. Use the existing limits as protection while improving execution paths.
4. **Reprocess and review a named release cohort (B03/B06).** Check original bytes and legal provenance, record explicit holds, measure reviewer throughput and set the publication target. Total review time is the remaining eligible volume divided by measured throughput; it is not covered by the index-build timing.
5. **Rehearse promotion and rollback (B08).** Use final-commit preview evidence and private smoke inputs, reconcile changes since capture, coordinate writers/cron/Queues, switch API/web/bindings in the tested order, and verify public positive/negative/filtered/source behavior. Time this sequence before setting a live window. A code-only rollback must not point an incompatible old writer at a migrated database.

### Source and private evidence map

| Evidence | Location |
| --- | --- |
| Actual migration SQL and publication holds | [API migrations](../../apps/api/migrations/), [publication policy](../../apps/api/src/services/publication-policy.ts) |
| Search batch progression and readiness | [substring builder](../../apps/api/src/services/search-index-build.ts), [phrase builder](../../apps/api/src/services/search-phrase-build.ts), [facet builder](../../apps/api/src/services/search-facet-build.ts), [readiness](../../apps/api/src/services/search-readiness.ts) |
| Reviewer payload and mutation compatibility | [reviewer builder](../../apps/api/src/services/reviewer-queue-index.ts), [edit form](../../apps/web/src/app/admin/ingestion/page.tsx), [shared mutation schema](../../packages/shared/src/index.ts), [reference edition validation](../../apps/api/src/services/reference-editions.ts) |
| Recovery writer ceiling and release process | [recovery library](../../apps/api/scripts/lib/recovery-bundle.mjs), [recovery runbook](../recovery-runbook.md), [release runbook](../release-readiness-and-rollout.md) |
| Private read-only inventory | `production-metadata-*.json`, `schema*.json`, `migrations*.json`, count/shape/limit files, bookmark bookends and `production-consistency-final.json` |
| Private current cohort | `cohort-copy.json`, `cohort-content-verification.json`, `cohort-document-value-verification.json`, `cohort-baseline-preservation.json`, `cohort-migration.json`, `cohort-build.json`, `cohort-behavior.json`, `cohort-integrity.json` |
| Private historical rehearsal | `historical-snapshot.json`, `historical-migration.json`, `historical-build.json`, `historical-behavior.json`, `historical-profile.json`, `historical-search-query-plan.json`, `historical-integrity.json`, `recovery-cap-probe.json` |

Private filenames are relative to the ignored assessment directory stated above. They contain operational or corpus details and must remain outside GitHub. No original source files were transferred or sent to an LLM during this assessment.

## Acceptance before production cutover

- [x] Owner reports users notified and accepts maintenance downtime whenever needed, including approximately 66 hours.
- [ ] Consistent full-production backup and successful restore verified against source/database manifests.
- [ ] Recovery size/atomicity design and import limits proven at actual corpus scale.
- [ ] Bounded, resumable backfill/reprocessing execution path tested, with actual throughput/progress reporting and peak storage measurements; the slower permitted API path is acceptable.
- [ ] Reviewer legacy-array compatibility fixed and full-scale search budget/query-plan failures resolved.
- [ ] Initial publication cohort, excluded cases and review ownership agreed; source and legal-reference uncertainty retained honestly.
- [ ] All required migrations, search/facet/reviewer builds and release-readiness checks pass on the target.
- [ ] Representative real-corpus queries, exclusions, original downloads and evidence behavior accepted.
- [ ] Fresh exact-commit preview workflow proof, production smoke inputs, previous-version compatibility and rollback evidence ready.
- [ ] Maintenance/replacement-target cutover rehearsed and timed before production publishing is resumed.
