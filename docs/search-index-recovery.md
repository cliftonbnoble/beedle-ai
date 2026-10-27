# Populated upgrades and index recovery (M10)

The historical 0008 triggers indexed future writes; 0010 did not backfill existing data. M06/M07 introduced bounded source-backed builds, and M09 gates search on their completion. M10 adds bounded physical coverage inspection and an explicit generation rebuild for imported/restored/uncertain indexes. Applied migration history is retained; the repair does not promote approval or change source text.

## Coverage that measures index entries

`GET /admin/search-index/coverage?kind=document&after=<cursor>` visits at most 100 source identities. Supported kinds are `metadata`, `document` and `retrieval`; follow `nextAfter` until `done`. It compares source/mapping identity, index flags and actual FTS `_docsize` storage entries through primary-key joins. It does not mistake an external-content view's `COUNT(*)` or rowid lookup for populated token postings. Empty/tokenless text still has a physical entry, so lack of vocabulary terms alone is not treated as missing coverage.

The response includes issue counts with at most 20 identifiers/reasons, index generation, corpus revision and consistency. Aggregate a diagnostic traversal only when its generation and corpus revision remain unchanged; a concurrent edit makes that page inconsistent and requires rechecking. Before the legacy bootstrap completes, the response reports `bootstrap_incomplete` instead of asserting rowid parity against the old self-contained phrase index.

`revisionVerified` is true after an explicit generation rebuild establishes source/index revision stamps. Existing healthy bootstrap indexes remain compatible and report false for that additional check. `deepIntegrityChecked` is always false: bounded identity/revision/storage-entry coverage does not prove every token posting is uncorrupted. SQLite's full FTS `integrity-check` with rank 1 is a separate maintenance/preview check and can be expensive. Do not run a corpus-wide integrity command on every search. See [FTS external-content integrity and storage](https://www.sqlite.org/fts5.html) and [D1 FTS5 support](https://developers.cloudflare.com/d1/sql-api/sql-statements/).

## Explicit recovery workflow

Use `GET /admin/search-index/recovery` for state, generation, archive slots and checkpoint. `POST /admin/search-index/rebuild` defaults to `dryRun:true`; `{}` only inspects the plan. A write requires a fresh UUID `generation`, the observed `expectedGeneration`, and `dryRun:false`. Initial generation is `legacy`. The fixed archive slots hold the preceding body, metadata and phrase indexes. Starting another completed recovery requires explicit `replaceArchive:true`; an unfinished recovery is resumed by default, never silently reset.

Startup is one guarded D1 batch: archive existing canonical indexes, create empty replacements with the same canonical names/tokenizers, install generation-aware source triggers, capture finite keyset high-water marks, invalidate search/cursor readiness and record the new generation. This can repair a missing canonical FTS table as well as missing/duplicate/stale postings. Unknown existing posting state is not guessed at or reinserted over. Failure rolls the entire schema/state transaction back. A lost acknowledgment is reconciled from the committed generation; replaying the same generation is idempotent. Generation history forbids reusing an older ID.

This is a **maintenance repair**, with search intentionally held at 503 until recovery and facet reconciliation complete. It is appropriate when current index completeness is uncertain. Healthy initial upgrades can use the existing bounded M06/M07 online builders. Do not start a full repair merely to avoid completing their checkpoints. Use an API version that honors the readiness gates; older reader versions must not serve traffic while canonical indexes are incomplete.

Advance the same generation with `POST /admin/search-index/rebuild/step` and `{"generation":"<same UUID>"}`. Each delivery handles at most 100 identities, without loading source text into Worker memory. It:

1. Indexes document metadata, primary chunks and activated chunks from current D1 source values.
2. Prunes orphaned old mappings in bounded integer-rowid ranges.
3. Verifies source identity, generation, indexed source revision and physical entries in bounded passes.
4. Marks both FTS build components complete only after those checks.

Then advance `POST /admin/search-index/step` with `{}` until overall `/admin/search-readiness.ready=true`; this finishes M09 facet reconciliation. That generic step endpoint also resumes an active recovery. The dedicated recovery endpoints skip unrelated vector maintenance, so dry runs issue no SQL/provider writes and a pure FTS repair does not schedule provider work. Normal admin rate controls still apply; clients should back off on contention/rate limits and retain their generation/checkpoint.

## Source changes, retries and partial restores

Insert/update/delete triggers maintain exact OLD-token deletions and the current canonical indexes in the source transaction. Integer mapping identities remain stable. Body and metadata mappings retain an index content-revision counter and indexed revision, plus a generation stamp. These counters describe index-maintenance events, not fabricated historical business revisions. A bounded backfill reads current source values inside its transaction and skips already indexed generation entries. New IDs above a captured high-water mark or below a visited cursor are maintained by the live triggers. Active/unindexed presentation fields still come from source-backed views.

A 60-second owner lease and transaction guard fence each checkpoint. Older bootstrap/facet deliveries are also fenced during recovery. A stale owner/generation cannot write into a restarted repair. Cursor advancement, posting writes and generation stamps commit together; lost acknowledgments recover committed progress. Source writes can continue through the maintained triggers. Direct SQL imports that bypass those triggers are not supported concurrently; quiesce them and restart recovery from authoritative sources.

If a restored partial snapshot or injected failure loses receipts in a generation that claimed them, verification holds readiness and records a bounded error category. Inspect the relevant coverage page. An explicit `restart:true`, a new UUID and the current `expectedGeneration` replace the unfinished canonical indexes, reset bounded traversal and preserve the preceding archive slots. A live unexpired owner prevents restart. Missing generation-control/history rows cannot claim readiness; use controlled repair rather than manufacturing completion markers.

## Rollout, storage and rollback risks

Migration 0028 adds recovery state/history and mapping revision fields; it does not rewrite the corpus. Search readiness now requires schema version at least 28 and a valid idle/completed recovery state. Normal bootstrap compatibility is preserved until explicit repair installs generation-aware maintenance.

A repair needs room for old and replacement posting stores. Canonical-name replacement uses a small fixed DDL batch, but dropping a large previous/unfinished index can still hit D1 execution/storage limits. Benchmark startup, 100-identity deliveries, verification, facet completion and index sizes on representative preview data. Large repairs can require a substantial maintenance window under admin quotas. If database/B-tree corruption prevents schema operations, restore a consistent database/source backup; an application index rebuild cannot repair arbitrary physical database corruption.

Archives are diagnostic derived-index state. After source changes their external-content views are **not point-in-time backups**, and the preceding index may already have been damaged. Never blindly swap an archive over newer sources. Retain actual D1/R2/source-generation backups before repair; M26 supplies target-aware backup/recovery tooling. Explicit archive replacement retires only the previous derived slot while atomically retaining the current pre-repair indexes. Restart discards unfinished indexes while retaining those pre-repair slots. No production action was taken here.

Keep the new schema and generation-aware triggers during application rollback. An in-progress repair must remain gated and be resumed/restarted; rolling older code over it does not make its partial indexes complete. Source rollback/reprocessing uses the existing retained-generation flow, followed by reconciliation. Vectorize is not rebuilt by this operation; use M04/M05 provider reconciliation after database restores. Authentication/H01 remains separate.

## Verification evidence

A genuine test starts at migrations 0001–0007 with valid primary and activated chunks, reproduces zero FTS rows after 0010, then applies the current migrations and completes repair. Primary/retrieval/metadata postings become complete while the legacy document stays unapproved and excluded from trusted search. Other fixtures cover partial/lost postings despite ready markers, missing canonical tables, stale ghost postings, orphan mappings, 105-row checkpoint boundaries, low-ID live inserts/edits, metadata changes, shared primary/retrieval IDs, activation toggles, rename/cascade maintenance and global FTS integrity on the fixture corpus.

Failure tests cover start/checkpoint rollback, lost acknowledgments, concurrent duplicate starts, stale leased deliveries after restart, archive rotation, forbidden generation reuse, interrupted verification/restart, partial generation metadata, dry-run purity and rowids beyond JavaScript integer precision. Coverage also rejects an apparently stable diagnostic page across a concurrent corpus edit.

Disposable actual workerd/D1/R2 lost all three posting stores while the external view count stayed nonzero. Coverage detected the hole, dry run made zero writes, recovery held search at 503, eight checkpoint steps plus facet completion restored recall, and all three external-content integrity checks passed. Approval/publication/source metadata hashes were unchanged. Newly uploaded/reviewed documents remained searchable afterward. A second repair resumed from the same checkpoint after a Worker restart. Injecting a SQL failure after replacement DDL rolled the whole start transaction back, preserving canonical indexes, archives and source records. Providers were synthetic; no paid/production calls occurred. Full local behavioral/source suites, workspace typechecks and Worker dry run accompany the change; real-corpus scale/relevance/restore validation remain release gates.
