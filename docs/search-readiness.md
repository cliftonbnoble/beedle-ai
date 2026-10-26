# Search readiness and bounded facet maintenance (M09)

Normal search no longer creates tables, indexes, triggers or vocabulary readers, backfills facets, or shares bootstrap I/O across requests. Migration 0027 owns those schema declarations. Capability data is a readonly record on the current search environment; there are no isolate-wide success/failure flags or shared readiness promises.

Each search invocation, including a cursor page, performs two small readonly checks: a fixed manifest of schema object names/types in `sqlite_master`, then the versioned marker and substring/phrase/facet build states. This checks physical schema presence as well as migration metadata. A missing migration/object or unfinished build returns a safe 503; there is no missing-schema JSON scan fallback. A transient readiness query fails that operation and the next request probes again. Optional vocabulary-query errors may fall back for that operation, with a category-only event, without permanently disabling future vocabulary queries. Structural loss after the readiness check fails the request explicitly.

The manifest checks search's source tables, materialized indexes/views, publication epoch plumbing, facet/index synchronization, session capacity guards and runtime indexes. It is a readiness check, not a claim that names/types prove all data/trigger bodies are uncorrupted. M10 extends source/posting reconciliation and restore checks; M20 adds release preflight. Do not manufacture a ready marker after copying an incomplete database.

## Facet reconciliation

A populated database starts `search_facet_build.phase=pending` under 0027, regardless of whether one or more existing facet tables contain rows. This removes the old OR-of-three-population check. A fresh empty database starts ready; migrated source triggers maintain subsequent document writes.

`POST /admin/search-index/step` with `{}` now completes the components in order: M06 substring build, M07 phrase build/cutover, then M09 facets. A facet delivery visits at most 100 captured document IDs. It atomically replaces all three facets from the current JSON source using the exact normalization of migration 0009, then advances its keyset cursor. SQL reads the source inside the transaction; no captured stale JavaScript text is replayed. Live document inserts below the cursor and edits to visited rows are maintained by the normal source triggers. It uses a 60-second owner lease and transactional guard; concurrent owners conflict, expired owners are reclaimable, and failure does not advance the cursor.

`GET /admin/search-index` includes `facets` and overall `readiness`. `GET /admin/search-readiness` reports `ready`, schema version, component states or missing/mistyped objects without running maintenance. Overall readiness requires schema version at least 27 and all three components ready. A legitimate empty facet is not mistaken for an incomplete backfill. There are no corpus-wide coverage/JSON scans in normal search or these status probes.

The bounded facet job is an operator/release action, not lazy work assigned to whichever user searches first. Direct imports/restores/repairs must invalidate and rerun readiness/reconciliation as part of their controlled procedure. Deleting data outside maintained source transactions while retaining an old ready marker is not a valid restore; M10/M26 provide the broader repair/recovery workflow.

## Rollout and rollback

1. Apply 0027 to preview before switching to the new reader. Keep existing live source synchronization and complete all predecessor index builds.
2. Advance `/admin/search-index/step` until `readiness.ready=true`, including `facets.phase=ready`. This is required for any populated upgrade, even when the old facets appear populated. Large databases need multiple bounded deliveries and normal admin rate controls still apply.
3. Inspect missing object names/types rather than letting a search request provision them. Use versioned schema repair for missing objects, and resume bounded maintenance for missing data. Do not replay historical unbounded backfill migrations against production.
4. Verify representative filtered queries and actual cold/concurrent requests in preview. A schema/readiness failure must be distinguishable from a genuine empty result, and a transient failure must recover on a later request without an isolate restart.
5. Retain the additive readiness/build schema and existing source/index triggers on application rollback. Older code can still read the compatible schema, although rolling back also restores its legacy bootstrap behavior. Do not drop required tables/indexes under active readers/writers. Coordinate populated upgrades before exposing the new gate; searches intentionally return 503 while maintenance is pending.

No production migrations or deployments were performed. Authentication/H01 remains separate. This removes request bootstrap; it does not silently widen publication or filter scope.

## Local evidence

Behavioral tests cover concurrent cold searches with no write/DDL statements, transient readiness/vocabulary recovery in the same isolate, missing indexes/facet tables without automatic provisioning, operator repair recovery, and independently populated facets. A 105-document fixture starts with a missing rules facet while index codes remain populated; 100-ID batches, lease conflict/expiry, transaction failure/replay, a low-ID concurrent insert and an edit to a visited document produce the correct final facets. Historical populated FTS upgrade tests run with the latest readiness migration and complete its facet phase.

Actual disposable workerd/D1/R2 first rejected missing physical guards despite existing migration names, then rejected a populated pending facet build with 503. After two bounded facet steps it served cold and warm searches with **zero DDL and zero bootstrap writes**, measured by a temporary binding-level SQL counter. Injecting one local readiness fault into two concurrent HTTP requests produced one 503 and one successful result; a subsequent request recovered without an isolate restart. The wrapper and fault injection are temporary test artifacts, not production endpoints. All providers were synthetic, with no paid/production calls. Full API behavior/source suites, workspace typechecks and a Worker dry run accompany the change.
