# Stable search sessions and count semantics (M08)

Search now constructs its candidate universe with a fixed internal page size of 20 and window of 40, independent of the requested page size. Recall, scoring, diversification and decision-layer selection run once for a paginated search. At most 200 final source-document identities can enter a snapshot; existing stage limits can produce fewer. A new search with a different page size uses the same candidate/ranking policy. This intentionally replaces offset-dependent recall and can change ordering relative to older small/deep page requests; real-corpus relevance comparison remains a release gate.

Presentation identity is `documentId`. Related/remand decisions sharing a citation or the first 120 snippet characters remain separate. Diversification anchors are keyed by document plus anchor, rather than globally by citation anchor. Multiple matched passages remain in the decision's structured passage fields. Actual duplicate ingestions with different source-document IDs are not guessed away; M22 addresses ingestion identity.

## API contract

Start `/search` (or `/admin/retrieval/debug`) with the existing query/filter fields, `offset: 0`, and `limit` from 1–100. If more snapshot entries exist, follow `nextCursor` in a subsequent request **with the same query, filters, corpus mode, snippet format and endpoint/query type**. Page size may change. Nonzero legacy offsets now return 400 with migration guidance; they do not silently rerun a different slate.

A cursor contains a random session ID, position, and HMAC. A per-session 256-bit random key authenticates the position and binds the canonical request digest, current public-corpus policy scope, corpus revision and ranking version. The request identity also binds the embedding/chunker profile, semantic readiness and source-proxy/deployment configuration. A cursor from a different format/query/filter set cannot be reused. Sessions live in D1 and survive Worker restarts; there is no shared query cache and no isolate-owned result promise. Internal research explicitly requests a single bounded page without storing a snapshot it will never follow.

The response exposes:

- `total`: exact number of source decisions in this **bounded candidate snapshot**, not all corpus matches.
- `countKind: "bounded_candidates"` and `corpusExhaustive: false`: explicit count/coverage semantics, including zero-hit candidate slates.
- `hasMore` and `nextCursor`: more entries in the same snapshot. No additional retrieval/reranking occurs on cursor pages.
- `candidateExhausted`: this page ends the snapshot; it does not prove the corpus has no further matches.
- `rankingVersion`, `corpusRevision`, `sessionExpiresAt`: the result contract and lifetime. No snapshot is stored when the first page already contains the complete slate.

The web client follows cursors, validates page revision/version/position/total before appending, merges by document identity, and labels candidate counts honestly. Page-size changes do not replace the submitted query/filters with unsent form edits. Applied summary, highlighting, preview cleanup and decision links retain the submitted context. Expired/changed sessions disable further paging and ask the user to search again; previously displayed cards are retained with that message.

## Revision and policy fences

Creation and every page check the authoritative corpus epoch before returning. Creation's insert is also conditioned on the captured epoch, so a changed corpus cannot publish a mixed candidate snapshot. Existing document/source/facet/catalog triggers advance the epoch; 0026 adds direct reference-link changes and a delayed phrase-index cutover. Any such change conservatively invalidates all older sessions with 409, even if a particular query would be unaffected. This favors correctness over session availability during active ingestion/review. No full-pipeline retry resets its work budget.

The binding is used directly for these fences; Cloudflare documents that queries without the D1 Sessions API execute on the primary. If read-replica sessions are introduced later, preserve current-primary fences rather than treating sequential consistency as the latest publication state. See [D1 read replication](https://developers.cloudflare.com/d1/best-practices/read-replication/).

This scope is the application's **existing globally shared published corpus policy**. Authentication/H01 was not implemented. Before introducing private/account-specific permissions, include the authenticated authorization scope in session identity, invalidate existing public-policy sessions, and enforce policy on each page. These per-request capability snapshots must not be repurposed as a cross-user query cache.

## Bounded storage and privacy

Snapshots expire for access after ten minutes. At most 256 active stored snapshots and 16 MiB of payload can be reserved globally, with a 512 KiB per-snapshot cap. Atomic conditional insertion plus transactional insert/delete capacity counters prevent concurrent requests exceeding those limits. Capacity exhaustion returns a safe 503 instead of an unresumable partial page. Source data is never written by pagination.

Expired rows are deleted in batches of 32 when creating a new snapshot and by the scheduled handler. Deletion is bounded maintenance; access expiry does not imply physical removal at exactly ten minutes. Raw query and filter fields are omitted from stored payloads, while the keyed request digest binds them. Result evidence/diagnostics remain captured for the short session lifetime. Keys/cursors are not logged or put in browser URLs. Result responses use `Cache-Control: private, no-store`.

## Migration, rollout and rollback

Apply migration 0026 before the API, then deploy the cursor-aware web client. Existing offset consumers must migrate; a mixed older API cannot safely serve a new cursor and the web client rejects incompatible append responses. Keep both additive session tables and their capacity triggers during application rollback. Stop/restart a search after a ranking version change rather than translating an old cursor into new ordering.

For database restoration, discard ephemeral sessions and reconcile/reset their derived capacity counter, then advance the corpus epoch before accepting traffic. Do not resurrect old pagination capabilities from a backup or swap a frozen index over newer source data. M26 carries target-aware backup/recovery tooling. Real preview checks should cover active ingestion/review invalidation frequency, storage/quota pressure, a large corpus, and judged relevance changes from fixed candidate generation. No production migration or deployment was performed.

## Verification

Behavioral SQL/service tests traverse 18 related decisions sharing citation/snippet/anchor prefixes with changing page sizes, assert no overlap/omissions against the fixed slate, and confirm cursor pages perform no candidate queries. They cover canonical query/filter/format binding, forged positions, corpus changes during and between requests, TTL expiry, concurrent capacity reservation, payload caps, and bounded cleanup/counter conservation. Debug cursors preserve their filters and diagnostics. The stricter mixed-revision contract rejects a search rather than returning a partially stale snapshot.

Actual disposable workerd/D1/R2 HTTP checks resumed a cursor after a Worker restart, changed page size without overlap, rejected altered query/position, and invalidated a fresh cursor after a synthetic reviewer edit. The real Next page, exercised in an isolated browser, loaded another page after unsent query/page-size edits while preserving submitted links/highlighting/counts; after an external corpus edit it showed a clear restart message and removed the paging control. All providers were synthetic, with no paid or production calls. Local behavioral/source/web checks, workspace typechecks and Worker dry run accompany this commit; production relevance/latency are not inferred from these fixtures.
