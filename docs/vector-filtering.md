# Semantic scopes before top-K

M05 applies the same accepted product-search scope before Vectorize chooses neighbors, and still applies D1's scope during hydration. It extends the generation/lifecycle protections from M04. Apply migration `0023_vector_filter_metadata.sql` before this API revision.

## Provider metadata

[The metadata-index manifest](../apps/api/vector-metadata-indexes.json) declares exactly ten supported index properties: `live`, `docKey`, `judgeKey`, `jurisdictionKey`, `fileType`, `trusted`, `decisionDate`, `series`, `cohortKey` and `sectionKey`. Create all ten indexes **before** re-upserting their metadata. String identity keys use full SHA-256 values rather than long raw names/IDs, avoiding the provider's 64-byte indexed-string prefix limit. Judge keys preserve the same canonicalization and SQLite case semantics as the D1 scope. Dates use the same stored ISO strings/ranges as SQL. Missing dates stay absent rather than acquiring a fabricated date.

Synchronization projects current D1 source, reviewer/corpus eligibility, catalog trust, scalar filters and chunk sections into provider metadata. It verifies the observed scope fields and digest before recording its v2 checkpoint. A separate filter checkpoint prevents an older Worker from accidentally acknowledging new metadata during a gradual rollout. Document, facet and legal-catalog changes invalidate the projection and corpus epoch before it can be reused. Unknown acknowledgements retain M04's held synchronization behavior.

Saved deployment configurations keep `VECTOR_FILTERS_READY=false`. Complete metadata-index provisioning, synchronization and reconciliation before setting it true. `GET /admin/vector-generations` reports this flag, the filter schema version and required index manifest separately from lifecycle consistency. Production resources were not changed by this implementation.

## Reference/index-code cohorts

Vectorize stores string arrays but does not index/filter their contents. A serialized list of codes is also unsafe: it is not exact Boolean membership and can exceed the indexed prefix. Instead, D1 materializes a cohort key from **all facet and valid-reference inputs** used by the existing index-code/rules/ordinance SQL predicates. Documents in one cohort therefore share those membership results, including legacy aliases and explicit AND/OR selections. Scalar constraints such as judge, trust, date, file type and jurisdiction remain in every provider query.

For a reference-constrained query, D1 selects eligible cohort keys using the same SQL scope as hydration. Vectorize searches those exact cohorts before top-K. Party-name substring scopes, and source cohorts too large to represent exactly, use exact D1-selected document keys. Normal judge/jurisdiction/document/trust/date/section searches use one scalar-filtered query without enumerating the corpus.

Filters are split into at most four disjoint partitions, each below 1,900 JSON bytes (the platform maximum is 2,048). Each of at most two curated query variants is embedded once and reused across its partitions. Every provider query has a 15-second deadline and preserves its profile namespace. Returned chunk scores merge by their maximum. Four partitions are sufficient to recover the global top-K of their eligible union: any eligible candidate below its partition's top-K cannot be in the union's top-K. The later application reranker retains its existing bounded candidate semantics; M08 addresses pagination/count contracts.

Cohort/document membership enumeration is capped at 112 distinct values and the byte/partition limits can lower that ceiling. Broader unsupported scopes return an explicit semantic-degradation reason and use lexical retrieval. They never query a global slate and pretend that post-filtering preserves recall. These limits bound fan-out/cost and should be tuned only with measured coverage, recall and latency. Broad index-code scopes with many distinct reference cohorts can still need the lexical path; that is a visible limitation, not an empty semantic success. Legacy soft index-code compatibility retains the existing D1 scope behavior; explicit Boolean selections remain strict.

## Verification and rollout

Tests build a target below 120 higher-scoring global distractors and verify that the judge filter recalls it before cutoff. They cover combined scalar/cohort constraints, explicit AND/OR codes, party-name partitions, dates/sections/trust, changed reviewer metadata, direct facet repairs, catalog changes, old-worker checkpoints, long-prefix collisions, byte/fan-out caps, empty scopes, index-readiness refusal and namespace errors with no unscoped retry.

A disposable actual workerd/D1/R2 run approved a synthetic decision, verified four vector receipts, then exercised the real `/search` route with 120 synthetic distractors. The provider received judge/trust/file-type/cohort filters in its versioned namespace; an empty D1 scope issued no provider query. No paid or production provider calls were made. Platform metadata-index behavior and real-corpus relevance/latency remain preview release gates.

Roll out the additive migration and metadata indexes to preview, run bounded synchronization until lifecycle and filter checkpoints are current, inspect all reconciliation pages, and compare a judged narrow-filter holdout against baseline. Include broad/many-cohort queries and verify their explicit fallback rate and latency. Enable `VECTOR_FILTERS_READY` only after those checks. Preserve old namespaces/configuration for rollback as described in [the vector-generation runbook](vector-generations.md). Keep the new tables while rolling code/configuration back; do not discard publication/source evidence or automatically remove old indexes. Authentication remains H01.

Platform references: [Vectorize filtering and index limits](https://developers.cloudflare.com/vectorize/reference/metadata-filtering/), [scalar `$in` semantics and unsupported stored arrays](https://developers.cloudflare.com/ai-search/configuration/retrieval/filtering/).
